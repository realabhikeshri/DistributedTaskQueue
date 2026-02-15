using DistributedTaskQueue.Core.Interfaces;
using DistributedTaskQueue.Core.Models;
using DistributedTaskQueue.Core.Observability;
using DistributedTaskQueue.Core.Options;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using System.Text.Json;
using TaskStatus = DistributedTaskQueue.Core.Models.TaskStatus;

namespace DistributedTaskQueue.Infrastructure.Redis;

public sealed class RedisTaskQueue : ITaskQueue
{
    private readonly IRedisConnectionFactory _connectionFactory;
    private readonly ITaskMetrics _metrics;
    private readonly ILogger<RedisTaskQueue> _logger;
    private readonly TimeSpan _baseRetryDelay;
    private readonly TimeSpan _maxRetryDelay;
    private readonly TimeSpan _visibilityTimeout;
    private readonly TimeSpan _idempotencyTtl;
    private readonly QueueOptions _options;
    private readonly LoadedLuaScript _loadedRequeueExpiredScript;


    private static readonly ThreadLocal<Random> _jitter =
        new(() => new Random());

    private static readonly RedisKey[] WeightedQueues =
    {
        RedisKeys.HighQueue,
        RedisKeys.HighQueue,
        RedisKeys.HighQueue,
        RedisKeys.HighQueue,
        RedisKeys.HighQueue,

        RedisKeys.MediumQueue,
        RedisKeys.MediumQueue,
        RedisKeys.MediumQueue,

        RedisKeys.LowQueue
    };

    private const string AtomicDequeueLua = @"
local payload = redis.call('LPOP', KEYS[1])
if not payload then
    return nil
end
redis.call('ZADD', KEYS[2], ARGV[1], payload)
return payload
";

    private const string AckLua = @"
return redis.call('ZREM', KEYS[1], ARGV[1])
";

    private const string TryStartExecutionLua = @"
local key = KEYS[1]
local now = tonumber(ARGV[1])
local staleAfter = tonumber(ARGV[2])
local ttl = tonumber(ARGV[3])

local value = redis.call('GET', key)

if not value then
    redis.call('SET', key, 'IN_PROGRESS:' .. now, 'EX', ttl)
    return 1
end

if value == 'COMPLETED' then
    return 0
end

local timestamp = tonumber(string.match(value, 'IN_PROGRESS:(%d+)'))

if timestamp and (now - timestamp) > staleAfter then
    redis.call('SET', key, 'IN_PROGRESS:' .. now, 'EX', ttl)
    return 1
end

return 0
";

    private const string PromoteRetriesLua = @"
local tasks = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
for i, task in ipairs(tasks) do
    redis.call('ZREM', KEYS[1], task)
end
return tasks
";
    private static readonly LuaScript RequeueExpiredScript =
    LuaScript.Prepare(@"
local processing = KEYS[1]
local dlq = KEYS[2]
local now = tonumber(ARGV[1])
local maxRetries = tonumber(ARGV[2])

-- Get expired tasks
local expired = redis.call('ZRANGEBYSCORE', processing, '-inf', now)

local recoveredCount = 0

for i, payload in ipairs(expired) do

    -- Remove from processing
    redis.call('ZREM', processing, payload)

    local task = cjson.decode(payload)

    task.Metadata.RetryCount =
        (task.Metadata.RetryCount or 0) + 1

    if task.Metadata.RetryCount > maxRetries then
        -- Move to DLQ
        redis.call('LPUSH', dlq, cjson.encode(task))
    else
        -- Re-enqueue to priority queue
        local queueKey = task.Metadata.QueueKey
        redis.call('LPUSH', queueKey, cjson.encode(task))
    end

    recoveredCount = recoveredCount + 1
end

return recoveredCount
");

    public RedisTaskQueue(
        IRedisConnectionFactory connectionFactory,
        ITaskMetrics metrics,
        ILogger<RedisTaskQueue> logger,
        IConfiguration configuration,
        QueueOptions options)
    {
        _connectionFactory = connectionFactory;
        _metrics = metrics;
        _logger = logger;

        var baseSeconds =
            configuration.GetValue<int>("Retry:BaseDelaySeconds", 2);

        var maxSeconds =
            configuration.GetValue<int>("Retry:MaxDelaySeconds", 120);

        var visibilitySeconds =
            configuration.GetValue<int>("Worker:VisibilityTimeoutSeconds", 30);

        _baseRetryDelay = TimeSpan.FromSeconds(baseSeconds);
        _maxRetryDelay = TimeSpan.FromSeconds(maxSeconds);
        _visibilityTimeout = TimeSpan.FromSeconds(visibilitySeconds);
        _idempotencyTtl = _visibilityTimeout + TimeSpan.FromSeconds(30);
        _options = options;
        var connection = _connectionFactory.GetConnection();
        _loadedRequeueExpiredScript =
            RequeueExpiredScript.Load(connection.GetServer(
                connection.GetEndPoints()[0]));
    }

    public async Task<bool> TryStartExecutionAsync(
    TaskMessage task,
    CancellationToken ct = default)
    {
        var db = await _connectionFactory.GetDatabaseAsync();

        var key = RedisKeys.IdempotencyPrefix + task.Metadata.TaskId;

        var nowUnix = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

        var staleAfterSeconds =
            (long)_visibilityTimeout.TotalSeconds;

        var ttlSeconds =
            (long)_idempotencyTtl.TotalSeconds;

        var result = await db.ScriptEvaluateAsync(
            TryStartExecutionLua,
            new RedisKey[] { key },
            new RedisValue[]
            {
            nowUnix,
            staleAfterSeconds,
            ttlSeconds
            });

        return (int)result == 1;
    }

    public async Task<IReadOnlyList<TaskMessage>> GetExpiredProcessingTasksAsync(
    DateTime utcNow,
    CancellationToken ct = default)
    {
        var db = await _connectionFactory.GetDatabaseAsync();

        var nowUnixSeconds =
            new DateTimeOffset(utcNow).ToUnixTimeSeconds();

        var expiredPayloads =
            await db.SortedSetRangeByScoreAsync(
                RedisKeys.Processing,
                double.NegativeInfinity,
                nowUnixSeconds);

        if (expiredPayloads.Length == 0)
            return Array.Empty<TaskMessage>();

        var result = new List<TaskMessage>(expiredPayloads.Length);

        foreach (var payload in expiredPayloads)
        {
            try
            {
                var task =
                    JsonSerializer.Deserialize<TaskMessage>(payload!);

                if (task is null)
                    continue;

                task.Metadata.ProcessingPayload = payload!;
                result.Add(task);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex,
                    "Failed to deserialize expired processing payload");
            }
        }

        return result;
    }

    public async Task<bool> TryAcquireLockAsync(
    string lockKey,
    TimeSpan expiry,
    CancellationToken cancellationToken)
    {
        var db = await _connectionFactory.GetDatabaseAsync();

        return await db.StringSetAsync(
            lockKey,
            Environment.MachineName,
            expiry,
            When.NotExists);
    }


    private static RedisKey GetQueueKey(TaskPriority priority)
    {
        return priority switch
        {
            TaskPriority.High => RedisKeys.HighQueue,
            TaskPriority.Low => RedisKeys.LowQueue,
            _ => RedisKeys.MediumQueue
        };
    }

    public async Task<int> RequeueExpiredTasksAsync(
    DateTime utcNow,
    CancellationToken cancellationToken)
    {
        var db = await _connectionFactory.GetDatabaseAsync();

        var result = (int)(long)await db.ScriptEvaluateAsync(
            _loadedRequeueExpiredScript,
            new
            {
                processing = (RedisKey)RedisKeys.Processing,
                dlq = (RedisKey)RedisKeys.DeadLetterQueue,
                now = utcNow.Ticks,
                maxRetries = _options.MaxRetryAttempts
            });

        return result;
    }


    public async Task EnqueueAsync(
    TaskMessage task,
    CancellationToken ct = default)
    {
        var db = await _connectionFactory.GetDatabaseAsync();

        task.Metadata.Status = TaskStatus.Queued;

        var queueKey = GetQueueKey(task.Metadata.Priority);

        // 🔥 CRITICAL: persist queue key for recovery logic
        task.Metadata.QueueKey = queueKey;

        var payload = JsonSerializer.Serialize(task);

        await db.ListLeftPushAsync(queueKey, payload);
    }


    public async Task<TaskMessage?> DequeueAsync(
        TimeSpan visibilityTimeout,
        CancellationToken ct = default)
    {
        var db = await _connectionFactory.GetDatabaseAsync();

        var visibilityUntil = DateTimeOffset.UtcNow
            .Add(visibilityTimeout)
            .ToUnixTimeSeconds();

        foreach (var queueKey in WeightedQueues)
        {
            var result = await db.ScriptEvaluateAsync(
                AtomicDequeueLua,
                new RedisKey[] { queueKey, RedisKeys.Processing },
                new RedisValue[] { visibilityUntil });

            if (result.IsNull)
                continue;

            var payload = result.ToString();

            var task = JsonSerializer.Deserialize<TaskMessage>(payload!)!;
            task.Metadata.Status = TaskStatus.Processing;
            task.Metadata.LastAttemptAtUtc = DateTime.UtcNow;
            task.Metadata.ProcessingPayload = payload!;

            return task;
        }

        return null;
    }

    public async Task AcknowledgeAsync(
        TaskMessage task,
        CancellationToken ct = default)
    {
        var db = await _connectionFactory.GetDatabaseAsync();

        await db.ScriptEvaluateAsync(
            AckLua,
            new RedisKey[] { RedisKeys.Processing },
            new RedisValue[] { task.Metadata.ProcessingPayload! });
    }

    public async Task FailAsync(
        TaskMessage task,
        Exception ex,
        CancellationToken ct = default)
    {
        task.Metadata.LastError = ex.Message;

        if (task.Metadata.RetryCount < task.Metadata.MaxRetries)
        {
            _metrics.TaskRetried();
            await ScheduleRetryInternalAsync(task, ct);
        }
        else
        {
            _metrics.TaskDeadLettered();
            await MoveToDeadLetterInternalAsync(task, ex.Message, ct);
        }
    }

    private async Task MoveToDeadLetterInternalAsync(
    TaskMessage task,
    string reason,
    CancellationToken ct)
    {
        var db = await _connectionFactory.GetDatabaseAsync();

        task.Metadata.Status = TaskStatus.Dead;
        task.Metadata.LastError = reason;

        var payload = JsonSerializer.Serialize(task);

        await db.SortedSetRemoveAsync(
            RedisKeys.Processing,
            task.Metadata.ProcessingPayload!);

        await db.ListRightPushAsync(
            RedisKeys.DeadLetterQueue,
            payload);

        _logger.LogError(
            "Task {TaskId} moved to DLQ after {Retries} retries. Reason: {Reason}",
            task.Metadata.TaskId,
            task.Metadata.RetryCount,
            reason);
    }


    private async Task ScheduleRetryInternalAsync(
        TaskMessage task,
        CancellationToken ct)
    {
        var db = await _connectionFactory.GetDatabaseAsync();

        task.Metadata.Status = TaskStatus.Retrying;

        var retryAttempt = task.Metadata.RetryCount;
        var exponentialDelaySeconds =
            _baseRetryDelay.TotalSeconds * Math.Pow(2, retryAttempt);

        task.Metadata.RetryCount++;

        var cappedDelaySeconds =
            Math.Min(exponentialDelaySeconds, _maxRetryDelay.TotalSeconds);

        var jitterFactor =
            0.5 + _jitter.Value!.NextDouble();

        var finalDelay =
            TimeSpan.FromSeconds(cappedDelaySeconds * jitterFactor);

        task.Metadata.NextRetryAtUtc =
            DateTime.UtcNow.Add(finalDelay);

        var payload = JsonSerializer.Serialize(task);
        var score = new DateTimeOffset(
            task.Metadata.NextRetryAtUtc.Value)
            .ToUnixTimeSeconds();

        await db.SortedSetRemoveAsync(
            RedisKeys.Processing,
            task.Metadata.ProcessingPayload!);

        await db.SortedSetAddAsync(
            RedisKeys.RetryZSet,
            payload,
            score);
    }

    public async Task<int> PromoteDueRetriesAsync(
        DateTime utcNow,
        CancellationToken ct = default)
    {
        var db = await _connectionFactory.GetDatabaseAsync();

        var nowUnixSeconds =
            new DateTimeOffset(utcNow).ToUnixTimeSeconds();

        var result = await db.ScriptEvaluateAsync(
            PromoteRetriesLua,
            new RedisKey[] { RedisKeys.RetryZSet },
            new RedisValue[] { nowUnixSeconds });

        if (result.IsNull)
            return 0;

        var tasks = (RedisResult[])result;

        foreach (var redisResult in tasks)
        {
            var payload = redisResult.ToString();
            var task = JsonSerializer.Deserialize<TaskMessage>(payload!)!;

            var queueKey = GetQueueKey(task.Metadata.Priority);

            await db.ListLeftPushAsync(queueKey, payload);
        }

        return tasks.Length;
    }

    public async Task MarkCompletedAsync(
        TaskMessage task,
        CancellationToken ct = default)
    {
        var db = await _connectionFactory.GetDatabaseAsync();

        var key = RedisKeys.IdempotencyPrefix + task.Metadata.TaskId;

        await db.StringSetAsync(
            key,
            "COMPLETED",
            TimeSpan.FromHours(24));
    }
}
