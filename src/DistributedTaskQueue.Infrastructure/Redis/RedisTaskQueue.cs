using DistributedTaskQueue.Core.Interfaces;
using DistributedTaskQueue.Core.Models;
using DistributedTaskQueue.Core.Observability;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using System.Text.Json;
using Microsoft.Extensions.Configuration;

using TaskStatus = DistributedTaskQueue.Core.Models.TaskStatus;

namespace DistributedTaskQueue.Infrastructure.Redis;

public sealed class RedisTaskQueue : ITaskQueue
{
    private readonly IRedisConnectionFactory _connectionFactory;
    private readonly ITaskMetrics _metrics;
    private readonly ILogger<RedisTaskQueue> _logger;
    private readonly TimeSpan _baseRetryDelay;
    private readonly TimeSpan _maxRetryDelay;
    private static readonly ThreadLocal<Random> _jitter =
        new(() => new Random());


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
local state = redis.call('HGET', KEYS[1], ARGV[1])

if state == 'COMPLETED' then
    return 0
end

if state == 'IN_PROGRESS' then
    return 0
end

redis.call('HSET', KEYS[1], ARGV[1], 'IN_PROGRESS')
return 1
";

    private const string MarkCompletedLua = @"
redis.call('HSET', KEYS[1], ARGV[1], 'COMPLETED')
return 1
";
    private const string PromoteRetriesLua = @"
local tasks = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])

for i, task in ipairs(tasks) do
    redis.call('ZREM', KEYS[1], task)
    redis.call('RPUSH', KEYS[2], task)
end

return tasks
";

    public RedisTaskQueue(
        IRedisConnectionFactory connectionFactory,
        ITaskMetrics metrics,
        ILogger<RedisTaskQueue> logger,
        IConfiguration configuration)
    {
        _connectionFactory = connectionFactory;
        _metrics = metrics;
        _logger = logger;
        var baseSeconds =
           configuration.GetValue<int>("Retry:BaseDelaySeconds", 2);

        var maxSeconds =
            configuration.GetValue<int>("Retry:MaxDelaySeconds", 120);

        _baseRetryDelay = TimeSpan.FromSeconds(baseSeconds);
        _maxRetryDelay = TimeSpan.FromSeconds(maxSeconds);
    }

    public async Task EnqueueAsync(
        TaskMessage task,
        CancellationToken ct = default)
    {
        var db = await _connectionFactory.GetDatabaseAsync();

        task.Metadata.Status = TaskStatus.Queued;

        var payload = JsonSerializer.Serialize(task);

        await db.ListRightPushAsync(RedisKeys.MainQueue, payload);
    }

    public async Task<TaskMessage?> DequeueAsync(
        TimeSpan visibilityTimeout,
        CancellationToken ct = default)
    {
        var db = await _connectionFactory.GetDatabaseAsync();

        var visibilityUntil = DateTimeOffset.UtcNow
            .Add(visibilityTimeout)
            .ToUnixTimeSeconds();

        var result = await db.ScriptEvaluateAsync(
            AtomicDequeueLua,
            new RedisKey[]
            {
                RedisKeys.MainQueue,
                RedisKeys.Processing
            },
            new RedisValue[]
            {
                visibilityUntil
            });

        if (result.IsNull)
            return null;

        var payload = result.ToString();

        var task = JsonSerializer.Deserialize<TaskMessage>(payload!)!;
        task.Metadata.Status = TaskStatus.Processing;
        task.Metadata.LastAttemptAtUtc = DateTime.UtcNow;
        task.Metadata.ProcessingPayload = payload!;

        return task;
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

    public async Task<bool> TryStartExecutionAsync(
        TaskMessage task,
        CancellationToken ct = default)
    {
        var db = await _connectionFactory.GetDatabaseAsync();

        var result = await db.ScriptEvaluateAsync(
            TryStartExecutionLua,
            new RedisKey[] { RedisKeys.Idempotency },
            new RedisValue[] { task.Metadata.TaskId });

        return (int)result == 1;
    }

    public async Task MarkCompletedAsync(
        TaskMessage task,
        CancellationToken ct = default)
    {
        var db = await _connectionFactory.GetDatabaseAsync();

        await db.ScriptEvaluateAsync(
            MarkCompletedLua,
            new RedisKey[] { RedisKeys.Idempotency },
            new RedisValue[] { task.Metadata.TaskId });
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




    private async Task ScheduleRetryInternalAsync(
    TaskMessage task,
    CancellationToken ct)
    {
        var db = await _connectionFactory.GetDatabaseAsync();

        task.Metadata.Status = TaskStatus.Retrying;
        task.Metadata.RetryCount++;

        var exponentialDelaySeconds =
            _baseRetryDelay.TotalSeconds *
            Math.Pow(2, task.Metadata.RetryCount);

        var cappedDelaySeconds =
            Math.Min(exponentialDelaySeconds,
                     _maxRetryDelay.TotalSeconds);

        var jitterFactor =
            0.5 + _jitter.Value!.NextDouble();

        var finalDelay =
            TimeSpan.FromSeconds(
                cappedDelaySeconds * jitterFactor);

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

    _logger.LogInformation(
    "Task {TaskId} scheduled for retry #{RetryCount} in {DelaySeconds}s (at {NextRetryUtc})",
    task.Metadata.TaskId,
    task.Metadata.RetryCount,
    finalDelay.TotalSeconds,
    task.Metadata.NextRetryAtUtc);


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
                var task = JsonSerializer
                    .Deserialize<TaskMessage>(payload!);

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

    public async Task<int> PromoteDueRetriesAsync(
    DateTime utcNow,
    CancellationToken ct = default)
    {
        var db = await _connectionFactory.GetDatabaseAsync();

        var nowUnixSeconds =
            new DateTimeOffset(utcNow).ToUnixTimeSeconds();

        var result = await db.ScriptEvaluateAsync(
            PromoteRetriesLua,
            new RedisKey[]
            {
            RedisKeys.RetryZSet,
            RedisKeys.MainQueue
            },
            new RedisValue[]
            {
            nowUnixSeconds
            });

        if (result.IsNull)
            return 0;

        var promotedTasks = (RedisResult[])result;

        return promotedTasks.Length;
    }

}
