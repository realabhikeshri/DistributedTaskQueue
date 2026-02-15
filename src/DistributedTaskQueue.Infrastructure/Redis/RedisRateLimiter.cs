using DistributedTaskQueue.Core.Interfaces;
using StackExchange.Redis;

namespace DistributedTaskQueue.Infrastructure.Redis;

public sealed class RedisRateLimiter : IRateLimiter
{
    private readonly IConnectionMultiplexer _redis;

    public RedisRateLimiter(IConnectionMultiplexer redis)
    {
        _redis = redis;
    }

    public async Task<bool> AllowAsync(
        string taskType,
        int maxPerSecond,
        CancellationToken cancellationToken = default)
    {
        var db = _redis.GetDatabase();

        var key = $"dtq:ratelimit:{taskType}:{DateTime.UtcNow:yyyyMMddHHmmss}";

        var count = await db.StringIncrementAsync(key);

        if (count == 1)
        {
            await db.KeyExpireAsync(key, TimeSpan.FromSeconds(2));
        }

        return count <= maxPerSecond;
    }
}
