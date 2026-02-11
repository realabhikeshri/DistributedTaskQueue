using DistributedTaskQueue.Core.Interfaces;
using StackExchange.Redis;

namespace DistributedTaskQueue.Infrastructure.Redis;

public sealed class RedisDistributedLock : IDistributedLock
{
    private readonly IRedisConnectionFactory _connectionFactory;

    public RedisDistributedLock(IRedisConnectionFactory connectionFactory)
    {
        _connectionFactory = connectionFactory;
    }

    public async Task<bool> AcquireAsync(
        string key,
        TimeSpan ttl,
        CancellationToken ct = default)
    {
        var db = await _connectionFactory.GetDatabaseAsync();
        return await db.StringSetAsync(
            key,
            value: Environment.MachineName,
            expiry: ttl,
            when: When.NotExists);
    }

    public async Task ReleaseAsync(string key, CancellationToken ct = default)
    {
        var db = await _connectionFactory.GetDatabaseAsync();
        await db.KeyDeleteAsync(key);
    }
}
