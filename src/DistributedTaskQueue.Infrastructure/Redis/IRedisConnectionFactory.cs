using StackExchange.Redis;

namespace DistributedTaskQueue.Infrastructure.Redis;

/// <summary>
/// Provides access to a shared Redis database instance.
/// Abstracts ConnectionMultiplexer lifecycle management.
/// </summary>
public interface IRedisConnectionFactory
{
    Task<IDatabase> GetDatabaseAsync();
}
