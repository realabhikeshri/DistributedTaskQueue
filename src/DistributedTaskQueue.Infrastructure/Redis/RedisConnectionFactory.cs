using StackExchange.Redis;

namespace DistributedTaskQueue.Infrastructure.Redis;

public sealed class RedisConnectionFactory : IRedisConnectionFactory, IDisposable
{
    private readonly Lazy<Task<ConnectionMultiplexer>> _connection;
    private bool _disposed;

    public RedisConnectionFactory(string connectionString)
    {
        _connection = new Lazy<Task<ConnectionMultiplexer>>(
            () => ConnectionMultiplexer.ConnectAsync(connectionString));
    }

    public async Task<IDatabase> GetDatabaseAsync()
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(RedisConnectionFactory));

        var connection = await _connection.Value;
        return connection.GetDatabase();
    }

    public void Dispose()
    {
        if (_disposed) return;

        if (_connection.IsValueCreated)
        {
            _connection.Value
                .GetAwaiter()
                .GetResult()
                .Dispose();
        }

        _disposed = true;
    }
}
