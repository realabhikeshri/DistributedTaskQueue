using StackExchange.Redis;

namespace DistributedTaskQueue.Infrastructure.Redis;

public sealed class RedisConnectionFactory
    : IRedisConnectionFactory, IDisposable
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
        ThrowIfDisposed();

        var connection = await _connection.Value
            .ConfigureAwait(false);

        return connection.GetDatabase();
    }

    public IConnectionMultiplexer GetConnection()
    {
        ThrowIfDisposed();

        // Safe here because script loading happens
        // after DI container is fully built.
        return _connection.Value
            .GetAwaiter()
            .GetResult();
    }

    private void ThrowIfDisposed()
    {
        if (_disposed)
            throw new ObjectDisposedException(
                nameof(RedisConnectionFactory));
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        if (_connection.IsValueCreated)
        {
            var connection = _connection.Value
                .GetAwaiter()
                .GetResult();

            connection.Dispose();
        }

        _disposed = true;
    }
}
