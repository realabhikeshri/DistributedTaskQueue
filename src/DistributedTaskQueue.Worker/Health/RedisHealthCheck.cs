using DistributedTaskQueue.Core.Interfaces;
using DistributedTaskQueue.Infrastructure.Redis;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace DistributedTaskQueue.Worker.Health;

public sealed class RedisHealthCheck : IHealthCheck
{
    private readonly IRedisConnectionFactory _connectionFactory;

    public RedisHealthCheck(IRedisConnectionFactory connectionFactory)
    {
        _connectionFactory = connectionFactory;
    }

    public async Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var db = await _connectionFactory.GetDatabaseAsync();
            await db.PingAsync();

            return HealthCheckResult.Healthy("Redis is reachable");
        }
        catch (Exception ex)
        {
            return HealthCheckResult.Unhealthy(
                "Redis is not reachable",
                ex);
        }
    }
}
