using DistributedTaskQueue.Core.Interfaces;
using DistributedTaskQueue.Infrastructure.Redis;
using DistributedTaskQueue.Infrastructure.Serialization;
using Microsoft.Extensions.DependencyInjection;

namespace DistributedTaskQueue.Infrastructure;

public static class DependencyInjection
{
    public static IServiceCollection AddInfrastructure(
        this IServiceCollection services,
        string redisConnectionString)
    {
        services.AddSingleton<IRedisConnectionFactory>(
            _ => new RedisConnectionFactory(redisConnectionString));

        services.AddSingleton<IDistributedLock, RedisDistributedLock>();
        services.AddSingleton<ITaskQueue, RedisTaskQueue>();
        services.AddSingleton<ITaskSerializer, JsonTaskSerializer>();

        return services;
    }
}
