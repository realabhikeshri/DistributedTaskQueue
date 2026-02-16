using DistributedTaskQueue.Core.Interfaces;
using DistributedTaskQueue.Infrastructure.Redis;
using DistributedTaskQueue.Worker.Metrics;
using Microsoft.Extensions.Hosting;
using StackExchange.Redis;

namespace DistributedTaskQueue.Worker.Services;

public sealed class QueueDepthMonitor : BackgroundService
{
    private readonly IRedisConnectionFactory _connectionFactory;

    public QueueDepthMonitor(IRedisConnectionFactory connectionFactory)
    {
        _connectionFactory = connectionFactory;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            var db = await _connectionFactory.GetDatabaseAsync();

            var high = await db.ListLengthAsync(RedisKeys.HighQueue);
            var medium = await db.ListLengthAsync(RedisKeys.MediumQueue);
            var low = await db.ListLengthAsync(RedisKeys.LowQueue);
            var dlq = await db.ListLengthAsync(RedisKeys.DeadLetterQueue);

            QueueMetrics.QueueDepth.WithLabels("high").Set(high);
            QueueMetrics.QueueDepth.WithLabels("medium").Set(medium);
            QueueMetrics.QueueDepth.WithLabels("low").Set(low);
            QueueMetrics.QueueDepth.WithLabels("dlq").Set(dlq);

            await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
        }
    }
}
