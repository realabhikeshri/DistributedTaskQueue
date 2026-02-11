using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using DistributedTaskQueue.Infrastructure.Redis;

namespace DistributedTaskQueue.Worker.Services
{
    public sealed class RetryScheduler : BackgroundService
    {
        private static readonly TimeSpan ScanInterval = TimeSpan.FromSeconds(5);

        private readonly IRedisConnectionFactory _redisConnectionFactory;
        private readonly ILogger<RetryScheduler> _logger;

        public RetryScheduler(
            IRedisConnectionFactory redisConnectionFactory,
            ILogger<RetryScheduler> logger)
        {
            _redisConnectionFactory = redisConnectionFactory;
            _logger = logger;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            IDatabase db = await _redisConnectionFactory.GetDatabaseAsync();

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    long nowTicks = DateTime.UtcNow.Ticks;

                    var readyTasks = await db.SortedSetRangeByScoreAsync(
                        RedisKeys.RetryZSet,
                        double.NegativeInfinity,
                        nowTicks);

                    foreach (var payload in readyTasks)
                    {
                        await db.ListLeftPushAsync(RedisKeys.MainQueue, payload);
                        await db.SortedSetRemoveAsync(RedisKeys.RetryZSet, payload);

                        _logger.LogInformation(
                            "Retry task moved back to main queue.");
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "RetryScheduler encountered an error.");
                }

                await Task.Delay(ScanInterval, stoppingToken);
            }
        }
    }
}
