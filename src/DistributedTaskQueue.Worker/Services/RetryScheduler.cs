using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using DistributedTaskQueue.Core.Interfaces;

namespace DistributedTaskQueue.Worker.Services
{
    public sealed class RetryScheduler : BackgroundService
    {
        private static readonly TimeSpan ScanInterval =
            TimeSpan.FromSeconds(5);

        private readonly ITaskQueue _taskQueue;
        private readonly ILogger<RetryScheduler> _logger;

        public RetryScheduler(
            ITaskQueue taskQueue,
            ILogger<RetryScheduler> logger)
        {
            _taskQueue = taskQueue;
            _logger = logger;
        }

        protected override async Task ExecuteAsync(
    CancellationToken stoppingToken)
        {
            const string lockKey = "dtq:retry:leader";
            var lockExpiry = TimeSpan.FromSeconds(10);

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var acquired =
                        await _taskQueue.TryAcquireLockAsync(
                            lockKey,
                            lockExpiry,
                            stoppingToken);

                    if (acquired)
                    {
                        var promotedCount =
                            await _taskQueue.PromoteDueRetriesAsync(
                                DateTime.UtcNow,
                                stoppingToken);

                        if (promotedCount > 0)
                        {
                            _logger.LogInformation(
                                "Retry leader promoted {Count} tasks.",
                                promotedCount);
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(
                        ex,
                        "RetryScheduler encountered an error.");
                }

                await Task.Delay(ScanInterval, stoppingToken);
            }
        }

    }
}
