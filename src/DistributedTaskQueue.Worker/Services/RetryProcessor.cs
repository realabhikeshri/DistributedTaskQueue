using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using DistributedTaskQueue.Core.Interfaces;

namespace DistributedTaskQueue.Worker.Services;

public sealed class RetryProcessor : BackgroundService
{
    private static readonly TimeSpan ScanInterval =
        TimeSpan.FromSeconds(5);

    private static readonly TimeSpan LockExpiry =
        TimeSpan.FromSeconds(10);

    private const string RetryLockKey =
        "retry:promotion:lock";

    private readonly ITaskQueue _taskQueue;
    private readonly ILogger<RetryProcessor> _logger;

    public RetryProcessor(
        ITaskQueue taskQueue,
        ILogger<RetryProcessor> logger)
    {
        _taskQueue = taskQueue;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(
        CancellationToken stoppingToken)
    {
        _logger.LogInformation("RetryProcessor started");

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                var lockAcquired =
                    await _taskQueue.TryAcquireLockAsync(
                        RetryLockKey,
                        LockExpiry,
                        stoppingToken);

                if (lockAcquired)
                {
                    var promotedCount =
                        await _taskQueue.PromoteDueRetriesAsync(
                            DateTime.UtcNow,
                            stoppingToken);

                    if (promotedCount > 0)
                    {
                        _logger.LogInformation(
                            "Promoted {Count} retry tasks back to main queue",
                            promotedCount);
                    }
                }

                await Task.Delay(
                    ScanInterval,
                    stoppingToken);
            }
        }
        catch (OperationCanceledException)
        {
            // Expected on shutdown
        }
        catch (Exception ex)
        {
            _logger.LogCritical(
                ex,
                "RetryProcessor crashed unexpectedly");
        }
        finally
        {
            _logger.LogInformation("RetryProcessor stopped");
        }
    }
}
