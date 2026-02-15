using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using DistributedTaskQueue.Core.Interfaces;

namespace DistributedTaskQueue.Worker.Services;

public sealed class VisibilityTimeoutMonitor : BackgroundService
{
    private static readonly TimeSpan ScanInterval =
        TimeSpan.FromSeconds(5);

    private readonly ITaskQueue _taskQueue;
    private readonly ILogger<VisibilityTimeoutMonitor> _logger;

    public VisibilityTimeoutMonitor(
        ITaskQueue taskQueue,
        ILogger<VisibilityTimeoutMonitor> logger)
    {
        _taskQueue = taskQueue;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(
    CancellationToken stoppingToken)
    {
        const string LockKey = "visibility:monitor:lock";
        var lockExpiry = TimeSpan.FromSeconds(10);

        _logger.LogInformation(
            "VisibilityTimeoutMonitor started.");

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var lockAcquired =
                    await _taskQueue.TryAcquireLockAsync(
                        LockKey,
                        lockExpiry,
                        stoppingToken);

                if (!lockAcquired)
                {
                    await Task.Delay(
                        ScanInterval,
                        stoppingToken);
                    continue;
                }

                var recoveredCount =
                    await _taskQueue.RequeueExpiredTasksAsync(
                        DateTime.UtcNow,
                        stoppingToken);

                if (recoveredCount > 0)
                {
                    _logger.LogWarning(
                        "Recovered {Count} expired tasks.",
                        recoveredCount);
                }
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogCritical(
                    ex,
                    "VisibilityTimeoutMonitor crashed unexpectedly.");
            }

            await Task.Delay(
                ScanInterval,
                stoppingToken);
        }

        _logger.LogInformation(
            "VisibilityTimeoutMonitor stopped.");
    }


}
