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
        _logger.LogInformation(
            "VisibilityTimeoutMonitor started with scan interval {IntervalSeconds}s",
            ScanInterval.TotalSeconds);

        const string LockKey = "visibility:monitor:lock";
        var lockExpiry = TimeSpan.FromSeconds(10);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                // 🔐 Try acquiring distributed lock
                var lockAcquired = await _taskQueue.TryAcquireLockAsync(
                    LockKey,
                    lockExpiry,
                    stoppingToken);

                if (!lockAcquired)
                {
                    // Another worker owns the lock
                    await Task.Delay(ScanInterval, stoppingToken);
                    continue;
                }

                var expiredTasks =
                    await _taskQueue.GetExpiredProcessingTasksAsync(
                        DateTime.UtcNow,
                        stoppingToken);

                if (expiredTasks.Count > 0)
                {
                    _logger.LogWarning(
                        "Found {ExpiredCount} expired processing tasks",
                        expiredTasks.Count);
                }

                foreach (var task in expiredTasks)
                {
                    if (task?.Metadata?.TaskId is null)
                        continue;

                    _logger.LogWarning(
                        "Re-queuing expired task {TaskId}",
                        task.Metadata.TaskId);

                    await _taskQueue.FailAsync(
                        task,
                        new TimeoutException(
                            "Visibility timeout expired"),
                        stoppingToken);
                }

                await Task.Delay(
                    ScanInterval,
                    stoppingToken);
            }
        }
        catch (OperationCanceledException)
        {
            // Expected during shutdown
        }
        catch (Exception ex)
        {
            _logger.LogCritical(
                ex,
                "VisibilityTimeoutMonitor crashed unexpectedly");
        }
        finally
        {
            _logger.LogInformation(
                "VisibilityTimeoutMonitor stopped");
        }
    }

}
