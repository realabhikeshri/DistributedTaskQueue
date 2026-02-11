using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using DistributedTaskQueue.Core.Interfaces;
using DistributedTaskQueue.Core.Models;

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
            "VisibilityTimeoutMonitor started");

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var expiredTasks =
                    await _taskQueue.GetExpiredProcessingTasksAsync(
                        DateTime.UtcNow,
                        stoppingToken);

                foreach (var task in expiredTasks)
                {
                    _logger.LogWarning(
                        "Visibility timeout expired for task {TaskId}",
                        task.Metadata.TaskId);

                    await _taskQueue.FailAsync(
                        task,
                        new TimeoutException(
                            "Visibility timeout expired"),
                        stoppingToken);
                }
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "VisibilityTimeoutMonitor encountered an error");
            }

            await Task.Delay(
                ScanInterval,
                stoppingToken);
        }

        _logger.LogInformation(
            "VisibilityTimeoutMonitor stopped");
    }
}
