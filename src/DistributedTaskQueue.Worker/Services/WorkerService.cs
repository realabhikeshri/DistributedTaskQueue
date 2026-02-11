using DistributedTaskQueue.Core.Interfaces;
using DistributedTaskQueue.Core.Models;
using DistributedTaskQueue.Core.Observability;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Diagnostics;

namespace DistributedTaskQueue.Worker.Services;

public sealed class WorkerService : BackgroundService
{
    private static readonly TimeSpan VisibilityTimeout =
        TimeSpan.FromSeconds(30);

    private readonly ITaskQueue _taskQueue;
    private readonly TaskExecutor _executor;
    private readonly ITaskMetrics _metrics;
    private readonly ILogger<WorkerService> _logger;

    public WorkerService(
        ITaskQueue taskQueue,
        TaskExecutor executor,
        ITaskMetrics metrics,
        ILogger<WorkerService> logger)
    {
        _taskQueue = taskQueue;
        _executor = executor;
        _metrics = metrics;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(
        CancellationToken stoppingToken)
    {
        _logger.LogInformation("Worker started");

        while (!stoppingToken.IsCancellationRequested)
        {
            TaskMessage? task = null;
            var stopwatch = new Stopwatch();

            try
            {
                task = await _taskQueue.DequeueAsync(
                    VisibilityTimeout,
                    stoppingToken);

                if (task is null)
                {
                    await Task.Delay(500, stoppingToken);
                    continue;
                }

                var canExecute = await _taskQueue
                    .TryStartExecutionAsync(task, stoppingToken);

                if (!canExecute)
                {
                    await _taskQueue.AcknowledgeAsync(
                        task,
                        stoppingToken);
                    continue;
                }

                stopwatch.Start();

                await _executor.ExecuteAsync(
                    task,
                    stoppingToken);

                stopwatch.Stop();

                _metrics.TaskExecuted(stopwatch.Elapsed);

                await _taskQueue.MarkCompletedAsync(
                    task,
                    stoppingToken);S

                await _taskQueue.AcknowledgeAsync(
                    task,
                    stoppingToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                _metrics.TaskFailed();

                _logger.LogError(ex, "Task failed");

                if (task is not null)
                {
                    await _taskQueue.FailAsync(
                        task,
                        ex,
                        stoppingToken);
                }
            }
        }

        _logger.LogInformation("Worker stopped");
    }
}
