using DistributedTaskQueue.Core.Interfaces;
using DistributedTaskQueue.Core.Models;
using DistributedTaskQueue.Core.Observability;
using DistributedTaskQueue.Core.Resilience;
using DistributedTaskQueue.Worker.Metrics;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Diagnostics;

namespace DistributedTaskQueue.Worker.Services;

public sealed class WorkerService : BackgroundService
{
    private readonly TimeSpan _visibilityTimeout;
    private readonly int _maxDegreeOfParallelism;

    private readonly ITaskQueue _taskQueue;
    private readonly TaskExecutor _executor;
    private readonly ITaskMetrics _metrics;
    private readonly TaskCircuitBreaker _circuitBreaker;
    private readonly ILogger<WorkerService> _logger;
    private readonly IRateLimiter _rateLimiter;

    private readonly SemaphoreSlim _semaphore;

    private static readonly TimeSpan EmptyQueueDelay =
        TimeSpan.FromMilliseconds(500);

    private int _emptyPollCount;

    public WorkerService(
        ITaskQueue taskQueue,
        TaskExecutor executor,
        ITaskMetrics metrics,
        IConfiguration configuration,
        ILogger<WorkerService> logger,
        IRateLimiter rateLimiter)
    {
        _taskQueue = taskQueue;
        _executor = executor;
        _metrics = metrics;
        _logger = logger;
        _rateLimiter = rateLimiter;

        _maxDegreeOfParallelism =
            Math.Max(1,
                configuration.GetValue<int>(
                    "Worker:MaxDegreeOfParallelism", 4));

        var visibilitySeconds =
            configuration.GetValue<int>(
                "Worker:VisibilityTimeoutSeconds", 30);

        _visibilityTimeout =
            TimeSpan.FromSeconds(visibilitySeconds);

        var failureThreshold =
            configuration.GetValue<int>(
                "CircuitBreaker:FailureThreshold", 5);

        var openSeconds =
            configuration.GetValue<int>(
                "CircuitBreaker:OpenDurationSeconds", 30);

        _circuitBreaker =
            new TaskCircuitBreaker(
                failureThreshold,
                TimeSpan.FromSeconds(openSeconds));

        _semaphore =
            new SemaphoreSlim(_maxDegreeOfParallelism);
    }

    protected override async Task ExecuteAsync(
        CancellationToken stoppingToken)
    {
        _logger.LogInformation(
            "Worker starting with concurrency {Concurrency} and visibility timeout {VisibilityTimeoutSeconds}s",
            _maxDegreeOfParallelism,
            _visibilityTimeout.TotalSeconds);

        var workers = new List<Task>();

        for (int i = 0; i < _maxDegreeOfParallelism; i++)
        {
            workers.Add(Task.Run(
                () => WorkerLoopAsync(stoppingToken),
                stoppingToken));
        }

        await Task.WhenAll(workers);
    }

    private async Task WorkerLoopAsync(
        CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            await ProcessNextAsync(stoppingToken);
        }
    }

    private async Task ProcessNextAsync(
        CancellationToken stoppingToken)
    {
        TaskMessage? task = null;
        var stopwatch = new Stopwatch();

        try
        {
            task = await _taskQueue.DequeueAsync(
                _visibilityTimeout,
                stoppingToken);

            if (task is null)
            {
                _emptyPollCount++;

                var delay = TimeSpan.FromMilliseconds(
                    Math.Min(2000, 200 * _emptyPollCount));

                await Task.Delay(delay, stoppingToken);
                return;
            }

            _emptyPollCount = 0;

            // 🔥 ACTIVE PROCESSING GAUGE
            QueueMetrics.ActiveProcessing.Inc();

            if (!_circuitBreaker.CanExecute(
                task.Metadata.TaskType))
            {
                await _taskQueue.FailAsync(
                    task,
                    new Exception("Circuit breaker open"),
                    stoppingToken);

                QueueMetrics.TasksFailed.Inc();
                QueueMetrics.ActiveProcessing.Dec();
                return;
            }

            var allowed = await _rateLimiter.AllowAsync(
                task.Metadata.TaskType,
                50,
                stoppingToken);

            if (!allowed)
            {
                await _taskQueue.FailAsync(
                    task,
                    new Exception("Rate limit exceeded"),
                    stoppingToken);

                QueueMetrics.TasksFailed.Inc();
                QueueMetrics.ActiveProcessing.Dec();
                return;
            }

            var canExecute =
                await _taskQueue.TryStartExecutionAsync(
                    task,
                    stoppingToken);

            if (!canExecute)
            {
                await _taskQueue.AcknowledgeAsync(
                    task,
                    stoppingToken);

                QueueMetrics.ActiveProcessing.Dec();
                return;
            }

            stopwatch.Start();

            await _executor.ExecuteAsync(
                task,
                stoppingToken);

            stopwatch.Stop();

            _metrics.TaskExecuted(stopwatch.Elapsed);
            QueueMetrics.TasksProcessed.Inc();

            _circuitBreaker.RecordSuccess(
                task.Metadata.TaskType);

            await _taskQueue.MarkCompletedAsync(
                task,
                stoppingToken);

            await _taskQueue.AcknowledgeAsync(
                task,
                stoppingToken);

            QueueMetrics.ActiveProcessing.Dec();
        }
        catch (OperationCanceledException)
        {
        }
        catch (Exception ex)
        {
            stopwatch.Stop();

            if (task is not null)
            {
                _metrics.TaskFailed();
                QueueMetrics.TasksFailed.Inc();

                _circuitBreaker.RecordFailure(
                    task.Metadata.TaskType);

                await _taskQueue.FailAsync(
                    task,
                    ex,
                    stoppingToken);

                QueueMetrics.TasksRetried.Inc();
                QueueMetrics.ActiveProcessing.Dec();
            }
            else
            {
                _logger.LogDebug(
                    "Worker loop exception without task: {Message}",
                    ex.Message);
            }
        }
    }
}
