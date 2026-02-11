using DistributedTaskQueue.Core.Interfaces;
using DistributedTaskQueue.Core.Models;
using DistributedTaskQueue.Core.Observability;
using DistributedTaskQueue.Core.Resilience;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Diagnostics;
using System.Threading;

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

    private readonly SemaphoreSlim _semaphore;

    private static readonly TimeSpan EmptyQueueDelay =
        TimeSpan.FromMilliseconds(500);

    public WorkerService(
        ITaskQueue taskQueue,
        TaskExecutor executor,
        ITaskMetrics metrics,
        IConfiguration configuration,
        ILogger<WorkerService> logger)
    {
        _taskQueue = taskQueue;
        _executor = executor;
        _metrics = metrics;
        _logger = logger;

        _maxDegreeOfParallelism =
            configuration.GetValue<int>("Worker:MaxDegreeOfParallelism", 4);

        var visibilitySeconds =
            configuration.GetValue<int>("Worker:VisibilityTimeoutSeconds", 30);

        _visibilityTimeout = TimeSpan.FromSeconds(visibilitySeconds);

        var failureThreshold =
            configuration.GetValue<int>("CircuitBreaker:FailureThreshold", 5);

        var openSeconds =
            configuration.GetValue<int>("CircuitBreaker:OpenDurationSeconds", 30);

        _circuitBreaker =
            new TaskCircuitBreaker(
                failureThreshold,
                TimeSpan.FromSeconds(openSeconds));

        _semaphore = new SemaphoreSlim(_maxDegreeOfParallelism);
    }

    protected override async Task ExecuteAsync(
        CancellationToken stoppingToken)
    {
        var runningTasks = new List<Task>();

        while (!stoppingToken.IsCancellationRequested)
        {
            await _semaphore.WaitAsync(stoppingToken);

            var task = ProcessNextAsync(stoppingToken);

            runningTasks.Add(task);

            _ = task.ContinueWith(_ =>
            {
                _semaphore.Release();
            }, TaskScheduler.Default);
        }

        await Task.WhenAll(runningTasks);
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
                await Task.Delay(
                    EmptyQueueDelay,
                    stoppingToken);
                return;
            }

            if (!_circuitBreaker.CanExecute(task.Metadata.TaskType))
            {
                await _taskQueue.FailAsync(
                    task,
                    new Exception("Circuit breaker open"),
                    stoppingToken);
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
                return;
            }

            stopwatch.Start();

            await _executor.ExecuteAsync(
                task,
                stoppingToken);

            stopwatch.Stop();

            _metrics.TaskExecuted(stopwatch.Elapsed);

            _circuitBreaker.RecordSuccess(task.Metadata.TaskType);

            await _taskQueue.MarkCompletedAsync(
                task,
                stoppingToken);

            await _taskQueue.AcknowledgeAsync(
                task,
                stoppingToken);
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _metrics.TaskFailed();

            if (task is not null)
            {
                _circuitBreaker.RecordFailure(task.Metadata.TaskType);

                await _taskQueue.FailAsync(
                    task,
                    ex,
                    stoppingToken);
            }
        }
    }
}
