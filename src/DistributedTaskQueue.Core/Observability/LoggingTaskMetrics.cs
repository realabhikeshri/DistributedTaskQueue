using DistributedTaskQueue.Core.Observability;
using Microsoft.Extensions.Logging;

namespace DistributedTaskQueue.Infrastructure.Observability;

public sealed class LoggingTaskMetrics : ITaskMetrics
{
    private readonly ILogger<LoggingTaskMetrics> _logger;

    public LoggingTaskMetrics(
        ILogger<LoggingTaskMetrics> logger)
    {
        _logger = logger;
    }

    public void TaskDequeued()
        => _logger.LogInformation("metric:task.dequeued");

    public void TaskExecuted(TimeSpan duration)
        => _logger.LogInformation(
            "metric:task.executed durationMs={Duration}",
            duration.TotalMilliseconds);

    public void TaskFailed()
        => _logger.LogWarning("metric:task.failed");

    public void TaskRetried()
        => _logger.LogWarning("metric:task.retried");

    public void TaskDeadLettered()
        => _logger.LogError("metric:task.deadlettered");

    public void QueueDepth(long depth)
        => _logger.LogInformation(
            "metric:queue.depth value={Depth}",
            depth);

    public void ProcessingCount(long count)
        => _logger.LogInformation(
            "metric:processing.count value={Count}",
            count);
}
