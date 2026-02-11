namespace DistributedTaskQueue.Core.Observability;

public interface ITaskMetrics
{
    void TaskDequeued();
    void TaskExecuted(TimeSpan duration);
    void TaskFailed();
    void TaskRetried();
    void TaskDeadLettered();

    void QueueDepth(long depth);
    void ProcessingCount(long count);
}
