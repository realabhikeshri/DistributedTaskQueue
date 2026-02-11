using DistributedTaskQueue.Core.Interfaces;
using DistributedTaskQueue.Core.Models;
using TaskStatus = DistributedTaskQueue.Core.Models.TaskStatus;

namespace DistributedTaskQueue.Producer.Services;

public sealed class TaskProducer
{
    private readonly ITaskQueue _taskQueue;
    private readonly ITaskSerializer _serializer;

    public TaskProducer(
        ITaskQueue taskQueue,
        ITaskSerializer serializer)
    {
        _taskQueue = taskQueue;
        _serializer = serializer;
    }

    public async Task EnqueueAsync<TTask>(
        TTask task,
        int maxRetries = 5,
        string? correlationId = null,
        CancellationToken ct = default)
        where TTask : DistributedTask
    {
        var metadata = new TaskMetadata
        {
            TaskId = task.GetTaskId(),
            TaskType = task.TaskType,
            MaxRetries = maxRetries,
            CorrelationId = correlationId,
            Status = TaskStatus.Created
        };

        var message = new TaskMessage
        {
            Metadata = metadata,
            Payload = _serializer.Serialize(task)
        };

        await _taskQueue.EnqueueAsync(message, ct);
    }
}
