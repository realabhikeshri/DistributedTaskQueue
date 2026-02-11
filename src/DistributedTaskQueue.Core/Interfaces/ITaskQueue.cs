using DistributedTaskQueue.Core.Models;

namespace DistributedTaskQueue.Core.Interfaces;

public interface ITaskQueue
{
    Task EnqueueAsync(TaskMessage task, CancellationToken ct = default);

    Task<TaskMessage?> DequeueAsync(
        TimeSpan visibilityTimeout,
        CancellationToken ct = default);

    // ✅ Payload-based ACK
    Task AcknowledgeAsync(
        TaskMessage task,
        CancellationToken ct = default);

    // 🔒 Idempotency
    Task<bool> TryStartExecutionAsync(
        TaskMessage task,
        CancellationToken ct = default);

    Task MarkCompletedAsync(
        TaskMessage task,
        CancellationToken ct = default);

    Task FailAsync(
        TaskMessage task,
        Exception ex,
        CancellationToken ct);

    Task<IReadOnlyList<TaskMessage>> GetExpiredProcessingTasksAsync(
    DateTime utcNow,
    CancellationToken ct);

}
