namespace DistributedTaskQueue.Core.Models;

public sealed class TaskMetadata
{
    public string TaskId { get; init; } = default!;
    public string TaskType { get; init; } = default!;

    public TaskStatus Status { get; set; }

    public int RetryCount { get; set; }
    public int MaxRetries { get; init; }

    public DateTime CreatedAtUtc { get; init; }
    public DateTime? LastAttemptAtUtc { get; set; }
    public DateTime? NextRetryAtUtc { get; set; }

    public string? CorrelationId { get; init; }

    public string? LastError { get; set; }
    public DateTime? LastFailedAtUtc { get; set; }

    public string? ProcessingPayload { get; set; }

    public TaskMetadata()
    {
        CreatedAtUtc = DateTime.UtcNow;
        Status = TaskStatus.Created;
    }
}
