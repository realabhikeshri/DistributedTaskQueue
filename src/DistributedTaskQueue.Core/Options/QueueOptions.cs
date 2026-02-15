namespace DistributedTaskQueue.Core.Options;

public sealed class QueueOptions
{
    public int MaxRetryAttempts { get; set; } = 5;
}
