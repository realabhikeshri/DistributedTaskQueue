namespace DistributedTaskQueue.Core.Interfaces;

public interface IRateLimiter
{
    Task<bool> AllowAsync(
        string taskType,
        int maxPerSecond,
        CancellationToken cancellationToken = default);
}
