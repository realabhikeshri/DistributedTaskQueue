namespace DistributedTaskQueue.Core.Interfaces;

public interface IDistributedLock
{
    Task<bool> AcquireAsync(
        string key,
        TimeSpan ttl,
        CancellationToken ct = default);

    Task ReleaseAsync(string key, CancellationToken ct = default);
}
