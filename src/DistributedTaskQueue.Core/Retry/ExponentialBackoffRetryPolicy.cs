namespace DistributedTaskQueue.Core.Retry;

public sealed class ExponentialBackoffRetryPolicy : IRetryPolicy
{
    private readonly TimeSpan _baseDelay;
    private readonly TimeSpan _maxDelay;

    public ExponentialBackoffRetryPolicy(
        TimeSpan baseDelay,
        TimeSpan maxDelay)
    {
        _baseDelay = baseDelay;
        _maxDelay = maxDelay;
    }

    public TimeSpan GetDelay(int retryCount)
    {
        var delay = TimeSpan.FromMilliseconds(
            _baseDelay.TotalMilliseconds * Math.Pow(2, retryCount));

        return delay > _maxDelay ? _maxDelay : delay;
    }
}
