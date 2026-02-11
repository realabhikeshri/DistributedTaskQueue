namespace DistributedTaskQueue.Core.Retry;

public interface IRetryPolicy
{
    TimeSpan GetDelay(int retryCount);
}
