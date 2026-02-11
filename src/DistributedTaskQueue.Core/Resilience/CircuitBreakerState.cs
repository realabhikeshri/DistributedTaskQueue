namespace DistributedTaskQueue.Core.Resilience;

public enum CircuitBreakerState
{
    Closed,
    Open,
    HalfOpen
}
