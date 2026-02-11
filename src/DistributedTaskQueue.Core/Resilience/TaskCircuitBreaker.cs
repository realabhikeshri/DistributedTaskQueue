using System.Collections.Concurrent;

namespace DistributedTaskQueue.Core.Resilience;

public sealed class TaskCircuitBreaker
{
    private readonly int _failureThreshold;
    private readonly TimeSpan _openDuration;

    private readonly ConcurrentDictionary<string, (int Failures, DateTime? OpenedAt, CircuitBreakerState State)>
        _states = new();

    public TaskCircuitBreaker(int failureThreshold, TimeSpan openDuration)
    {
        _failureThreshold = failureThreshold;
        _openDuration = openDuration;
    }

    public bool CanExecute(string taskType)
    {
        var state = _states.GetOrAdd(taskType, _ => (0, null, CircuitBreakerState.Closed));

        if (state.State == CircuitBreakerState.Open)
        {
            if (DateTime.UtcNow - state.OpenedAt >= _openDuration)
            {
                _states[taskType] = (state.Failures, null, CircuitBreakerState.HalfOpen);
                return true;
            }

            return false;
        }

        return true;
    }

    public void RecordSuccess(string taskType)
    {
        _states[taskType] = (0, null, CircuitBreakerState.Closed);
    }

    public void RecordFailure(string taskType)
    {
        var state = _states.GetOrAdd(taskType, _ => (0, null, CircuitBreakerState.Closed));

        var failures = state.Failures + 1;

        if (failures >= _failureThreshold)
        {
            _states[taskType] =
                (failures, DateTime.UtcNow, CircuitBreakerState.Open);
        }
        else
        {
            _states[taskType] =
                (failures, state.OpenedAt, CircuitBreakerState.Closed);
        }
    }
}
