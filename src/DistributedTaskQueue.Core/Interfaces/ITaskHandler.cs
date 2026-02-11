using DistributedTaskQueue.Core.Models;

namespace DistributedTaskQueue.Core.Interfaces;

public interface ITaskHandler<in TTask>
    where TTask : DistributedTask
{
    Task<TaskResult> HandleAsync(TTask task, CancellationToken ct);
}

