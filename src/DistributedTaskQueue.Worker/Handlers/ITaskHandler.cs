using DistributedTaskQueue.Core.Models;

namespace DistributedTaskQueue.Worker.Handlers;

public interface ITaskHandler
{
    string TaskType { get; }

    Task HandleAsync(
        TaskMessage task,
        CancellationToken cancellationToken);
}
