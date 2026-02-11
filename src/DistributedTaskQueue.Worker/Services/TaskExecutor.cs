using DistributedTaskQueue.Core.Interfaces;
using DistributedTaskQueue.Core.Models;
using DistributedTaskQueue.Worker.Handlers;

namespace DistributedTaskQueue.Worker.Services;

public sealed class TaskExecutor
{
    private readonly IDictionary<string, ITaskHandler> _handlers;

    public TaskExecutor(IEnumerable<ITaskHandler> handlers)
    {
        _handlers = handlers.ToDictionary(
            h => h.TaskType,
            StringComparer.OrdinalIgnoreCase);
    }


    public async Task ExecuteAsync(
        TaskMessage task,
        CancellationToken ct)
    {
        if (!_handlers.TryGetValue(task.Metadata.TaskType, out var handler))
            throw new InvalidOperationException(
                $"No handler registered for {task.Metadata.TaskType}");

        await handler.HandleAsync(task, ct);
    }
}
