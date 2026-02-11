using DistributedTaskQueue.Core.Models;

namespace DistributedTaskQueue.Producer.Tasks;

public sealed class EmailTask : DistributedTask
{
    public string To { get; init; } = default!;
    public string Subject { get; init; } = default!;
    public string Body { get; init; } = default!;

    // Deterministic ID = idempotent enqueue
    public override string GetTaskId()
        => $"email:{To}:{Subject}";
}
