using System.Text.Json;

namespace DistributedTaskQueue.Core.Models;

public sealed class TaskMessage
{
    public TaskMetadata Metadata { get; init; } = default!;
    public string Payload { get; init; } = default!;
}
