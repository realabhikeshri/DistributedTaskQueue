namespace DistributedTaskQueue.Core.Models;

public enum TaskStatus
{
    Created = 0,
    Queued = 1,
    Processing = 2,
    Completed = 3,
    Retrying = 4,
    Dead = 5
}