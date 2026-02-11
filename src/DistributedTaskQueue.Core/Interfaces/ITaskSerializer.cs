using DistributedTaskQueue.Core.Models;

namespace DistributedTaskQueue.Core.Interfaces;

public interface ITaskSerializer
{
    string Serialize<T>(T task) where T : DistributedTask;

    DistributedTask Deserialize(
        string payload,
        string taskType);
}
