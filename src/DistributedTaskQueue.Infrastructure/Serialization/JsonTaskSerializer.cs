using System.Text.Json;
using DistributedTaskQueue.Core.Interfaces;
using DistributedTaskQueue.Core.Models;

namespace DistributedTaskQueue.Infrastructure.Serialization;

public sealed class JsonTaskSerializer : ITaskSerializer
{
    private readonly JsonSerializerOptions _options =
        new() { PropertyNameCaseInsensitive = true };

    public string Serialize<T>(T task) where T : DistributedTask
    {
        return JsonSerializer.Serialize(task, task.GetType(), _options);
    }

    public DistributedTask Deserialize(string payload, string taskType)
    {
        var type = AppDomain.CurrentDomain
            .GetAssemblies()
            .SelectMany(a => a.GetTypes())
            .FirstOrDefault(t => t.Name == taskType);

        if (type == null)
            throw new InvalidOperationException($"Unknown task type: {taskType}");

        return (DistributedTask)JsonSerializer.Deserialize(payload, type, _options)!;
    }
}
