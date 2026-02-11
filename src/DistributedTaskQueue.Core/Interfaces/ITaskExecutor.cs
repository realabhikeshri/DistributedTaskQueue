using DistributedTaskQueue.Core.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DistributedTaskQueue.Core.Interfaces
{
    public interface ITaskExecutor
    {
        Task<TaskResult> ExecuteAsync(TaskMessage message, CancellationToken ct);
    }
}
