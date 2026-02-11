using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace DistributedTaskQueue.Core.Models
{
    public abstract class DistributedTask
    {
        public virtual string TaskType => GetType().Name;

        /// <summary>
        /// Used for idempotency. Override for deterministic IDs.
        /// </summary>
        public virtual string GetTaskId()
            => Guid.NewGuid().ToString("N");
    }
}
