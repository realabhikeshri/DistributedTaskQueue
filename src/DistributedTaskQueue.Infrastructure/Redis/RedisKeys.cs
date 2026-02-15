using StackExchange.Redis;

namespace DistributedTaskQueue.Infrastructure.Redis
{
    public static class RedisKeys
    {
        public static readonly RedisKey HighQueue = "dtq:queue:high";
        public static readonly RedisKey MediumQueue = "dtq:queue:medium";
        public static readonly RedisKey LowQueue = "dtq:queue:low";

        public static readonly RedisKey Processing = "dtq:processing";
        public static readonly RedisKey RetryZSet = "dtq:retry:zset";
        public static readonly RedisKey DeadLetterQueue = "dtq:queue:dlq";

        // 🔒 Central idempotency hash
        // HSET dtq:idempotency {taskId} IN_PROGRESS | COMPLETED
        public const string IdempotencyPrefix = "dtq:idempotency:";

    }
}
