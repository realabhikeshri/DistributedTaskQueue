using StackExchange.Redis;

namespace DistributedTaskQueue.Infrastructure.Redis
{
    public static class RedisKeys
    {
        public static readonly RedisKey MainQueue = "dtq:queue:main";
        public static readonly RedisKey Processing = "dtq:processing";
        public static readonly RedisKey RetryZSet = "dtq:retry:zset";
        public static readonly RedisKey DeadLetterQueue = "dtq:queue:dlq";

        // 🔒 Central idempotency hash
        // HSET dtq:idempotency {taskId} IN_PROGRESS | COMPLETED
        public static readonly RedisKey Idempotency = "dtq:idempotency";
    }
}
