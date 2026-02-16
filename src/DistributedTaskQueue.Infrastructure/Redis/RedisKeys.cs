namespace DistributedTaskQueue.Infrastructure.Redis;

public static class RedisKeys
{
    // Hash tag ensures same slot in Redis Cluster
    private const string Prefix = "dtq:{core}:";

    public static readonly string HighQueue = Prefix + "queue:high";
    public static readonly string MediumQueue = Prefix + "queue:medium";
    public static readonly string LowQueue = Prefix + "queue:low";

    public static readonly string Processing = Prefix + "processing";
    public static readonly string RetryZSet = Prefix + "retry";
    public static readonly string DeadLetterQueue = Prefix + "dlq";

    public static readonly string IdempotencyPrefix = Prefix + "idemp:";
}
