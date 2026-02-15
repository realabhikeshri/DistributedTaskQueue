using Prometheus;

namespace DistributedTaskQueue.Worker.Metrics;

public static class QueueMetrics
{
    public static readonly Counter TasksProcessed =
        Prometheus.Metrics.CreateCounter(
            "dtq_tasks_processed_total",
            "Total successfully processed tasks");

    public static readonly Counter TasksFailed =
        Prometheus.Metrics.CreateCounter(
            "dtq_tasks_failed_total",
            "Total failed tasks");

    public static readonly Counter TasksRetried =
        Prometheus.Metrics.CreateCounter(
            "dtq_tasks_retried_total",
            "Total retried tasks");

    public static readonly Counter TasksDeadLettered =
        Prometheus.Metrics.CreateCounter(
            "dtq_tasks_dlq_total",
            "Total tasks moved to DLQ");

    public static readonly Gauge ActiveProcessing =
        Prometheus.Metrics.CreateGauge(
            "dtq_tasks_processing",
            "Currently processing tasks");

    public static readonly Gauge QueueDepth =
        Prometheus.Metrics.CreateGauge(
            "dtq_queue_depth",
            "Current queue depth",
            new GaugeConfiguration
            {
                LabelNames = new[] { "priority" }
            });
}
