using DistributedTaskQueue.Core.Observability;
using DistributedTaskQueue.Infrastructure;
using DistributedTaskQueue.Infrastructure.Observability;
using DistributedTaskQueue.Worker.Handlers;
using DistributedTaskQueue.Worker.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

var host = Host.CreateDefaultBuilder(args)
    .ConfigureServices(services =>
    {
        // 🔌 Redis + Infrastructure
        services.AddInfrastructure("redis:6379");

        // 📊 Observability
        services.AddSingleton<ITaskMetrics, LoggingTaskMetrics>();

        // 🧠 Task execution
        services.AddSingleton<ITaskHandler, EmailTaskHandler>();
        services.AddSingleton<TaskExecutor>();

        // 👷 Workers
        services.AddHostedService<WorkerService>();

        // ⏱ Visibility timeout reaper
        services.AddHostedService<VisibilityTimeoutMonitor>();
    })
    .ConfigureLogging(logging =>
    {
        logging.ClearProviders();
        logging.AddConsole();
    })
    .Build();

await host.RunAsync();
