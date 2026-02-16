using DistributedTaskQueue.Core.Interfaces;
using DistributedTaskQueue.Core.Options;
using DistributedTaskQueue.Infrastructure;
using DistributedTaskQueue.Worker.Admin;
using DistributedTaskQueue.Worker.Handlers;
using DistributedTaskQueue.Worker.Health;
using DistributedTaskQueue.Worker.Services;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using Prometheus;
using Serilog;
using Serilog.Formatting.Compact;

Log.Logger = new LoggerConfiguration()
    .Enrich.FromLogContext()
    .WriteTo.Console(new CompactJsonFormatter())
    .CreateLogger();

var builder = Host.CreateDefaultBuilder(args)
    .UseSerilog()
    .ConfigureServices((context, services) =>
    {
        services.AddSingleton(new QueueOptions
        {
            MaxRetryAttempts = 5
        });

        services.AddInfrastructure("localhost:6379,abortConnect=false");

        services.AddSingleton<ITaskHandler, EmailTaskHandler>();
        services.AddSingleton<TaskExecutor>();

        services.AddHostedService<WorkerService>();
        services.AddHostedService<VisibilityTimeoutMonitor>();
        services.AddHostedService<RetryScheduler>();
        services.AddHostedService<QueueDepthMonitor>();

        services.AddControllers();

        services.AddHealthChecks()
            .AddCheck<RedisHealthCheck>("redis");

        services.AddOpenTelemetry()
            .WithTracing(tracer =>
            {
                tracer
                    .SetResourceBuilder(
                        ResourceBuilder.CreateDefault()
                            .AddService("DistributedTaskQueue.Worker"))
                    .AddAspNetCoreInstrumentation()
                    .AddHttpClientInstrumentation()
                    .AddRuntimeInstrumentation()
                    .AddProcessInstrumentation()
                    .AddConsoleExporter();
            });
    })
    .ConfigureWebHostDefaults(webBuilder =>
    {
        webBuilder.Configure(app =>
        {
            app.UseRouting();

            app.UseHttpMetrics();

            app.UseEndpoints(endpoints =>
            {
                endpoints.MapControllers();
                endpoints.MapHealthChecks("/health");
                endpoints.MapMetrics();
            });
        });
    });

await builder.Build().RunAsync();
