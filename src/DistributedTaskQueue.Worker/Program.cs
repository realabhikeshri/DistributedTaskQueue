using DistributedTaskQueue.Infrastructure;
using DistributedTaskQueue.Worker.Handlers;
using DistributedTaskQueue.Worker.Health;
using DistributedTaskQueue.Worker.Services;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Serilog;
using Serilog.Formatting.Compact;

Log.Logger = new LoggerConfiguration()
    .Enrich.FromLogContext()
    .WriteTo.Console(new CompactJsonFormatter())
    .CreateLogger();

try
{
    Log.Information("Starting DistributedTaskQueue Worker");

    var builder = Host.CreateDefaultBuilder(args)
        .UseSerilog()
        .ConfigureServices((context, services) =>
        {
            services.AddInfrastructure("localhost:6379,abortConnect=false");

            services.AddSingleton<ITaskHandler, EmailTaskHandler>();
            services.AddSingleton<TaskExecutor>();

            services.AddHostedService<WorkerService>();
            services.AddHostedService<VisibilityTimeoutMonitor>();
            services.AddHostedService<RetryProcessor>();

            // ✅ Health Checks
            services.AddHealthChecks()
                .AddCheck<RedisHealthCheck>("redis");
        })
        .ConfigureWebHostDefaults(webBuilder =>
        {
            webBuilder.Configure(app =>
            {
                app.UseRouting();

                app.UseEndpoints(endpoints =>
                {
                    endpoints.MapHealthChecks("/health");
                });
            });
        });

    var host = builder.Build();

    await host.RunAsync();
}
catch (Exception ex)
{
    Log.Fatal(ex, "Worker terminated unexpectedly");
}
finally
{
    Log.CloseAndFlush();
}
