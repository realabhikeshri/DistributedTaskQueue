using DistributedTaskQueue.Infrastructure;
using DistributedTaskQueue.Producer.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

var host = Host.CreateDefaultBuilder(args)
    .ConfigureServices(services =>
    {
        services.AddInfrastructure("localhost:6379");
        services.AddSingleton<TaskProducer>();
    })
    .ConfigureLogging(logging =>
    {
        logging.ClearProviders();
        logging.AddConsole();
    })
    .Build();

var producer = host.Services.GetRequiredService<TaskProducer>();
var logger = host.Services.GetRequiredService<ILogger<Program>>();

await producer.EnqueueAsync(
    new DistributedTaskQueue.Producer.Tasks.EmailTask
    {
        To = "user@example.com",
        Subject = "Welcome",
        Body = "Hello from DistributedTaskQueue!"
    },
    correlationId: Guid.NewGuid().ToString()
);

logger.LogInformation("Email task enqueued");

await host.RunAsync();
