using DistributedTaskQueue.Core.Models;
using Microsoft.Extensions.Logging;

namespace DistributedTaskQueue.Worker.Handlers;

public sealed class EmailTaskHandler : ITaskHandler
{
    private readonly ILogger<EmailTaskHandler> _logger;

    public EmailTaskHandler(ILogger<EmailTaskHandler> logger)
    {
        _logger = logger;
    }

    public string TaskType => "EmailTask";

    public async Task HandleAsync(
        TaskMessage task,
        CancellationToken cancellationToken)
    {
        _logger.LogInformation(
            "Sending email to {To}",
            task.Payload);

        // Simulate IO
        await Task.Delay(500, cancellationToken);

        // Throw here to test retry
        // throw new Exception("SMTP failure");
    }
}
