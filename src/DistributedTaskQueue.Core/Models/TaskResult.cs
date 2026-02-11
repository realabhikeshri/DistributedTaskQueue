namespace DistributedTaskQueue.Core.Models;

public sealed class TaskResult
{
    public bool IsSuccess { get; }
    public string? ErrorMessage { get; }

    private TaskResult(bool isSuccess, string? errorMessage)
    {
        IsSuccess = isSuccess;
        ErrorMessage = errorMessage;
    }

    public static TaskResult Success()
        => new(true, null);

    public static TaskResult Failure(string error)
        => new(false, error);
}
