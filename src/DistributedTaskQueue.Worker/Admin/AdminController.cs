using DistributedTaskQueue.Core.Interfaces;
using DistributedTaskQueue.Infrastructure.Redis;
using Microsoft.AspNetCore.Mvc;
using StackExchange.Redis;
using System.Text.Json;

namespace DistributedTaskQueue.Worker.Admin;

[ApiController]
[Route("admin")]
public sealed class AdminController : ControllerBase
{
    private readonly IRedisConnectionFactory _connectionFactory;
    private readonly ITaskQueue _taskQueue;

    public AdminController(
        IRedisConnectionFactory connectionFactory,
        ITaskQueue taskQueue)
    {
        _connectionFactory = connectionFactory;
        _taskQueue = taskQueue;
    }

    [HttpGet("stats")]
    public async Task<IActionResult> GetStats()
    {
        var db = await _connectionFactory.GetDatabaseAsync();

        var stats = new
        {
            High = await db.ListLengthAsync(RedisKeys.HighQueue),
            Medium = await db.ListLengthAsync(RedisKeys.MediumQueue),
            Low = await db.ListLengthAsync(RedisKeys.LowQueue),
            Processing = await db.SortedSetLengthAsync(RedisKeys.Processing),
            Retry = await db.SortedSetLengthAsync(RedisKeys.RetryZSet),
            DeadLetter = await db.ListLengthAsync(RedisKeys.DeadLetterQueue)
        };

        return Ok(stats);
    }

    [HttpGet("dlq")]
    public async Task<IActionResult> GetDlq(int take = 50)
    {
        var db = await _connectionFactory.GetDatabaseAsync();

        var items = await db.ListRangeAsync(
            RedisKeys.DeadLetterQueue,
            0,
            take - 1);

        return Ok(items.Select(x => JsonSerializer.Deserialize<object>(x!)));
    }

    [HttpPost("dlq/replay")]
    public async Task<IActionResult> ReplayDlq(int take = 10)
    {
        var db = await _connectionFactory.GetDatabaseAsync();

        for (int i = 0; i < take; i++)
        {
            var item = await db.ListLeftPopAsync(
                RedisKeys.DeadLetterQueue);

            if (item.IsNullOrEmpty)
                break;

            var task = JsonSerializer.Deserialize<
                DistributedTaskQueue.Core.Models.TaskMessage>(item!);

            if (task is null)
                continue;

            await _taskQueue.EnqueueAsync(task);
        }

        return Ok(new { Message = "Replay completed" });
    }
}
