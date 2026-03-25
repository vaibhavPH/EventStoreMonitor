using EventStoreMonitor.Configuration;
using EventStoreMonitor.Services;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace EventStoreMonitor.IntegrationTests;

public sealed class CheckpointServiceTests : IClassFixture<EventStoreFixture>
{
    private readonly EventStoreFixture _fixture;

    public CheckpointServiceTests(EventStoreFixture fixture) => _fixture = fixture;

    private CheckpointService CreateService(string prefix = "test-checkpoint")
    {
        var options = Options.Create(new EventStoreOptions
        {
            ConnectionString = "unused",
            MonitoredStreams = ["test-stream"],
            ConsumerGroupName = "test-group",
            CheckpointStreamPrefix = prefix
        });
        return new CheckpointService(_fixture.Client, options, NullLogger<CheckpointService>.Instance);
    }

    [Fact]
    public async Task LoadAsync_ReturnsNull_WhenNoCheckpointExists()
    {
        var svc = CreateService($"cp-{Guid.NewGuid():N}");

        var result = await svc.LoadAsync("nonexistent-stream", CancellationToken.None);

        Assert.Null(result);
    }

    [Fact]
    public async Task SaveAndLoad_RoundTrips_Position()
    {
        var prefix = $"cp-{Guid.NewGuid():N}";
        var svc = CreateService(prefix);
        var stream = $"test-{Guid.NewGuid():N}";

        await svc.SaveAsync(stream, 42, CancellationToken.None);
        var loaded = await svc.LoadAsync(stream, CancellationToken.None);

        Assert.Equal(42UL, loaded);
    }

    [Fact]
    public async Task SaveAsync_OverwritesCheckpoint_WithLatestPosition()
    {
        var prefix = $"cp-{Guid.NewGuid():N}";
        var svc = CreateService(prefix);
        var stream = $"test-{Guid.NewGuid():N}";

        await svc.SaveAsync(stream, 10, CancellationToken.None);
        await svc.SaveAsync(stream, 99, CancellationToken.None);
        var loaded = await svc.LoadAsync(stream, CancellationToken.None);

        Assert.Equal(99UL, loaded);
    }

    [Fact]
    public async Task SaveAsync_HandlesMultipleStreams_Independently()
    {
        var prefix = $"cp-{Guid.NewGuid():N}";
        var svc = CreateService(prefix);
        var stream1 = $"stream-a-{Guid.NewGuid():N}";
        var stream2 = $"stream-b-{Guid.NewGuid():N}";

        await svc.SaveAsync(stream1, 5, CancellationToken.None);
        await svc.SaveAsync(stream2, 50, CancellationToken.None);

        Assert.Equal(5UL, await svc.LoadAsync(stream1, CancellationToken.None));
        Assert.Equal(50UL, await svc.LoadAsync(stream2, CancellationToken.None));
    }
}
