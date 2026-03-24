using System.Text;
using System.Text.Json;
using EventStore.Client;
using EventStoreMonitor.Configuration;
using EventStoreMonitor.Services;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace EventStoreMonitor.IntegrationTests;

public sealed class StreamMonitorWorkerTests : IClassFixture<EventStoreFixture>
{
    private readonly EventStoreFixture _fixture;

    public StreamMonitorWorkerTests(EventStoreFixture fixture) => _fixture = fixture;

    private CheckpointService CreateCheckpointService(string prefix = "test-cp")
    {
        var options = Options.Create(new EventStoreOptions
        {
            ConnectionString = "unused",
            MonitoredStreams = ["test"],
            ConsumerGroupName = "test",
            CheckpointStreamPrefix = prefix
        });
        return new CheckpointService(_fixture.Client, options, NullLogger<CheckpointService>.Instance);
    }

    [Fact]
    public async Task Worker_ProcessesNewEvents_AndSavesCheckpoint()
    {
        var streamName = $"test-stream-{Guid.NewGuid():N}";
        var cpPrefix = $"cp-{Guid.NewGuid():N}";
        var checkpointSvc = CreateCheckpointService(cpPrefix);
        var emailSvc = new FakeEmailNotificationService();

        // Append an event before subscribing
        var payload = JsonSerializer.SerializeToUtf8Bytes(new { OrderId = "ABC-123", Amount = 42.5 });
        var eventData = new EventData(Uuid.NewUuid(), "OrderPlaced", payload);
        await _fixture.Client.AppendToStreamAsync(streamName, StreamState.NoStream, [eventData]);

        var worker = new StreamMonitorWorker(
            streamName, _fixture.Client, checkpointSvc, emailSvc,
            NullLogger<StreamMonitorWorker>.Instance);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));

        // Start worker and wait until it processes the event
        var workerTask = worker.StartAsync(cts.Token);
        await emailSvc.WaitForNotificationsAsync(1, cts.Token);
        await cts.CancelAsync();

        try { await workerTask; } catch (OperationCanceledException) { }

        Assert.Single(emailSvc.SentNotifications);
        Assert.Equal("OrderPlaced", emailSvc.SentNotifications[0].EventType);
        Assert.Equal(streamName, emailSvc.SentNotifications[0].StreamName);

        // Verify checkpoint was saved
        var checkpoint = await checkpointSvc.LoadAsync(streamName, CancellationToken.None);
        Assert.NotNull(checkpoint);
    }

    [Fact]
    public async Task Worker_SkipsSystemEvents()
    {
        var streamName = $"test-stream-{Guid.NewGuid():N}";
        var cpPrefix = $"cp-{Guid.NewGuid():N}";
        var checkpointSvc = CreateCheckpointService(cpPrefix);
        var emailSvc = new FakeEmailNotificationService();

        // Append a user event (system events like $metadata are auto-created)
        var eventData = new EventData(Uuid.NewUuid(), "UserCreated", ReadOnlyMemory<byte>.Empty);
        await _fixture.Client.AppendToStreamAsync(streamName, StreamState.NoStream, [eventData]);

        var worker = new StreamMonitorWorker(
            streamName, _fixture.Client, checkpointSvc, emailSvc,
            NullLogger<StreamMonitorWorker>.Instance);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        var workerTask = worker.StartAsync(cts.Token);
        await emailSvc.WaitForNotificationsAsync(1, cts.Token);
        await cts.CancelAsync();

        try { await workerTask; } catch (OperationCanceledException) { }

        // Only user events should be notified, not system events
        Assert.All(emailSvc.SentNotifications, n => Assert.DoesNotContain("$", n.EventType));
    }

    [Fact]
    public async Task Worker_ResumesFromCheckpoint()
    {
        var streamName = $"test-stream-{Guid.NewGuid():N}";
        var cpPrefix = $"cp-{Guid.NewGuid():N}";
        var checkpointSvc = CreateCheckpointService(cpPrefix);

        // Append two events
        var event1 = new EventData(Uuid.NewUuid(), "Event1", ReadOnlyMemory<byte>.Empty);
        var event2 = new EventData(Uuid.NewUuid(), "Event2", ReadOnlyMemory<byte>.Empty);
        await _fixture.Client.AppendToStreamAsync(streamName, StreamState.NoStream, [event1]);
        await _fixture.Client.AppendToStreamAsync(streamName, StreamState.Any, [event2]);

        // Save checkpoint at position 0 (first event already processed)
        await checkpointSvc.SaveAsync(streamName, 0, CancellationToken.None);

        var emailSvc = new FakeEmailNotificationService();
        var worker = new StreamMonitorWorker(
            streamName, _fixture.Client, checkpointSvc, emailSvc,
            NullLogger<StreamMonitorWorker>.Instance);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        var workerTask = worker.StartAsync(cts.Token);
        await emailSvc.WaitForNotificationsAsync(1, cts.Token);
        await cts.CancelAsync();

        try { await workerTask; } catch (OperationCanceledException) { }

        // Should only process Event2 (resumed after position 0)
        Assert.Single(emailSvc.SentNotifications);
        Assert.Equal("Event2", emailSvc.SentNotifications[0].EventType);
    }
}

/// <summary>
/// Test double for EmailNotificationService that records sent notifications.
/// </summary>
public sealed class FakeEmailNotificationService : EmailNotificationService
{
    private readonly SemaphoreSlim _signal = new(0);

    public List<EventNotification> SentNotifications { get; } = [];

    public FakeEmailNotificationService()
        : base(
            Options.Create(new EmailOptions
            {
                SmtpHost = "localhost",
                SmtpPort = 25,
                SenderAddress = "test@test.com",
                RecipientAddress = "test@test.com",
                AppPassword = null
            }),
            NullLogger<EmailNotificationService>.Instance)
    {
    }

    public override Task SendEventNotificationAsync(string streamName, string eventType, string eventId,
        ulong eventNumber, DateTime created, string? dataJson, CancellationToken ct = default)
    {
        SentNotifications.Add(new EventNotification(streamName, eventType, eventId, eventNumber));
        _signal.Release();
        return Task.CompletedTask;
    }

    public override Task SendCrashNotificationAsync(Exception ex, CancellationToken ct = default) =>
        Task.CompletedTask;

    public override Task SendStartupNotificationAsync(IEnumerable<string> streams, CancellationToken ct = default) =>
        Task.CompletedTask;

    public async Task WaitForNotificationsAsync(int count, CancellationToken ct)
    {
        for (var i = 0; i < count; i++)
            await _signal.WaitAsync(ct);
    }

    public record EventNotification(string StreamName, string EventType, string EventId, ulong EventNumber);
}
