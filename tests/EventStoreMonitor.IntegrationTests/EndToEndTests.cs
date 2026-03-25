using System.Text.Json;
using EventStore.Client;
using EventStoreMonitor.Configuration;
using EventStoreMonitor.Services;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace EventStoreMonitor.IntegrationTests;

public sealed class EndToEndTests : IClassFixture<EventStoreFixture>, IClassFixture<Smtp4devFixture>, IAsyncLifetime
{
    private readonly EventStoreFixture _eventStore;
    private readonly Smtp4devFixture _smtp;

    public EndToEndTests(EventStoreFixture eventStore, Smtp4devFixture smtp)
    {
        _eventStore = eventStore;
        _smtp = smtp;
    }

    public Task InitializeAsync() => _smtp.ClearMessagesAsync();
    public Task DisposeAsync() => Task.CompletedTask;

    private EmailNotificationService CreateEmailService() =>
        new(
            Options.Create(new EmailOptions
            {
                SmtpHost = _smtp.SmtpHost,
                SmtpPort = _smtp.SmtpPort,
                UseTls = false,
                SenderAddress = "monitor@test.local",
                SenderDisplayName = "Test Monitor",
                RecipientAddress = "alerts@test.local",
                AppPassword = null
            }),
            NullLogger<EmailNotificationService>.Instance);

    private CheckpointService CreateCheckpointService(string prefix) =>
        new(
            _eventStore.Client,
            Options.Create(new EventStoreOptions
            {
                ConnectionString = "unused",
                MonitoredStreams = ["test"],
                ConsumerGroupName = "test",
                CheckpointStreamPrefix = prefix
            }),
            NullLogger<CheckpointService>.Instance);

    [Fact]
    public async Task Worker_ProcessesEvent_AndDeliversRealEmail()
    {
        var streamName = $"e2e-stream-{Guid.NewGuid():N}";
        var cpPrefix = $"e2e-cp-{Guid.NewGuid():N}";
        var checkpointSvc = CreateCheckpointService(cpPrefix);
        var emailSvc = CreateEmailService();

        // Append an event
        var payload = JsonSerializer.SerializeToUtf8Bytes(new { OrderId = "E2E-001", Total = 150.00 });
        var eventData = new EventData(Uuid.NewUuid(), "OrderPlaced", payload);
        await _eventStore.Client.AppendToStreamAsync(streamName, StreamState.NoStream, [eventData]);

        // Start worker
        var worker = new StreamMonitorWorker(
            streamName, _eventStore.Client, checkpointSvc, emailSvc,
            NullLogger<StreamMonitorWorker>.Instance);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var workerTask = worker.StartAsync(cts.Token);

        // Wait for email to arrive at smtp4dev
        var messages = await _smtp.WaitForMessagesAsync(1, cts.Token);
        await cts.CancelAsync();
        try { await workerTask; } catch (OperationCanceledException) { }

        // Verify email was actually delivered
        Assert.Single(messages);
        Assert.Contains("OrderPlaced", messages[0].Subject);
        Assert.Contains("alerts@test.local", messages[0].To);

        // Verify checkpoint was saved
        var checkpoint = await checkpointSvc.LoadAsync(streamName, CancellationToken.None);
        Assert.NotNull(checkpoint);
        Assert.Equal(0UL, checkpoint);
    }

    [Fact]
    public async Task Worker_ProcessesMultipleEvents_DeliversAllEmails()
    {
        var streamName = $"e2e-multi-{Guid.NewGuid():N}";
        var cpPrefix = $"e2e-cp-{Guid.NewGuid():N}";
        var checkpointSvc = CreateCheckpointService(cpPrefix);
        var emailSvc = CreateEmailService();

        // Append 3 events
        var events = new[]
        {
            new EventData(Uuid.NewUuid(), "OrderPlaced", JsonSerializer.SerializeToUtf8Bytes(new { Id = 1 })),
            new EventData(Uuid.NewUuid(), "OrderConfirmed", JsonSerializer.SerializeToUtf8Bytes(new { Id = 1 })),
            new EventData(Uuid.NewUuid(), "OrderShipped", JsonSerializer.SerializeToUtf8Bytes(new { Id = 1 }))
        };
        await _eventStore.Client.AppendToStreamAsync(streamName, StreamState.NoStream, events);

        var worker = new StreamMonitorWorker(
            streamName, _eventStore.Client, checkpointSvc, emailSvc,
            NullLogger<StreamMonitorWorker>.Instance);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var workerTask = worker.StartAsync(cts.Token);

        // Wait for all 3 emails
        var messages = await _smtp.WaitForMessagesAsync(3, cts.Token);
        await cts.CancelAsync();
        try { await workerTask; } catch (OperationCanceledException) { }

        Assert.Equal(3, messages.Count);
        Assert.Contains(messages, m => m.Subject.Contains("OrderPlaced"));
        Assert.Contains(messages, m => m.Subject.Contains("OrderConfirmed"));
        Assert.Contains(messages, m => m.Subject.Contains("OrderShipped"));

        // Checkpoint should be at last event position
        var checkpoint = await checkpointSvc.LoadAsync(streamName, CancellationToken.None);
        Assert.Equal(2UL, checkpoint);
    }

    [Fact]
    public async Task Orchestrator_SendsStartupEmail_ThenProcessesEvents()
    {
        var streamName = $"e2e-orch-{Guid.NewGuid():N}";
        var cpPrefix = $"e2e-cp-{Guid.NewGuid():N}";

        // Pre-populate stream with an event
        var eventData = new EventData(Uuid.NewUuid(), "IncidentReopened", ReadOnlyMemory<byte>.Empty);
        await _eventStore.Client.AppendToStreamAsync(streamName, StreamState.NoStream, [eventData]);

        var emailSvc = CreateEmailService();
        var checkpointSvc = CreateCheckpointService(cpPrefix);
        var esOptions = Options.Create(new EventStoreOptions
        {
            ConnectionString = "unused",
            MonitoredStreams = [streamName],
            ConsumerGroupName = "test",
            CheckpointStreamPrefix = cpPrefix
        });

        var orchestrator = new MonitorOrchestratorService(
            esOptions, _eventStore.Client, checkpointSvc, emailSvc,
            NullLogger<StreamMonitorWorker>.Instance,
            NullLogger<MonitorOrchestratorService>.Instance);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var orchTask = orchestrator.StartAsync(cts.Token);

        // Expect: 1 startup email + 1 event email
        var messages = await _smtp.WaitForMessagesAsync(2, cts.Token);
        await cts.CancelAsync();
        try { await orchTask; } catch (OperationCanceledException) { }

        Assert.Contains(messages, m => m.Subject.Contains("started"));
        Assert.Contains(messages, m => m.Subject.Contains("IncidentReopened"));
    }
}
