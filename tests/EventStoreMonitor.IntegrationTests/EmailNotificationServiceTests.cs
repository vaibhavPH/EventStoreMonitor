using EventStoreMonitor.Configuration;
using EventStoreMonitor.Services;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace EventStoreMonitor.IntegrationTests;

public sealed class EmailNotificationServiceTests : IClassFixture<Smtp4devFixture>, IAsyncLifetime
{
    private readonly Smtp4devFixture _smtp;

    public EmailNotificationServiceTests(Smtp4devFixture smtp) => _smtp = smtp;

    public Task InitializeAsync() => _smtp.ClearMessagesAsync();
    public Task DisposeAsync() => Task.CompletedTask;

    private EmailNotificationService CreateService() =>
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

    [Fact]
    public async Task SendEventNotification_DeliversEmail_WithCorrectSubjectAndRecipient()
    {
        var svc = CreateService();

        await svc.SendEventNotificationAsync(
            streamName: "$et-OrderPlaced",
            eventType: "OrderPlaced",
            eventId: Guid.NewGuid().ToString(),
            eventNumber: 42,
            created: DateTime.UtcNow,
            dataJson: """{"orderId": "ABC-123", "amount": 99.95}""");

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var messages = await _smtp.WaitForMessagesAsync(1, cts.Token);

        Assert.Single(messages);
        Assert.Contains("OrderPlaced", messages[0].Subject);
        Assert.Contains("alerts@test.local", messages[0].To);
    }

    [Fact]
    public async Task SendCrashNotification_DeliversEmail_WithCrashDetails()
    {
        var svc = CreateService();
        var exception = new InvalidOperationException("Something broke");

        await svc.SendCrashNotificationAsync(exception);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var messages = await _smtp.WaitForMessagesAsync(1, cts.Token);

        Assert.Single(messages);
        Assert.Contains("crashed", messages[0].Subject);
    }

    [Fact]
    public async Task SendStartupNotification_DeliversEmail_WithStreamList()
    {
        var svc = CreateService();

        await svc.SendStartupNotificationAsync(["$et-OrderPlaced", "$et-PaymentFailed"]);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var messages = await _smtp.WaitForMessagesAsync(1, cts.Token);

        Assert.Single(messages);
        Assert.Contains("started", messages[0].Subject);
    }

    [Fact]
    public async Task SendMultipleEmails_AllDelivered()
    {
        var svc = CreateService();

        await svc.SendStartupNotificationAsync(["stream-1"]);
        await svc.SendEventNotificationAsync("stream-1", "EventA", Guid.NewGuid().ToString(),
            0, DateTime.UtcNow, null);
        await svc.SendEventNotificationAsync("stream-1", "EventB", Guid.NewGuid().ToString(),
            1, DateTime.UtcNow, """{"key": "value"}""");

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var messages = await _smtp.WaitForMessagesAsync(3, cts.Token);

        Assert.Equal(3, messages.Count);
    }
}
