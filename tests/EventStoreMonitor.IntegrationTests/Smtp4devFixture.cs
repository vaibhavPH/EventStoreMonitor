using System.Net.Http.Json;
using System.Text.Json;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;

namespace EventStoreMonitor.IntegrationTests;

public sealed class Smtp4devFixture : IAsyncLifetime
{
    private readonly IContainer _container = new ContainerBuilder()
        .WithImage("rnwood/smtp4dev:v3")
        .WithPortBinding(0, 25)
        .WithPortBinding(0, 80)
        .WithEnvironment("ServerOptions__HostName", "localhost")
        .WithWaitStrategy(Wait.ForUnixContainer().UntilPortIsAvailable(80))
        .Build();

    private readonly HttpClient _httpClient = new();

    public string SmtpHost => _container.Hostname;
    public int SmtpPort => _container.GetMappedPublicPort(25);
    public string ApiBaseUrl => $"http://{_container.Hostname}:{_container.GetMappedPublicPort(80)}";

    public async ValueTask InitializeAsync()
    {
        await _container.StartAsync();
    }

    public async ValueTask DisposeAsync()
    {
        _httpClient.Dispose();
        await _container.DisposeAsync();
    }

    /// <summary>
    /// Get all messages received by smtp4dev.
    /// </summary>
    public async Task<List<Smtp4devMessage>> GetMessagesAsync()
    {
        var response = await _httpClient.GetFromJsonAsync<Smtp4devMessageList>($"{ApiBaseUrl}/api/Messages");
        return response?.Results ?? [];
    }

    /// <summary>
    /// Wait until the expected number of messages have been received, with timeout.
    /// </summary>
    public async Task<List<Smtp4devMessage>> WaitForMessagesAsync(int expectedCount, CancellationToken ct, int pollIntervalMs = 250)
    {
        while (!ct.IsCancellationRequested)
        {
            var messages = await GetMessagesAsync();
            if (messages.Count >= expectedCount)
                return messages;
            await Task.Delay(pollIntervalMs, ct);
        }
        ct.ThrowIfCancellationRequested();
        return []; // unreachable
    }

    /// <summary>
    /// Delete all messages from smtp4dev.
    /// </summary>
    public async Task ClearMessagesAsync()
    {
        var messages = await GetMessagesAsync();
        foreach (var msg in messages)
            await _httpClient.DeleteAsync($"{ApiBaseUrl}/api/Messages/{msg.Id}");
    }
}

public sealed class Smtp4devMessageList
{
    public List<Smtp4devMessage> Results { get; set; } = [];
}

public sealed class Smtp4devMessage
{
    public string Id { get; set; } = "";
    public string From { get; set; } = "";
    public string To { get; set; } = "";
    public string Subject { get; set; } = "";
    public DateTime ReceivedDate { get; set; }
}
