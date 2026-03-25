using EventStore.Client;
using Testcontainers.EventStoreDb;

namespace EventStoreMonitor.IntegrationTests;

public sealed class EventStoreFixture : IAsyncLifetime
{
    private readonly EventStoreDbContainer _container = new EventStoreDbBuilder()
        .WithImage("eventstore/eventstore:lts")
        .Build();

    public EventStoreClient Client { get; private set; } = null!;

    public async ValueTask InitializeAsync()
    {
        await _container.StartAsync();
        var settings = EventStoreClientSettings.Create(_container.GetConnectionString());
        settings.DefaultDeadline = TimeSpan.FromSeconds(10);
        Client = new EventStoreClient(settings);
    }

    public async ValueTask DisposeAsync()
    {
        Client.Dispose();
        await _container.DisposeAsync();
    }
}
