using EventStore.Client;
using EventStoreMonitor.Configuration;
using Microsoft.Extensions.Options;
using System.Text.Json;

namespace EventStoreMonitor.Services;

public sealed class CheckpointService
{
    private readonly EventStoreClient _client;
    private readonly EventStoreOptions _options;
    private readonly ILogger<CheckpointService> _logger;
    private static readonly JsonSerializerOptions _jsonOptions = new() { PropertyNamingPolicy = JsonNamingPolicy.CamelCase };

    public CheckpointService(EventStoreClient client, IOptions<EventStoreOptions> options, ILogger<CheckpointService> logger)
    {
        _client = client;
        _options = options.Value;
        _logger = logger;
    }

    public async Task<ulong?> LoadAsync(string monitoredStream, CancellationToken ct)
    {
        var checkpointStream = GetCheckpointStreamName(monitoredStream);
        try
        {
            var result = _client.ReadStreamAsync(Direction.Backwards, checkpointStream, StreamPosition.End, maxCount: 1, cancellationToken: ct);
            if (await result.ReadState == ReadState.StreamNotFound)
            {
                _logger.LogInformation("No checkpoint for {Stream} — subscribing from beginning", monitoredStream);
                return null;
            }
            var @event = await result.FirstOrDefaultAsync(ct);
            if (@event.Event is null) return null;
            var checkpoint = JsonSerializer.Deserialize<CheckpointData>(@event.Event.Data.Span, _jsonOptions);
            _logger.LogInformation("Loaded checkpoint for {Stream}: {Position}", monitoredStream, checkpoint?.Position);
            return checkpoint?.Position;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to load checkpoint for {Stream} — subscribing from beginning", monitoredStream);
            return null;
        }
    }

    public async Task SaveAsync(string monitoredStream, ulong position, CancellationToken ct)
    {
        var checkpointStream = GetCheckpointStreamName(monitoredStream);
        try
        {
            var data = JsonSerializer.SerializeToUtf8Bytes(new CheckpointData(position, DateTime.UtcNow), _jsonOptions);
            var eventData = new EventData(Uuid.NewUuid(), "CheckpointRecorded", data);
            try
            {
                await _client.AppendToStreamAsync(checkpointStream, StreamState.StreamExists, [eventData], cancellationToken: ct);
            }
            catch (WrongExpectedVersionException)
            {
                await _client.SetStreamMetadataAsync(checkpointStream, StreamState.NoStream, new StreamMetadata(maxCount: 1), cancellationToken: ct);
                await _client.AppendToStreamAsync(checkpointStream, StreamState.NoStream, [eventData], cancellationToken: ct);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to save checkpoint for {Stream} at {Position}", monitoredStream, position);
        }
    }

    private string GetCheckpointStreamName(string monitoredStream) =>
        $"{_options.CheckpointStreamPrefix}-{monitoredStream.TrimStart('$')}";

    private sealed record CheckpointData(ulong Position, DateTime SavedAt);
}
