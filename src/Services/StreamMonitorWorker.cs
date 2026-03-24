using EventStore.Client;
using System.Text;
using System.Text.Json;

namespace EventStoreMonitor.Services;

public sealed class StreamMonitorWorker : BackgroundService
{
    private readonly string _streamName;
    private readonly EventStoreClient _eventStoreClient;
    private readonly CheckpointService _checkpointService;
    private readonly EmailNotificationService _emailService;
    private readonly ILogger<StreamMonitorWorker> _logger;
    private static readonly TimeSpan RetryDelay = TimeSpan.FromSeconds(10);

    public StreamMonitorWorker(string streamName, EventStoreClient eventStoreClient,
        CheckpointService checkpointService, EmailNotificationService emailService,
        ILogger<StreamMonitorWorker> logger)
    {
        _streamName = streamName;
        _eventStoreClient = eventStoreClient;
        _checkpointService = checkpointService;
        _emailService = emailService;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Starting monitor for stream: {Stream}", _streamName);
        while (!stoppingToken.IsCancellationRequested)
        {
            try { await SubscribeAndProcessAsync(stoppingToken); }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { break; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Subscription to {Stream} dropped — retrying in {Delay}s", _streamName, RetryDelay.TotalSeconds);
                try { await Task.Delay(RetryDelay, stoppingToken); } catch (OperationCanceledException) { break; }
            }
        }
    }

    private async Task SubscribeAndProcessAsync(CancellationToken ct)
    {
        var lastPosition = await _checkpointService.LoadAsync(_streamName, ct);
        var fromPosition = lastPosition.HasValue
            ? FromStream.After(StreamPosition.FromInt64((long)lastPosition.Value))
            : FromStream.Start;

        _logger.LogInformation("Subscribing to {Stream} from {Position}", _streamName,
            lastPosition.HasValue ? lastPosition.Value.ToString() : "Start");

        await foreach (var resolvedEvent in _eventStoreClient.SubscribeToStream(
            _streamName, fromPosition, resolveLinkTos: true, cancellationToken: ct))
        {
            var evt = resolvedEvent.Event;
            if (evt.EventType.StartsWith('\$')) continue;

            _logger.LogInformation("[{Stream}] [{EventNumber}] {EventType} ({EventId})",
                _streamName, evt.EventNumber, evt.EventType, evt.EventId);

            string? prettyJson = null;
            try
            {
                if (evt.Data.Length > 0)
                {
                    var raw = JsonDocument.Parse(evt.Data);
                    prettyJson = JsonSerializer.Serialize(raw, new JsonSerializerOptions { WriteIndented = true });
                }
            }
            catch { prettyJson = Encoding.UTF8.GetString(evt.Data.Span); }

            await _emailService.SendEventNotificationAsync(
                _streamName, evt.EventType, evt.EventId.ToString(),
                evt.EventNumber.ToUInt64(), evt.Created, prettyJson, ct);

            await _checkpointService.SaveAsync(_streamName, evt.EventNumber.ToUInt64(), ct);
        }
    }
}
