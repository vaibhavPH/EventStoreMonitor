using EventStore.Client;
using EventStoreMonitor.Configuration;
using Microsoft.Extensions.Options;

namespace EventStoreMonitor.Services;

public sealed class MonitorOrchestratorService : BackgroundService
{
    private readonly EventStoreOptions _esOptions;
    private readonly EventStoreClient _eventStoreClient;
    private readonly CheckpointService _checkpointService;
    private readonly EmailNotificationService _emailService;
    private readonly ILogger<StreamMonitorWorker> _workerLogger;
    private readonly ILogger<MonitorOrchestratorService> _logger;

    public MonitorOrchestratorService(IOptions<EventStoreOptions> esOptions, EventStoreClient eventStoreClient,
        CheckpointService checkpointService, EmailNotificationService emailService,
        ILogger<StreamMonitorWorker> workerLogger, ILogger<MonitorOrchestratorService> logger)
    {
        _esOptions = esOptions.Value;
        _eventStoreClient = eventStoreClient;
        _checkpointService = checkpointService;
        _emailService = emailService;
        _workerLogger = workerLogger;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var streams = _esOptions.MonitoredStreams;
        _logger.LogInformation("Watching {Count} stream(s): {Streams}", streams.Length, string.Join(", ", streams));
        await _emailService.SendStartupNotificationAsync(streams, stoppingToken);

        var workerTasks = streams.Select(stream =>
        {
            var worker = new StreamMonitorWorker(stream, _eventStoreClient,
                _checkpointService, _emailService, _workerLogger);
            return worker.StartAsync(stoppingToken);
        }).ToArray();

        await Task.WhenAll(workerTasks);
    }
}
