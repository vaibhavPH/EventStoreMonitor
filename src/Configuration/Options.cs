namespace EventStoreMonitor.Configuration;

public sealed class EventStoreOptions
{
    public const string SectionName = "EventStore";
    public required string ConnectionString { get; init; }
    public required string[] MonitoredStreams { get; init; }
    public required string ConsumerGroupName { get; init; }
    public string CheckpointStreamPrefix { get; init; } = "monitor-checkpoint";
}

public sealed class EmailOptions
{
    public const string SectionName = "Email";
    public required string SmtpHost { get; init; }
    public int SmtpPort { get; init; } = 587;
    public bool UseTls { get; init; } = true;
    public required string SenderAddress { get; init; }
    public string SenderDisplayName { get; init; } = "EventStore Monitor";
    public required string RecipientAddress { get; init; }
    public required string AppPassword { get; init; }
}
