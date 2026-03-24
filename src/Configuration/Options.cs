using System.ComponentModel.DataAnnotations;

namespace EventStoreMonitor.Configuration;

public sealed class EventStoreOptions
{
    public const string SectionName = "EventStore";
    [Required] public required string ConnectionString { get; init; }
    [Required, MinLength(1)] public required string[] MonitoredStreams { get; init; }
    [Required] public required string ConsumerGroupName { get; init; }
    public string CheckpointStreamPrefix { get; init; } = "monitor-checkpoint";
}

public sealed class EmailOptions
{
    public const string SectionName = "Email";
    [Required] public required string SmtpHost { get; init; }
    [Range(1, 65535)] public int SmtpPort { get; init; } = 587;
    public bool UseTls { get; init; } = true;
    [Required, EmailAddress] public required string SenderAddress { get; init; }
    public string SenderDisplayName { get; init; } = "EventStore Monitor";
    [Required, EmailAddress] public required string RecipientAddress { get; init; }
    public string? AppPassword { get; init; }
}
