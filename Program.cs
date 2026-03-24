using EventStore.Client;
using EventStoreMonitor.Configuration;
using EventStoreMonitor.Services;

var host = Host.CreateApplicationBuilder(args);

var config = host.Configuration;

host.Services
    .AddOptions<EventStoreOptions>()
    .Bind(config.GetSection(EventStoreOptions.SectionName))
    .ValidateDataAnnotations()
    .ValidateOnStart();

host.Services
    .AddOptions<EmailOptions>()
    .Bind(config.GetSection(EmailOptions.SectionName))
    .ValidateDataAnnotations()
    .ValidateOnStart();

host.Services.AddSingleton(sp =>
{
    var opts = sp.GetRequiredService<IConfiguration>()
        .GetSection(EventStoreOptions.SectionName)
        .Get<EventStoreOptions>()
        ?? throw new InvalidOperationException("EventStore configuration is missing.");
    var settings = EventStoreClientSettings.Create(opts.ConnectionString);
    return new EventStoreClient(settings);
});

host.Services.AddSingleton<EmailNotificationService>();
host.Services.AddSingleton<CheckpointService>();
host.Services.AddHostedService<CrashNotificationService>();
host.Services.AddHostedService<MonitorOrchestratorService>();

AppDomain.CurrentDomain.UnhandledException += (_, e) =>
{
    Console.Error.WriteLine($"[FATAL] Unhandled exception: {e.ExceptionObject}");
};

var app = host.Build();

Console.WriteLine("""
    ╔═══════════════════════════════════════════════════╗
    ║       EventStore Monitor — Personal Alerts        ║
    ║  Read-only catch-up subscriptions. No side effects║
    ╚═══════════════════════════════════════════════════╝
    Press Ctrl+C to stop.
    """);

await app.RunAsync();
