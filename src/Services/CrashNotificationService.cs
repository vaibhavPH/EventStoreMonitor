namespace EventStoreMonitor.Services;

public sealed class CrashNotificationService : IHostedService
{
    private readonly EmailNotificationService _emailService;
    private readonly ILogger<CrashNotificationService> _logger;

    public CrashNotificationService(EmailNotificationService emailService, ILogger<CrashNotificationService> logger)
    {
        _emailService = emailService;
        _logger = logger;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        AppDomain.CurrentDomain.UnhandledException += OnUnhandledException;
        TaskScheduler.UnobservedTaskException += OnUnobservedTaskException;
        _logger.LogInformation("Crash notification handler registered");
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        AppDomain.CurrentDomain.UnhandledException -= OnUnhandledException;
        TaskScheduler.UnobservedTaskException -= OnUnobservedTaskException;
        return Task.CompletedTask;
    }

    private void OnUnhandledException(object sender, UnhandledExceptionEventArgs e)
    {
        if (e.ExceptionObject is Exception ex)
        {
            _logger.LogCritical(ex, "Unhandled exception — sending crash email");
            _emailService.SendCrashNotificationAsync(ex, CancellationToken.None).GetAwaiter().GetResult();
        }
    }

    private void OnUnobservedTaskException(object? sender, UnobservedTaskExceptionEventArgs e)
    {
        e.SetObserved();
        _logger.LogError(e.Exception, "Unobserved task exception");
        _ = _emailService.SendCrashNotificationAsync(e.Exception, CancellationToken.None);
    }
}
