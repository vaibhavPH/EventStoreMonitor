using EventStoreMonitor.Configuration;
using MailKit.Net.Smtp;
using MailKit.Security;
using Microsoft.Extensions.Options;
using MimeKit;

namespace EventStoreMonitor.Services;

public sealed class EmailNotificationService
{
    private readonly EmailOptions _options;
    private readonly ILogger<EmailNotificationService> _logger;

    public EmailNotificationService(IOptions<EmailOptions> options, ILogger<EmailNotificationService> logger)
    {
        _options = options.Value;
        _logger = logger;
    }

    public async Task SendEventNotificationAsync(string streamName, string eventType, string eventId,
        ulong eventNumber, DateTime created, string? dataJson, CancellationToken ct = default)
    {
        var subject = $"[EventStore] New event: {eventType}";
        var body = $"""
            <html><body style="font-family:Arial,sans-serif;color:#333;">
            <h2 style="color:#2c5282;">🔔 New EventStoreDB Event Detected</h2>
            <table style="border-collapse:collapse;width:100%;max-width:600px;">
              <tr><td style="padding:8px;background:#edf2f7;font-weight:bold;width:140px;">Stream</td>
                  <td style="padding:8px;border-bottom:1px solid #e2e8f0;"><code>{HtmlEncode(streamName)}</code></td></tr>
              <tr><td style="padding:8px;background:#edf2f7;font-weight:bold;">Event Type</td>
                  <td style="padding:8px;border-bottom:1px solid #e2e8f0;"><code>{HtmlEncode(eventType)}</code></td></tr>
              <tr><td style="padding:8px;background:#edf2f7;font-weight:bold;">Event ID</td>
                  <td style="padding:8px;border-bottom:1px solid #e2e8f0;"><code>{HtmlEncode(eventId)}</code></td></tr>
              <tr><td style="padding:8px;background:#edf2f7;font-weight:bold;">Event #</td>
                  <td style="padding:8px;border-bottom:1px solid #e2e8f0;">{eventNumber}</td></tr>
              <tr><td style="padding:8px;background:#edf2f7;font-weight:bold;">Created (UTC)</td>
                  <td style="padding:8px;border-bottom:1px solid #e2e8f0;">{created:u}</td></tr>
            </table>
            {(dataJson is not null ? $"<h3 style='margin-top:16px;'>Event Data</h3><pre style='background:#f7fafc;padding:12px;border-radius:4px;overflow-x:auto;'>{HtmlEncode(dataJson)}</pre>" : "")}
            <p style="color:#718096;font-size:12px;margin-top:24px;">Sent by EventStoreMonitor — personal read-only monitoring.</p>
            </body></html>
            """;
        await SendAsync(subject, body, ct);
    }

    public async Task SendCrashNotificationAsync(Exception ex, CancellationToken ct = default)
    {
        var subject = "[EventStore Monitor] ⚠️ Service crashed";
        var body = $"""
            <html><body style="font-family:Arial,sans-serif;color:#333;">
            <h2 style="color:#c53030;">⚠️ EventStore Monitor Service Crashed</h2>
            <p>The monitoring service encountered an unhandled exception and is shutting down.</p>
            <table style="border-collapse:collapse;width:100%;max-width:600px;">
              <tr><td style="padding:8px;background:#fff5f5;font-weight:bold;width:140px;">Type</td>
                  <td style="padding:8px;border-bottom:1px solid #fed7d7;"><code>{HtmlEncode(ex.GetType().FullName ?? "Unknown")}</code></td></tr>
              <tr><td style="padding:8px;background:#fff5f5;font-weight:bold;">Message</td>
                  <td style="padding:8px;border-bottom:1px solid #fed7d7;">{HtmlEncode(ex.Message)}</td></tr>
              <tr><td style="padding:8px;background:#fff5f5;font-weight:bold;">Time (UTC)</td>
                  <td style="padding:8px;border-bottom:1px solid #fed7d7;">{DateTime.UtcNow:u}</td></tr>
            </table>
            <h3 style="margin-top:16px;">Stack Trace</h3>
            <pre style="background:#fff5f5;padding:12px;border-radius:4px;font-size:12px;">{HtmlEncode(ex.ToString())}</pre>
            </body></html>
            """;
        await SendAsync(subject, body, ct);
    }

    public async Task SendStartupNotificationAsync(IEnumerable<string> streams, CancellationToken ct = default)
    {
        var streamList = string.Join(", ", streams.Select(s => $"<code>{HtmlEncode(s)}</code>"));
        var subject = "[EventStore Monitor] ✅ Service started";
        var body = $"""
            <html><body style="font-family:Arial,sans-serif;color:#333;">
            <h2 style="color:#276749;">✅ EventStore Monitor Started</h2>
            <p>Monitoring streams: {streamList}</p>
            <p><strong>Started at (UTC):</strong> {DateTime.UtcNow:u}</p>
            <p style="color:#718096;font-size:12px;">Read-only catch-up subscriptions. No data written to production streams.</p>
            </body></html>
            """;
        await SendAsync(subject, body, ct);
    }

    private async Task SendAsync(string subject, string htmlBody, CancellationToken ct)
    {
        try
        {
            var message = new MimeMessage();
            message.From.Add(new MailboxAddress(_options.SenderDisplayName, _options.SenderAddress));
            message.To.Add(MailboxAddress.Parse(_options.RecipientAddress));
            message.Subject = subject;
            message.Body = new TextPart("html") { Text = htmlBody };
            using var smtp = new SmtpClient();
            try
            {
                await smtp.ConnectAsync(_options.SmtpHost, _options.SmtpPort,
                    _options.UseTls ? SecureSocketOptions.StartTls : SecureSocketOptions.None, ct);
                await smtp.AuthenticateAsync(_options.SenderAddress, _options.AppPassword, ct);
                await smtp.SendAsync(message, ct);
                _logger.LogInformation("Email sent: {Subject}", subject);
            }
            finally
            {
                if (smtp.IsConnected)
                    await smtp.DisconnectAsync(true, ct);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to send email '{Subject}' — monitoring continues", subject);
        }
    }

    private static string HtmlEncode(string? value) =>
        System.Web.HttpUtility.HtmlEncode(value ?? string.Empty);
}
