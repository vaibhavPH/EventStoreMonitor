# EventStore Monitor

Personal, zero-side-effect EventStoreDB notification service.
Sends Gmail alerts when new events appear in configured `$et-*` streams, and emails you if the service crashes.

## Quick Start

1. Fill in `appsettings.json` with your EventStoreDB connection string and Gmail App Password
2. Generate App Password at https://myaccount.google.com/apppasswords (requires 2FA)
3. Ensure `$by_event_type` projection is enabled in EventStoreDB
4. `dotnet run`

## Adding more streams
```json
"MonitoredStreams": [
  "$et-ShipmentIncidentReopened",
  "$et-OrderCancelled"
]
```

No code changes needed — just update config and restart.

## Zero Side Effects

- Uses **catch-up subscriptions** (read-only, client-side only — nothing created server-side)
- Checkpoints stored in isolated `monitor-checkpoint-*` streams with `$maxCount=1`
- Email failures never affect the subscription
