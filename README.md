# webhook

Pilot Protocol webhook plugin — POSTs daemon events (node-discovered,
connection-accepted, message-received, etc.) to an HTTP(S) endpoint
the operator configures. Subscribes to the in-process event bus,
handles URL hot-swap via the `WebhookManager` interface that
`pkg/daemon` exposes, and includes a circuit-breaker + exponential
retry backoff so a flaky downstream doesn't melt the daemon.

## Files

| File | What it does |
|---|---|
| `webhook.go` | Core: HTTP client, circuit breaker, retry queue, bus subscriber. |
| `service.go` | `*Service` — `coreapi.Service` adapter (Name/Order/Start/Stop) + `SetURL` + `Stats`. Build tag `!no_webhook`. |
| `service_disabled.go` | Stub `*Service` for `-tags no_webhook` builds. |
| `zz_logic_test.go` + `zz_circuit_breaker_bug_test.go` | Tests. |

## Daemon wiring (in cmd/daemon/main.go of the protocol repo)

```go
import "github.com/pilot-protocol/webhook"

webhookSvc := webhook.NewService(cfg.WebhookURL)
rt.Register(webhookSvc)
// Pass &webhookManagerAdapter{webhookSvc} into daemon.New(...) as
// the WebhookManager so IPC's `set-webhook` can hot-swap the URL.
```

## Disabling

`go build -tags no_webhook` → stub that no-ops `Start/Stop/SetURL`.
