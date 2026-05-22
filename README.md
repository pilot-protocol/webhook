# webhook

Webhook plugin for the Pilot Protocol daemon. POSTs daemon events
(node-discovered, connection-accepted, message-received, etc.) to an
HTTP(S) endpoint the operator configures. Subscribes to the in-process
event bus, supports URL hot-swap via the `WebhookManager` interface
exposed by `pkg/daemon`, and includes a circuit breaker plus
exponential retry backoff so a flaky downstream doesn't melt the
daemon.

## Install

```go
import "github.com/pilot-protocol/webhook"
```

## Usage

```go
webhookSvc := webhook.NewService(cfg.WebhookURL)
rt.Register(webhookSvc)

// Pass &webhookManagerAdapter{webhookSvc} into daemon.New(...) as
// the WebhookManager so IPC's `set-webhook` can hot-swap the URL.
```

## Layout

| File | What it does |
|---|---|
| `webhook.go` | Core: HTTP client, circuit breaker, retry queue, bus subscriber. |
| `service.go` | `*Service` — `coreapi.Service` adapter (Name/Order/Start/Stop) + `SetURL` + `Stats`. Build tag `!no_webhook`. |
| `service_disabled.go` | Stub `*Service` for `-tags no_webhook` builds. |

## Build tags

| Tag | Effect |
|---|---|
| `no_webhook` | Compiles a stub that no-ops `Start/Stop/SetURL`. |
