# arbitro-go

Official Go client for the [Arbitro](https://github.com/arbitro-io/arbitro) message broker.

Full parity with the Rust and TypeScript clients, leveraging Go's concurrency primitives (goroutines, channels, `select`, `context.Context`).

## Requirements

- Go 1.22+
- Arbitro broker reachable on `127.0.0.1:9898`

## Install

```bash
go get github.com/arbitro-io/arbitro-go
```

## Run the Broker (Docker)

```bash
docker run --rm -p 9898:9898 ghcr.io/arbitro-io/arbitro-server:latest
```

Pin a version tag for production:

- `ghcr.io/arbitro-io/arbitro-server:0.5.3` -- immutable release tag
- `ghcr.io/arbitro-io/arbitro-server:0.5`   -- auto-updates within `0.5.*`
- `ghcr.io/arbitro-io/arbitro-server:latest` -- latest tagged release

## Quick Start

```go
package main

import (
    "context"
    "fmt"

    "github.com/arbitro-io/arbitro-go"
)

func main() {
    ctx := context.Background()

    client, _ := arbitro.Connect(ctx, "127.0.0.1:9898")
    defer client.Close()

    client.CreateStream(ctx, "orders", arbitro.StreamConfig{
        SubjectFilter: "orders.>",
        MaxMsgs:       100_000,
        Journal:       arbitro.JournalTolerant,
    })

    client.Publish(ctx, "orders", "orders.created", []byte(`{"id":1}`))

    sub, _ := client.Subscribe(ctx, "orders", arbitro.ConsumerConfig{
        Name:   "workers",
        Filter: "orders.>",
    })

    for msg := range sub.Messages() {
        fmt.Println(msg.Subject(), string(msg.Data()))
        msg.Ack()
    }
}
```

## Publish

```go
// Sync -- waits for broker confirmation (RepOk)
err := client.Publish(ctx, "orders", "orders.created", payload)

// With dedup
err = client.Publish(ctx, "orders", "orders.created", payload,
    arbitro.WithMsgID("order-abc-123"),
)

// Async -- fire-and-forget, returns immediately. err is non-nil only when the
// outbound queue had no room to take the frame (see "Bounded Backpressure").
err = client.PublishAsync("orders", "orders.created", payload)

// Fire-and-forget with pre-resolved stream ID (fastest path)
streamID, _ := client.ResolveStreamID(ctx, "orders")
err = client.PublishFireAndForget(streamID, "orders.created", payload)

// Batch -- atomic, returns first seq
firstSeq, err := client.PublishBatch(ctx, "orders", []arbitro.BatchEntry{
    {Subject: "orders.a", Payload: payloadA},
    {Subject: "orders.b", Payload: payloadB, MsgID: "dedup-key"},
})

// Batch fire-and-forget (write-coalesced, highest throughput)
err = client.PublishBatchAsync("orders", entries)

// Delayed -- delivered after duration
err = client.PublishDelayed(ctx, "orders", "orders.reminder", payload, 30*time.Second)
```

Request/reply is not a publish operation -- it belongs to a service. See the
Service section below.

## Publish with Headers

Attach arbitrary key-value metadata to messages. Headers are persisted alongside the payload and stripped on delivery -- consumers always receive only the user payload.

```go
// Custom headers (tracing, routing metadata)
err := client.Publish(ctx, "orders", "orders.created", payload,
    arbitro.WithHeaders(map[string]string{
        "trace-id": "abc-123",
        "source":   "checkout-svc",
    }),
)

// Headers + dedup
err = client.Publish(ctx, "orders", "orders.created", payload,
    arbitro.WithMsgID("order-abc-123"),
    arbitro.WithHeaders(map[string]string{
        "priority": "high",
        "region":   "us-east-1",
    }),
)

// Batch with per-entry headers
firstSeq, err := client.PublishBatch(ctx, "orders", []arbitro.BatchEntry{
    {Subject: "orders.a", Payload: payloadA, Headers: map[string]string{"priority": "high"}},
    {Subject: "orders.b", Payload: payloadB, Headers: map[string]string{"priority": "low"}},
})
```

Headers use a zero-copy TLV wire format -- no serialization overhead. The broker persists them with the entry and strips them at delivery time. The client handles `string` → `[]byte` conversion internally.

## Subscribe

```go
// Channel-based
sub, _ := client.Subscribe(ctx, "orders", arbitro.ConsumerConfig{
    Name:   "workers",
    Filter: "orders.>",
})

for msg := range sub.Messages() {
    process(msg.Data())
    msg.Ack()
}

// Multi-stream fan-in with select
select {
case msg := <-ordersSub.Messages():
    processOrder(msg)
    msg.Ack()
case msg := <-paymentsSub.Messages():
    processPayment(msg)
    msg.Ack()
case <-ctx.Done():
    return
}

// Callback mode (no channel overhead)
sub, _ := client.Subscribe(ctx, "orders", cfg,
    arbitro.WithHandler(func(msg *arbitro.Msg) {
        process(msg.Data())
        msg.Ack()
    }),
)

// Pull-based fetch (N messages with timeout)
msgs, _ := sub.Fetch(ctx, 10)
```

## Service (Request/Reply RPC)

Build named services with automatic stream/consumer creation, handler dispatch, and correlated request/reply.

```go
ctx := context.Background()

// Build a service — creates backing stream + consumer automatically
svc, _ := client.Service("calculator").SetMaxInflight(1024).Build(ctx)
defer svc.Close()

// Register method handlers.
// Return non-nil bytes — the framework replies (if a reply address is
// present) and acks. Return an error — the framework nacks for redelivery.
// Return nil bytes with no error — the framework acks without replying.
// Each handler runs in its own goroutine; slow handlers do not block the
// dispatcher.
svc.Handle("add", func(req *arbitro.Request) ([]byte, error) {
    return []byte(fmt.Sprintf("sum=%d", compute(req.Data()))), nil
})

svc.Handle("multiply", func(req *arbitro.Request) ([]byte, error) {
    return []byte(fmt.Sprintf("product=%d", computeMul(req.Data()))), nil
})

// Send a request to another service (or self)
response, err := svc.Request(ctx, "calculator", "add", []byte("2+3"), 5*time.Second)
fmt.Println(string(response)) // "sum=5"

// Fire-and-forget
svc.Send(ctx, "audit", "log", []byte("event-data"))

// Cross-service RPC (Go advantage: blocks in goroutine, no callback hell)
gateway, _ := client.Service("gateway").Build(ctx)
resp, _ := gateway.Request(ctx, "calculator", "multiply", []byte("3*4"), 5*time.Second)
```

Handlers answer by returning bytes. There is no `Reply()` to call: the
framework publishes the return value to the requester and pairs it with
exactly one ack or nack. Returning an error nacks for redelivery.

## Per-Subject Inflight Limits

```go
_, _ = client.CreateConsumer(ctx, "orders", arbitro.ConsumerConfig{
    Name:        "workers",
    Filter:      "orders.>",
    AckPolicy:   arbitro.AckExplicit,
    MaxInflight: 1000,
    AckWait:     30 * time.Second,
    MaxDeliver:  5,
    MaxSubjectInflights: []arbitro.SubjectLimit{
        {Pattern: "orders.priority.>", Limit: 1},
        {Pattern: "orders.bulk.>", Limit: 100},
    },
})
```

## Stream Management

```go
stream, _ := client.CreateStream(ctx, "orders", arbitro.StreamConfig{
    SubjectFilter:     "orders.>",
    MaxMsgs:           1_000_000,
    MaxBytes:          1 << 30,
    MaxAge:            24 * time.Hour,
    Replicas:          3,
    Journal:           arbitro.JournalTolerant,
    IdempotencyWindow: 5 * time.Second,
})

client.DeleteStream(ctx, "orders")
client.DeleteStream(ctx, "orders", arbitro.KeepData())
info, _ := client.StreamInfo(ctx, "orders")
streams, _ := client.ListStreams(ctx)
exists, _ := client.StreamExists(ctx, "orders")
n, _ := client.PurgeStream(ctx, "orders")
n, _ = client.DrainSubject(ctx, "orders", "orders.cancelled.>")
ok, _ := client.DeleteMessage(ctx, "orders", 42)
```

## Consumer Management

```go
client.DeleteConsumer(ctx, "orders", "workers")
client.PauseConsumer(ctx, "orders", "workers")
client.ResumeConsumer(ctx, "orders", "workers")
info, _ := client.ConsumerInfo(ctx, "orders", "workers")
consumers, _ := client.ListConsumers(ctx, "orders")
pending, _ := client.GetPending(ctx, "orders", "workers")
```

## Cron Scheduling

Distributed cron jobs with queue semantics -- multiple workers, single delivery per fire.

```go
handle, _ := client.Cron("daily-report").
    Every("0 8 * * *").
    Timezone("America/New_York").
    Timeout(60 * time.Second).
    Overlap(false).
    Run(ctx, func(fire arbitro.CronFire) error {
        return generateReport(fire.Time, fire.Index)
    })

handle.Stop(ctx)
```

Crons re-register automatically on reconnect.

## Workflow / Saga

Client-side workflow pipelines over Arbitro streams. The broker has no workflow-specific code -- everything uses streams, consumer groups, and idempotent publish.

```go
wf, _ := client.Workflow("order-process").
    Trigger("orders.created").
    TriggerStream("orders").
    Step("validate", validateFn).
    Compensate("validate", rollbackValidation).
    Step("charge", chargeFn).
    Compensate("charge", refundFn).
    SuspendStep("payment-auth", 30_000,
        func(ctx StepContext) (StepOutcome, error) {
            state, err := prepareAuth(ctx.Input)
            if err != nil {
                return StepOutcome{}, err
            }
            return OutcomeSuspend(state, 30_000), nil
        },
        func(rctx ResumeContext) (StepResult, error) {
            result, err := processAuthResult(rctx.State, rctx.Event)
            if err != nil {
                return StepResult{}, err
            }
            return StepResult{Context: result}, nil
        },
    ).
    OnTimeout(func(tctx TimeoutContext) (StepResult, error) {
        cancelled, err := cancelAuth(tctx.State)
        if err != nil {
            return StepResult{}, err
        }
        return StepResult{Context: cancelled}, nil
    }).
    Step("ship", shipFn).
    MaxRetries(3).
    AckWait(30 * time.Second).
    MaxInflight(10).
    Start(ctx)

// Trigger
instanceID, _ := wf.Trigger(ctx, payload)

// Trigger with explicit ID (dedup-safe)
wf.TriggerWithID(ctx, "order-123", payload)

// Resume a suspended instance
wf.Resume(ctx, "order-123", authResultPayload)

// Cancel a running or suspended instance
wf.Cancel(ctx, "order-123")

// Source: external stream triggers (streamName + subject)
wf2, _ := client.Workflow("event-driven").
    Source("external-events", "events.>").
    Step("process", processFn).
    Start(ctx)

wf.Stop(ctx)
```

## Delayed Publish

```go
err := client.PublishDelayed(ctx, "orders", "orders.reminder", payload, 5*time.Second)
```

## Metrics

```go
m := client.Metrics()
// m.PublishesSent     uint64
// m.DeliveriesRecv   uint64
// m.AcksSent         uint64
// m.NacksSent        uint64
// m.Reconnects       uint64
// m.PendingRequests  uint64
// m.ActiveSubs       uint64
// m.BatchFramesRecv  uint64
```

## Message Type

```go
msg.Subject()      // string
msg.SubjectBytes() // []byte (zero-alloc)
msg.Data()         // []byte (zero-copy into frame buffer)
msg.ReplyTo()      // []byte (reply_to field, nil if none)
msg.Seq()          // uint64
msg.ConsumerID()   // uint32
msg.Dup()          // bool (redelivery flag)
msg.Ack()          // explicit ack (batched)
msg.Nack()         // immediate requeue
msg.NackDelay(d)   // delayed requeue
msg.Copy()         // MsgCopy (heap-safe)
```

## Connection Options

```go
client, _ := arbitro.Connect(ctx, "127.0.0.1:9898",
    arbitro.WithTimeout(5*time.Second),
    arbitro.WithReconnect(true, 10, 500*time.Millisecond),
    arbitro.WithPrefix("myapp"),
    arbitro.WithTLS(tlsConfig),
    arbitro.WithLogger(slog.Default()),
)
```

## Bounded Backpressure (Write Queue)

Every connection has one outbound queue feeding the TCP socket. `WithWriteQueue`
sets its depth and how long a blocking send will wait for room:

```go
client, _ := arbitro.Connect(ctx, "127.0.0.1:9898",
    // Absorb up to 8192 unsent frames; give a blocking Publish at most
    // 2 seconds to find room before giving up.
    arbitro.WithWriteQueue(8192, 2*time.Second),
)

err := client.Publish(ctx, "orders", "orders.created", payload)
if err != nil {
    // A deadline reached while the queue was still full surfaces here as
    // arbitro.ErrQueueFull ("arbitro: outgoing queue full") -- transient
    // backpressure, not a dead connection. Retry, shed the message, or
    // slow the producer. A closed connection is a different, terminal
    // error and must not be handled the same way.
    if errors.Is(err, arbitro.ErrQueueFull) {
        // back off and retry -- the message was NOT sent
    }
}
```

- `cap` (first argument) is the memory-vs-tolerance dial: a deeper queue rides
  out longer broker stalls at the cost of more frames held in RAM. Zero keeps
  the default (4096).
- `maxBlock` (second argument) bounds how long a blocking `Send` -- used by
  `Publish`, request/reply, and the tail chunks of `PublishBatch` -- waits for
  room when the caller's `context.Context` carries no deadline of its own; the
  caller's own `ctx` deadline or cancellation always wins if it fires first.
  Zero keeps the default (5s). A negative value restores the old
  wait-forever behaviour, but it has to be asked for explicitly -- that is
  how a stalled broker used to turn into a hung publisher.
- `Connection.TrySend` never blocks at all: it returns `arbitro.ErrQueueFull`
  immediately if there is no room, and deliberately takes no
  `context.Context` -- it cannot wait, so there is nothing to cancel.
  `PublishAsync`, `Stream.PublishAsync`, and `PublishBatchAsync` use it
  internally, which is why they now return an `error` (see migration note
  below).
- `arbitro.ErrQueueFull` is the sentinel to put in `errors.Is`. The queue lives
  in `internal/conn`, which nothing outside this module can import, so the
  package re-exports the sentinel at the root -- same pattern as
  `arbitro.ErrAckStoreLocked`. It is the one error on these paths that means
  *retry*; everything else is terminal for the connection.

### Migrating from before this change (breaking)

`Client.PublishAsync`, `Stream.PublishAsync`, and `Client.PublishBatchAsync`
now return `error`. They used to return nothing -- a failed enqueue was fed
into a metric counter and discarded, so a caller had no way to learn a
message never left. Fire-and-forget means not waiting for the **broker**; it
was never a license to drop work silently. Update call sites that ignored the
return value:

```go
// before
client.PublishAsync("orders", "orders.created", payload)

// after
if err := client.PublishAsync("orders", "orders.created", payload); err != nil {
    // queue full (transient, see above) -- retry, shed, or slow down
}
```

## Redelivery Dedup and Where It Is Stored

The broker delivers at-least-once, so a crash between "processed" and "acked"
can produce a redelivery. The client keeps a dedup set keyed by
`(stream_name, consumer_name, seq)` so the handler is not re-run for work
already done.

By default that set is **in memory** — it dedups within one process lifetime and
is gone on restart. `WithAckStoreDir` swaps in a durable write-ahead log:

```go
client, _ := arbitro.Connect(ctx, "127.0.0.1:9898",
    // Where the dedup WAL lives. This is the one setting a deployment
    // almost always wants to pin.
    arbitro.WithAckStoreDir("/var/lib/myapp/ackstore"),
)

// Also set the entry TTL and fsync policy:
arbitro.WithAckPersistence("/var/lib/myapp/ackstore", time.Hour, true)

// Or turn dedup off entirely (handlers must be idempotent):
arbitro.WithoutAckDedup()
```

### Default location

`WithAckStoreDir("")` (and `WithAckPersistence("", ...)`) resolves the directory
in this order:

1. `$ARBITRO_ACKSTORE_DIR` — operator override, no code change, honoured
   identically by the Rust, Go and TS clients.
2. The platform state directory:

   | platform    | default directory                                  |
   |-------------|----------------------------------------------------|
   | Linux / BSD | `$XDG_STATE_HOME/arbitro/ackstore`, else `~/.local/state/arbitro/ackstore` |
   | macOS       | `~/Library/Application Support/arbitro/ackstore`    |
   | Windows     | `%LOCALAPPDATA%\arbitro\ackstore`                   |

3. Nothing resolvable (no `HOME` / `%LOCALAPPDATA%`, e.g. a bare systemd unit)
   → `Connect` fails with `arbitro.ErrNoDefaultAckStoreDir`.

There is deliberately **no** cwd-relative and **no** temp-dir fallback. A
cwd-relative store moves whenever the service is started from a different
directory, and a temp store is erased on reboot — both silently resurrect the
duplicate processing the store exists to prevent, while looking healthy. An
explicit error naming the two fixes is better than either.

`arbitro.DefaultAckStoreDir()` reports the resolved path without opening
anything; log it at startup.

### One writer per directory

Opening the store takes an OS advisory lock (`flock` on unix, an exclusive
share-mode open on Windows) on `<dir>/ackstore.lock`. A second `Connect` against
the same directory fails with `arbitro.ErrAckStoreLocked` instead of
interleaving writes into one log.

This is enforced rather than documented because two writers do not merely
corrupt bytes: each numbers slots from its own counter, so after a restart
replay attributes one process's records to the other's `(stream, consumer)` —
and a false dedup hit is a message whose handler never runs. The lock is
released by the kernel when the process exits, so a crash never wedges the
directory. It does not extend across a network filesystem; a WAL shared between
hosts is not supported.

Running several clients concurrently? Give each its own directory.

## Replication

Replication is transparent to the client -- `Replicas` is set at `CreateStream` time. The client publishes normally; the broker handles replication internally.

## Testing

```bash
go test ./...
```

Everything is unit-only and needs no infrastructure, with one exception:
`replay_backlog_test.go` is the first integration test in this repo -- the Go
twin of the Rust replay bench and of the TypeScript/C replay tests. It fills
several streams as fast as the client will take the work, then reads
everything back, to catch a published tail the client accepted but never
sent. It skips automatically when no broker answers at `127.0.0.1:9898`, so
`go test ./...` stays runnable with no setup. Point it at a broker running
elsewhere with `ARBITRO_ADDR`:

```bash
ARBITRO_ADDR=127.0.0.1:9898 go test ./...
```

## License

MIT
