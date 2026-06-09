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

// Async -- fire-and-forget, returns immediately
client.PublishAsync("orders", "orders.created", payload)

// Fire-and-forget with pre-resolved stream ID (fastest path)
streamID, _ := client.ResolveStreamID(ctx, "orders")
client.PublishFireAndForget(streamID, "orders.created", payload)

// Batch -- atomic, returns first seq
firstSeq, err := client.PublishBatch(ctx, "orders", []arbitro.BatchEntry{
    {Subject: "orders.a", Payload: payloadA},
    {Subject: "orders.b", Payload: payloadB, MsgID: "dedup-key"},
})

// Batch fire-and-forget (write-coalesced, highest throughput)
client.PublishBatchAsync("orders", entries)

// Delayed -- delivered after duration
err = client.PublishDelayed(ctx, "orders", "orders.reminder", payload, 30*time.Second)

// Request/Reply -- RPC
response, err := client.Request(ctx, "orders", "orders.validate", requestPayload, 5*time.Second)
```

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
    SuspendStep("payment-auth", prepareAuthFn).
    OnTimeout(handleTimeoutFn).
    Step("ship", shipFn).
    MaxRetries(3).
    AckWait(30 * time.Second).
    MaxInflight(10).
    Start(ctx)

// Trigger
instanceID, _ := wf.Trigger(ctx, payload)

// Trigger with explicit ID (dedup-safe)
wf.TriggerWithID(ctx, []byte("order-123"), payload)

// Resume a suspended instance
wf.Resume(ctx, []byte("order-123"), authResultPayload)

// Cancel a running or suspended instance
wf.Cancel(ctx, []byte("order-123"))

// Source: external stream triggers
wf2, _ := client.Workflow("event-driven").
    Source("external-events").
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

## Replication

Replication is transparent to the client -- `Replicas` is set at `CreateStream` time. The client publishes normally; the broker handles replication internally.

## License

MIT
