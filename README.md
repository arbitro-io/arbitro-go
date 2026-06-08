# arbitro-go

Official Go client for the [Arbitro](https://github.com/arbitro-io/arbitro) message broker.

Full parity with the Rust and TypeScript clients, leveraging Go's concurrency primitives (goroutines, channels, `select`, `context.Context`).

## Install

```bash
go get github.com/arbitro-io/arbitro-go
```

Requires Go 1.22+.

## Quick Start

```go
package main

import (
    "context"
    "fmt"
    "time"

    "github.com/arbitro-io/arbitro-go"
)

func main() {
    ctx := context.Background()

    client, _ := arbitro.Connect(ctx, "127.0.0.1:9898")
    defer client.Close()

    // Create stream
    client.CreateStream(ctx, "orders", arbitro.StreamConfig{
        SubjectFilter: "orders.>",
        MaxMsgs:       100_000,
        Journal:       arbitro.JournalTolerant,
    })

    // Publish
    client.Publish(ctx, "orders", "orders.created", []byte(`{"id":1}`))

    // Subscribe
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

## Features

### Publishing

```go
// Sync — waits for broker confirmation
err := client.Publish(ctx, "orders", "orders.created", payload)

// With dedup
err = client.Publish(ctx, "orders", "orders.created", payload,
    arbitro.WithMsgID("order-abc-123"),
)

// Async — fire-and-forget
client.PublishAsync("orders", "orders.created", payload)

// Batch — atomic, returns first seq
firstSeq, err := client.PublishBatch(ctx, "orders", []arbitro.BatchEntry{
    {Subject: "orders.a", Payload: payloadA},
    {Subject: "orders.b", Payload: payloadB, MsgID: "dedup-key"},
})

// Delayed — delivered after duration
err = client.PublishDelayed(ctx, "orders", "orders.reminder", payload, 30*time.Second)

// Request/Reply — RPC
response, err := client.Request(ctx, "orders", "orders.validate", requestPayload, 5*time.Second)
```

### Subscribing

```go
// Channel-based (Go's killer feature)
sub, _ := client.Subscribe(ctx, "orders", arbitro.ConsumerConfig{
    Name:   "workers",
    Filter: "orders.>",
})

for msg := range sub.Messages() {
    process(msg.Data())
    msg.Ack()
}

// Multi-stream fan-in with select (impossible in Rust/TS without extra libs)
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

// Callback mode (zero-alloc hot path)
sub, _ := client.Subscribe(ctx, "orders", cfg,
    arbitro.WithHandler(func(msg *arbitro.Msg) {
        process(msg.Data())
        msg.Ack()
    }),
)

// Pull-based fetch
msgs, _ := sub.Fetch(ctx, 10)
```

### Stream Management

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

### Consumer Management

```go
_, _ = client.CreateConsumer(ctx, "orders", arbitro.ConsumerConfig{
    Name:        "workers",
    Group:       "workers",
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

client.DeleteConsumer(ctx, "orders", "workers")
client.PauseConsumer(ctx, "orders", "workers")
client.ResumeConsumer(ctx, "orders", "workers")
info, _ := client.ConsumerInfo(ctx, "orders", "workers")
consumers, _ := client.ListConsumers(ctx, "orders")
pending, _ := client.GetPending(ctx, "orders", "workers")
```

### Sugar Helpers

```go
s := client.Stream("orders")
s.Publish(ctx, "orders.new", payload)
s.PublishAsync("orders.new", payload)
s.PublishBatch(ctx, entries)
s.DeleteMessage(ctx, 42)
s.Info(ctx)

c := s.Consumer(arbitro.ConsumerConfig{Name: "workers", Filter: "orders.>"})
sub, _ := c.Subscribe(ctx)
c.Pending(ctx)
```

### Cron

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

### Workflow / Saga

```go
wf, _ := client.Workflow("order-process").
    Trigger("orders.created").
    TriggerStream("orders").
    Step("validate", validateFn).
    Compensate("validate", rollbackValidation).
    Step("charge", chargeFn).
    Compensate("charge", refundFn).
    Step("ship", shipFn).
    MaxRetries(3).
    AckWait(30 * time.Second).
    MaxInflight(10).
    Start(ctx)

instanceID, _ := wf.Trigger(ctx, payload)
wf.Stop(ctx)
```

### Metrics

```go
m := client.Metrics()
// m.PublishesSent     uint64
// m.DeliveriesRecv   uint64
// m.AcksSent         uint64
// m.NacksSent        uint64
// m.Reconnects       uint64
// m.PendingRequests  uint64
// m.ActiveSubs       uint64
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

## Message Type

```go
msg.Subject()      // string
msg.SubjectBytes() // []byte (zero-alloc)
msg.Data()         // []byte (zero-copy into frame buffer)
msg.Seq()          // uint64
msg.ConsumerID()   // uint32
msg.Dup()          // bool (redelivery flag)
msg.Ack()          // explicit ack
msg.Nack()         // immediate requeue
msg.NackDelay(d)   // delayed requeue
msg.Copy()         // MsgCopy (heap-safe, escapes sync.Pool)
```

## Testing

```bash
# Unit tests (no broker needed)
go test ./internal/... -v -race

# Integration tests (broker must be running on :9898)
go test ./tests/... -v -race -tags=integration -timeout=60s
```

## Go Advantages

| Pattern | Go | Rust | TS |
|---------|:--:|:----:|:--:|
| Multi-stream select | Native `select{}` | `tokio::select!` macro | `Promise.race` |
| Fire-and-forget | `PublishAsync` | Separate API split | Unhandled promise |
| Graceful shutdown | `context.Context` | `CancellationToken` | No standard |
| Worker pool | N goroutines ranging channel | Manual tokio::spawn | Worker threads |
| Zero-copy + safe | `Msg.Copy()` escape hatch | Borrow checker | GC (no zero-copy) |
| Race detection | `go test -race` | miri (slow) | None |

## License

MIT
