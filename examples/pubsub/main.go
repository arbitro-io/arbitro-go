// Example: basic pub/sub with Arbitro.
// Demonstrates creating a stream, publishing messages, and consuming via channels.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/arbitro-io/arbitro-go"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	// Connect to broker
	client, err := arbitro.Connect(ctx, "127.0.0.1:9898",
		arbitro.WithTimeout(5*time.Second),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	// Create stream
	_, err = client.CreateStream(ctx, "orders", arbitro.StreamConfig{
		SubjectFilter:     "orders.>",
		MaxMsgs:           100_000,
		MaxAge:            24 * time.Hour,
		Journal:           arbitro.JournalTolerant,
		IdempotencyWindow: 5 * time.Second,
	})
	if err != nil && !arbitro.IsAlreadyExists(err) {
		log.Fatal(err)
	}

	// Subscribe
	sub, err := client.Subscribe(ctx, "orders", arbitro.ConsumerConfig{
		Name:        "worker",
		Filter:      "orders.>",
		AckPolicy:   arbitro.AckExplicit,
		MaxInflight: 100,
		AckWait:     30 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}

	// Publish in background
	go func() {
		for i := 0; i < 10; i++ {
			payload := fmt.Sprintf(`{"order_id": %d, "item": "widget"}`, i+1)
			err := client.Publish(ctx, "orders", "orders.created", []byte(payload))
			if err != nil {
				log.Printf("publish error: %v", err)
				return
			}
			log.Printf("published order %d", i+1)
			time.Sleep(100 * time.Millisecond)
		}
	}()

	// Consume — idiomatic Go: range over channel
	for msg := range sub.Messages() {
		fmt.Printf("  received: subject=%s data=%s\n", msg.Subject(), msg.Data())
		msg.Ack()
	}
}
