// Example: batch publishing and multi-stream fan-in with select.
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

	client, err := arbitro.Connect(ctx, "127.0.0.1:9898")
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	// Create two streams
	for _, name := range []string{"orders", "payments"} {
		_, err = client.CreateStream(ctx, name, arbitro.StreamConfig{
			SubjectFilter: name + ".>",
			MaxMsgs:       100_000,
			Journal:       arbitro.JournalTolerant,
		})
		if err != nil && !arbitro.IsAlreadyExists(err) {
			log.Fatal(err)
		}
	}

	// Batch publish to orders
	entries := []arbitro.BatchEntry{
		{Subject: "orders.a", Payload: []byte(`{"id":"a"}`)},
		{Subject: "orders.b", Payload: []byte(`{"id":"b"}`)},
		{Subject: "orders.c", Payload: []byte(`{"id":"c"}`)},
	}
	firstSeq, err := client.PublishBatch(ctx, "orders", entries)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("batch published, first_seq=%d\n", firstSeq)

	// Subscribe to both streams
	ordersSub, err := client.Subscribe(ctx, "orders", arbitro.ConsumerConfig{
		Name: "batch-demo", Filter: "orders.>",
		AckPolicy: arbitro.AckExplicit, MaxInflight: 100, AckWait: 10 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}

	paymentsSub, err := client.Subscribe(ctx, "payments", arbitro.ConsumerConfig{
		Name: "batch-demo", Filter: "payments.>",
		AckPolicy: arbitro.AckExplicit, MaxInflight: 100, AckWait: 10 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}

	// Go advantage: multi-stream fan-in with select
	for i := 0; i < 5; i++ {
		select {
		case msg := <-ordersSub.Messages():
			fmt.Printf("[orders] %s: %s\n", msg.Subject(), msg.Data())
			msg.Ack()
		case msg := <-paymentsSub.Messages():
			fmt.Printf("[payments] %s: %s\n", msg.Subject(), msg.Data())
			msg.Ack()
		case <-ctx.Done():
			return
		case <-time.After(3 * time.Second):
			fmt.Println("no more messages")
			return
		}
	}
}
