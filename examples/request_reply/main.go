// Example: request/reply RPC pattern.
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

	_, err = client.CreateStream(ctx, "services", arbitro.StreamConfig{
		SubjectFilter: "services.>",
		MaxMsgs:       100_000,
		Journal:       arbitro.JournalTolerant,
	})
	if err != nil && !arbitro.IsAlreadyExists(err) {
		log.Fatal(err)
	}

	// Request/Reply — blocks in goroutine, no callback hell
	response, err := client.Request(ctx, "services", "services.validate", []byte(`{"order":"123"}`), 5*time.Second)
	if err != nil {
		fmt.Printf("request error (expected if no responder): %v\n", err)
	} else {
		fmt.Printf("response: %s\n", response)
	}

	// Fire-and-forget with PublishAsync (Go advantage: no Future/Promise needed)
	client.PublishAsync("services", "services.audit", []byte(`{"action":"validate","order":"123"}`))
	fmt.Println("async audit event sent")

	// Delayed publish — broker delivers after duration
	err = client.PublishDelayed(ctx, "services", "services.reminder", []byte(`{"msg":"follow up"}`), 5*time.Second)
	if err != nil {
		fmt.Printf("delayed publish: %v\n", err)
	} else {
		fmt.Println("delayed message scheduled for 5s from now")
	}
}
