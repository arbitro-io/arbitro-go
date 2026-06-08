// Example: workflow/saga pattern with compensation.
package main

import (
	"context"
	"encoding/json"
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

	// Create the trigger stream
	_, err = client.CreateStream(ctx, "orders", arbitro.StreamConfig{
		SubjectFilter: "orders.>",
		MaxMsgs:       100_000,
		Journal:       arbitro.JournalTolerant,
	})
	if err != nil && !arbitro.IsAlreadyExists(err) {
		log.Fatal(err)
	}

	// Build workflow with saga compensation
	wf, err := client.Workflow("order-process").
		Trigger("orders.created").
		TriggerStream("orders").
		Step("validate", func(step arbitro.StepContext) ([]byte, error) {
			fmt.Printf("  step:validate input=%s\n", step.Input)
			// Validate order
			return json.Marshal(map[string]string{"status": "valid"})
		}).
		Compensate("validate", func(step arbitro.StepContext) error {
			fmt.Println("  compensate:validate — rolling back validation")
			return nil
		}).
		Step("charge", func(step arbitro.StepContext) ([]byte, error) {
			fmt.Printf("  step:charge prev=%s\n", step.Input)
			// Charge payment
			return json.Marshal(map[string]string{"tx_id": "tx_abc123"})
		}).
		Compensate("charge", func(step arbitro.StepContext) error {
			fmt.Println("  compensate:charge — issuing refund")
			return nil
		}).
		Step("ship", func(step arbitro.StepContext) ([]byte, error) {
			fmt.Printf("  step:ship prev=%s\n", step.Input)
			// Ship order
			return json.Marshal(map[string]string{"tracking": "TRACK-999"})
		}).
		MaxRetries(3).
		AckWait(30 * time.Second).
		MaxInflight(10).
		Start(ctx)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("workflow started, publishing trigger...")

	// Publish a trigger message
	err = client.Publish(ctx, "orders", "orders.created", []byte(`{"order_id":"ORD-001","amount":99.99}`))
	if err != nil {
		log.Fatal(err)
	}

	// Wait a bit for processing
	time.Sleep(2 * time.Second)

	// Manual trigger
	instanceID, err := wf.Trigger(ctx, []byte(`{"order_id":"ORD-002","amount":149.50}`))
	if err != nil {
		log.Printf("manual trigger: %v", err)
	} else {
		fmt.Printf("triggered instance: %s\n", instanceID)
	}

	// Wait for interrupt
	<-ctx.Done()

	shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = wf.Stop(shutCtx)
	fmt.Println("workflow stopped")
}
