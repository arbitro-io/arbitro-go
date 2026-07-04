// Example: request/reply RPC pattern using the Service abstraction.
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

	// Build a service — creates backing stream + consumer automatically
	svc, err := client.Service("calculator").Build(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer svc.Close()

	// Register handlers
	svc.Handle("add", func(msg *arbitro.Msg) {
		result := fmt.Sprintf("result: %s + ok", msg.Data())
		msg.Reply([]byte(result))
		msg.Ack()
	})

	// Make a request to ourselves (same service)
	resp, err := svc.Request(ctx, "calculator", "add", []byte("1+2"), 5*time.Second)
	if err != nil {
		fmt.Printf("request error: %v\n", err)
	} else {
		fmt.Printf("response: %s\n", resp)
	}
}
