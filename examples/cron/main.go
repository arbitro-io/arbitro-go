// Example: cron job scheduling with Arbitro.
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

	// Register a cron job that fires every minute
	handle, err := client.Cron("daily-report").
		Every("* * * * *"). // every minute for demo
		Timezone("America/New_York").
		Timeout(60 * time.Second).
		Overlap(false).
		Run(ctx, func(fire arbitro.CronFire) error {
			fmt.Printf("cron fired: name=%s time=%s index=%d\n",
				fire.Name, fire.Time.Format(time.RFC3339), fire.Index)
			// Simulate work
			time.Sleep(100 * time.Millisecond)
			return nil
		})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("cron registered, waiting for fires... (Ctrl+C to stop)")

	// Wait for interrupt
	<-ctx.Done()

	// Graceful shutdown
	shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := handle.Stop(shutCtx); err != nil {
		log.Printf("stop cron: %v", err)
	}
	fmt.Println("cron stopped")
}
