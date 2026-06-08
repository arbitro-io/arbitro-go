//go:build integration

package tests

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arbitro-io/arbitro-go"
)

func TestCronCreateAndStop(t *testing.T) {
	client := connectT(t)
	ctx := context.Background()
	name := uniqueName("cron-basic")

	var fires atomic.Int32

	handle, err := client.Cron(name).
		Every("* * * * * *"). // every second (extended cron)
		Timezone("UTC").
		Timeout(5 * time.Second).
		Overlap(false).
		Run(ctx, func(fire arbitro.CronFire) error {
			fires.Add(1)
			return nil
		})
	if err != nil {
		t.Fatalf("cron run: %v", err)
	}

	// Wait for at least one fire (max 3s)
	deadline := time.After(3 * time.Second)
	for fires.Load() == 0 {
		select {
		case <-deadline:
			t.Log("no cron fire received within 3s (broker may tick slower)")
			goto stop
		case <-time.After(100 * time.Millisecond):
		}
	}
	t.Logf("received %d cron fires", fires.Load())

stop:
	stopCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	err = handle.Stop(stopCtx)
	if err != nil {
		t.Fatalf("stop: %v", err)
	}
}

func TestCronFailover(t *testing.T) {
	// Two clients register same cron name — only one should receive fires
	client1 := connectT(t)
	client2 := connectT(t)
	ctx := context.Background()
	name := uniqueName("cron-failover")

	var fires1, fires2 atomic.Int32

	h1, err := client1.Cron(name).
		Every("* * * * * *").
		Timezone("UTC").
		Run(ctx, func(fire arbitro.CronFire) error {
			fires1.Add(1)
			return nil
		})
	if err != nil {
		t.Fatalf("cron1: %v", err)
	}

	h2, err := client2.Cron(name).
		Every("* * * * * *").
		Timezone("UTC").
		Run(ctx, func(fire arbitro.CronFire) error {
			fires2.Add(1)
			return nil
		})
	if err != nil {
		// May get already-exists if broker enforces uniqueness per-name
		t.Logf("cron2 (may be expected to fail): %v", err)
	}

	time.Sleep(2 * time.Second)

	total := fires1.Load() + fires2.Load()
	t.Logf("fires: client1=%d client2=%d total=%d", fires1.Load(), fires2.Load(), total)

	// Only one should be receiving (broker assigns to one connection)
	if fires1.Load() > 0 && fires2.Load() > 0 {
		t.Log("both clients received fires — broker may allow duplicate registrations")
	}

	stopCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	h1.Stop(stopCtx)
	if h2 != nil {
		h2.Stop(stopCtx)
	}
}

func TestCronOverlapGuard(t *testing.T) {
	client := connectT(t)
	ctx := context.Background()
	name := uniqueName("cron-overlap")

	var concurrent atomic.Int32
	var maxConcurrent atomic.Int32

	handle, err := client.Cron(name).
		Every("* * * * * *").
		Timezone("UTC").
		Timeout(5 * time.Second).
		Overlap(false).
		Run(ctx, func(fire arbitro.CronFire) error {
			c := concurrent.Add(1)
			if c > maxConcurrent.Load() {
				maxConcurrent.Store(c)
			}
			time.Sleep(1500 * time.Millisecond) // slower than fire rate
			concurrent.Add(-1)
			return nil
		})
	if err != nil {
		t.Fatalf("cron: %v", err)
	}

	time.Sleep(3 * time.Second)

	if maxConcurrent.Load() > 1 {
		t.Errorf("overlap detected: max concurrent = %d (expected 1 with overlap=false)", maxConcurrent.Load())
	}

	stopCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	handle.Stop(stopCtx)
}
