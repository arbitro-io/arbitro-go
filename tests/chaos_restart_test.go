//go:build integration

package tests

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/arbitro-io/arbitro-go"
)

// Restart chaos, the invariant the other three clients already pin:
//
//	every message the broker ACKED is eventually delivered.
//
// Duplicates are fine and expected -- a redelivery after reconnect is the
// queue doing its job. Loss is not. Publishes that fail while the broker is
// down are not losses either: they were never confirmed, so the caller knows
// they did not land.
//
// The broker is restarted mid-run. Two ways to trigger that:
//
//	ARBITRO_CHAOS_CONTAINER=<name>  -- the test runs `docker restart <name>`
//	                                   itself (mirrors the TS bench)
//	ARBITRO_CHAOS_EXTERNAL=1        -- an operator restarts it during the
//	                                   run (mirrors the C bench), useful
//	                                   where the docker socket is not
//	                                   reachable from the test host
//
// With neither set the test skips rather than passing vacuously: a chaos test
// that never injects chaos proves nothing, and silently "passing" would be
// worse than not having it.

const (
	chaosRunSecs   = 10
	chaosRateMS    = 20 // ~50 msg/s
	chaosRestartAt = 4 * time.Second
)

func restartBroker(t *testing.T, container string) {
	t.Helper()
	cmd := exec.Command("docker", "restart", container)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("docker restart %s: %v (%s)", container, err, out)
	}
}

func TestChaosBrokerRestartNoMessageLoss(t *testing.T) {
	container := os.Getenv("ARBITRO_CHAOS_CONTAINER")
	external := os.Getenv("ARBITRO_CHAOS_EXTERNAL") != ""
	if container == "" && !external {
		t.Skip("set ARBITRO_CHAOS_CONTAINER=<name> (self-driving) or " +
			"ARBITRO_CHAOS_EXTERNAL=1 (operator restarts the broker) -- " +
			"refusing to pass without actually injecting a restart")
	}

	ctx := context.Background()
	admin := connectT(t)
	stream := uniqueName("chaos-restart")

	_, err := admin.CreateStream(ctx, stream, arbitro.StreamConfig{
		SubjectFilter: stream + ".>",
		MaxMsgs:       100000,
		Journal:       arbitro.JournalTolerant,
	})
	if err != nil {
		t.Fatalf("create stream: %v", err)
	}
	t.Cleanup(func() { admin.DeleteStream(context.Background(), stream) })

	// Consumer reconnects on its own; that is the client behaviour under test.
	worker := connectT(t)
	sub, err := worker.QueueSubscribe(ctx, stream, arbitro.QueueOptions{
		Group:   "chaos-workers",
		AckWait: 10 * time.Second,
	})
	if err != nil {
		t.Fatalf("queue subscribe: %v", err)
	}
	defer sub.Close()

	var mu sync.Mutex
	received := make(map[string]int)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for msg := range sub.Messages() {
			mu.Lock()
			received[string(msg.Data())]++
			mu.Unlock()
			msg.Ack()
		}
	}()

	acked := make([]string, 0, 512)
	publishErrs := 0
	restarted := false

	start := time.Now()
	for i := 0; time.Since(start) < chaosRunSecs*time.Second; i++ {
		if !restarted && time.Since(start) >= chaosRestartAt {
			restarted = true
			if container != "" {
				t.Logf("t=%.1fs: restarting container %s", time.Since(start).Seconds(), container)
				restartBroker(t, container)
			} else {
				t.Logf("t=%.1fs: waiting for the operator's restart", time.Since(start).Seconds())
			}
		}

		payload := fmt.Sprintf("m-%d", i)
		pctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		err := admin.Publish(pctx, stream, stream+".job", []byte(payload))
		cancel()
		if err != nil {
			// Broker down: never confirmed, so never owed to us.
			publishErrs++
		} else {
			acked = append(acked, payload)
		}
		time.Sleep(chaosRateMS * time.Millisecond)
	}

	// Let redelivery after the reconnect settle before judging.
	deadline := time.After(15 * time.Second)
	for {
		mu.Lock()
		got := len(received)
		mu.Unlock()
		if got >= len(acked) {
			break
		}
		select {
		case <-deadline:
			goto check
		case <-time.After(200 * time.Millisecond):
		}
	}

check:
	sub.Close()
	<-done

	mu.Lock()
	defer mu.Unlock()

	missing := make([]string, 0)
	dupes := 0
	for _, p := range acked {
		n, ok := received[p]
		if !ok {
			missing = append(missing, p)
		} else if n > 1 {
			dupes += n - 1
		}
	}

	t.Logf("published (acked): %d  publish errors: %d (broker down)  "+
		"received unique: %d  duplicates: %d  restarted: %v",
		len(acked), publishErrs, len(received), dupes, restarted)

	if !restarted {
		t.Fatal("no restart was injected -- the run proves nothing")
	}
	if len(acked) == 0 {
		t.Fatal("nothing was acked; the broker was never reachable")
	}
	if len(missing) > 0 {
		show := missing
		if len(show) > 10 {
			show = show[:10]
		}
		t.Fatalf("LOSS: %d of %d acked messages never arrived (first few: %v) -- "+
			"the broker confirmed them, so they must survive the restart",
			len(missing), len(acked), show)
	}
}
