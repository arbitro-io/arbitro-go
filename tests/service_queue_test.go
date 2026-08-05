//go:build integration

package tests

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arbitro-io/arbitro-go"
)

// Integration test -- needs a live broker (see helpers_test.go).
//
// A service's worker consumers form a QUEUE: N instances of the same service
// share the request load and each request is handled exactly once. Two
// independent defects break that, and this file asserts against both.
//
//  1. Shared consumer NAME. Every instance created the worker consumer under
//     "_svc-<name>-worker", so they all collapsed onto one broker consumer id
//     and therefore one subscription id. The broker allows a single binding per
//     subscription, so the last instance to subscribe RETIRED its sibling's
//     binding and silently took 100% of the traffic. Caught by the spread
//     assertion (one instance handles 0).
//
//  2. Fanout deliver_mode. deliver_mode 0 makes the broker force QueueId(0) and
//     discard the group, so every instance gets its own full copy of the stream
//     and each request runs twice. Caught by the exactly-once assertion (total
//     comes back as 2N).
//
// The two assertions are not redundant: a total-only test passes while one
// instance sits offline, and a spread-only test passes while every request is
// duplicated.

// svcSeq keeps names unique even when two are minted inside the same
// millisecond, which uniqueName's timestamp alone does not guarantee.
var svcSeq atomic.Uint64

func uniqueSvcName(prefix string) string {
	return fmt.Sprintf("%s-%d-%d", prefix, time.Now().UnixNano(), svcSeq.Add(1))
}

// handledLog records what a service instance actually handled. Handlers run in
// their own goroutines, so the slice needs a lock.
type handledLog struct {
	mu   sync.Mutex
	seen []string
}

func (h *handledLog) record(s string) {
	h.mu.Lock()
	h.seen = append(h.seen, s)
	h.mu.Unlock()
}

func (h *handledLog) snapshot() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.seen...)
}

// buildServiceInstance starts one instance of svcName on its own connection,
// with a "work" handler that logs the request and replies "<tag>:<body>".
func buildServiceInstance(
	t *testing.T,
	ctx context.Context,
	client *arbitro.Client,
	svcName, tag string,
	log *handledLog,
) *arbitro.Service {
	t.Helper()
	svc, err := client.Service(svcName).Build(ctx)
	if err != nil {
		t.Fatalf("build service instance %s: %v", tag, err)
	}
	svc.Handle("work", func(req *arbitro.Request) ([]byte, error) {
		body := string(req.Data())
		log.record(body)
		return []byte(tag + ":" + body), nil
	})
	t.Cleanup(func() { svc.Close() })
	return svc
}

// Two instances of the same service, requests issued from a third connection.
// Every request must be handled exactly once, and both instances must do work.
func TestServiceWorkersShareLoadAcrossInstances(t *testing.T) {
	ctx := context.Background()

	connA := connectT(t)
	connB := connectT(t)
	connCaller := connectT(t)

	svcName := uniqueSvcName("svcq")
	callerName := uniqueSvcName("svcc")

	var logA, logB handledLog
	buildServiceInstance(t, ctx, connA, svcName, "A", &logA)
	buildServiceInstance(t, ctx, connB, svcName, "B", &logB)
	t.Cleanup(func() { connA.DeleteStream(context.Background(), "_svc-"+svcName) })

	caller, err := connCaller.Service(callerName).Build(ctx)
	if err != nil {
		t.Fatalf("build caller service: %v", err)
	}
	t.Cleanup(func() {
		caller.Close()
		connCaller.DeleteStream(context.Background(), "_svc-"+callerName)
	})

	// Both worker subscriptions must be live before the first publish. An early
	// request that lands while only one instance is bound makes the split look
	// lopsided for reasons that have nothing to do with the queue.
	time.Sleep(300 * time.Millisecond)

	const total = 20
	replies := make([]string, 0, total)
	for i := 0; i < total; i++ {
		body := fmt.Sprintf("m%d", i)
		rep, err := caller.Request(ctx, svcName, "work", []byte(body), 5*time.Second)
		if err != nil {
			t.Fatalf("request %d (%s): %v", i, body, err)
		}
		replies = append(replies, string(rep))
	}

	// A duplicate delivery races the reply the caller already accepted, so give
	// the second copy time to land before counting. Without this pause a fanout
	// regression can slip through as a green run.
	time.Sleep(1500 * time.Millisecond)

	seenA := logA.snapshot()
	seenB := logB.snapshot()

	// EXACTLY-ONCE: the two instances together saw every request, no dupes and
	// nothing dropped.
	handled := append(append([]string(nil), seenA...), seenB...)
	sort.Strings(handled)
	want := make([]string, 0, total)
	for i := 0; i < total; i++ {
		want = append(want, fmt.Sprintf("m%d", i))
	}
	sort.Strings(want)

	if len(handled) != total {
		t.Fatalf("instances handled %d requests in total (A=%d, B=%d), want exactly %d; "+
			"more means the worker consumers fanned out instead of sharing a queue, "+
			"fewer means work was lost\nA saw: %v\nB saw: %v",
			len(handled), len(seenA), len(seenB), total, seenA, seenB)
	}
	for i := range want {
		if handled[i] != want[i] {
			t.Fatalf("handled set mismatch at %d: got %q, want %q\nfull handled: %v",
				i, handled[i], want[i], handled)
		}
	}

	// SPREAD: neither instance was starved. Asserted as "both did real work"
	// rather than an exact split -- the broker's drain rotates its match set, so
	// the balance is even in practice but is not a contract worth pinning to.
	// A zero here is the binding-takeover defect: one instance went offline the
	// moment its sibling subscribed under the same consumer name.
	if len(seenA) == 0 || len(seenB) == 0 {
		t.Fatalf("load was not shared: A handled %d, B handled %d of %d; "+
			"a zero means that instance never received anything, which is what "+
			"happens when both instances collapse onto one consumer name and the "+
			"broker retires the earlier binding", len(seenA), len(seenB), total)
	}

	// Replies came back to the caller in order, each tagged by whichever
	// instance ran it. This is what proves the reply consumer stayed
	// per-instance instead of being load-balanced to a sibling.
	if len(replies) != total {
		t.Fatalf("got %d replies, want %d", len(replies), total)
	}
	for i, rep := range replies {
		body := fmt.Sprintf("m%d", i)
		if rep != "A:"+body && rep != "B:"+body {
			t.Fatalf("reply %d was %q, want \"A:%s\" or \"B:%s\"", i, rep, body, body)
		}
	}

	t.Logf("split across instances: A=%d B=%d total=%d", len(seenA), len(seenB), total)
}
