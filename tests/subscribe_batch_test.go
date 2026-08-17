//go:build integration

package tests

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/arbitro-io/arbitro-go"
)

// SubscribeBatch — N filtered subscriptions in ONE round-trip.
//
// The sibling fan-out is pinned elsewhere; what is new here is that opening
// the siblings in a single frame reaches the same place as opening them one
// at a time. The failure shape matters as much: a filter outside the
// consumer's slice must come back naming ITS index, and its peers must stay
// open — that is the whole point of a per-entry verdict.

const batchQuiet = 700 * time.Millisecond

func batchStream(t *testing.T, c *arbitro.Client, name string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := c.CreateStream(ctx, name, arbitro.StreamConfig{
		SubjectFilter: name + ".>",
	}); err != nil {
		t.Fatalf("create stream: %v", err)
	}
	t.Cleanup(func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer ccancel()
		_ = c.DeleteStream(cctx, name)
	})
}

func wideCfg(name, stream string) arbitro.ConsumerConfig {
	return arbitro.ConsumerConfig{
		Name: name, Group: name, Fanout: true,
		AckPolicy: arbitro.AckExplicit, MaxInflight: 1000,
		AckWait: 30 * time.Second, Filter: stream + ".>",
	}
}

// Three filtered siblings in one frame split exactly as three separate
// subscribes would.
func TestSubscribeBatchSplitsIdentically(t *testing.T) {
	c := connectT(t)
	stream := uniqueName("gobatch")
	batchStream(t, c, stream)

	var mu sync.Mutex
	orders, pay, all := []string{}, []string{}, []string{}
	collect := func(dst *[]string) func(*arbitro.Msg) {
		return func(m *arbitro.Msg) {
			mu.Lock()
			*dst = append(*dst, m.Subject())
			mu.Unlock()
			m.Ack()
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	subs, err := c.SubscribeBatch(ctx, stream, wideCfg(uniqueName("gow"), stream),
		[]arbitro.BatchSubscribeEntry{
			{Filter: stream + ".orders.*", Handler: collect(&orders)},
			{Filter: stream + ".payments.*", Handler: collect(&pay)},
			// No filter — inherits the consumer's.
			{Handler: collect(&all)},
		})
	if err != nil {
		t.Fatalf("subscribe batch: %v", err)
	}
	if len(subs) != 3 {
		t.Fatalf("want 3 subscriptions, got %d", len(subs))
	}
	// Distinct non-zero ids: zero is the broker's "unnamed" sentinel and
	// equal ids would leave the siblings indistinguishable on ack.
	seen := map[uint32]bool{}
	for _, s := range subs {
		if s.ID() == 0 || seen[s.ID()] {
			t.Fatalf("subscription ids collided or were zero: %d", s.ID())
		}
		seen[s.ID()] = true
	}

	for i := 0; i < 3; i++ {
		if err := c.PublishWait(ctx, stream, fmt.Sprintf("%s.orders.%d", stream, i), []byte("o")); err != nil {
			t.Fatalf("publish orders: %v", err)
		}
	}
	for i := 0; i < 2; i++ {
		if err := c.PublishWait(ctx, stream, fmt.Sprintf("%s.payments.%d", stream, i), []byte("p")); err != nil {
			t.Fatalf("publish payments: %v", err)
		}
	}
	if err := c.PublishWait(ctx, stream, stream+".audit.trail", []byte("x")); err != nil {
		t.Fatalf("publish audit: %v", err)
	}
	time.Sleep(batchQuiet)

	mu.Lock()
	defer mu.Unlock()
	if len(orders) != 3 {
		t.Errorf("orders.* saw %d of 3: %v", len(orders), orders)
	}
	if len(pay) != 2 {
		t.Errorf("payments.* saw %d of 2: %v", len(pay), pay)
	}
	if len(all) != 6 {
		t.Errorf("the inherited catch-all saw %d of 6: %v", len(all), all)
	}
}

// A filter outside the consumer's slice is refused ALONE, by index, and its
// legal peers stay open.
func TestSubscribeBatchNamesTheOffendingEntry(t *testing.T) {
	c := connectT(t)
	stream := uniqueName("gobadf")
	batchStream(t, c, stream)

	name := uniqueName("gonarrow")
	// Deliberately narrow: a payments.* sibling escapes orders.>.
	cfg := arbitro.ConsumerConfig{
		Name: name, Group: name, Fanout: true,
		AckPolicy: arbitro.AckExplicit, MaxInflight: 100,
		AckWait: 30 * time.Second, Filter: stream + ".orders.>",
	}
	noop := func(m *arbitro.Msg) { m.Ack() }

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := c.SubscribeBatch(ctx, stream, cfg, []arbitro.BatchSubscribeEntry{
		{Filter: stream + ".orders.a", Handler: noop},
		{Filter: stream + ".payments.*", Handler: noop},
		{Filter: stream + ".orders.b", Handler: noop},
	})

	var bErr *arbitro.SubscribeBatchError
	if !errors.As(err, &bErr) {
		t.Fatalf("a filter outside the consumer was accepted: err=%v", err)
	}
	if len(bErr.Failures) != 1 {
		t.Fatalf("want exactly one refusal, got %d: %+v", len(bErr.Failures), bErr.Failures)
	}
	if bErr.Failures[0].Index != 1 {
		t.Errorf("the refusal named entry %d, want 1", bErr.Failures[0].Index)
	}
	if bErr.Failures[0].Code != arbitro.ErrCodeInvalidSubscriptionFilter {
		t.Errorf("code 0x%04x, want 0x%04x", bErr.Failures[0].Code, arbitro.ErrCodeInvalidSubscriptionFilter)
	}
	// Per entry, not all-or-nothing.
	if len(bErr.Accepted) != 2 {
		t.Errorf("a single bad entry took the whole batch down: %d accepted", len(bErr.Accepted))
	}
	for _, s := range bErr.Accepted {
		s.Close()
	}
}

// A hundred at once — the scenario the batch exists for, and the only case
// that catches an id or route clobbered at scale.
func TestSubscribeBatchHundred(t *testing.T) {
	c := connectT(t)
	stream := uniqueName("gohund")
	batchStream(t, c, stream)

	const n = 100
	var mu sync.Mutex
	hits := make([]int, n)
	entries := make([]arbitro.BatchSubscribeEntry, n)
	for i := 0; i < n; i++ {
		i := i
		entries[i] = arbitro.BatchSubscribeEntry{
			Filter: fmt.Sprintf("%s.tenant%d.>", stream, i),
			Handler: func(m *arbitro.Msg) {
				mu.Lock()
				hits[i]++
				mu.Unlock()
				m.Ack()
			},
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	subs, err := c.SubscribeBatch(ctx, stream, wideCfg(uniqueName("gohw"), stream), entries)
	if err != nil {
		t.Fatalf("subscribe batch: %v", err)
	}
	if len(subs) != n {
		t.Fatalf("want %d subscriptions, got %d", n, len(subs))
	}

	for i := 0; i < n; i++ {
		if err := c.PublishWait(ctx, stream, fmt.Sprintf("%s.tenant%d.evt", stream, i), []byte("t")); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	time.Sleep(2 * time.Second)

	mu.Lock()
	defer mu.Unlock()
	for i, got := range hits {
		if got != 1 {
			t.Errorf("subscription %d saw %d messages, want 1", i, got)
		}
	}
}
