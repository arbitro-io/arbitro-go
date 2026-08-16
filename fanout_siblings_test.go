package arbitro

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

// One consumer, several filtered subscriptions — same connection and across
// connections. Plus: may two consumers share a filter, or a name?
//
// Ported from the Rust suite, which is the source of truth for what the
// broker actually does: arbitro-e2e/tests/one_consumer_many_filters.rs. Same
// fixture, same shapes, same expected counts, so a divergence between the
// clients shows up as a disagreement between two suites rather than as a
// silence in one of them.
//
// The broker sends ONE wire copy per (connection, consumer) and stamps it
// with whichever subscription won the round; the client hands it to every
// sibling whose filter accepts the subject. So per consumer the wire carries
// 6 copies and the handles see 11 deliveries — the gap is what the collapse
// buys, measured rather than argued.
//
// Skipped unless a broker is reachable; point it elsewhere with ARBITRO_ADDR.

const (
	fanOrders   = 3
	fanPayments = 2
	fanAudit    = 1
	fanTotal    = fanOrders + fanPayments + fanAudit
	// Per consumer: its own copy for orders.*, for payments.*, and the
	// catch-all sees everything.
	fanPerConsumer = fanOrders + fanPayments + fanTotal
)

// unique keeps parallel and repeated local runs from colliding on durable
// stream and consumer names.
func unique(prefix string) string {
	return fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
}

// wideConsumer is the shared shape: one consumer, wide open, fanout. Every
// narrowing is subscription-side.
func wideConsumer(name string) ConsumerConfig {
	return ConsumerConfig{
		Name:        name,
		Fanout:      true,
		AckPolicy:   1, // Explicit
		MaxInflight: 1000,
		AckWait:     30 * time.Second,
	}
}

// setupStream creates the stream and registers its deletion, so a failing
// assert can never leave it behind.
//
// The stream owns `<name>.>` rather than a global slice. Rust can afford
// `*.>` because every test there spawns its own broker; these share one, and
// two streams may not claim overlapping subjects — a second `*.>` comes back
// as `StreamAlreadyExists` even under a fresh name. Scoping by name is also
// what the project requires of any stream.
func setupStream(t *testing.T, c *Client, ctx context.Context, name string) {
	t.Helper()
	if _, err := c.CreateStream(ctx, name, StreamConfig{SubjectFilter: name + ".>"}); err != nil {
		t.Fatalf("create stream %s: %v", name, err)
	}
	// Cleanup dials its own connection: `t.Cleanup` runs after the test's
	// defers, so every client the test held is already closed by then and a
	// delete on `c` would fail silently, leaving the stream behind.
	t.Cleanup(func() {
		cctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		admin, err := Connect(cctx, replayAddr())
		if err != nil {
			t.Errorf("cleanup could not dial to delete %s: %v", name, err)
			return
		}
		defer admin.Close()
		if err := admin.DeleteStream(cctx, name); err != nil {
			t.Errorf("cleanup failed to delete %s: %v", name, err)
		}
	})
}

// publishFixture writes the six messages every case reads back.
func publishFixture(t *testing.T, c *Client, ctx context.Context, stream string) {
	t.Helper()
	for i := 0; i < fanOrders; i++ {
		if err := c.PublishWait(ctx, stream, fmt.Sprintf("%s.orders.%d", stream, i), []byte("o")); err != nil {
			t.Fatalf("publish order: %v", err)
		}
	}
	for i := 0; i < fanPayments; i++ {
		if err := c.PublishWait(ctx, stream, fmt.Sprintf("%s.payments.%d", stream, i), []byte("p")); err != nil {
			t.Fatalf("publish payment: %v", err)
		}
	}
	// Matches the consumer's wide filter and only the widest subscription.
	// Handed to a narrow one, it would be stranded.
	if err := c.PublishWait(ctx, stream, stream+".audit.trail", []byte("x")); err != nil {
		t.Fatalf("publish audit: %v", err)
	}
}

// drain reads until the subscription goes quiet, acking as it goes.
func drain(sub *Subscription) []string {
	var out []string
	for {
		select {
		case m, ok := <-sub.Messages():
			if !ok {
				return out
			}
			out = append(out, m.Subject())
			m.Ack()
		case <-time.After(400 * time.Millisecond):
			return out
		}
	}
}

// subscribeTrio opens the three filtered subscriptions every case uses.
func subscribeTrio(t *testing.T, c *Client, ctx context.Context, stream string, cfg ConsumerConfig) (*Subscription, *Subscription, *Subscription) {
	t.Helper()
	orders, err := c.Subscribe(ctx, stream, cfg, withSubFilter(stream+".orders.*"))
	if err != nil {
		t.Fatalf("subscribe orders: %v", err)
	}
	pay, err := c.Subscribe(ctx, stream, cfg, withSubFilter(stream+".payments.*"))
	if err != nil {
		t.Fatalf("subscribe payments: %v", err)
	}
	all, err := c.Subscribe(ctx, stream, cfg, withSubFilter(stream+".>"))
	if err != nil {
		t.Fatalf("subscribe catch-all: %v", err)
	}
	return orders, pay, all
}

// assertOutcome is the Rust `assert_outcome`, verbatim in intent.
func assertOutcome(t *testing.T, label string, orders, pay, all []string) {
	t.Helper()

	// 1. Nobody may receive what its own filter excludes. Subjects are scoped
	//    under the stream name, so the discriminating token sits in the middle.
	for _, s := range orders {
		if !strings.Contains(s, ".orders.") {
			t.Errorf("[%s] orders subscription got a foreign subject: %v", label, orders)
			break
		}
	}
	for _, s := range pay {
		if !strings.Contains(s, ".payments.") {
			t.Errorf("[%s] payments subscription got a foreign subject: %v", label, pay)
			break
		}
	}

	// 2. Fanout: each subscription sees every message its filter accepts, so
	//    the counts are exact and independent — a short count is a lost
	//    message, not a split.
	if len(orders) != fanOrders {
		t.Errorf("[%s] orders.* saw %d of %d: %v", label, len(orders), fanOrders, orders)
	}
	if len(pay) != fanPayments {
		t.Errorf("[%s] payments.* saw %d of %d: %v", label, len(pay), fanPayments, pay)
	}
	if len(all) != fanTotal {
		t.Errorf("[%s] the catch-all saw %d of %d: %v", label, len(all), fanTotal, all)
	}
}

// TestFilteredSubsOnOneConsumerSameConnection — one connection, one consumer,
// three filters. Before routing was keyed by subscription this was
// unreachable from Go: the dispatch table was keyed by consumer, so the
// second subscribe overwrote the first.
func TestFilteredSubsOnOneConsumerSameConnection(t *testing.T) {
	c := dialOrSkip(t)
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	stream := unique("triage_same")
	setupStream(t, c, ctx, stream)
	cfg := wideConsumer(unique("worker_same"))

	orders, pay, all := subscribeTrio(t, c, ctx, stream, cfg)
	defer orders.Close()
	defer pay.Close()
	defer all.Close()

	// Distinct, non-zero ids are the premise: zero is the broker's "unnamed"
	// sentinel and identical ids would leave the siblings indistinguishable.
	if orders.ID() == pay.ID() || pay.ID() == all.ID() || orders.ID() == all.ID() {
		t.Fatalf("sibling subscriptions share an id: %d %d %d", orders.ID(), pay.ID(), all.ID())
	}
	if orders.ID() == 0 {
		t.Fatalf("the allocator handed out the unnamed sentinel")
	}

	publishFixture(t, c, ctx, stream)
	assertOutcome(t, "same connection", drain(orders), drain(pay), drain(all))
}

// TestFilteredSubsOnOneConsumerAcrossConnections — three CONNECTIONS, one
// consumer, one filter each. The collapse is keyed by (connection, consumer),
// so each connection is owed its own copy.
func TestFilteredSubsOnOneConsumerAcrossConnections(t *testing.T) {
	admin := dialOrSkip(t)
	defer admin.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	stream := unique("triage_across")
	setupStream(t, admin, ctx, stream)
	cfg := wideConsumer(unique("worker_across"))

	c1, c2, c3 := dialOrSkip(t), dialOrSkip(t), dialOrSkip(t)
	defer c1.Close()
	defer c2.Close()
	defer c3.Close()

	orders, err := c1.Subscribe(ctx, stream, cfg, withSubFilter(stream+".orders.*"))
	if err != nil {
		t.Fatalf("subscribe orders: %v", err)
	}
	defer orders.Close()
	pay, err := c2.Subscribe(ctx, stream, cfg, withSubFilter(stream+".payments.*"))
	if err != nil {
		t.Fatalf("subscribe payments: %v", err)
	}
	defer pay.Close()
	all, err := c3.Subscribe(ctx, stream, cfg, withSubFilter(stream+".>"))
	if err != nil {
		t.Fatalf("subscribe catch-all: %v", err)
	}
	defer all.Close()

	publishFixture(t, admin, ctx, stream)
	assertOutcome(t, "across connections", drain(orders), drain(pay), drain(all))
}

// TestFilteredSubsOnFourConnections — the same arrangement four times over.
// Proves the two halves compose: collapsing per (connection, consumer) really
// is per connection, and four clients reading one consumer neither steal from
// nor duplicate each other.
//
// Two ways this fails, meaning opposite things: a row of [0 0 6] is that
// connection's client merging its three handles into one; rows that disagree
// with each other would be the broker singling out a connection.
func TestFilteredSubsOnFourConnections(t *testing.T) {
	admin := dialOrSkip(t)
	defer admin.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	stream := unique("triage_quad")
	setupStream(t, admin, ctx, stream)
	cfg := wideConsumer(unique("worker_quad"))

	const conns = 4
	type row struct{ orders, pay, all *Subscription }
	rows := make([]row, 0, conns)
	for i := 0; i < conns; i++ {
		c := dialOrSkip(t)
		defer c.Close()
		// Same three filters on every connection — nothing distinguishes
		// them but the socket, which is the variable under test.
		o, p, a := subscribeTrio(t, c, ctx, stream, cfg)
		defer o.Close()
		defer p.Close()
		defer a.Close()
		rows = append(rows, row{o, p, a})
	}

	publishFixture(t, admin, ctx, stream)

	shape := make([][3]int, 0, conns)
	for i, r := range rows {
		gotO, gotP, gotA := drain(r.orders), drain(r.pay), drain(r.all)
		label := fmt.Sprintf("four conns/%d", i)
		assertOutcome(t, label, gotO, gotP, gotA)
		if total := len(gotO) + len(gotP) + len(gotA); total != fanPerConsumer {
			t.Errorf("[%s] made %d deliveries, expected %d", label, total, fanPerConsumer)
		}
		// A connection that funnelled its three subscriptions into one handle.
		if len(gotO) == 0 || len(gotP) == 0 {
			t.Errorf("[%s] funnelled its subscriptions into one handle: %d/%d/%d",
				label, len(gotO), len(gotP), len(gotA))
		}
		shape = append(shape, [3]int{len(gotO), len(gotP), len(gotA)})
	}

	// The rows must be identical. One that differs means the broker treated a
	// connection differently — a distribution bug, not a client one.
	for i := 1; i < len(shape); i++ {
		if shape[i] != shape[0] {
			t.Errorf("connections disagree: %v — under fanout every connection is owed the same fixture", shape)
			break
		}
	}
}

// TestNineConsumersEachWithThreeFilteredSubscriptions — both shapes at once:
// three connections × three CONSUMERS × three filtered SUBSCRIPTIONS.
//
// Consumers are independent (separate cursors, no collapse between them);
// subscriptions of one consumer are not. So per connection the wire carries
// 3 × 6 = 18 copies and the handles see 3 × 11 = 33 deliveries.
func TestNineConsumersEachWithThreeFilteredSubscriptions(t *testing.T) {
	admin := dialOrSkip(t)
	defer admin.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	stream := unique("triage_nine")
	setupStream(t, admin, ctx, stream)

	const conns, perConn = 3, 3
	type row struct{ orders, pay, all *Subscription }
	rows := make([]row, 0, conns*perConn)
	for ci := 0; ci < conns; ci++ {
		c := dialOrSkip(t)
		defer c.Close()
		for si := 0; si < perConn; si++ {
			cfg := wideConsumer(unique(fmt.Sprintf("w_%d_%d", ci, si)))
			o, p, a := subscribeTrio(t, c, ctx, stream, cfg)
			defer o.Close()
			defer p.Close()
			defer a.Close()
			rows = append(rows, row{o, p, a})
		}
	}

	publishFixture(t, admin, ctx, stream)

	total := 0
	for i, r := range rows {
		gotO, gotP, gotA := drain(r.orders), drain(r.pay), drain(r.all)
		label := fmt.Sprintf("nine/%d", i)
		assertOutcome(t, label, gotO, gotP, gotA)
		n := len(gotO) + len(gotP) + len(gotA)
		if n != fanPerConsumer {
			t.Errorf("[%s] %d deliveries, expected %d", label, n, fanPerConsumer)
		}
		total += n
	}

	// Nine independent consumers: nothing shared, nobody short of a full copy.
	if want := conns * perConn * fanPerConsumer; total != want {
		t.Errorf("%d deliveries across the nine consumers, expected %d", total, want)
	}
}

// TestTwoConsumersMaySharedOneFilter — two consumers, same stream, SAME
// filter, different names.
//
// Streams may not have overlapping filters. Nothing says the same about
// consumers, and the natural reading is that they are independent readers of
// one log. This pins whether the broker agrees.
func TestTwoConsumersMaySharedOneFilter(t *testing.T) {
	admin := dialOrSkip(t)
	defer admin.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	stream := unique("shared_filter")
	setupStream(t, admin, ctx, stream)

	mk := func(name string) ConsumerConfig {
		return ConsumerConfig{
			Name: name, Filter: stream + ".orders.>", AckPolicy: 1,
			MaxInflight: 100, AckWait: 30 * time.Second,
		}
	}

	firstCfg := mk(unique("reader_one"))
	firstID, err := admin.CreateConsumer(ctx, stream, firstCfg)
	if err != nil {
		t.Fatalf("first consumer with orders.> filter: %v", err)
	}
	secondCfg := mk(unique("reader_two"))
	secondID, err := admin.CreateConsumer(ctx, stream, secondCfg)
	if err != nil {
		t.Fatalf("the broker refused a second consumer with filter orders.>: %v — "+
			"if that is intentional it belongs in the docs; today only STREAM "+
			"filters are documented as non-overlapping", err)
	}
	if firstID == secondID {
		t.Fatalf("two consumers sharing a filter collapsed onto one id: %d", firstID)
	}

	// Independent readers: each should see every matching message.
	c1, c2 := dialOrSkip(t), dialOrSkip(t)
	defer c1.Close()
	defer c2.Close()
	// One subscription per CONSUMER. Both on the same consumer would be a work
	// queue splitting the three messages, which is not what this pins.
	s1, err := c1.Subscribe(ctx, stream, firstCfg)
	if err != nil {
		t.Fatalf("subscribe reader_one: %v", err)
	}
	defer s1.Close()
	s2, err := c2.Subscribe(ctx, stream, secondCfg)
	if err != nil {
		t.Fatalf("subscribe reader_two: %v", err)
	}
	defer s2.Close()

	for i := 0; i < 3; i++ {
		if err := admin.PublishWait(ctx, stream, fmt.Sprintf("%s.orders.%d", stream, i), []byte("o")); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}

	got1, got2 := drain(s1), drain(s2)
	if len(got1) != 3 {
		t.Errorf("reader_one missed messages: %v — two consumers on one filter are not independent", got1)
	}
	if len(got2) != 3 {
		t.Errorf("reader_two missed messages: %v — the second consumer is starved by the first", got2)
	}
}

// TestTwoConsumersWithTheSameName — the same NAME, twice, on one stream.
//
// Consumer ids are namespaced by (stream, name), so a repeat of the same name
// must resolve to the SAME consumer. And a conflicting config must be
// refused, not silently ignored: a no-op is worse than an error, because the
// config the caller believes in never took effect.
func TestTwoConsumersWithTheSameName(t *testing.T) {
	admin := dialOrSkip(t)
	defer admin.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	stream := unique("same_name")
	setupStream(t, admin, ctx, stream)
	name := unique("duplicate")

	base := ConsumerConfig{
		Name: name, Filter: stream + ".orders.>", AckPolicy: 1,
		MaxInflight: 100, AckWait: 30 * time.Second,
	}

	first, err := admin.CreateConsumer(ctx, stream, base)
	if err != nil {
		t.Fatalf("first consumer: %v", err)
	}

	// Identical config. If names are keys, this is an idempotent re-create.
	same, err := admin.CreateConsumer(ctx, stream, base)
	if err == nil && same != first {
		t.Errorf("the same name with the same config produced a SECOND consumer (%d != %d) — "+
			"ids are supposed to be namespaced by (stream, name)", same, first)
	}

	// A different filter under the same name. If this is accepted AND returns
	// the first id, the second call's config is silently discarded.
	other := base
	other.Filter = stream + ".telemetry.>"
	other.MaxInflight = 999
	if id, err := admin.CreateConsumer(ctx, stream, other); err == nil {
		t.Errorf("the same name with a DIFFERENT filter was accepted and returned id=%d (first=%d) — "+
			"the caller asked for telemetry.> and max_inflight=999 and got neither", id, first)
	}
}
