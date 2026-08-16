package arbitro

import "testing"

// The routing registry, without a broker.
//
// The Rust client is the contract: arbitro-client-tokio's
// state/subscriptions.rs. Both cases below are ports of what it already
// guarantees and Go did not.

// TestMatcherAgreesWithRust pins subject matching case by case against
// Matcher::new + subject_matches in the Rust client.
func TestMatcherAgreesWithRust(t *testing.T) {
	cases := []struct {
		filter, subject string
		want            bool
	}{
		{"", "anything", true},
		{">", "a.b.c", true},
		{"orders.created", "orders.created", true},
		{"orders.created", "orders.updated", false},
		{"orders.*", "orders.created", true},
		{"orders.*", "orders.a.b", false},
		{"orders.*", "payments.created", false},
		{"orders.>", "orders.a.b", true},
		// `>` is one or MORE. This is the case Go used to get wrong: it
		// returned true before checking a token was left, so a sibling
		// filtered on `orders.>` collected the bare `orders` too.
		{"orders.>", "orders", false},
		{"a.*.c", "a.b.c", true},
		{"a.*.c", "a.b.d", false},
	}
	for _, c := range cases {
		m := newMatcher([]byte(c.filter))
		if got := m.accepts([]byte(c.subject)); got != c.want {
			t.Errorf("filter %q vs subject %q: got %v, want %v", c.filter, c.subject, got, c.want)
		}
	}
}

// TestSiblingSliceIsNeverMutatedInPlace pins the reason the sibling slice is
// copied on every change: lookup releases the read lock before the caller
// walks rt.subs, so a route already handed out must keep seeing what it saw.
// Compacting or appending in place would rewrite that array underneath it.
func TestSiblingSliceIsNeverMutatedInPlace(t *testing.T) {
	r := newRegistry()
	a := &Subscription{id: 1, consumerID: 7, match: newMatcher([]byte("a.*"))}
	b := &Subscription{id: 2, consumerID: 7, match: newMatcher([]byte("b.*"))}
	c := &Subscription{id: 3, consumerID: 7, match: newMatcher([]byte("c.*"))}
	r.register(a)
	r.register(b)
	r.register(c)

	// What a fan-out in flight is holding.
	snapshot := r.lookup(2).subs
	if len(snapshot) != 3 {
		t.Fatalf("three siblings expected, got %d", len(snapshot))
	}

	r.remove(2)
	r.register(&Subscription{id: 4, consumerID: 7, match: newMatcher([]byte("d.*"))})

	// The in-flight walk still sees the three it started with, in order.
	if len(snapshot) != 3 || snapshot[0].id != 1 || snapshot[1].id != 2 || snapshot[2].id != 3 {
		t.Errorf("the shared slice was rewritten under a live route: %v %v %v",
			snapshot[0].id, snapshot[1].id, snapshot[2].id)
	}

	// And the registry itself moved on.
	if r.lookup(2) != nil {
		t.Error("the removed subscription still resolves")
	}
	if got := len(r.lookup(1).subs); got != 3 {
		t.Errorf("current route sees %d siblings, expected 3 (1, 3, 4)", got)
	}
}
