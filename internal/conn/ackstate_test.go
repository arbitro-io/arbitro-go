package conn

import (
	"encoding/binary"
	"testing"

	"github.com/arbitro-io/arbitro-go/internal/ackrel"
	"github.com/arbitro-io/arbitro-go/internal/proto"
)

// ackStateRepBody builds a raw 40-byte AckStateRep body:
// consumer_id(4) + generation(4) + cursor(8) + low_seq(8) + high_seq(8) +
// status(4) + _pad(4).
func ackStateRepBody(consumerID, generation uint32, cursor, lowSeq, highSeq uint64, status uint8) []byte {
	body := make([]byte, 40)
	binary.LittleEndian.PutUint32(body[0:4], consumerID)
	binary.LittleEndian.PutUint32(body[4:8], generation)
	binary.LittleEndian.PutUint64(body[8:16], cursor)
	binary.LittleEndian.PutUint64(body[16:24], lowSeq)
	binary.LittleEndian.PutUint64(body[24:32], highSeq)
	binary.LittleEndian.PutUint32(body[32:36], uint32(status))
	return body
}

type confirmCall struct {
	cid    uint32
	cursor uint64
}

// The on-connect ackstore purge (onAckConfirm with the broker cursor) must
// fire on a status-OK AckStateRep even when no ack relay is attached — the
// reply may be answering the reconnect-purge AckStateReq of a fresh process
// with zero deferred-ack state.
func TestHandleAckStateRepPurgesOnStatusOK(t *testing.T) {
	c := &Connection{}
	var calls []confirmCall
	c.SetAckConfirmHandler(func(cid uint32, cursor uint64) {
		calls = append(calls, confirmCall{cid, cursor})
	})

	c.handleAckStateRep(ackStateRepBody(3, 0, 42, 1, 100, proto.AckStatusOK))

	if len(calls) != 1 || calls[0] != (confirmCall{3, 42}) {
		t.Fatalf("expected exactly one confirm(3, 42), got %v", calls)
	}
}

// When the broker does NOT vouch for the cursor (status != OK, e.g. the
// consumer no longer exists), nothing may be purged — even if the frame
// carries a non-zero cursor. Conservative by design: a wrongly kept entry
// costs disk, a wrongly dropped one costs a duplicate execution.
func TestHandleAckStateRepNonOKStatusNoPurge(t *testing.T) {
	c := &Connection{}
	var calls []confirmCall
	c.SetAckConfirmHandler(func(cid uint32, cursor uint64) {
		calls = append(calls, confirmCall{cid, cursor})
	})

	// Adversarial: ConsumerUnknown but a non-zero cursor.
	c.handleAckStateRep(ackStateRepBody(4, 0, 10_000, 0, 0, proto.AckStatusConsumerUnknown))

	if len(calls) != 0 {
		t.Fatalf("expected no confirm calls without an OK status, got %v", calls)
	}
}

// A relay-generation mismatch (fresh process: local generation 0, broker
// generation bumped) must still run the ackstore purge — the store is keyed
// by durable names, so entries recorded by a dead session are covered by the
// broker's cursor regardless of the in-memory relay state. The relay itself
// is still reconciled (wiped) exactly as before.
func TestHandleAckStateRepGenerationMismatchStillPurges(t *testing.T) {
	c := &Connection{}
	c.AckRelay = ackrel.NewRelay(0, nil)
	var calls []confirmCall
	c.SetAckConfirmHandler(func(cid uint32, cursor uint64) {
		calls = append(calls, confirmCall{cid, cursor})
	})

	// Local relay generation is 0; broker replies with generation 7.
	c.handleAckStateRep(ackStateRepBody(5, 7, 15, 1, 20, proto.AckStatusOK))

	if len(calls) != 1 || calls[0] != (confirmCall{5, 15}) {
		t.Fatalf("expected confirm(5, 15) despite generation mismatch, got %v", calls)
	}
	if got := c.AckRelay.Generation(5); got != 7 {
		t.Fatalf("relay generation must be reconciled to 7, got %d", got)
	}
}
