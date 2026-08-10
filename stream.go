package arbitro

import (
	"context"
	"sync/atomic"

	"github.com/arbitro-io/arbitro-go/internal/proto"
)

// Stream is a handle bound to one stream name, mirroring the Rust client's
// pre-resolved `stream_id: u32` pattern. The first publish/subscribe/info
// call lazy-resolves the broker-assigned ID and caches it (atomic.Uint32);
// subsequent calls skip the map lookup entirely.
//
// prefixBytes holds the client-configured subject prefix (with trailing dot)
// pre-converted to bytes, so PublishAsync can compose subjects without the
// double string→[]byte alloc that Client.PublishAsync suffers.
type Stream struct {
	client      *Client
	name        string
	streamID    atomic.Uint32
	prefixBytes []byte
}

// Stream returns a handle bound to a stream name. No network calls — the
// stream ID is resolved lazily on the first Publish/Info call.
func (c *Client) Stream(name string) *Stream {
	s := &Stream{client: c, name: name}
	if c.opts.prefix != "" {
		s.prefixBytes = []byte(c.opts.prefix + ".")
	}
	if id, ok := c.streams.get(name); ok {
		s.streamID.Store(id)
	}
	return s
}

// resolvedID returns the cached stream ID or resolves + caches it. Cold path
// only fires once per stream per process (barring CreateStream races).
func (s *Stream) resolvedID(ctx context.Context) (uint32, error) {
	if id := s.streamID.Load(); id != 0 {
		return id, nil
	}
	id, err := s.client.resolveStreamID(ctx, s.name)
	if err != nil {
		return 0, err
	}
	s.streamID.Store(id)
	return id, nil
}

// Publish sends a message to this stream and waits for broker confirmation.
func (s *Stream) Publish(ctx context.Context, subject string, payload []byte, opts ...PublishOption) error {
	return s.client.Publish(ctx, s.name, subject, payload, opts...)
}

// PublishAsync sends a message without waiting for confirmation. Hot path:
// once the stream ID is cached, this issues ONE allocation for the frame and
// encodes in-place (down from client.PublishAsync's 3 allocs: prefixed
// string, subject bytes, frame). Matches the Rust client's
// client.publish(stream_id, subject, payload) architecturally — a stack
// scratch was considered but Connection.Send only enqueues the slice, so
// the buffer must outlive the call.
//
// Returns ErrQueueFull when the outbound queue has no room — transient
// backpressure, not a dead connection; test for it with errors.Is.
func (s *Stream) PublishAsync(subject string, payload []byte, opts ...PublishOption) error {
	streamID := s.streamID.Load()
	if streamID == 0 {
		return s.client.PublishAsync(s.name, subject, payload, opts...)
	}

	var msgID []byte
	if len(opts) > 0 {
		var po publishOptions
		for _, fn := range opts {
			fn(&po)
		}
		if po.msgID != "" {
			msgID = []byte(po.msgID)
		}
	}

	// Total frame size drives one contiguous allocation. When a prefix is
	// configured, the subject bytes live inside `frame` (encoded directly);
	// no separate subject buffer is materialised.
	subjLen := len(s.prefixBytes) + len(subject)
	total := proto.PublishWireSize(subjLen, len(msgID), len(payload))
	frame := make([]byte, total)

	seq := s.client.getConn().NextSeq()
	if s.prefixBytes == nil {
		// unsafe.StringData avoids the string→[]byte copy for the read-only
		// subject bytes — EncodePublishInto only reads from this slice, never
		// writes. Safe because the string is passed by value and outlives
		// the encode call.
		proto.EncodePublishInto(frame, seq, streamID, unsafeStringBytes(subject), msgID, payload, proto.FlagNone)
	} else {
		// Compose subject in-place inside the frame body. We hand the encoder
		// a temporary slice sized to subjLen; the encoder copies from this
		// slice into the body region. Cost is one extra copy of subjLen
		// bytes vs zero-copy, but avoids the string-concat alloc.
		subjScratch := make([]byte, subjLen)
		copy(subjScratch, s.prefixBytes)
		copy(subjScratch[len(s.prefixBytes):], subject)
		proto.EncodePublishInto(frame, seq, streamID, subjScratch, msgID, payload, proto.FlagNone)
	}

	if err := s.client.getConn().TrySend(frame); err != nil {
		s.client.publishErrors.Add(1)
		return err
	}
	s.client.publishesSent.Add(1)
	return nil
}

// PublishBatch atomically publishes multiple messages.
func (s *Stream) PublishBatch(ctx context.Context, entries []BatchEntry) (uint64, error) {
	return s.client.PublishBatch(ctx, s.name, entries)
}

// DeleteMessage tombstones a single message by sequence number.
func (s *Stream) DeleteMessage(ctx context.Context, seq uint64) (bool, error) {
	return s.client.DeleteMessage(ctx, s.name, seq)
}

// Info returns stream metadata.
func (s *Stream) Info(ctx context.Context) (*StreamInfo, error) {
	return s.client.StreamInfo(ctx, s.name)
}

// Consumer returns a consumer context helper bound to this stream.
func (s *Stream) Consumer(cfg ConsumerConfig) *Consumer {
	return &Consumer{client: s.client, streamName: s.name, cfg: cfg}
}
