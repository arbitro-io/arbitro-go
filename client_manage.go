package arbitro

import (
	"context"

	"github.com/arbitro-io/arbitro-go/internal/proto"
)

// CreateStream creates a new stream on the broker.
func (c *Client) CreateStream(ctx context.Context, name string, cfg StreamConfig) (*Stream, error) {
	seq := c.getConn().NextSeq()
	frame, err := proto.EncodeCreateStream(
		seq, []byte(name), []byte(cfg.SubjectFilter),
		cfg.MaxMsgs, cfg.MaxBytes, uint64(cfg.MaxAge.Seconds()),
		cfg.Replicas, cfg.Journal, 0, 0,
		uint32(cfg.IdempotencyWindow.Milliseconds()),
	)
	if err != nil {
		return nil, err
	}
	reply, err := c.getConn().SendExpectReply(ctx, frame, seq)
	if err != nil {
		return nil, err
	}
	if err := c.checkReply(reply); err != nil {
		return nil, err
	}
	body := reply[proto.HeaderSize:]
	if len(body) < 8 {
		return nil, &ArbitroError{Code: ErrCodeInternalError, Message: "create stream: reply body too short"}
	}
	streamID := uint32(proto.RepOkRefSeq(body))
	c.streams.set(name, streamID)
	return &Stream{client: c, name: name, streamID: streamID}, nil
}

// UpsertStream creates or re-uses an existing stream with equivalent config.
func (c *Client) UpsertStream(ctx context.Context, name string, cfg StreamConfig) (*Stream, error) {
	s, err := c.CreateStream(ctx, name, cfg)
	if err != nil && IsAlreadyExists(err) {
		// Stream exists — resolve its ID
		id, err2 := c.resolveStreamID(ctx, name)
		if err2 != nil {
			return nil, err2
		}
		return &Stream{client: c, name: name, streamID: id}, nil
	}
	return s, err
}

// DeleteStream removes a stream from the broker.
func (c *Client) DeleteStream(ctx context.Context, name string, opts ...DeleteStreamOption) error {
	do := deleteStreamOptions{}
	for _, fn := range opts {
		fn(&do)
	}
	seq := c.getConn().NextSeq()
	frame, err := proto.EncodeDeleteStream(seq, []byte(name), !do.keepData)
	if err != nil {
		return err
	}
	reply, err := c.getConn().SendExpectReply(ctx, frame, seq)
	if err != nil {
		return err
	}
	return c.checkReply(reply)
}

// StreamInfo returns metadata about a stream.
func (c *Client) StreamInfo(ctx context.Context, name string) (*StreamInfo, error) {
	seq := c.getConn().NextSeq()
	frame, err := proto.EncodeGetStream(seq, []byte(name))
	if err != nil {
		return nil, err
	}
	reply, err := c.getConn().SendExpectReply(ctx, frame, seq)
	if err != nil {
		return nil, err
	}
	if err := c.checkReply(reply); err != nil {
		if IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	body := reply[proto.HeaderSize:]
	if len(body) < 8 {
		return nil, &ArbitroError{Code: ErrCodeInternalError, Message: "stream info: reply body too short"}
	}
	id := uint32(proto.RepOkRefSeq(body))
	c.streams.set(name, id)
	return &StreamInfo{Name: name, StreamID: id}, nil
}

// ListStreams returns all streams on the broker.
// The reply is paginated server-side (default limit ~1000); the client walks
// pages until the broker returns an empty batch.
func (c *Client) ListStreams(ctx context.Context) ([]StreamInfo, error) {
	const pageLimit uint32 = 1000
	var out []StreamInfo
	for offset := uint32(0); ; {
		seq := c.getConn().NextSeq()
		frame, err := proto.EncodeListStreams(seq, offset, pageLimit)
		if err != nil {
			return nil, err
		}
		reply, err := c.getConn().SendExpectReply(ctx, frame, seq)
		if err != nil {
			return nil, err
		}
		if err := c.checkReply(reply); err != nil {
			return nil, err
		}
		entries, err := proto.DecodeListStreamsBody(reply[proto.HeaderSize:])
		if err != nil {
			return nil, &ArbitroError{Code: ErrCodeInternalError, Message: "list streams: " + err.Error()}
		}
		if len(entries) == 0 {
			break
		}
		for i := range entries {
			out = append(out, StreamInfo{
				Name:     string(entries[i].Name),
				StreamID: entries[i].StreamID,
			})
			c.streams.set(string(entries[i].Name), entries[i].StreamID)
		}
		if uint32(len(entries)) < pageLimit {
			break
		}
		offset += uint32(len(entries))
	}
	return out, nil
}

// StreamExists checks if a stream exists.
func (c *Client) StreamExists(ctx context.Context, name string) (bool, error) {
	info, err := c.StreamInfo(ctx, name)
	if err != nil {
		return false, err
	}
	return info != nil, nil
}

// PurgeStream deletes all messages in a stream. Returns message count purged.
func (c *Client) PurgeStream(ctx context.Context, name string) (uint64, error) {
	seq := c.getConn().NextSeq()
	frame, err := proto.EncodePurgeStream(seq, []byte(name))
	if err != nil {
		return 0, err
	}
	reply, err := c.getConn().SendExpectReply(ctx, frame, seq)
	if err != nil {
		return 0, err
	}
	if err := c.checkReply(reply); err != nil {
		return 0, err
	}
	body := reply[proto.HeaderSize:]
	if len(body) < 8 {
		return 0, &ArbitroError{Code: ErrCodeInternalError, Message: "purge stream: reply body too short"}
	}
	return proto.RepOkRefSeq(body), nil
}

// DrainSubject deletes all messages matching a subject pattern.
func (c *Client) DrainSubject(ctx context.Context, stream, subject string) (uint64, error) {
	seq := c.getConn().NextSeq()
	frame, err := proto.EncodeDrainSubject(seq, []byte(stream), []byte(subject))
	if err != nil {
		return 0, err
	}
	reply, err := c.getConn().SendExpectReply(ctx, frame, seq)
	if err != nil {
		return 0, err
	}
	if err := c.checkReply(reply); err != nil {
		return 0, err
	}
	body := reply[proto.HeaderSize:]
	if len(body) < 8 {
		return 0, &ArbitroError{Code: ErrCodeInternalError, Message: "drain subject: reply body too short"}
	}
	return proto.RepOkRefSeq(body), nil
}

// DeleteMessage tombstones a single message by sequence number.
func (c *Client) DeleteMessage(ctx context.Context, stream string, msgSeq uint64) (bool, error) {
	seq := c.getConn().NextSeq()
	frame, err := proto.EncodeDeleteMessage(seq, []byte(stream), msgSeq)
	if err != nil {
		return false, err
	}
	reply, err := c.getConn().SendExpectReply(ctx, frame, seq)
	if err != nil {
		return false, err
	}
	if err := c.checkReply(reply); err != nil {
		return false, nil // not found / already deleted
	}
	body := reply[proto.HeaderSize:]
	if len(body) < 8 {
		return false, nil
	}
	return proto.RepOkRefSeq(body) > 0, nil
}

// CreateConsumer creates a consumer on the broker. Returns the consumer ID.
func (c *Client) CreateConsumer(ctx context.Context, stream string, cfg ConsumerConfig) (uint32, error) {
	return c.ensureConsumer(ctx, stream, cfg)
}

// DeleteConsumer removes a consumer by stream name and consumer name.
// The wire body carries only `consumer_id`, so (stream, name) is resolved to a
// numeric ID before the delete is sent.
func (c *Client) DeleteConsumer(ctx context.Context, stream, name string) error {
	streamID, err := c.resolveStreamID(ctx, stream)
	if err != nil {
		return err
	}
	consumerID, err := c.resolveConsumerID(ctx, streamID, name)
	if err != nil {
		return err
	}
	seq := c.getConn().NextSeq()
	frame, err := proto.EncodeDeleteConsumer(seq, consumerID)
	if err != nil {
		return err
	}
	reply, err := c.getConn().SendExpectReply(ctx, frame, seq)
	if err != nil {
		return err
	}
	return c.checkReply(reply)
}

// GetPending returns the number of unacknowledged (delivered but not yet
// acked) messages for a consumer — a live broker round-trip via
// ConsumerStats (G13), equivalent to NATS JetStream's num_ack_pending.
// Mirrors arbitro-client-tokio's Client::get_pending (client.rs:703).
func (c *Client) GetPending(ctx context.Context, stream, name string) (uint64, error) {
	streamID, err := c.resolveStreamID(ctx, stream)
	if err != nil {
		return 0, err
	}
	consumerID, err := c.resolveConsumerID(ctx, streamID, name)
	if err != nil {
		return 0, err
	}
	seq := c.getConn().NextSeq()
	frame, err := proto.EncodeConsumerStats(seq, consumerID)
	if err != nil {
		return 0, err
	}
	reply, err := c.getConn().SendExpectReply(ctx, frame, seq)
	if err != nil {
		return 0, err
	}
	if err := c.checkReply(reply); err != nil {
		return 0, err
	}
	body := reply[proto.HeaderSize:]
	if len(body) < 8 {
		return 0, &ArbitroError{Code: ErrCodeInternalError, Message: "get pending: reply body too short"}
	}
	// RepOk body packs the live pending-ack count in place of ref_seq.
	return proto.RepOkRefSeq(body), nil
}

// ConsumerInfo returns metadata about a consumer.
func (c *Client) ConsumerInfo(ctx context.Context, stream, name string) (*ConsumerInfo, error) {
	streamID, err := c.resolveStreamID(ctx, stream)
	if err != nil {
		return nil, err
	}
	seq := c.getConn().NextSeq()
	frame, err := proto.EncodeGetConsumer(seq, streamID, []byte(name))
	if err != nil {
		return nil, err
	}
	reply, err := c.getConn().SendExpectReply(ctx, frame, seq)
	if err != nil {
		return nil, err
	}
	if err := c.checkReply(reply); err != nil {
		if IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	// TODO: parse full consumer info from JSON body
	return &ConsumerInfo{Name: name, StreamID: streamID}, nil
}

// ListConsumers returns all consumers for a stream. Pass stream="" to list
// consumers across every stream (broker semantic: stream_id=0).
func (c *Client) ListConsumers(ctx context.Context, stream string) ([]ConsumerInfo, error) {
	var streamID uint32
	if stream != "" {
		id, err := c.resolveStreamID(ctx, stream)
		if err != nil {
			return nil, err
		}
		streamID = id
	}

	const pageLimit uint32 = 1000
	var out []ConsumerInfo
	for offset := uint32(0); ; {
		seq := c.getConn().NextSeq()
		frame, err := proto.EncodeListConsumers(seq, streamID, offset, pageLimit)
		if err != nil {
			return nil, err
		}
		reply, err := c.getConn().SendExpectReply(ctx, frame, seq)
		if err != nil {
			return nil, err
		}
		if err := c.checkReply(reply); err != nil {
			return nil, err
		}
		entries, err := proto.DecodeListConsumersBody(reply[proto.HeaderSize:])
		if err != nil {
			return nil, &ArbitroError{Code: ErrCodeInternalError, Message: "list consumers: " + err.Error()}
		}
		if len(entries) == 0 {
			break
		}
		for i := range entries {
			out = append(out, ConsumerInfo{
				ConsumerID: entries[i].ConsumerID,
				StreamID:   entries[i].StreamID,
			})
		}
		if uint32(len(entries)) < pageLimit {
			break
		}
		offset += uint32(len(entries))
	}
	return out, nil
}

// PauseConsumer pauses delivery to a consumer.
func (c *Client) PauseConsumer(ctx context.Context, stream, name string) error {
	streamID, err := c.resolveStreamID(ctx, stream)
	if err != nil {
		return err
	}
	seq := c.getConn().NextSeq()
	frame, err := proto.EncodePauseConsumer(seq, streamID, []byte(name))
	if err != nil {
		return err
	}
	reply, err := c.getConn().SendExpectReply(ctx, frame, seq)
	if err != nil {
		return err
	}
	return c.checkReply(reply)
}

// ResumeConsumer resumes delivery to a paused consumer.
func (c *Client) ResumeConsumer(ctx context.Context, stream, name string) error {
	streamID, err := c.resolveStreamID(ctx, stream)
	if err != nil {
		return err
	}
	seq := c.getConn().NextSeq()
	frame, err := proto.EncodeResumeConsumer(seq, streamID, []byte(name))
	if err != nil {
		return err
	}
	reply, err := c.getConn().SendExpectReply(ctx, frame, seq)
	if err != nil {
		return err
	}
	return c.checkReply(reply)
}
