package arbitro

import (
	"context"

	"github.com/arbitro-io/arbitro-go/internal/proto"
)

// CreateStream creates a new stream on the broker.
func (c *Client) CreateStream(ctx context.Context, name string, cfg StreamConfig) (*Stream, error) {
	seq := c.conn.NextSeq()
	frame, err := proto.EncodeCreateStream(
		seq, []byte(name), []byte(cfg.SubjectFilter),
		cfg.MaxMsgs, cfg.MaxBytes, uint64(cfg.MaxAge.Seconds()),
		cfg.Replicas, cfg.Journal, 0, 0,
		uint32(cfg.IdempotencyWindow.Milliseconds()),
	)
	if err != nil {
		return nil, err
	}
	reply, err := c.conn.SendExpectReply(ctx, frame, seq)
	if err != nil {
		return nil, err
	}
	if err := c.checkReply(reply); err != nil {
		return nil, err
	}
	body := reply[proto.HeaderSize:]
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
	seq := c.conn.NextSeq()
	frame, err := proto.EncodeDeleteStream(seq, []byte(name), !do.keepData)
	if err != nil {
		return err
	}
	reply, err := c.conn.SendExpectReply(ctx, frame, seq)
	if err != nil {
		return err
	}
	return c.checkReply(reply)
}

// StreamInfo returns metadata about a stream.
func (c *Client) StreamInfo(ctx context.Context, name string) (*StreamInfo, error) {
	seq := c.conn.NextSeq()
	frame, err := proto.EncodeGetStream(seq, []byte(name))
	if err != nil {
		return nil, err
	}
	reply, err := c.conn.SendExpectReply(ctx, frame, seq)
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
	id := uint32(proto.RepOkRefSeq(body))
	c.streams.set(name, id)
	return &StreamInfo{Name: name, StreamID: id}, nil
}

// ListStreams returns all streams on the broker.
func (c *Client) ListStreams(ctx context.Context) ([]StreamInfo, error) {
	seq := c.conn.NextSeq()
	frame, err := proto.EncodeListStreams(seq)
	if err != nil {
		return nil, err
	}
	reply, err := c.conn.SendExpectReply(ctx, frame, seq)
	if err != nil {
		return nil, err
	}
	if err := c.checkReply(reply); err != nil {
		return nil, err
	}
	// TODO: parse JSON response body
	return nil, nil
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
	seq := c.conn.NextSeq()
	frame, err := proto.EncodePurgeStream(seq, []byte(name))
	if err != nil {
		return 0, err
	}
	reply, err := c.conn.SendExpectReply(ctx, frame, seq)
	if err != nil {
		return 0, err
	}
	if err := c.checkReply(reply); err != nil {
		return 0, err
	}
	body := reply[proto.HeaderSize:]
	return proto.RepOkRefSeq(body), nil
}

// DrainSubject deletes all messages matching a subject pattern.
func (c *Client) DrainSubject(ctx context.Context, stream, subject string) (uint64, error) {
	seq := c.conn.NextSeq()
	frame, err := proto.EncodeDrainSubject(seq, []byte(stream), []byte(subject))
	if err != nil {
		return 0, err
	}
	reply, err := c.conn.SendExpectReply(ctx, frame, seq)
	if err != nil {
		return 0, err
	}
	if err := c.checkReply(reply); err != nil {
		return 0, err
	}
	body := reply[proto.HeaderSize:]
	return proto.RepOkRefSeq(body), nil
}

// DeleteMessage tombstones a single message by sequence number.
func (c *Client) DeleteMessage(ctx context.Context, stream string, msgSeq uint64) (bool, error) {
	seq := c.conn.NextSeq()
	frame, err := proto.EncodeDeleteMessage(seq, []byte(stream), msgSeq)
	if err != nil {
		return false, err
	}
	reply, err := c.conn.SendExpectReply(ctx, frame, seq)
	if err != nil {
		return false, err
	}
	if err := c.checkReply(reply); err != nil {
		return false, nil // not found / already deleted
	}
	body := reply[proto.HeaderSize:]
	return proto.RepOkRefSeq(body) > 0, nil
}

// CreateConsumer creates a consumer on the broker. Returns the consumer ID.
func (c *Client) CreateConsumer(ctx context.Context, stream string, cfg ConsumerConfig) (uint32, error) {
	return c.ensureConsumer(ctx, stream, cfg)
}

// DeleteConsumer removes a consumer by stream name and consumer name.
func (c *Client) DeleteConsumer(ctx context.Context, stream, name string) error {
	streamID, err := c.resolveStreamID(ctx, stream)
	if err != nil {
		return err
	}
	seq := c.conn.NextSeq()
	frame, err := proto.EncodeDeleteConsumer(seq, streamID, []byte(name))
	if err != nil {
		return err
	}
	reply, err := c.conn.SendExpectReply(ctx, frame, seq)
	if err != nil {
		return err
	}
	return c.checkReply(reply)
}

// GetPending returns the number of unacknowledged messages for a consumer.
func (c *Client) GetPending(ctx context.Context, stream, name string) (uint64, error) {
	streamID, err := c.resolveStreamID(ctx, stream)
	if err != nil {
		return 0, err
	}
	seq := c.conn.NextSeq()
	frame, err := proto.EncodeGetConsumer(seq, streamID, []byte(name))
	if err != nil {
		return 0, err
	}
	reply, err := c.conn.SendExpectReply(ctx, frame, seq)
	if err != nil {
		return 0, err
	}
	if err := c.checkReply(reply); err != nil {
		return 0, err
	}
	// TODO: parse consumer info JSON to extract pending count
	return 0, nil
}

// ConsumerInfo returns metadata about a consumer.
func (c *Client) ConsumerInfo(ctx context.Context, stream, name string) (*ConsumerInfo, error) {
	streamID, err := c.resolveStreamID(ctx, stream)
	if err != nil {
		return nil, err
	}
	seq := c.conn.NextSeq()
	frame, err := proto.EncodeGetConsumer(seq, streamID, []byte(name))
	if err != nil {
		return nil, err
	}
	reply, err := c.conn.SendExpectReply(ctx, frame, seq)
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

// ListConsumers returns all consumers for a stream.
func (c *Client) ListConsumers(ctx context.Context, stream string) ([]ConsumerInfo, error) {
	seq := c.conn.NextSeq()
	frame, err := proto.EncodeListConsumers(seq)
	if err != nil {
		return nil, err
	}
	reply, err := c.conn.SendExpectReply(ctx, frame, seq)
	if err != nil {
		return nil, err
	}
	if err := c.checkReply(reply); err != nil {
		return nil, err
	}
	// TODO: parse JSON response body
	return nil, nil
}

// PauseConsumer pauses delivery to a consumer.
func (c *Client) PauseConsumer(ctx context.Context, stream, name string) error {
	streamID, err := c.resolveStreamID(ctx, stream)
	if err != nil {
		return err
	}
	seq := c.conn.NextSeq()
	frame, err := proto.EncodePauseConsumer(seq, streamID, []byte(name))
	if err != nil {
		return err
	}
	reply, err := c.conn.SendExpectReply(ctx, frame, seq)
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
	seq := c.conn.NextSeq()
	frame, err := proto.EncodeResumeConsumer(seq, streamID, []byte(name))
	if err != nil {
		return err
	}
	reply, err := c.conn.SendExpectReply(ctx, frame, seq)
	if err != nil {
		return err
	}
	return c.checkReply(reply)
}
