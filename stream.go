package arbitro

import "context"

// Stream is a convenience helper bound to a stream name.
type Stream struct {
	client   *Client
	name     string
	streamID uint32
}

// Stream returns a context helper bound to a stream name. No network calls.
func (c *Client) Stream(name string) *Stream {
	return &Stream{client: c, name: name}
}

// Publish sends a message to this stream and waits for broker confirmation.
func (s *Stream) Publish(ctx context.Context, subject string, payload []byte, opts ...PublishOption) error {
	return s.client.Publish(ctx, s.name, subject, payload, opts...)
}

// PublishAsync sends a message without waiting for confirmation.
func (s *Stream) PublishAsync(subject string, payload []byte, opts ...PublishOption) {
	s.client.PublishAsync(s.name, subject, payload, opts...)
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
