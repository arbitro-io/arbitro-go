package arbitro

import "context"

// Consumer is a convenience helper bound to a stream + consumer config.
type Consumer struct {
	client     *Client
	streamName string
	cfg        ConsumerConfig
}

// Subscribe creates the consumer (if needed) and starts receiving messages.
func (c *Consumer) Subscribe(ctx context.Context, opts ...SubscribeOption) (*Subscription, error) {
	return c.client.Subscribe(ctx, c.streamName, c.cfg, opts...)
}

// DeleteMessage tombstones a message by sequence number on the owning stream.
func (c *Consumer) DeleteMessage(ctx context.Context, seq uint64) (bool, error) {
	return c.client.DeleteMessage(ctx, c.streamName, seq)
}

// Pending returns the number of unacknowledged messages.
func (c *Consumer) Pending(ctx context.Context) (uint64, error) {
	name := c.cfg.Name
	if name == "" {
		name = c.streamName
	}
	return c.client.GetPending(ctx, c.streamName, name)
}
