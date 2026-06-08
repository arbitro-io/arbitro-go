package arbitro

import (
	"crypto/tls"
	"log/slog"
	"time"
)

// Option configures the client.
type Option func(*clientOptions)

type clientOptions struct {
	timeout   time.Duration
	reconnect bool
	maxRetries int
	retryInterval time.Duration
	prefix    string
	tlsConfig *tls.Config
	logger    *slog.Logger
}

func defaultOptions() clientOptions {
	return clientOptions{
		timeout:       5 * time.Second,
		reconnect:     true,
		maxRetries:    10,
		retryInterval: 500 * time.Millisecond,
	}
}

// WithTimeout sets the default timeout for management operations.
func WithTimeout(d time.Duration) Option {
	return func(o *clientOptions) { o.timeout = d }
}

// WithReconnect enables/disables automatic reconnection.
func WithReconnect(enabled bool, maxRetries int, interval time.Duration) Option {
	return func(o *clientOptions) {
		o.reconnect = enabled
		o.maxRetries = maxRetries
		o.retryInterval = interval
	}
}

// WithPrefix sets a subject prefix applied to all publish/subscribe operations.
func WithPrefix(prefix string) Option {
	return func(o *clientOptions) { o.prefix = prefix }
}

// WithTLS enables TLS for the broker connection.
func WithTLS(cfg *tls.Config) Option {
	return func(o *clientOptions) { o.tlsConfig = cfg }
}

// WithLogger sets a structured logger for the client.
func WithLogger(l *slog.Logger) Option {
	return func(o *clientOptions) { o.logger = l }
}

// SubscribeOption configures a subscription.
type SubscribeOption func(*subscribeOptions)

type subscribeOptions struct {
	handler func(*Msg)
}

// WithHandler sets a callback-based handler instead of channel delivery.
func WithHandler(fn func(*Msg)) SubscribeOption {
	return func(o *subscribeOptions) { o.handler = fn }
}

// PublishOption configures a publish call.
type PublishOption func(*publishOptions)

type publishOptions struct {
	msgID string
}

// WithMsgID sets an explicit dedup message ID for idempotent publish.
func WithMsgID(id string) PublishOption {
	return func(o *publishOptions) { o.msgID = id }
}

// DeleteStreamOption configures stream deletion.
type DeleteStreamOption func(*deleteStreamOptions)

type deleteStreamOptions struct {
	keepData bool
}

// KeepData preserves journal bytes when deleting a stream.
func KeepData() DeleteStreamOption {
	return func(o *deleteStreamOptions) { o.keepData = true }
}
