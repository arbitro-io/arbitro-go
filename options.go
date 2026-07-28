package arbitro

import (
	"crypto/tls"
	"log/slog"
	"time"

	"github.com/arbitro-io/arbitro-go/internal/ackstore"
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
	keepAlive KeepAlive

	// ackStore backs the client's redelivery-dedup guarantee. Default is an
	// in-memory store (dedup within the process lifetime). WithAckPersistence
	// swaps in a durable WAL store that survives process restart. Nil disables
	// dedup entirely (at-least-once, handler may run more than once).
	ackStore    ackstore.Store
	ackStoreErr error // deferred error from WithAckPersistence, checked at Connect
	ackDedup    bool  // whether to consult the store on delivery
}

// KeepAlive configures the client heartbeat / dead-connection watchdog.
// Mirrors the Rust client's conn::heartbeat module: a Ping (action 0x0601,
// header-only body) is sent every Interval; if no Pong is observed for
// Timeout, the connection is declared dead and closed (which in turn feeds
// the reconnect supervisor).
type KeepAlive struct {
	Interval time.Duration
	Timeout  time.Duration
}

func defaultKeepAlive() KeepAlive {
	return KeepAlive{
		Interval: 30 * time.Second,
		Timeout:  60 * time.Second,
	}
}

func defaultOptions() clientOptions {
	return clientOptions{
		timeout:       5 * time.Second,
		reconnect:     true,
		maxRetries:    10,
		retryInterval: 500 * time.Millisecond,
		keepAlive:     defaultKeepAlive(),
		ackDedup:      true, // in-memory dedup on by default
	}
}

// WithAckStore injects a custom ackstore.Store for redelivery dedup. Advanced
// callers use this to supply a pre-configured WAL (ackstore.OpenWAL) with a
// specific directory, TTL, fsync policy, and cap. Passing a store implies
// dedup is enabled.
func WithAckStore(store ackstore.Store) Option {
	return func(o *clientOptions) {
		o.ackStore = store
		o.ackDedup = store != nil
	}
}

// WithAckPersistence enables durable, restart-surviving redelivery dedup
// backed by a WAL at dir. ttl bounds how long a recorded ack is remembered
// (0 = never expire). fsync=true trades throughput for power-loss durability.
// This is a convenience over WithAckStore(ackstore.OpenWAL(...)).
func WithAckPersistence(dir string, ttl time.Duration, fsync bool) Option {
	return func(o *clientOptions) {
		w, err := ackstore.OpenWAL(ackstore.Config{
			Dir:            dir,
			TTL:            ttl,
			Fsync:          fsync,
			CompactAtBytes: 64 * 1024 * 1024, // auto-compact past 64 MiB
		})
		if err != nil {
			// Surface the failure at Connect time via a sentinel; the client
			// checks o.ackStore != nil and o.ackStoreErr.
			o.ackStoreErr = err
			return
		}
		o.ackStore = w
		o.ackDedup = true
	}
}

// WithoutAckDedup disables redelivery dedup entirely (pure at-least-once; the
// user handler may run more than once for a redelivered message). Slightly
// faster; requires idempotent handlers.
func WithoutAckDedup() Option {
	return func(o *clientOptions) {
		o.ackStore = nil
		o.ackDedup = false
	}
}

// WithKeepAlive configures the heartbeat interval and dead-connection
// timeout. Pass interval<=0 to disable the heartbeat goroutine entirely.
func WithKeepAlive(interval, timeout time.Duration) Option {
	return func(o *clientOptions) {
		o.keepAlive = KeepAlive{Interval: interval, Timeout: timeout}
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
	// Overrides ConsumerConfig.Filter for the Subscribe frame only, letting
	// a caller narrow the subscription while the consumer itself is created
	// with an empty subject. Unexported: internal to QueueSubscribe.
	subFilter *string
}

// WithHandler sets a callback-based handler instead of channel delivery.
func WithHandler(fn func(*Msg)) SubscribeOption {
	return func(o *subscribeOptions) { o.handler = fn }
}

func withSubFilter(filter string) SubscribeOption {
	return func(o *subscribeOptions) { o.subFilter = &filter }
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
