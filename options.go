package arbitro

import (
	"crypto/tls"
	"log/slog"
	"os"
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
	// authToken is the bearer token sent in the handshake. Empty = no Auth
	// frame. See WithAuthToken.
	authToken string

	// writeQueueCap and maxBlock bound the outbound path. See WithWriteQueue.
	writeQueueCap int
	maxBlock      time.Duration

	// ackStore backs the client's redelivery-dedup guarantee. Default is an
	// in-memory store (dedup within the process lifetime). WithAckPersistence
	// swaps in a durable WAL store that survives process restart. Nil disables
	// dedup entirely (at-least-once, handler may run more than once).
	ackStore    ackstore.Store
	ackStoreErr error // deferred error from WithAckPersistence, checked at Connect
	ackDedup    bool  // whether to consult the store on delivery
	// ackStoreOwned marks a store this package opened (WithAckPersistence /
	// WithAckStoreDir) rather than one the caller handed us. Only an owned
	// store is closed automatically when a later option supersedes it —
	// without that, `WithAckStoreDir(d), WithoutAckDedup()` would drop the WAL
	// reference while its directory lock stayed held for the process lifetime,
	// and the next Connect on d would fail with ErrLocked for no reason.
	ackStoreOwned bool
}

// releaseOwnedStore closes a store this package opened, so a superseding option
// does not leak its file handle and directory lock.
func (o *clientOptions) releaseOwnedStore() {
	if o.ackStore != nil && o.ackStoreOwned {
		_ = o.ackStore.Close()
	}
	o.ackStore = nil
	o.ackStoreOwned = false
	o.ackStoreErr = nil
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
		// Env fallback so a deployment can turn auth on without a code change.
		// WithAuthToken overrides it — options are applied after defaults.
		authToken: os.Getenv("ARBITRO_TOKEN"),
	}
}

// WithAckStore injects a custom ackstore.Store for redelivery dedup. Advanced
// callers use this to supply a pre-configured WAL (ackstore.OpenWAL) with a
// specific directory, TTL, fsync policy, and cap. Passing a store implies
// dedup is enabled.
func WithAckStore(store ackstore.Store) Option {
	return func(o *clientOptions) {
		o.releaseOwnedStore()
		o.ackStore = store
		o.ackDedup = store != nil
	}
}

// WithAckStoreDir enables durable, restart-surviving redelivery dedup backed by
// a WAL in dir. This is the option to reach for when the only thing you need to
// decide is *where the store lives*:
//
//	arbitro.Connect(ctx, addr, arbitro.WithAckStoreDir("/var/lib/myapp/ackstore"))
//
// Passing "" selects the platform default directory — $ARBITRO_ACKSTORE_DIR,
// else $XDG_STATE_HOME/arbitro/ackstore (Linux/BSD),
// ~/Library/Application Support/arbitro/ackstore (macOS), or
// %LOCALAPPDATA%\arbitro\ackstore (Windows). It is never the working directory
// and never a temp dir; see ackstore.DefaultDir (re-exported as
// DefaultAckStoreDir) for the reasoning.
//
// Exactly one process may hold a store directory at a time; a second Connect
// against the same directory fails with ackstore.ErrLocked rather than
// interleaving writes into one log. Give each client its own directory.
//
// Entries never expire and writes are buffered without fsync — the same
// defaults the Rust client's WalConfig uses. Buffered writes are two orders of
// magnitude faster and cost only that an OS-level crash may lose the last
// unflushed records, which degrades to the broker's own at-least-once
// behaviour. Use WithAckPersistence to set a TTL or turn fsync on.
func WithAckStoreDir(dir string) Option {
	return WithAckPersistence(dir, 0, false)
}

// WithAckPersistence enables durable, restart-surviving redelivery dedup
// backed by a WAL at dir. ttl bounds how long a recorded ack is remembered
// (0 = never expire). fsync=true trades throughput for power-loss durability.
// This is a convenience over WithAckStore(ackstore.OpenWAL(...)).
//
// dir == "" selects the platform default directory — see WithAckStoreDir.
func WithAckPersistence(dir string, ttl time.Duration, fsync bool) Option {
	return func(o *clientOptions) {
		o.releaseOwnedStore()
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
		o.ackStoreOwned = true
		o.ackDedup = true
	}
}

// DefaultAckStoreDir reports the directory WithAckStoreDir("") resolves to,
// without opening anything. Useful for logging the path at startup or for
// deciding whether the environment can support a durable store at all (it
// returns ackstore.ErrNoDefaultDir when neither $ARBITRO_ACKSTORE_DIR nor a
// platform state directory is available).
func DefaultAckStoreDir() (string, error) { return ackstore.DefaultDir() }

// ErrAckStoreLocked reports that another live process already holds the
// single-writer lock on the configured ack-store directory. Test for it with
// errors.Is on the error returned by Connect.
var ErrAckStoreLocked = ackstore.ErrLocked

// ErrNoDefaultAckStoreDir reports that no ack-store directory was configured
// and none could be defaulted (no $ARBITRO_ACKSTORE_DIR, no platform state
// directory). Test for it with errors.Is on the error returned by Connect.
var ErrNoDefaultAckStoreDir = ackstore.ErrNoDefaultDir

// WithoutAckDedup disables redelivery dedup entirely (pure at-least-once; the
// user handler may run more than once for a redelivered message). Slightly
// faster; requires idempotent handlers.
func WithoutAckDedup() Option {
	return func(o *clientOptions) {
		o.releaseOwnedStore()
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

// WithWriteQueue sets the outbound queue depth and the wait budget.
//
// cap is how many frames may sit unsent before the queue is full. It is the
// memory-vs-tolerance dial: deeper absorbs longer broker stalls at the cost
// of more frames held in RAM. Zero keeps conn.DefaultWriteQueueCap (4096).
//
// maxBlock caps how long a blocking Publish waits for room when its context
// carries no deadline of its own, after which it returns ErrQueueFull —
// transient backpressure, not a closed connection; the caller may retry. Same
// role as Kafka's max.block.ms. Zero keeps conn.DefaultMaxBlock (5s);
// negative means wait forever, which has to be asked for explicitly because
// it is how a stalled broker turns into a hung publisher.
//
// Neither value affects PublishAsync / PublishFireAndForget: those never
// wait, and report the same transient ErrQueueFull the moment the queue
// is full.
func WithWriteQueue(cap int, maxBlock time.Duration) Option {
	return func(o *clientOptions) {
		o.writeQueueCap = cap
		o.maxBlock = maxBlock
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

// WithAuthToken sets the bearer token sent once per connection, right after
// the Hello frame.
//
// Unset (and ARBITRO_TOKEN empty) sends no Auth frame at all, which is what a
// broker with authentication disabled expects.
//
// The token is re-sent on every reconnect — it travels in the handshake, so a
// reconnect cannot silently drop it. Authentication happens once per
// connection and is never re-checked per message; rotating a credential means
// reconnecting. A wrong token is terminal: the broker replies AuthFailed and
// closes, and the client stops reconnecting instead of hammering the broker
// with a credential that will never work.
func WithAuthToken(token string) Option {
	return func(o *clientOptions) { o.authToken = token }
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
