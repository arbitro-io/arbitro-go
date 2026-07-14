package arbitro

import (
	"context"
	"time"

	"github.com/arbitro-io/arbitro-go/internal/proto"
)

// CronFire is the payload delivered when a cron triggers.
type CronFire struct {
	Name      string    // cron name
	Time      time.Time // scheduled fire time
	Index     uint64    // monotonic fire counter
	Partition uint32    // assigned partition (for sharded crons)
}

// CronHandler processes a cron fire event. Return nil to ack, error to signal failure.
type CronHandler func(fire CronFire) error

// CronBuilder constructs a cron job with fluent API.
type CronBuilder struct {
	client   *Client
	name     string
	expr     string
	tz       string
	timeout  time.Duration
	overlap  bool
}

// CronHandle is the live handle to a running cron. Use Stop() to deregister.
type CronHandle struct {
	client *Client
	name   string
	cancel context.CancelFunc
	done   chan struct{}
}

// Cron starts building a cron job.
func (c *Client) Cron(name string) *CronBuilder {
	return &CronBuilder{
		client:  c,
		name:    name,
		overlap: false,
		timeout: 30 * time.Second,
	}
}

// Every sets the cron expression (standard 5-field or extended).
func (b *CronBuilder) Every(expr string) *CronBuilder {
	b.expr = expr
	return b
}

// Timezone sets the IANA timezone for schedule evaluation.
func (b *CronBuilder) Timezone(tz string) *CronBuilder {
	b.tz = tz
	return b
}

// Timeout sets the maximum time allowed for a handler invocation.
func (b *CronBuilder) Timeout(d time.Duration) *CronBuilder {
	b.timeout = d
	return b
}

// Overlap sets whether concurrent fires are allowed.
func (b *CronBuilder) Overlap(allow bool) *CronBuilder {
	b.overlap = allow
	return b
}

// Run registers the cron on the broker and starts dispatching fires to the handler.
func (b *CronBuilder) Run(ctx context.Context, handler CronHandler) (*CronHandle, error) {
	// Register cron on broker
	timeout := b.timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	timeoutMs := uint32(timeout.Milliseconds())

	seq := b.client.getConn().NextSeq()
	frame, err := proto.EncodeCreateCron(seq, []byte(b.name), b.expr, b.tz, timeoutMs, b.overlap)
	if err != nil {
		return nil, err
	}

	reply, err := b.client.getConn().SendExpectReply(ctx, frame, seq)
	if err != nil {
		return nil, err
	}
	if err := b.client.checkReply(reply); err != nil {
		return nil, err
	}

	// Register fire handler
	childCtx, cancel := context.WithCancel(ctx)
	handle := &CronHandle{
		client: b.client,
		name:   b.name,
		cancel: cancel,
		done:   make(chan struct{}),
	}

	// Register in client's cron registry
	b.client.cronMu.Lock()
	if b.client.crons == nil {
		b.client.crons = make(map[string]*cronEntry)
	}
	b.client.crons[b.name] = &cronEntry{
		name:    b.name,
		expr:    b.expr,
		tz:      b.tz,
		overlap: b.overlap,
		handler: handler,
		timeout: timeout,
		handle:  handle,
	}
	b.client.cronMu.Unlock()

	// Monitor context cancellation for cleanup
	go func() {
		defer close(handle.done)
		<-childCtx.Done()
	}()

	return handle, nil
}

// Stop deregisters the cron from the broker.
func (h *CronHandle) Stop(ctx context.Context) error {
	h.cancel()
	<-h.done

	// Remove from client registry
	h.client.cronMu.Lock()
	delete(h.client.crons, h.name)
	h.client.cronMu.Unlock()

	// Send DeleteCron to broker
	seq := h.client.getConn().NextSeq()
	frame, err := proto.EncodeDeleteCron(seq, []byte(h.name))
	if err != nil {
		return err
	}
	reply, err := h.client.getConn().SendExpectReply(ctx, frame, seq)
	if err != nil {
		return err
	}
	return h.client.checkReply(reply)
}

// --- internal ---

type cronEntry struct {
	// name/expr/tz/overlap are retained (in addition to the CronBuilder that
	// created them) so the reconnect supervisor can replay EncodeCreateCron
	// against a freshly redialed connection (G02 cron replay).
	name    string
	expr    string
	tz      string
	overlap bool

	handler CronHandler
	timeout time.Duration
	handle  *CronHandle
}

// cronRegistry holds registered cron handlers. Added to Client struct via extension.
// The cronMu and crons fields are added to the Client in client_cron.go.

// dispatchCronFire is called by the connection dispatch when a CronFire frame
// arrives (registered via conn.SetCronFireHandler in Connect). CronFire uses
// a fixed BINARY body layout (see arbitro-proto wire/cron.rs), not JSON.
func (c *Client) dispatchCronFire(frame []byte) {
	if len(frame) < proto.HeaderSize {
		return
	}
	body := frame[proto.HeaderSize:]

	name, fireTimeMs, fireCount, err := proto.DecodeCronFireBody(body)
	if err != nil {
		return
	}

	c.cronMu.Lock()
	entry, ok := c.crons[name]
	c.cronMu.Unlock()

	if !ok {
		return
	}

	go c.runCronHandler(entry, name, fireTimeMs, fireCount)
}

// runCronHandler invokes the user's cron handler with the configured
// timeout enforced. The handler runs in its own goroutine so a select can
// race it against ctx.Done(); if the timeout fires first, an error CronAck
// is sent immediately (releasing the fire for another worker) and the
// eventual handler result — even if it later succeeds — is discarded and
// never produces a second, contradicting ack.
func (c *Client) runCronHandler(entry *cronEntry, name string, fireTimeMs, fireCount uint64) {
	timeout := entry.timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	fire := CronFire{
		Name:  name,
		Time:  time.UnixMilli(int64(fireTimeMs)),
		Index: fireCount,
	}

	resultCh := make(chan error, 1)
	go func() {
		resultCh <- entry.handler(fire)
	}()

	var ackOK bool
	select {
	case err := <-resultCh:
		ackOK = err == nil
	case <-ctx.Done():
		// Handler timed out — nack so the broker can reassign the fire.
		ackOK = false
	}

	ackSeq := c.getConn().NextSeq()
	ackFrame, ackErr := proto.EncodeCronAck(ackSeq, []byte(name), ackOK)
	if ackErr == nil {
		_ = c.getConn().Send(ackFrame)
	}
}

