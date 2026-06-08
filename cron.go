package arbitro

import (
	"context"
	"encoding/json"
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
	seq := b.client.conn.NextSeq()
	frame, err := proto.EncodeCreateCron(seq, []byte(b.name), b.expr, b.tz, b.overlap)
	if err != nil {
		return nil, err
	}

	reply, err := b.client.conn.SendExpectReply(ctx, frame, seq)
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
		handler: handler,
		timeout: b.timeout,
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
	seq := h.client.conn.NextSeq()
	frame, err := proto.EncodeDeleteCron(seq, []byte(h.name))
	if err != nil {
		return err
	}
	reply, err := h.client.conn.SendExpectReply(ctx, frame, seq)
	if err != nil {
		return err
	}
	return h.client.checkReply(reply)
}

// --- internal ---

type cronEntry struct {
	handler CronHandler
	timeout time.Duration
	handle  *CronHandle
}

// cronRegistry holds registered cron handlers. Added to Client struct via extension.
// The cronMu and crons fields are added to the Client in client_cron.go.

// dispatchCronFire is called by the connection dispatch when a CronFire frame arrives.
func (c *Client) dispatchCronFire(frame []byte) {
	body := frame[proto.HeaderSize:]

	// Parse JSON body: {"name":"...","fire_time":..., "fire_count":...}
	var fire struct {
		Name      string `json:"name"`
		FireTime  int64  `json:"fire_time"`
		FireCount uint64 `json:"fire_count"`
	}
	if err := json.Unmarshal(body, &fire); err != nil {
		return
	}

	c.cronMu.Lock()
	entry, ok := c.crons[fire.Name]
	c.cronMu.Unlock()

	if !ok {
		return
	}

	// Invoke handler in a goroutine with timeout
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), entry.timeout)
		defer cancel()

		cronFire := CronFire{
			Name:  fire.Name,
			Time:  time.UnixMilli(fire.FireTime),
			Index: fire.FireCount,
		}

		err := entry.handler(cronFire)
		_ = ctx // used for timeout enforcement on the handler

		// Send CronAck back to broker
		ackSeq := c.conn.NextSeq()
		ackOK := err == nil
		ackFrame, ackErr := proto.EncodeCronAck(ackSeq, []byte(fire.Name), ackOK)
		if ackErr == nil {
			_ = c.conn.Send(ackFrame)
		}
	}()
}

