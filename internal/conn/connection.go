package conn

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"time"

	"github.com/arbitro-io/arbitro-go/internal/ackrel"
	"github.com/arbitro-io/arbitro-go/internal/proto"
)

// Connection manages a single TCP connection to the Arbitro broker.
type Connection struct {
	addr    string
	conn    net.Conn
	seq     atomic.Uint64
	pending *PendingMap

	// Channel-based writer — replaces writeMu for lock-free publish path.
	writeCh chan []byte

	// Ack batcher — batches individual acks into BatchAck frames.
	AckBatch *AckBatcher

	// AckRelay is the client's ack-reliability hot tier (G01). It outlives
	// any single Connection (the owning Client re-attaches it after every
	// reconnect), so this field is set by the caller after Dial returns —
	// never allocated here. Nil-safe: dispatch/ackbatcher no-op if unset.
	AckRelay *ackrel.Relay

	// Subscription dispatch. onDeliver handles single Deliver (0x0200) with
	// the already-decoded header + full frame; onBatchDeliver handles
	// RepBatch/FanoutBatch (0x0205/0x0207) with just the body slice (envelope
	// header is not needed by the subscription layer). Split callbacks avoid
	// the redundant DecodeHeader that used to happen in the client layer
	// (Go-HP5 audit finding).
	onDeliver      func(hdr proto.Header, frame []byte)
	onBatchDeliver func(body []byte)

	// Cron fire dispatch: raw frame dispatch (for cron layer)
	onCronFire func(frame []byte)

	// onAckConfirm fires when the broker reports its ack cursor for a
	// consumer (AckBatchResp.new_cursor / AckStateRep.cursor). The client
	// uses it to bulk-drop dedup entries ≤ cursor from the ackstore — the
	// broker will never redeliver at or below it, so the automatic ack path
	// keeps the dedup set tiny with no periodic job.
	onAckConfirm func(consumerID uint32, cursor uint64)

	// Diagnostics
	BatchRecv atomic.Uint64

	closed  atomic.Bool
	done    chan struct{}
	timeout time.Duration

	// Heartbeat / dead-connection watchdog (G09). lastPongMs is stamped on
	// every inbound Pong (and initialized at Dial time so a broker that
	// never responds is still caught after keepAliveTimeout).
	lastPongMs        atomic.Int64
	keepAliveInterval time.Duration
	keepAliveTimeout  time.Duration
}

// Config holds connection parameters.
type Config struct {
	Addr    string
	Timeout time.Duration

	// KeepAliveInterval is how often a Ping is sent while idle. <= 0 disables
	// the heartbeat goroutine.
	KeepAliveInterval time.Duration
	// KeepAliveTimeout is how long to wait without a Pong before the
	// connection is declared dead and closed.
	KeepAliveTimeout time.Duration

	// TLS, when non-nil, upgrades the broker connection to TLS (G15). Nil
	// (the zero value) dials a plain TCP connection, matching prior
	// behavior when WithTLS was never wired into the dial path.
	TLS *tls.Config
}

// Dial creates a new connection to the broker. If cfg.TLS is set, the
// connection is established over TLS via tls.DialWithDialer instead of a
// plain net.Dialer — fixes WithTLS(cfg) being a silent no-op (G15).
func Dial(ctx context.Context, cfg Config) (*Connection, error) {
	d := net.Dialer{Timeout: cfg.Timeout}

	var conn net.Conn
	var err error
	if cfg.TLS != nil {
		conn, err = tls.DialWithDialer(&d, "tcp", cfg.Addr, cfg.TLS)
	} else {
		conn, err = d.DialContext(ctx, "tcp", cfg.Addr)
	}
	if err != nil {
		return nil, fmt.Errorf("arbitro: dial %s: %w", cfg.Addr, err)
	}

	c := &Connection{
		addr:              cfg.Addr,
		conn:              conn,
		pending:           NewPendingMap(),
		writeCh:           make(chan []byte, writeQueueCap),
		done:              make(chan struct{}),
		timeout:           cfg.Timeout,
		keepAliveInterval: cfg.KeepAliveInterval,
		keepAliveTimeout:  cfg.KeepAliveTimeout,
	}
	c.seq.Store(1)
	// Initialize the watchdog clock at connect time so a broker that never
	// sends a Pong is still caught after keepAliveTimeout (matches Rust's
	// last_pong_ns initialization in conn/heartbeat.rs).
	c.lastPongMs.Store(time.Now().UnixMilli())

	// Send handshake (direct write before writer goroutine starts)
	if err := c.sendHello(); err != nil {
		conn.Close()
		return nil, err
	}

	// Start writer goroutine (drains writeCh → coalesced TCP writes)
	go writeLoop(conn, c.writeCh, c.done)

	// Start ack batcher (batches individual acks → BatchAck frames)
	c.AckBatch = NewAckBatcher(c)

	// Start read loop
	go c.readLoop()

	// Start heartbeat goroutine (Ping every interval, watchdog on Pong
	// staleness). Stopped implicitly when c.done is closed by Close()/
	// readLoop(). Disabled when KeepAliveInterval <= 0.
	if c.keepAliveInterval > 0 {
		go c.heartbeatLoop()
	}

	return c, nil
}

// heartbeatLoop sends a Ping every keepAliveInterval and declares the
// connection dead (closing it) if no Pong has been observed for
// keepAliveTimeout. Mirrors conn/heartbeat.rs:35-77 in the Rust client.
func (c *Connection) heartbeatLoop() {
	ticker := time.NewTicker(c.keepAliveInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.done:
			return
		case <-ticker.C:
			last := c.lastPongMs.Load()
			staleness := time.Now().UnixMilli() - last
			if c.keepAliveTimeout > 0 && staleness > c.keepAliveTimeout.Milliseconds() {
				// Dead connection — close it. This feeds the reconnect
				// supervisor via Done().
				_ = c.Close()
				return
			}
			seq := c.NextSeq()
			frame := proto.EncodePing(seq)
			_ = c.Send(frame)
		}
	}
}

// NextSeq returns the next monotonically increasing sequence number.
func (c *Connection) NextSeq() uint64 {
	return c.seq.Add(1) - 1
}

// Send enqueues a frame for writing (non-blocking, lock-free hot path).
func (c *Connection) Send(frame []byte) error {
	if c.closed.Load() {
		return errors.New("arbitro: connection closed")
	}
	select {
	case c.writeCh <- frame:
		return nil
	default:
		// Channel full — apply backpressure (blocking send with close guard).
		select {
		case c.writeCh <- frame:
			return nil
		case <-c.done:
			return errors.New("arbitro: connection closed")
		}
	}
}

// SendExpectReply sends a frame and waits for the broker's reply (correlated by seq).
func (c *Connection) SendExpectReply(ctx context.Context, frame []byte, seq uint64) ([]byte, error) {
	ch := c.pending.Register(seq)
	if err := c.Send(frame); err != nil {
		c.pending.Remove(seq)
		return nil, err
	}
	select {
	case reply := <-ch:
		if reply == nil {
			return nil, errors.New("arbitro: connection closed while waiting for reply")
		}
		return reply, nil
	case <-ctx.Done():
		c.pending.Remove(seq)
		return nil, ctx.Err()
	}
}

// SetDeliverHandler wires the subscription-layer dispatchers. single fires on
// action 0x0200 with the pre-decoded header + full frame; batch fires on
// 0x0205/0x0207 with the body slice only.
func (c *Connection) SetDeliverHandler(single func(hdr proto.Header, frame []byte), batch func(body []byte)) {
	c.onDeliver = single
	c.onBatchDeliver = batch
}

// SetAckConfirmHandler wires the broker-cursor callback (AckBatchResp /
// AckStateRep). Called with (consumer_id, cursor) so the client can drop
// dedup entries ≤ cursor from its ackstore.
func (c *Connection) SetAckConfirmHandler(fn func(consumerID uint32, cursor uint64)) {
	c.onAckConfirm = fn
}

// SetCronFireHandler sets the raw CronFire dispatch function. Unlike
// ordinary replies, CronFire frames carry a broker-chosen seq that is not
// registered in the PendingMap, so they must be routed through this
// dedicated callback rather than pending.Resolve.
func (c *Connection) SetCronFireHandler(fn func(frame []byte)) {
	c.onCronFire = fn
}

// Close shuts down the connection gracefully.
func (c *Connection) Close() error {
	if c.closed.Swap(true) {
		return nil
	}
	close(c.done)
	c.pending.CloseAll()
	return c.conn.Close()
}

// Done returns a channel that's closed when the connection is terminated.
func (c *Connection) Done() <-chan struct{} {
	return c.done
}

// PendingLen returns the number of in-flight requests.
func (c *Connection) PendingLen() int {
	return c.pending.Len()
}

func (c *Connection) sendHello() error {
	hello := make([]byte, proto.HelloSize)
	proto.EncodeHello(hello, proto.DefaultCaps())
	_, err := c.conn.Write(hello)
	return err
}

// readLoop reads frames from the socket and dispatches them. Single alloc
// per frame (down from 3 mallocs + 2 copies), header decoded in-place — same
// shape as Rust reader's single BytesMut with split_to (transport/reader.rs).
// The frame buffer may be retained by *Msg pointers (delivery) or the pending
// map (RepOk/RepError), so we must allocate fresh; scratch reuse would race
// with those consumers (Go-HP2 audit finding).
func (c *Connection) readLoop() {
	defer func() {
		c.closed.Store(true)
		c.pending.CloseAll()
		select {
		case <-c.done:
		default:
			close(c.done)
		}
	}()

	reader := bufio.NewReaderSize(c.conn, 65536)

	for {
		// Peek 2 bytes to size the frame before allocating. bufio.Reader
		// buffers reads; Peek is cheap and lets us do one alloc = total size,
		// then one ReadFull for the whole frame — no intermediate buffers.
		peek, err := reader.Peek(proto.HeaderSize)
		if err != nil {
			return
		}

		action := binary.LittleEndian.Uint16(peek[0:2])

		var hdr proto.Header
		var msgLen uint32

		if proto.IsEnvelopeAction(action) {
			env := proto.DecodeEnvelope(peek)
			msgLen = env.MsgLen
			hdr = proto.Header{
				Action: env.Action,
				Flags:  env.Flags,
				MsgLen: env.MsgLen,
			}
		} else {
			hdr = proto.DecodeHeader(peek)
			msgLen = hdr.MsgLen
		}

		total := int(proto.HeaderSize) + int(msgLen)
		frame := make([]byte, total)
		if _, err := io.ReadFull(reader, frame); err != nil {
			return
		}

		var body []byte
		if msgLen > 0 {
			body = frame[proto.HeaderSize:total]
		}
		c.dispatch(hdr, frame, body)
	}
}

func (c *Connection) dispatch(hdr proto.Header, frame, body []byte) {
	switch hdr.Action {
	case proto.ActionRepOk, proto.ActionRepError:
		// Resolve pending request by seq
		c.pending.Resolve(hdr.Seq, frame)

	case proto.ActionListStreams, proto.ActionListConsumers:
		// List replies reuse the request's action code as their header action
		// (see arbitro-server dispatch_v2::v2_list_streams / v2_list_consumers).
		// Route them through the pending map by seq like any other reply.
		c.pending.Resolve(hdr.Seq, frame)

	case proto.ActionDeliver:
		if c.onDeliver != nil {
			c.onDeliver(hdr, frame)
		}

	case proto.ActionRepBatch, proto.ActionFanoutBatch:
		if c.onBatchDeliver != nil {
			c.BatchRecv.Add(1)
			c.onBatchDeliver(body)
		}

	case proto.ActionPong:
		// Heartbeat response — stamp the watchdog clock (G09).
		c.lastPongMs.Store(time.Now().UnixMilli())

	case proto.ActionCronFire:
		// Cron fire: broker-initiated push, not correlated to any pending
		// request seq. Route to the dedicated cron dispatcher.
		if c.onCronFire != nil {
			c.onCronFire(frame)
		}

	case proto.ActionAckStateRep:
		c.handleAckStateRep(body)

	case proto.ActionAckBatchResp:
		c.handleAckBatchResp(body)

	}
}

// handleAckStateRep reconciles the ack-reliability hot tier against the
// broker's authoritative cursor/retention snapshot for one consumer (reply
// to an AckStateReq sent on reconnect). Mirrors dispatch_ack_state_rep in
// the Rust client's transport/reader.rs.
func (c *Connection) handleAckStateRep(body []byte) {
	if c.AckRelay == nil {
		return
	}
	consumerID, generation, cursor, lowSeq, _, _, err := proto.DecodeAckStateRep(body)
	if err != nil {
		return
	}
	if generation != c.AckRelay.Generation(consumerID) {
		// Local hot state is stale relative to the broker — wipe wholesale
		// rather than reconcile entry-by-entry (matches AckRelay::ensure).
		c.AckRelay.EnsureGeneration(consumerID, generation)
		return
	}

	c.AckRelay.ConfirmUpTo(consumerID, cursor)
	if lowSeq > 0 {
		// Below the broker's retention floor — will never be confirmable.
		c.AckRelay.ConfirmUpTo(consumerID, lowSeq-1)
	}
	if c.onAckConfirm != nil {
		c.onAckConfirm(consumerID, cursor)
		if lowSeq > 0 {
			c.onAckConfirm(consumerID, lowSeq-1)
		}
	}

	seqs := c.AckRelay.PendingSeqs(consumerID)
	for start := 0; start < len(seqs); start += proto.AckBatchMaxSeqs {
		end := start + proto.AckBatchMaxSeqs
		if end > len(seqs) {
			end = len(seqs)
		}
		seq := c.NextSeq()
		frame := proto.EncodeAckBatch(seq, consumerID, generation, 0, seqs[start:end])
		_ = c.Send(frame)
	}
}

// handleAckBatchResp purges the broker-confirmed range from the hot tier.
// Mirrors dispatch_ack_batch_resp in the Rust client.
func (c *Connection) handleAckBatchResp(body []byte) {
	if c.AckRelay == nil {
		return
	}
	consumerID, newCursor, _, _, _, _, _, err := proto.DecodeAckBatchResp(body)
	if err != nil {
		return
	}
	c.AckRelay.ConfirmUpTo(consumerID, newCursor)
	if c.onAckConfirm != nil {
		c.onAckConfirm(consumerID, newCursor)
	}
}

func (c *Connection) dispatchBatch(frame, body []byte) {
	c.BatchRecv.Add(1)
	if c.onBatchDeliver != nil {
		c.onBatchDeliver(body)
	}
}
