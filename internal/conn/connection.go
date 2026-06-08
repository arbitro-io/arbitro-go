package conn

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"time"

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

	// Subscription dispatch: consumer_id → handler
	onDeliver func(frame []byte) // raw frame dispatch (for subscription layer)

	// Diagnostics
	BatchRecv atomic.Uint64

	closed atomic.Bool
	done   chan struct{}
	timeout time.Duration
}

// Config holds connection parameters.
type Config struct {
	Addr    string
	Timeout time.Duration
}

// Dial creates a new connection to the broker.
func Dial(ctx context.Context, cfg Config) (*Connection, error) {
	d := net.Dialer{Timeout: cfg.Timeout}
	conn, err := d.DialContext(ctx, "tcp", cfg.Addr)
	if err != nil {
		return nil, fmt.Errorf("arbitro: dial %s: %w", cfg.Addr, err)
	}

	c := &Connection{
		addr:    cfg.Addr,
		conn:    conn,
		pending: NewPendingMap(),
		writeCh: make(chan []byte, writeQueueCap),
		done:    make(chan struct{}),
		timeout: cfg.Timeout,
	}
	c.seq.Store(1)

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

	return c, nil
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

// SetDeliverHandler sets the raw deliver dispatch function.
func (c *Connection) SetDeliverHandler(fn func(frame []byte)) {
	c.onDeliver = fn
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
		// Read 16-byte header (same size for both v2 Header and Envelope)
		headerBuf := make([]byte, proto.HeaderSize)
		if _, err := io.ReadFull(reader, headerBuf); err != nil {
			return
		}

		// Peek action to determine header format
		action := binary.LittleEndian.Uint16(headerBuf[0:2])

		var hdr proto.Header
		var msgLen uint32

		if proto.IsEnvelopeAction(action) {
			// Envelope format: msg_len at offset 8-11
			env := proto.DecodeEnvelope(headerBuf)
			msgLen = env.MsgLen
			// Convert to Header for dispatch compatibility
			hdr = proto.Header{
				Action: env.Action,
				Flags:  env.Flags,
				MsgLen: env.MsgLen,
			}
		} else {
			hdr = proto.DecodeHeader(headerBuf)
			msgLen = hdr.MsgLen
		}

		// Read body
		var body []byte
		if msgLen > 0 {
			body = make([]byte, msgLen)
			if _, err := io.ReadFull(reader, body); err != nil {
				return
			}
		}

		// Build full frame for dispatch
		frame := make([]byte, proto.HeaderSize+int(msgLen))
		copy(frame, headerBuf)
		if body != nil {
			copy(frame[proto.HeaderSize:], body)
		}

		c.dispatch(hdr, frame, body)
	}
}

func (c *Connection) dispatch(hdr proto.Header, frame, body []byte) {
	switch hdr.Action {
	case proto.ActionRepOk, proto.ActionRepError:
		// Resolve pending request by seq
		c.pending.Resolve(hdr.Seq, frame)

	case proto.ActionDeliver:
		if c.onDeliver != nil {
			c.onDeliver(frame)
		}

	case proto.ActionRepBatch, proto.ActionFanoutBatch:
		// Batch delivery: dispatch each entry
		if c.onDeliver != nil {
			c.dispatchBatch(frame, body)
		}

	case proto.ActionPong:
		// Heartbeat response — no action needed

	case proto.ActionCronFire:
		// Cron fire: resolve via pending (uses seq correlation)
		c.pending.Resolve(hdr.Seq, frame)

	}
}

func (c *Connection) dispatchBatch(frame, body []byte) {
	c.BatchRecv.Add(1)
	// Pass the full frame (including envelope header) to the deliver handler
	if c.onDeliver != nil {
		c.onDeliver(frame)
	}
}
