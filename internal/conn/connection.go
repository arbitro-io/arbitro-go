package conn

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
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
	writeMu sync.Mutex

	// Subscription dispatch: consumer_id → handler
	handlers   sync.Map // map[uint32]DeliverHandler
	onDeliver  func(frame []byte) // raw frame dispatch (for subscription layer)

	closed  atomic.Bool
	done    chan struct{}
	timeout time.Duration
}

// DeliverHandler is called for each Deliver frame routed to a consumer.
type DeliverHandler func(frame []byte)

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
		done:    make(chan struct{}),
		timeout: cfg.Timeout,
	}
	c.seq.Store(1)

	// Send handshake
	if err := c.sendHello(); err != nil {
		conn.Close()
		return nil, err
	}

	// Start read loop
	go c.readLoop()

	return c, nil
}

// NextSeq returns the next monotonically increasing sequence number.
func (c *Connection) NextSeq() uint64 {
	return c.seq.Add(1) - 1
}

// Send writes a raw frame to the connection (thread-safe).
func (c *Connection) Send(frame []byte) error {
	if c.closed.Load() {
		return errors.New("arbitro: connection closed")
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	_, err := c.conn.Write(frame)
	return err
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
		// Read header
		headerBuf := make([]byte, proto.HeaderSize)
		if _, err := io.ReadFull(reader, headerBuf); err != nil {
			return
		}

		hdr := proto.DecodeHeader(headerBuf)

		// Read body
		var body []byte
		if hdr.MsgLen > 0 {
			body = make([]byte, hdr.MsgLen)
			if _, err := io.ReadFull(reader, body); err != nil {
				return
			}
		}

		// Build full frame for dispatch
		frame := make([]byte, proto.HeaderSize+int(hdr.MsgLen))
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
		// Fanout batch: dispatch each entry as a virtual deliver
		if c.onDeliver != nil {
			c.dispatchBatch(body)
		}

	case proto.ActionPong:
		// Heartbeat response — no action needed

	case proto.ActionCronFire:
		// Cron fire: resolve via pending (uses seq correlation)
		c.pending.Resolve(hdr.Seq, frame)
	}
}

func (c *Connection) dispatchBatch(body []byte) {
	if len(body) < 4 {
		return
	}
	count := binary.LittleEndian.Uint16(body[0:2])
	off := 4 // skip count(2) + pad(2)

	for i := 0; i < int(count); i++ {
		if off+24 > len(body) {
			break
		}
		// Entry: consumer_id(4) + deliver_seq(8) + subject_len(2) + reply_len(2) + data_len(4) + subject_hash(4)
		dataLen := binary.LittleEndian.Uint32(body[off+16 : off+20])
		entryEnd := off + 24 + int(dataLen)
		if entryEnd > len(body) {
			break
		}
		// Reconstruct as a synthetic Deliver frame for the handler
		// For now, pass the raw batch frame — subscription layer handles it
		off = entryEnd
	}

	// Pass entire batch frame to the deliver handler for batch processing
	if c.onDeliver != nil {
		// The subscription layer distinguishes batch vs single by checking action in header
		fullFrame := make([]byte, proto.HeaderSize+len(body))
		proto.EncodeHeader(fullFrame, proto.Header{
			Action: proto.ActionRepBatch,
			MsgLen: uint32(len(body)),
		})
		copy(fullFrame[proto.HeaderSize:], body)
		c.onDeliver(fullFrame)
	}
}
