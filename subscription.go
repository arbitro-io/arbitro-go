package arbitro

import (
	"context"
	"sync"
	"time"

	"github.com/arbitro-io/arbitro-go/internal/proto"
)

// Subscription represents an active message subscription.
type Subscription struct {
	client     *Client
	consumerID uint32
	ch         chan *Msg
	handler    func(*Msg)
	closed     chan struct{}
	closeOnce  sync.Once
}

// Messages returns the delivery channel. Range over it for push-mode consumption.
// The channel is closed when the subscription is closed or the connection drops.
func (s *Subscription) Messages() <-chan *Msg {
	return s.ch
}

// Fetch pulls up to count messages with a timeout. Returns partial results on timeout.
func (s *Subscription) Fetch(ctx context.Context, count int) ([]*Msg, error) {
	msgs := make([]*Msg, 0, count)
	for i := 0; i < count; i++ {
		select {
		case msg, ok := <-s.ch:
			if !ok {
				return msgs, nil
			}
			msgs = append(msgs, msg)
		case <-ctx.Done():
			return msgs, ctx.Err()
		}
	}
	return msgs, nil
}

// Close stops the subscription and unsubscribes from the broker.
func (s *Subscription) Close() {
	s.closeOnce.Do(func() {
		close(s.closed)
		s.client.unregisterSubscription(s.consumerID)
		// Send Unsubscribe frame
		seq := s.client.conn.NextSeq()
		frame, _ := proto.EncodeUnsubscribe(seq, s.consumerID)
		_ = s.client.conn.Send(frame)
		s.client.activeSubs.Add(^uint64(0)) // decrement
		close(s.ch)
	})
}

// Msg represents a delivered message. Zero-copy: Subject/Data are slices into the frame buffer.
type Msg struct {
	frame       []byte
	consumerID  uint32
	subjectHash uint32
	seq         uint64
	subjectOff  int
	subjectLen  int
	payloadOff  int
	payloadLen  int
	client      *Client
	acked       bool
}

// Subject returns the message subject as a string.
func (m *Msg) Subject() string {
	return string(m.frame[m.subjectOff : m.subjectOff+m.subjectLen])
}

// SubjectBytes returns the raw subject bytes (no allocation).
func (m *Msg) SubjectBytes() []byte {
	return m.frame[m.subjectOff : m.subjectOff+m.subjectLen]
}

// Data returns the message payload (zero-copy slice into frame buffer).
func (m *Msg) Data() []byte {
	return m.frame[m.payloadOff : m.payloadOff+m.payloadLen]
}

// Seq returns the delivery sequence number.
func (m *Msg) Seq() uint64 {
	return m.seq
}

// ConsumerID returns the consumer that received this message.
func (m *Msg) ConsumerID() uint32 {
	return m.consumerID
}

// Dup returns true if this is a redelivery.
func (m *Msg) Dup() bool {
	hdr := proto.DecodeHeader(m.frame)
	return hdr.Flags&proto.FlagDup != 0
}

// Ack acknowledges the message (batched for throughput — flushed every 1ms or 64 acks).
func (m *Msg) Ack() {
	if m.acked {
		return
	}
	m.acked = true
	m.client.conn.AckBatch.Ack(m.consumerID, m.subjectHash, m.seq)
	m.client.acksSent.Add(1)
}

// Nack negatively acknowledges — broker requeues immediately.
func (m *Msg) Nack() {
	if m.acked {
		return
	}
	m.acked = true
	seq := m.client.conn.NextSeq()
	frame := proto.EncodeNack(seq, m.consumerID, m.subjectHash, m.seq)
	_ = m.client.conn.Send(frame)
	m.client.nacksSent.Add(1)
}

// NackDelay negatively acknowledges with a redelivery delay.
func (m *Msg) NackDelay(d time.Duration) {
	if m.acked {
		return
	}
	m.acked = true
	seq := m.client.conn.NextSeq()
	entry := proto.NackEntry{
		Seq:         m.seq,
		SubjectHash: m.subjectHash,
		DelayMs:     uint32(d.Milliseconds()),
	}
	frame := proto.EncodeBatchNack(seq, m.consumerID, []proto.NackEntry{entry})
	_ = m.client.conn.Send(frame)
	m.client.nacksSent.Add(1)
}

// Copy creates a long-lived copy of the message data (escapes sync.Pool lifecycle).
func (m *Msg) Copy() MsgCopy {
	subj := make([]byte, m.subjectLen)
	copy(subj, m.frame[m.subjectOff:m.subjectOff+m.subjectLen])
	data := make([]byte, m.payloadLen)
	copy(data, m.frame[m.payloadOff:m.payloadOff+m.payloadLen])
	return MsgCopy{
		Subject: string(subj),
		Data:    data,
		Seq:     m.seq,
	}
}

// MsgCopy is a heap-allocated copy safe to hold indefinitely.
type MsgCopy struct {
	Subject string
	Data    []byte
	Seq     uint64
}

// Subscribe creates a consumer (if needed) and starts receiving messages.
func (c *Client) Subscribe(ctx context.Context, stream string, cfg ConsumerConfig, opts ...SubscribeOption) (*Subscription, error) {
	so := subscribeOptions{}
	for _, fn := range opts {
		fn(&so)
	}

	// Resolve or create consumer
	consumerID, err := c.ensureConsumer(ctx, stream, cfg)
	if err != nil {
		return nil, err
	}

	sub := &Subscription{
		client:     c,
		consumerID: consumerID,
		ch:         make(chan *Msg, 256),
		handler:    so.handler,
		closed:     make(chan struct{}),
	}

	// Register subscription in dispatch table
	c.registerSubscription(consumerID, sub)
	c.activeSubs.Add(1)

	// Send Subscribe frame
	var filters [][]byte
	if cfg.Filter != "" {
		filters = [][]byte{[]byte(c.prefixed(cfg.Filter))}
	}
	seq := c.conn.NextSeq()
	frame, err := proto.EncodeSubscribe(seq, consumerID, filters)
	if err != nil {
		return nil, err
	}
	_, err = c.conn.SendExpectReply(ctx, frame, seq)
	if err != nil {
		return nil, err
	}

	return sub, nil
}

func (c *Client) ensureConsumer(ctx context.Context, stream string, cfg ConsumerConfig) (uint32, error) {
	// Try to get existing consumer first
	streamID, err := c.resolveStreamID(ctx, stream)
	if err != nil {
		return 0, err
	}

	name := cfg.Name
	if name == "" {
		name = stream
	}
	group := cfg.Group
	if !cfg.Fanout && group == "" {
		group = name
	}

	subjectLimits := make([]proto.SubjectLimitJSON, len(cfg.MaxSubjectInflights))
	for i, sl := range cfg.MaxSubjectInflights {
		subjectLimits[i] = proto.SubjectLimitJSON{
			Pattern: bytesArr([]byte(sl.Pattern)),
			Limit:   sl.Limit,
		}
	}

	var deliverMode uint32 = 1 // Queue
	if cfg.Fanout {
		deliverMode = 0 // Fanout
	}

	seq := c.conn.NextSeq()
	frame, err := proto.EncodeCreateConsumer(
		seq, streamID,
		[]byte(name), []byte(group), []byte(c.prefixed(cfg.Filter)),
		cfg.MaxInflight, cfg.AckPolicy, cfg.DeliverPolicy, deliverMode,
		uint32(cfg.AckWait.Milliseconds()), cfg.StartSeq,
		subjectLimits,
	)
	if err != nil {
		return 0, err
	}

	reply, err := c.conn.SendExpectReply(ctx, frame, seq)
	if err != nil {
		return 0, err
	}
	if err := c.checkReply(reply); err != nil {
		if !IsAlreadyExists(err) {
			return 0, err
		}
		// Consumer exists — resolve its real ID via GetConsumer
		return c.resolveConsumerID(ctx, streamID, name)
	}
	body := reply[proto.HeaderSize:]
	if len(body) < 8 {
		return 0, &ArbitroError{Code: ErrCodeInternalError, Message: "create consumer: reply body too short"}
	}
	consumerID := uint32(proto.RepOkRefSeq(body))
	return consumerID, nil
}

func (c *Client) registerSubscription(consumerID uint32, sub *Subscription) {
	c.subs.Store(consumerID, sub)
}

func (c *Client) unregisterSubscription(consumerID uint32) {
	c.subs.Delete(consumerID)
}

func bytesArr(b []byte) []int {
	arr := make([]int, len(b))
	for i, v := range b {
		arr[i] = int(v)
	}
	return arr
}
