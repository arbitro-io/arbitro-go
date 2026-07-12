// Package arbitro provides the official Go client for the Arbitro message broker.
//
// Connect to a broker and start publishing/subscribing:
//
//	client, err := arbitro.Connect(ctx, "127.0.0.1:9898")
//	defer client.Close()
//
//	err = client.Publish(ctx, "orders", "orders.created", payload)
//
//	sub, err := client.Subscribe(ctx, "orders", arbitro.ConsumerConfig{
//	    Name:   "workers",
//	    Filter: "orders.>",
//	})
//	for msg := range sub.Messages() {
//	    msg.Ack()
//	}
package arbitro

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/arbitro-io/arbitro-go/internal/conn"
	"github.com/arbitro-io/arbitro-go/internal/proto"
)

// Client is the main handle to the Arbitro broker.
type Client struct {
	conn    *conn.Connection
	opts    clientOptions
	streams streamCache

	// Subscription dispatch: consumer_id → subscription
	subs   sync.Map // map[uint32]*Subscription

	// Cron registry
	cronMu sync.Mutex
	crons  map[string]*cronEntry

	// Request/reply correlation (standalone, outside Service)
	reqCorrID  atomic.Uint64
	reqPending sync.Map // map[uint64]chan []byte

	// metrics
	publishesSent  atomic.Uint64
	deliveriesRecv atomic.Uint64
	acksSent       atomic.Uint64
	nacksSent      atomic.Uint64
	reconnects     atomic.Uint64
	activeSubs     atomic.Uint64
}

// Connect establishes a connection to the Arbitro broker.
func Connect(ctx context.Context, addr string, opts ...Option) (*Client, error) {
	o := defaultOptions()
	for _, fn := range opts {
		fn(&o)
	}

	c, err := conn.Dial(ctx, conn.Config{
		Addr:    addr,
		Timeout: o.timeout,
	})
	if err != nil {
		return nil, err
	}

	client := &Client{
		conn: c,
		opts: o,
		streams: streamCache{
			cache: make(map[string]uint32),
		},
	}

	// Wire up deliver dispatch
	c.SetDeliverHandler(client.handleDeliver)

	// Wire up cron fire dispatch
	c.SetCronFireHandler(client.dispatchCronFire)

	return client, nil
}

// Close gracefully shuts down the client connection.
func (c *Client) Close() error {
	return c.conn.Close()
}

// Metrics returns a point-in-time snapshot of client counters.
func (c *Client) Metrics() MetricsSnapshot {
	return MetricsSnapshot{
		PublishesSent:   c.publishesSent.Load(),
		DeliveriesRecv:  c.deliveriesRecv.Load(),
		AcksSent:        c.acksSent.Load(),
		NacksSent:       c.nacksSent.Load(),
		Reconnects:      c.reconnects.Load(),
		PendingRequests: uint64(c.conn.PendingLen()),
		ActiveSubs:      c.activeSubs.Load(),
		BatchFramesRecv: c.conn.BatchRecv.Load(),
	}
}

// Publish sends a message and waits for broker confirmation.
func (c *Client) Publish(ctx context.Context, stream, subject string, payload []byte, opts ...PublishOption) error {
	po := publishOptions{}
	for _, fn := range opts {
		fn(&po)
	}

	streamID, err := c.resolveStreamID(ctx, stream)
	if err != nil {
		return err
	}

	subj := c.prefixed(subject)
	seq := c.conn.NextSeq()
	frame := proto.EncodePublish(seq, streamID, []byte(subj), []byte(po.msgID), payload, proto.FlagAckReq)

	reply, err := c.conn.SendExpectReply(ctx, frame, seq)
	if err != nil {
		return err
	}

	c.publishesSent.Add(1)
	return c.checkReply(reply)
}

// PublishAsync sends a message without waiting for confirmation (fire-and-forget).
// Uses the write channel — no mutex, no syscall on the calling goroutine.
func (c *Client) PublishAsync(stream, subject string, payload []byte, opts ...PublishOption) {
	po := publishOptions{}
	for _, fn := range opts {
		fn(&po)
	}

	streamID, _ := c.streams.get(stream)
	subj := c.prefixed(subject)
	seq := c.conn.NextSeq()
	frame := proto.EncodePublish(seq, streamID, []byte(subj), []byte(po.msgID), payload, proto.FlagNone)
	_ = c.conn.Send(frame)
	c.publishesSent.Add(1)
}

// PublishFireAndForget is the fastest publish path — uses a pre-resolved stream ID,
// no options parsing, no string conversion overhead. Equivalent to Rust's publish().
func (c *Client) PublishFireAndForget(streamID uint32, subject string, payload []byte) error {
	seq := c.conn.NextSeq()
	frame := proto.EncodePublish(seq, streamID, []byte(subject), nil, payload, proto.FlagNone)
	err := c.conn.Send(frame)
	if err == nil {
		c.publishesSent.Add(1)
	}
	return err
}

// publishBatchMax mirrors Rust's PUBLISH_BATCH_MAX (arbitro-client-tokio) —
// the largest number of entries the broker accepts in a single PubBatch frame.
const publishBatchMax = 256

// encodeBatchEntries maps public BatchEntry to the proto wire form with the
// client's subject prefix applied.
func (c *Client) encodeBatchEntries(entries []BatchEntry) []proto.BatchEntry {
	out := make([]proto.BatchEntry, len(entries))
	for i := range entries {
		out[i] = proto.BatchEntry{
			Subject: []byte(c.prefixed(entries[i].Subject)),
			MsgID:   []byte(entries[i].MsgID),
			Payload: entries[i].Payload,
		}
	}
	return out
}

// PublishBatchAsync sends a batch of messages without waiting for confirmation.
// Equivalent to Rust's publish_batch() — fire-and-forget, write-coalesced.
// Batches larger than publishBatchMax entries are split into multiple frames,
// matching the Rust client's automatic chunking.
func (c *Client) PublishBatchAsync(stream string, entries []BatchEntry) {
	if len(entries) == 0 {
		return
	}
	streamID, _ := c.streams.get(stream)
	protoEntries := c.encodeBatchEntries(entries)

	for start := 0; start < len(protoEntries); start += publishBatchMax {
		end := start + publishBatchMax
		if end > len(protoEntries) {
			end = len(protoEntries)
		}
		seq := c.conn.NextSeq()
		frame := proto.EncodePublishBatch(seq, streamID, protoEntries[start:end], proto.FlagNone)
		_ = c.conn.Send(frame)
	}
	c.publishesSent.Add(1)
}

// PublishBatch atomically publishes multiple messages. Returns the first seq
// assigned by the broker. Batches larger than publishBatchMax are chunked: the
// first chunk is sent with AckReq (so the caller can await first_seq); the
// remaining chunks are fire-and-forget, matching the Rust client.
func (c *Client) PublishBatch(ctx context.Context, stream string, entries []BatchEntry) (uint64, error) {
	if len(entries) == 0 {
		return 0, &ArbitroError{Code: ErrCodeInternalError, Message: "publish batch: empty entries"}
	}
	streamID, err := c.resolveStreamID(ctx, stream)
	if err != nil {
		return 0, err
	}

	protoEntries := c.encodeBatchEntries(entries)

	// First chunk — synchronous, carries FlagAckReq so we can read first_seq.
	firstEnd := publishBatchMax
	if firstEnd > len(protoEntries) {
		firstEnd = len(protoEntries)
	}
	seq := c.conn.NextSeq()
	frame := proto.EncodePublishBatch(seq, streamID, protoEntries[:firstEnd], proto.FlagAckReq)

	reply, err := c.conn.SendExpectReply(ctx, frame, seq)
	if err != nil {
		return 0, err
	}
	if err := c.checkReply(reply); err != nil {
		return 0, err
	}

	body := reply[proto.HeaderSize:]
	if len(body) < 8 {
		return 0, &ArbitroError{Code: ErrCodeInternalError, Message: "publish batch: reply body too short"}
	}
	firstSeq := proto.RepOkRefSeq(body)

	// Remaining chunks — fire-and-forget, mirroring Rust's publish_batch_sync.
	for start := firstEnd; start < len(protoEntries); start += publishBatchMax {
		end := start + publishBatchMax
		if end > len(protoEntries) {
			end = len(protoEntries)
		}
		s := c.conn.NextSeq()
		f := proto.EncodePublishBatch(s, streamID, protoEntries[start:end], proto.FlagNone)
		if err := c.conn.Send(f); err != nil {
			return firstSeq, err
		}
	}

	return firstSeq, nil
}

// PublishDelayed publishes a message that the broker delivers after the specified delay.
func (c *Client) PublishDelayed(ctx context.Context, stream, subject string, payload []byte, delay time.Duration) error {
	streamID, err := c.resolveStreamID(ctx, stream)
	if err != nil {
		return err
	}

	subj := c.prefixed(subject)
	seq := c.conn.NextSeq()
	frame := proto.EncodePublishDelayed(seq, streamID, []byte(subj), payload, uint64(delay.Milliseconds()), proto.FlagAckReq)

	reply, err := c.conn.SendExpectReply(ctx, frame, seq)
	if err != nil {
		return err
	}
	c.publishesSent.Add(1)
	return c.checkReply(reply)
}

// Request performs a request/reply RPC using the service correlation pattern.
// The stream must already have a service registered (via client.Service(name).Build()).
// For standalone request/reply without a full service, use Service.Request() directly.
func (c *Client) Request(ctx context.Context, stream, subject string, payload []byte, timeout time.Duration) ([]byte, error) {
	streamID, err := c.resolveStreamID(ctx, stream)
	if err != nil {
		return nil, err
	}

	corrID := c.reqCorrID.Add(1)
	subj := c.prefixed(subject)
	replySubject := fmt.Sprintf("_reply.%s.%d", stream, corrID)

	replyTo := make([]byte, 5+len(replySubject))
	replyTo[0] = ReplyToMagic
	binary.LittleEndian.PutUint32(replyTo[1:5], streamID)
	copy(replyTo[5:], replySubject)

	seq := c.conn.NextSeq()
	frame := proto.EncodePublishWithReply(seq, streamID, []byte(subj), replyTo, nil, payload, proto.FlagAckReq)

	ch := make(chan []byte, 1)
	c.reqPending.Store(corrID, ch)
	defer c.reqPending.Delete(corrID)

	tctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	_, err = c.conn.SendExpectReply(tctx, frame, seq)
	if err != nil {
		return nil, err
	}

	select {
	case data := <-ch:
		return data, nil
	case <-tctx.Done():
		return nil, &ArbitroError{Code: ErrCodeTimeout, Message: "request timeout"}
	}
}

// ResolveStreamID resolves the stream name to its broker-assigned numeric ID.
// Cache the result and pass it to PublishFireAndForget for maximum throughput.
func (c *Client) ResolveStreamID(ctx context.Context, stream string) (uint32, error) {
	return c.resolveStreamID(ctx, stream)
}

// --- helpers ---

func (c *Client) prefixed(subject string) string {
	if c.opts.prefix == "" {
		return subject
	}
	return c.opts.prefix + "." + subject
}

func (c *Client) checkReply(frame []byte) error {
	if len(frame) < proto.HeaderSize {
		return &ArbitroError{Code: ErrCodeInternalError, Message: "reply frame too short"}
	}
	hdr := proto.DecodeHeader(frame)
	if hdr.Action == proto.ActionRepError {
		body := frame[proto.HeaderSize:]
		if len(body) < 10 {
			return &ArbitroError{Code: ErrCodeInternalError, Message: "malformed error reply"}
		}
		code := proto.RepErrorCode(body)
		return &ArbitroError{Code: code}
	}
	return nil
}

func (c *Client) resolveStreamID(ctx context.Context, name string) (uint32, error) {
	if id, ok := c.streams.get(name); ok {
		return id, nil
	}
	// GetStream to resolve the ID
	seq := c.conn.NextSeq()
	frame, err := proto.EncodeGetStream(seq, []byte(name))
	if err != nil {
		return 0, err
	}
	reply, err := c.conn.SendExpectReply(ctx, frame, seq)
	if err != nil {
		return 0, err
	}
	if err := c.checkReply(reply); err != nil {
		return 0, err
	}
	body := reply[proto.HeaderSize:]
	if len(body) < 8 {
		return 0, &ArbitroError{Code: ErrCodeInternalError, Message: "get stream: reply body too short"}
	}
	id := uint32(proto.RepOkRefSeq(body))
	c.streams.set(name, id)
	return id, nil
}

func (c *Client) resolveConsumerID(ctx context.Context, streamID uint32, name string) (uint32, error) {
	seq := c.conn.NextSeq()
	frame, err := proto.EncodeGetConsumer(seq, streamID, []byte(name))
	if err != nil {
		return 0, err
	}
	reply, err := c.conn.SendExpectReply(ctx, frame, seq)
	if err != nil {
		return 0, err
	}
	if err := c.checkReply(reply); err != nil {
		return 0, err
	}
	body := reply[proto.HeaderSize:]
	if len(body) < 8 {
		return 0, &ArbitroError{Code: ErrCodeInternalError, Message: "get consumer: reply body too short"}
	}
	return uint32(proto.RepOkRefSeq(body)), nil
}

func (c *Client) handleDeliver(frame []byte) {
	if len(frame) < proto.HeaderSize {
		return
	}

	hdr := proto.DecodeHeader(frame)

	switch hdr.Action {
	case proto.ActionDeliver:
		c.dispatchSingleDeliver(frame, hdr)
	case proto.ActionRepBatch, proto.ActionFanoutBatch:
		c.dispatchBatchDeliver(frame[proto.HeaderSize:])
	}
}

func (c *Client) dispatchSingleDeliver(frame []byte, hdr proto.Header) {
	c.deliveriesRecv.Add(1)

	if len(frame) < proto.HeaderSize+proto.DeliverBodyOffset {
		return
	}

	body := frame[proto.HeaderSize:]
	dh := proto.DecodeDeliverHeader(body)

	sub := c.lookupSub(dh.ConsumerID)
	if sub == nil {
		return
	}

	subjOff := proto.HeaderSize + proto.DeliverBodyOffset
	subjLen := int(dh.SubjectLen)
	payloadOff := subjOff + subjLen
	payloadLen := len(frame) - payloadOff

	msg := &Msg{
		frame:       frame,
		consumerID:  dh.ConsumerID,
		subjectHash: dh.SubjectHash,
		seq:         hdr.Seq,
		subjectOff:  subjOff,
		subjectLen:  subjLen,
		payloadOff:  payloadOff,
		payloadLen:  payloadLen,
		client:      c,
	}

	c.deliverToSub(sub, msg)
}

func (c *Client) dispatchBatchDeliver(body []byte) {
	if len(body) < 4 {
		return
	}
	count := int(binary.LittleEndian.Uint16(body[0:2]))
	off := 4 // skip count(2) + pad(2)

	for i := 0; i < count; i++ {
		if off+24 > len(body) {
			break
		}
		// Entry header: consumer_id(4) + seq(8) + subj_len(2) + reply_len(2) + data_len(4) + subject_hash(4) = 24
		consumerID := binary.LittleEndian.Uint32(body[off : off+4])
		seq := binary.LittleEndian.Uint64(body[off+4 : off+12])
		subjLen := int(binary.LittleEndian.Uint16(body[off+12 : off+14]))
		replyLen := int(binary.LittleEndian.Uint16(body[off+14 : off+16]))
		dataLen := int(binary.LittleEndian.Uint32(body[off+16 : off+20]))
		subjectHash := binary.LittleEndian.Uint32(body[off+20 : off+24])

		entryPayloadStart := off + 24
		// data_len = total variable tail (subject + reply_to + payload)
		totalTail := dataLen
		if entryPayloadStart+totalTail > len(body) {
			break
		}

		c.deliveriesRecv.Add(1)

		sub := c.lookupSub(consumerID)
		if sub == nil {
			off = entryPayloadStart + totalTail
			continue
		}

		// Build Msg referencing the batch buffer slice
		subjOff := entryPayloadStart
		replyToOff := entryPayloadStart + subjLen
		payloadOff := entryPayloadStart + subjLen + replyLen
		payloadLen := dataLen - subjLen - replyLen

		msg := &Msg{
			frame:       body, // reference batch buffer
			consumerID:  consumerID,
			subjectHash: subjectHash,
			seq:         seq,
			subjectOff:  subjOff,
			subjectLen:  subjLen,
			replyToOff:  replyToOff,
			replyToLen:  replyLen,
			payloadOff:  payloadOff,
			payloadLen:  payloadLen,
			client:      c,
		}

		c.deliverToSub(sub, msg)
		off = entryPayloadStart + totalTail
	}
}

func (c *Client) lookupSub(consumerID uint32) *Subscription {
	val, ok := c.subs.Load(consumerID)
	if !ok {
		return nil
	}
	return val.(*Subscription)
}

func (c *Client) deliverToSub(sub *Subscription, msg *Msg) {
	// Callback mode — invoke directly on reader goroutine (zero-alloc hot path)
	if sub.handler != nil {
		sub.handler(msg)
		return
	}

	// Channel mode — try non-blocking first, then block with close guard
	select {
	case sub.ch <- msg:
	case <-sub.closed:
	default:
		select {
		case sub.ch <- msg:
		case <-sub.closed:
		}
	}
}

