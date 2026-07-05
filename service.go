package arbitro

import (
	"context"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/arbitro-io/arbitro-go/internal/proto"
)

const svcPrefix = "_svc."
const replyInfix = "._r."

// Request is a read-only view over an incoming service request.
//
// Ack, nack, and reply are managed by the framework based on the handler's
// return value, so this type intentionally does not expose them.
type Request struct {
	subject    []byte
	payload    []byte
	hasReply   bool
	seq        uint64
	consumerID uint32
}

// Subject returns the full subject (e.g., "_svc.orders.charge").
func (r *Request) Subject() []byte { return r.subject }

// Data returns the request payload bytes.
func (r *Request) Data() []byte { return r.payload }

// HasReply reports whether the requester is waiting for a reply.
func (r *Request) HasReply() bool { return r.hasReply }

// Seq returns the delivery sequence assigned by the broker.
func (r *Request) Seq() uint64 { return r.seq }

// ConsumerID returns the consumer that received this request.
func (r *Request) ConsumerID() uint32 { return r.consumerID }

// Method returns the method segment after the service prefix
// (e.g., "charge"). Returns nil if the subject is malformed.
func (r *Request) Method(serviceName string) []byte {
	prefixLen := len(svcPrefix) + len(serviceName) + 1
	if len(r.subject) <= prefixLen {
		return nil
	}
	return r.subject[prefixLen:]
}

// ServiceHandler handles an incoming service request.
//
// Return value semantics (framework handles ack/nack/reply automatically):
//   - Return non-nil bytes — framework replies to the requester (if a
//     reply address is present) and acks the delivery.
//   - Return nil bytes with no error — framework acks without replying.
//   - Return an error — framework nacks the delivery for redelivery.
//
// The framework guarantees exactly one ack or nack per invocation. Each
// handler runs in its own goroutine, so slow handlers do not block the
// dispatcher.
type ServiceHandler func(req *Request) ([]byte, error)

// ServiceConfig holds optional configuration for a Service.
type ServiceConfig struct {
	MaxInflight uint32
}

// Service is an RPC service that handles incoming requests and supports outbound request/reply.
type Service struct {
	name     string
	streamID uint32
	client   *Client

	handlers sync.Map // map[string]ServiceHandler (subject prefix → handler)

	streamCache sync.Map // map[string]uint32 (target name → stream ID)
	corrID      atomic.Uint64
	pending     sync.Map // map[uint64]chan []byte

	closed atomic.Bool
}

// Handle registers a handler for the given method on this service.
func (s *Service) Handle(method string, handler ServiceHandler) {
	prefix := svcPrefix + s.name + "." + method
	s.handlers.Store(prefix, handler)
}

// Request sends an RPC request to a target service and waits for the reply.
func (s *Service) Request(ctx context.Context, target, method string, payload []byte, timeout time.Duration) ([]byte, error) {
	targetStreamID, err := s.resolveStream(ctx, target)
	if err != nil {
		return nil, err
	}

	corrID := s.corrID.Add(1)
	subject := []byte(svcPrefix + target + "." + method)
	replySubject := svcPrefix + s.name + replyInfix + strconv.FormatUint(corrID, 10)

	replyTo := make([]byte, 5+len(replySubject))
	replyTo[0] = ReplyToMagic
	binary.LittleEndian.PutUint32(replyTo[1:5], s.streamID)
	copy(replyTo[5:], replySubject)

	seq := s.client.conn.NextSeq()
	frame := proto.EncodePublishWithReply(seq, targetStreamID, subject, replyTo, nil, payload, proto.FlagAckReq)

	ch := make(chan []byte, 1)
	s.pending.Store(corrID, ch)
	defer s.pending.Delete(corrID)

	tctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	_, err = s.client.conn.SendExpectReply(tctx, frame, seq)
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

// Send publishes a fire-and-forget message to a target service method.
func (s *Service) Send(ctx context.Context, target, method string, payload []byte) error {
	targetStreamID, err := s.resolveStream(ctx, target)
	if err != nil {
		return err
	}
	subject := []byte(svcPrefix + target + "." + method)
	seq := s.client.conn.NextSeq()
	frame := proto.EncodePublish(seq, targetStreamID, subject, nil, payload, proto.FlagAckReq)
	return s.client.conn.Send(frame)
}

// Close shuts down the service and cancels pending requests.
func (s *Service) Close() {
	s.closed.Store(true)
	s.pending.Range(func(key, value any) bool {
		s.pending.Delete(key)
		return true
	})
}

func (s *Service) dispatch(msg *Msg) {
	if s.closed.Load() {
		return
	}

	subject := msg.Subject()
	replyPrefix := svcPrefix + s.name + replyInfix

	if strings.HasPrefix(subject, replyPrefix) {
		corrStr := subject[len(replyPrefix):]
		corrID, err := strconv.ParseUint(corrStr, 10, 64)
		if err == nil {
			if val, ok := s.pending.LoadAndDelete(corrID); ok {
				ch := val.(chan []byte)
				data := make([]byte, msg.payloadLen)
				copy(data, msg.Data())
				ch <- data
			}
			msg.Ack()
			return
		}
	}

	var handler ServiceHandler
	s.handlers.Range(func(key, value any) bool {
		prefix := key.(string)
		if strings.HasPrefix(subject, prefix) {
			handler = value.(ServiceHandler)
			return false
		}
		return true
	})

	if handler != nil {
		hasReply := len(msg.ReplyTo()) > 0
		req := &Request{
			subject:    []byte(msg.Subject()),
			payload:    msg.Data(),
			hasReply:   hasReply,
			seq:        msg.Seq(),
			consumerID: msg.ConsumerID(),
		}
		go func() {
			resp, err := handler(req)
			if err != nil {
				msg.Nack()
				return
			}
			if len(resp) > 0 && hasReply {
				msg.Reply(resp)
			}
			msg.Ack()
		}()
		return
	}

	msg.Nack()
}

func (s *Service) resolveStream(ctx context.Context, target string) (uint32, error) {
	if val, ok := s.streamCache.Load(target); ok {
		return val.(uint32), nil
	}
	streamName := "_svc-" + target
	seq := s.client.conn.NextSeq()
	frame, err := proto.EncodeGetStream(seq, []byte(streamName))
	if err != nil {
		return 0, err
	}
	reply, err := s.client.conn.SendExpectReply(ctx, frame, seq)
	if err != nil {
		return 0, err
	}
	if err := s.client.checkReply(reply); err != nil {
		return 0, err
	}
	body := reply[proto.HeaderSize:]
	if len(body) < 8 {
		return 0, &ArbitroError{Code: ErrCodeInternalError, Message: "resolve stream: reply too short"}
	}
	id := uint32(proto.RepOkRefSeq(body))
	s.streamCache.Store(target, id)
	return id, nil
}

// ServiceBuilder constructs a Service step by step.
type ServiceBuilder struct {
	client      *Client
	name        string
	maxInflight uint32
}

// SetMaxInflight sets the max inflight messages for the service consumer.
func (b *ServiceBuilder) SetMaxInflight(n uint32) *ServiceBuilder {
	b.maxInflight = n
	return b
}

// Build creates the backing stream, consumer, subscription, and returns the ready Service.
func (b *ServiceBuilder) Build(ctx context.Context) (*Service, error) {
	streamName := "_svc-" + b.name
	filter := svcPrefix + b.name + ".>"

	// Create stream
	seq := b.client.conn.NextSeq()
	frame, err := proto.EncodeCreateStream(seq, []byte(streamName), []byte(filter), 0, 0, 3600, 1, 0, 0, 0, 0)
	if err != nil {
		return nil, fmt.Errorf("service encode create stream: %w", err)
	}
	reply, err := b.client.conn.SendExpectReply(ctx, frame, seq)
	if err != nil {
		return nil, fmt.Errorf("service create stream: %w", err)
	}
	if err := b.client.checkReply(reply); err != nil {
		if !IsAlreadyExists(err) {
			return nil, err
		}
		// Stream exists — resolve ID
		id, err2 := b.client.resolveStreamID(ctx, streamName)
		if err2 != nil {
			return nil, err2
		}
		reply = make([]byte, proto.HeaderSize+8)
		binary.LittleEndian.PutUint64(reply[proto.HeaderSize:], uint64(id))
	}
	body := reply[proto.HeaderSize:]
	if len(body) < 8 {
		return nil, &ArbitroError{Code: ErrCodeInternalError, Message: "create stream: reply too short"}
	}
	streamID := uint32(proto.RepOkRefSeq(body))

	// Create consumer
	consumerName := "_svc-" + b.name + "-worker"
	maxInfl := b.maxInflight
	if maxInfl == 0 {
		maxInfl = 1024
	}

	seq = b.client.conn.NextSeq()
	frame, err = proto.EncodeCreateConsumer(
		seq, streamID,
		[]byte(consumerName), []byte(consumerName), []byte(filter),
		uint16(maxInfl), AckExplicit, DeliverAll, 1, // Queue mode
		30_000, 0, nil,
	)
	if err != nil {
		return nil, fmt.Errorf("service create consumer: %w", err)
	}
	reply, err = b.client.conn.SendExpectReply(ctx, frame, seq)
	if err != nil {
		return nil, err
	}
	var consumerID uint32
	if err := b.client.checkReply(reply); err != nil {
		if !IsAlreadyExists(err) {
			return nil, err
		}
		consumerID, err = b.client.resolveConsumerID(ctx, streamID, consumerName)
		if err != nil {
			return nil, err
		}
	} else {
		body = reply[proto.HeaderSize:]
		if len(body) < 8 {
			return nil, &ArbitroError{Code: ErrCodeInternalError, Message: "create consumer: reply too short"}
		}
		consumerID = uint32(proto.RepOkRefSeq(body))
	}

	svc := &Service{
		name:     b.name,
		streamID: streamID,
		client:   b.client,
	}
	svc.streamCache.Store(b.name, streamID)

	// Subscribe with callback dispatch
	sub := &Subscription{
		client:     b.client,
		consumerID: consumerID,
		ch:         make(chan *Msg, 256),
		handler:    svc.dispatch,
		closed:     make(chan struct{}),
	}
	b.client.registerSubscription(consumerID, sub)
	b.client.activeSubs.Add(1)

	seq = b.client.conn.NextSeq()
	subFrame, err := proto.EncodeSubscribe(seq, consumerID, [][]byte{[]byte(filter)})
	if err != nil {
		return nil, err
	}
	_, err = b.client.conn.SendExpectReply(ctx, subFrame, seq)
	if err != nil {
		return nil, err
	}

	return svc, nil
}

// Service returns a ServiceBuilder for the given name.
func (c *Client) Service(name string) *ServiceBuilder {
	return &ServiceBuilder{
		client:      c,
		name:        name,
		maxInflight: 1024,
	}
}
