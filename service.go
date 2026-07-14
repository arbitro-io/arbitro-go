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
const methodInfix = ".m."
const replyInfix = "._r."

var nextSvcInstanceID atomic.Uint32

func newSvcInstanceID() uint32 {
	for {
		id := nextSvcInstanceID.Add(1)
		if id != 0 {
			return id
		}
	}
}

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
	// Subject: _svc.<serviceName>.m.<method>
	prefixLen := len(svcPrefix) + len(serviceName) + len(methodInfix)
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
	name       string
	streamID   uint32
	instanceID uint32
	client     *Client

	handlers sync.Map // map[string]ServiceHandler (subject prefix → handler)

	streamCache sync.Map // map[string]uint32 (target name → stream ID)
	corrID      atomic.Uint64
	pending     sync.Map // map[uint64]chan []byte

	closed atomic.Bool
}

// Handle registers a handler for the given method on this service.
func (s *Service) Handle(method string, handler ServiceHandler) {
	prefix := svcPrefix + s.name + methodInfix + method
	s.handlers.Store(prefix, handler)
}

// Request sends an RPC request to a target service and waits for the reply.
func (s *Service) Request(ctx context.Context, target, method string, payload []byte, timeout time.Duration) ([]byte, error) {
	targetStreamID, err := s.resolveStream(ctx, target)
	if err != nil {
		return nil, err
	}

	corrID := s.corrID.Add(1)
	subject := []byte(svcPrefix + target + methodInfix + method)
	replySubject := svcPrefix + s.name + replyInfix + strconv.FormatUint(uint64(s.instanceID), 10) + "." + strconv.FormatUint(corrID, 10)

	replyTo := make([]byte, 5+len(replySubject))
	replyTo[0] = ReplyToMagic
	binary.LittleEndian.PutUint32(replyTo[1:5], s.streamID)
	copy(replyTo[5:], replySubject)

	seq := s.client.getConn().NextSeq()
	frame := proto.EncodePublishWithReply(seq, targetStreamID, subject, replyTo, nil, payload, proto.FlagAckReq)

	ch := make(chan []byte, 1)
	s.pending.Store(corrID, ch)
	defer s.pending.Delete(corrID)

	tctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	_, err = s.client.getConn().SendExpectReply(tctx, frame, seq)
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
	subject := []byte(svcPrefix + target + methodInfix + method)
	seq := s.client.getConn().NextSeq()
	frame := proto.EncodePublish(seq, targetStreamID, subject, nil, payload, proto.FlagAckReq)
	return s.client.getConn().Send(frame)
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
	// Reply subject: `_svc.<name>._r.<instanceID>.<corrID>` — instance-scoped.
	replyPrefix := svcPrefix + s.name + replyInfix + strconv.FormatUint(uint64(s.instanceID), 10) + "."

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
	seq := s.client.getConn().NextSeq()
	frame, err := proto.EncodeGetStream(seq, []byte(streamName))
	if err != nil {
		return 0, err
	}
	reply, err := s.client.getConn().SendExpectReply(ctx, frame, seq)
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

// Build creates the backing stream, two consumers (worker + reply),
// two subscriptions, and returns the ready Service.
func (b *ServiceBuilder) Build(ctx context.Context) (*Service, error) {
	streamName := "_svc-" + b.name
	// Stream captures BOTH method and reply subjects.
	streamFilter := svcPrefix + b.name + ".>"

	// Create stream
	seq := b.client.getConn().NextSeq()
	frame, err := proto.EncodeCreateStream(seq, []byte(streamName), []byte(streamFilter), 0, 0, 3600, 1, 0, 0, 0, 0)
	if err != nil {
		return nil, fmt.Errorf("service encode create stream: %w", err)
	}
	reply, err := b.client.getConn().SendExpectReply(ctx, frame, seq)
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

	svc := &Service{
		name:       b.name,
		streamID:   streamID,
		instanceID: newSvcInstanceID(),
		client:     b.client,
	}
	svc.streamCache.Store(b.name, streamID)

	maxInfl := b.maxInflight
	if maxInfl == 0 {
		maxInfl = 1024
	}

	// ── Worker consumer: queue-grouped, receives ONLY method calls. ──
	workerFilter := svcPrefix + b.name + methodInfix + ">"
	workerName := "_svc-" + b.name + "-worker"

	seq = b.client.getConn().NextSeq()
	frame, err = proto.EncodeCreateConsumer(
		seq, streamID,
		[]byte(workerName), []byte(workerName), []byte(workerFilter),
		uint16(maxInfl), AckExplicit, DeliverAll, 1, // Queue mode
		30_000, 0, nil,
	)
	if err != nil {
		return nil, fmt.Errorf("service create worker consumer: %w", err)
	}
	reply, err = b.client.getConn().SendExpectReply(ctx, frame, seq)
	if err != nil {
		return nil, err
	}
	var workerConsumerID uint32
	if err := b.client.checkReply(reply); err != nil {
		if !IsAlreadyExists(err) {
			return nil, err
		}
		workerConsumerID, err = b.client.resolveConsumerID(ctx, streamID, workerName)
		if err != nil {
			return nil, err
		}
	} else {
		body = reply[proto.HeaderSize:]
		if len(body) < 8 {
			return nil, &ArbitroError{Code: ErrCodeInternalError, Message: "create worker consumer: reply too short"}
		}
		workerConsumerID = uint32(proto.RepOkRefSeq(body))
	}

	// ── Reply consumer: per-instance, NO group. ──
	// Replies MUST NOT be load-balanced to sibling instances.
	replyFilter := svcPrefix + b.name + replyInfix + strconv.FormatUint(uint64(svc.instanceID), 10) + ".>"
	replyName := "_svc-" + b.name + "-reply-" + strconv.FormatUint(uint64(svc.instanceID), 10)

	seq = b.client.getConn().NextSeq()
	frame, err = proto.EncodeCreateConsumer(
		seq, streamID,
		[]byte(replyName), nil, []byte(replyFilter),
		uint16(maxInfl), AckExplicit, DeliverNew, 0, // Fanout mode, no group
		30_000, 0, nil,
	)
	if err != nil {
		return nil, fmt.Errorf("service create reply consumer: %w", err)
	}
	reply, err = b.client.getConn().SendExpectReply(ctx, frame, seq)
	if err != nil {
		return nil, err
	}
	var replyConsumerID uint32
	if err := b.client.checkReply(reply); err != nil {
		return nil, err
	}
	body = reply[proto.HeaderSize:]
	if len(body) < 8 {
		return nil, &ArbitroError{Code: ErrCodeInternalError, Message: "create reply consumer: reply too short"}
	}
	replyConsumerID = uint32(proto.RepOkRefSeq(body))

	// Subscribe both consumers to the shared dispatcher.
	for _, entry := range []struct {
		cid    uint32
		filter string
	}{
		{workerConsumerID, workerFilter},
		{replyConsumerID, replyFilter},
	} {
		sub := &Subscription{
			client:     b.client,
			consumerID: entry.cid,
			ch:         make(chan *Msg, 256),
			handler:    svc.dispatch,
			closed:     make(chan struct{}),
		}
		b.client.registerSubscription(entry.cid, sub, [][]byte{[]byte(entry.filter)})
		b.client.activeSubs.Add(1)

		seq = b.client.getConn().NextSeq()
		subFrame, err := proto.EncodeSubscribe(seq, entry.cid, [][]byte{[]byte(entry.filter)})
		if err != nil {
			return nil, err
		}
		_, err = b.client.getConn().SendExpectReply(ctx, subFrame, seq)
		if err != nil {
			return nil, err
		}
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
