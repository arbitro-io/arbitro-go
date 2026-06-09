package arbitro

import (
	"context"
	"encoding/binary"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ── Types ─────────────────────────────────────────────────────────────────

// StepResult is returned by a step handler to pass context to the next step.
type StepResult struct {
	Context []byte
}

// StepContext provides per-step execution context.
type StepContext struct {
	Context    context.Context
	Name       string
	InstanceID string
	StepIndex  uint16
	Attempt    uint8
	Input      []byte // accumulated context from previous steps
}

// StepOutcome is the return type for suspend step run handlers.
type StepOutcome struct {
	// If Done is non-nil, proceed to next step (or finish).
	Done *StepResult
	// If Suspend is non-nil, park the instance.
	Suspend *SuspendData
}

// SuspendData holds the state to persist while suspended.
type SuspendData struct {
	State     []byte // opaque state passed back to resume/timeout handler
	TimeoutMs uint64 // 0 = no timeout
}

// OutcomeDone creates a StepOutcome that proceeds to the next step.
func OutcomeDone(result StepResult) StepOutcome {
	return StepOutcome{Done: &result}
}

// OutcomeSuspend creates a StepOutcome that suspends the instance.
func OutcomeSuspend(state []byte, timeoutMs uint64) StepOutcome {
	return StepOutcome{Suspend: &SuspendData{State: state, TimeoutMs: timeoutMs}}
}

// ResumeContext is passed to resume handlers when a suspended step
// receives an external event.
type ResumeContext struct {
	Name       string
	InstanceID string
	StepIndex  uint16
	State      []byte // state persisted by the run handler
	Event      []byte // payload of the resume event
}

// TimeoutContext is passed to timeout handlers when a suspended step
// times out without receiving a resume event.
type TimeoutContext struct {
	Name       string
	InstanceID string
	StepIndex  uint16
	State      []byte // state persisted by the run handler
}

// StepFunc is the function signature for a normal workflow step.
type StepFunc func(StepContext) ([]byte, error)

// SuspendRunFunc is the function signature for a suspend step's run handler.
type SuspendRunFunc func(StepContext) (StepOutcome, error)

// ResumeFunc is the function signature for a resume handler.
type ResumeFunc func(ResumeContext) (StepResult, error)

// TimeoutFunc is the function signature for a timeout handler.
type TimeoutFunc func(TimeoutContext) (StepResult, error)

// CompensateFunc is the function signature for a compensation (rollback).
type CompensateFunc func(StepContext) error

// ── Internal types ────────────────────────────────────────────────────────

type stepKind int

const (
	stepNormal  stepKind = 0
	stepSuspend stepKind = 1
)

type stepDef struct {
	name       string
	kind       stepKind
	handler    StepFunc        // normal step
	run        SuspendRunFunc  // suspend step
	onResume   ResumeFunc      // suspend step
	onTimeout  TimeoutFunc     // suspend step (optional)
	timeoutMs  uint64          // default timeout for suspend step
	compensate CompensateFunc  // optional compensation
}

type suspendedEntry struct {
	stepIndex uint16
	state     []byte
	context   []byte
}

type sourceDef struct {
	streamName string
	subject    string
}

// ── Task payload encoding ─────────────────────────────────────────────────
// Format: [id_len:2 LE][instance_id:id_len][step_index:2 LE][attempt:1][context...]

const compensationBit uint16 = 0x8000
const minTaskPayload = 5

func encodeTask(instanceID string, stepIndex uint16, attempt uint8, ctx []byte) []byte {
	idBytes := []byte(instanceID)
	idLen := uint16(len(idBytes))
	buf := make([]byte, 2+len(idBytes)+2+1+len(ctx))
	binary.LittleEndian.PutUint16(buf[0:2], idLen)
	copy(buf[2:], idBytes)
	off := 2 + len(idBytes)
	binary.LittleEndian.PutUint16(buf[off:off+2], stepIndex)
	buf[off+2] = attempt
	copy(buf[off+3:], ctx)
	return buf
}

func decodeTask(payload []byte) (instanceID string, stepIndex uint16, attempt uint8, ctx []byte, ok bool) {
	if len(payload) < minTaskPayload {
		return
	}
	idLen := int(binary.LittleEndian.Uint16(payload[0:2]))
	header := 2 + idLen + 2 + 1
	if len(payload) < header {
		return
	}
	instanceID = string(payload[2 : 2+idLen])
	off := 2 + idLen
	stepIndex = binary.LittleEndian.Uint16(payload[off : off+2])
	attempt = payload[off+2]
	ctx = payload[header:]
	ok = true
	return
}

// ── Park / Remove encoding (state stream) ───────────────────────────────
// Format: [step_index:2LE][state_len:4LE][state bytes][context bytes]

func encodePark(stepIndex uint16, state, context []byte) []byte {
	buf := make([]byte, 2+4+len(state)+len(context))
	binary.LittleEndian.PutUint16(buf[0:2], stepIndex)
	binary.LittleEndian.PutUint32(buf[2:6], uint32(len(state)))
	copy(buf[6:], state)
	copy(buf[6+len(state):], context)
	return buf
}

func decodePark(payload []byte) (stepIndex uint16, state, context []byte, ok bool) {
	if len(payload) < 6 {
		return
	}
	stepIndex = binary.LittleEndian.Uint16(payload[0:2])
	stateLen := int(binary.LittleEndian.Uint32(payload[2:6]))
	if len(payload) < 6+stateLen {
		return
	}
	state = make([]byte, stateLen)
	copy(state, payload[6:6+stateLen])
	context = make([]byte, len(payload)-6-stateLen)
	copy(context, payload[6+stateLen:])
	ok = true
	return
}

// ── Instance ID generator ─────────────────────────────────────────────────

var nextInstanceID atomic.Uint32

func init() {
	nextInstanceID.Store(1)
}

func genInstanceID() string {
	return fmt.Sprintf("%d", nextInstanceID.Add(1)-1)
}

// ── WorkflowBuilder ───────────────────────────────────────────────────────

// WorkflowBuilder constructs a workflow with fluent API.
type WorkflowBuilder struct {
	client         *Client
	name           string
	triggerSubject string
	triggerStream  string
	sources        []sourceDef
	steps          []stepDef
	maxRetries     uint8
	ackWait        time.Duration
	maxInflight    uint16
	maxContextSize int
}

// Workflow starts building a client-side workflow/saga.
func (c *Client) Workflow(name string) *WorkflowBuilder {
	return &WorkflowBuilder{
		client:         c,
		name:           name,
		maxRetries:     3,
		ackWait:        30 * time.Second,
		maxInflight:    10,
		maxContextSize: 256 * 1024,
	}
}

// Trigger sets the subject filter that triggers the workflow.
func (b *WorkflowBuilder) Trigger(subject string) *WorkflowBuilder {
	b.triggerSubject = subject
	return b
}

// TriggerStream sets the stream to subscribe for triggers.
func (b *WorkflowBuilder) TriggerStream(stream string) *WorkflowBuilder {
	b.triggerStream = stream
	return b
}

// Source pipes messages from an external stream into this workflow.
// Each message matching subject on the given stream creates a new
// workflow instance. Multiple sources can be registered.
func (b *WorkflowBuilder) Source(streamName, subject string) *WorkflowBuilder {
	b.sources = append(b.sources, sourceDef{
		streamName: streamName,
		subject:    subject,
	})
	return b
}

// Step adds a named step to the workflow.
func (b *WorkflowBuilder) Step(name string, handler StepFunc) *WorkflowBuilder {
	b.steps = append(b.steps, stepDef{
		name:    name,
		kind:    stepNormal,
		handler: handler,
	})
	return b
}

// SuspendStep adds a suspend step — the run handler can return
// OutcomeSuspend to release the worker and wait for an external event
// or timeout.
func (b *WorkflowBuilder) SuspendStep(name string, timeoutMs uint64, run SuspendRunFunc, onResume ResumeFunc) *WorkflowBuilder {
	b.steps = append(b.steps, stepDef{
		name:      name,
		kind:      stepSuspend,
		run:       run,
		onResume:  onResume,
		timeoutMs: timeoutMs,
	})
	return b
}

// OnTimeout sets the timeout handler for the most recently added suspend step.
func (b *WorkflowBuilder) OnTimeout(handler TimeoutFunc) *WorkflowBuilder {
	if len(b.steps) > 0 {
		last := &b.steps[len(b.steps)-1]
		if last.kind == stepSuspend {
			last.onTimeout = handler
		}
	}
	return b
}

// Compensate attaches a compensation function to the named step.
func (b *WorkflowBuilder) Compensate(name string, handler CompensateFunc) *WorkflowBuilder {
	for i := range b.steps {
		if b.steps[i].name == name {
			b.steps[i].compensate = handler
			break
		}
	}
	return b
}

// MaxRetries sets how many times a step can be retried before DLQ.
func (b *WorkflowBuilder) MaxRetries(n int) *WorkflowBuilder {
	b.maxRetries = uint8(n)
	return b
}

// AckWait sets the timeout for each step execution.
func (b *WorkflowBuilder) AckWait(d time.Duration) *WorkflowBuilder {
	b.ackWait = d
	return b
}

// MaxInflight sets the maximum concurrent workflow instances.
func (b *WorkflowBuilder) MaxInflight(n int) *WorkflowBuilder {
	b.maxInflight = uint16(n)
	return b
}

// MaxContextSize sets the maximum accumulated context bytes.
func (b *WorkflowBuilder) MaxContextSize(n int) *WorkflowBuilder {
	b.maxContextSize = n
	return b
}

// Start registers the workflow and begins processing.
func (b *WorkflowBuilder) Start(ctx context.Context) (*WorkflowHandle, error) {
	if b.triggerSubject == "" && len(b.sources) == 0 {
		return nil, fmt.Errorf("arbitro: workflow %q needs Trigger or Source", b.name)
	}
	if len(b.steps) == 0 {
		return nil, fmt.Errorf("arbitro: workflow %q needs at least one step", b.name)
	}

	name := b.name

	// Stream/subject naming conventions (match Rust/TS).
	taskStreamName := fmt.Sprintf("_wf_%s_tasks", name)
	taskSubject := fmt.Sprintf("_wf.%s.>", name)
	groupName := fmt.Sprintf("_wf_%s_workers", name)

	workerUID := genInstanceID()
	consumerName := fmt.Sprintf("_wf_%s_w%s", name, workerUID)

	// Create internal task stream (idempotent).
	taskStreamID, err := b.upsertInternalStream(ctx, taskStreamName, taskSubject, 300_000)
	if err != nil {
		return nil, fmt.Errorf("arbitro: workflow %q create task stream: %w", name, err)
	}

	// Create DLQ stream (idempotent).
	dlqStreamName := fmt.Sprintf("_wf_%s_dlq", name)
	dlqSubject := fmt.Sprintf("_wf.%s.dlq.>", name)
	dlqStreamID, err := b.upsertInternalStream(ctx, dlqStreamName, dlqSubject, 0)
	if err != nil {
		return nil, fmt.Errorf("arbitro: workflow %q create DLQ stream: %w", name, err)
	}

	// Create state stream for cross-worker suspend registry.
	stateStreamName := fmt.Sprintf("_wf_%s_state", name)
	stateSubject := fmt.Sprintf("_wf.%s.__state.>", name)
	stateStreamID, err := b.upsertInternalStream(ctx, stateStreamName, stateSubject, 300_000)
	if err != nil {
		return nil, fmt.Errorf("arbitro: workflow %q create state stream: %w", name, err)
	}

	// Fanout consumer on state stream — unique per worker, DeliverPolicy::All.
	stateConsumerName := fmt.Sprintf("_wf_%s_state_w%s", name, workerUID)
	stateSub, err := b.client.Subscribe(ctx, stateStreamName, ConsumerConfig{
		Name:        stateConsumerName,
		Group:       "", // empty group → fanout
		Fanout:      true,
		Filter:      stateSubject,
		AckPolicy:   AckExplicit,
		MaxInflight: 100,
		AckWait:     30 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("arbitro: workflow %q state subscribe: %w", name, err)
	}

	// Task consumer in shared group for round-robin.
	taskSub, err := b.client.Subscribe(ctx, taskStreamName, ConsumerConfig{
		Name:        consumerName,
		Group:       groupName,
		Filter:      taskSubject,
		AckPolicy:   AckExplicit,
		MaxInflight: b.maxInflight,
		AckWait:     b.ackWait,
		MaxDeliver:  uint32(b.maxRetries),
	})
	if err != nil {
		return nil, fmt.Errorf("arbitro: workflow %q task subscribe: %w", name, err)
	}

	childCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})

	// Shared suspended-instance registry.
	suspended := &syncMap[string, *suspendedEntry]{}
	// Tracks message seqs nacked once for cross-worker retry.
	nackedSeqs := &syncMap[uint64, struct{}]{}

	handle := &WorkflowHandle{
		client:        b.client,
		name:          name,
		taskStreamID:  taskStreamID,
		dlqStreamID:   dlqStreamID,
		stateStreamID: stateStreamID,
		cancel:        cancel,
		done:          done,
		taskSub:       taskSub,
		stateSub:      stateSub,
		subs:          make([]*Subscription, 0),
	}

	// Processor config shared by all goroutines.
	cfg := &processorConfig{
		client:         b.client,
		name:           name,
		taskStreamID:   taskStreamID,
		dlqStreamID:    dlqStreamID,
		stateStreamID:  stateStreamID,
		steps:          b.steps,
		totalSteps:     uint16(len(b.steps)),
		maxRetries:     b.maxRetries,
		maxContextSize: b.maxContextSize,
		suspended:      suspended,
		nackedSeqs:     nackedSeqs,
	}

	// ── State sync goroutine ──
	go stateSync(childCtx, cfg, stateSub)

	// ── Main dispatch goroutine ──
	go func() {
		defer close(done)
		processLoop(childCtx, cfg, taskSub)
	}()

	// ── Auto-trigger subscription ──
	if b.triggerSubject != "" && b.triggerStream != "" {
		triggerConsumerName := fmt.Sprintf("_wf_%s_trigger", name)
		triggerSub, err := b.client.Subscribe(ctx, b.triggerStream, ConsumerConfig{
			Name:        triggerConsumerName,
			Group:       triggerConsumerName,
			Filter:      b.triggerSubject,
			AckPolicy:   AckExplicit,
			MaxInflight: 1,
			AckWait:     b.ackWait,
			MaxDeliver:  uint32(b.maxRetries),
		})
		if err != nil {
			cancel()
			return nil, fmt.Errorf("arbitro: workflow %q trigger subscribe: %w", name, err)
		}
		handle.subs = append(handle.subs, triggerSub)

		step0Subject := fmt.Sprintf("_wf.%s.step.0", name)
		go triggerLoop(childCtx, b.client, triggerSub, taskStreamName, step0Subject)
	}

	// ── Source subscriptions ──
	for i, src := range b.sources {
		srcConsumerName := fmt.Sprintf("_wf_%s_src_%d", name, i)
		srcSub, err := b.client.Subscribe(ctx, src.streamName, ConsumerConfig{
			Name:        srcConsumerName,
			Group:       srcConsumerName,
			Filter:      src.subject,
			AckPolicy:   AckExplicit,
			MaxInflight: 1,
			AckWait:     b.ackWait,
			MaxDeliver:  uint32(b.maxRetries),
		})
		if err != nil {
			cancel()
			return nil, fmt.Errorf("arbitro: workflow %q source[%d] subscribe: %w", name, i, err)
		}
		handle.subs = append(handle.subs, srcSub)

		step0Subject := fmt.Sprintf("_wf.%s.step.0", name)
		go triggerLoop(childCtx, b.client, srcSub, taskStreamName, step0Subject)
	}

	return handle, nil
}

// upsertInternalStream creates a stream or gets its ID if it already exists.
func (b *WorkflowBuilder) upsertInternalStream(ctx context.Context, name, subject string, idempotencyWindowMs uint32) (uint32, error) {
	s, err := b.client.CreateStream(ctx, name, StreamConfig{
		SubjectFilter:     subject,
		IdempotencyWindow: time.Duration(idempotencyWindowMs) * time.Millisecond,
	})
	if err != nil {
		if IsAlreadyExists(err) {
			id, err2 := b.client.resolveStreamID(ctx, name)
			if err2 != nil {
				return 0, err2
			}
			return id, nil
		}
		return 0, err
	}
	return s.streamID, nil
}

// ── WorkflowHandle ────────────────────────────────────────────────────────

// WorkflowHandle is the live handle to a running workflow.
type WorkflowHandle struct {
	client        *Client
	name          string
	taskStreamID  uint32
	dlqStreamID   uint32
	stateStreamID uint32
	cancel        context.CancelFunc
	done          chan struct{}
	taskSub       *Subscription
	stateSub      *Subscription
	subs          []*Subscription // trigger + source subs
	resumeSeq     atomic.Uint32
}

// Trigger manually fires the workflow with an auto-generated instance ID.
// Returns the generated instance ID.
func (h *WorkflowHandle) Trigger(ctx context.Context, payload []byte) (string, error) {
	instanceID := genInstanceID()
	err := h.TriggerWithID(ctx, instanceID, payload)
	return instanceID, err
}

// TriggerWithID fires the workflow with an explicit instance ID (e.g. a
// business key like "ord_123"). The same ID can be used by external
// systems to address this workflow instance.
func (h *WorkflowHandle) TriggerWithID(ctx context.Context, instanceID string, payload []byte) error {
	msgID := fmt.Sprintf("wf:%s:0:0", instanceID)
	subject := fmt.Sprintf("_wf.%s.step.0", h.name)
	task := encodeTask(instanceID, 0, 0, payload)
	return h.client.Publish(ctx, fmt.Sprintf("_wf_%s_tasks", h.name), subject, task, WithMsgID(msgID))
}

// Resume a suspended workflow instance with an external event.
func (h *WorkflowHandle) Resume(ctx context.Context, instanceID string, event []byte) error {
	seq := h.resumeSeq.Add(1) - 1
	subject := fmt.Sprintf("_wf.%s.resume.%s", h.name, instanceID)
	msgID := fmt.Sprintf("wf:%s:resume:%d", instanceID, seq)
	return h.client.Publish(ctx, fmt.Sprintf("_wf_%s_tasks", h.name), subject, event, WithMsgID(msgID))
}

// Cancel a suspended workflow instance. If the instance is currently
// running or doesn't exist, the cancel is a no-op (idempotent).
func (h *WorkflowHandle) Cancel(ctx context.Context, instanceID string) error {
	subject := fmt.Sprintf("_wf.%s.cancel.%s", h.name, instanceID)
	msgID := fmt.Sprintf("wf:%s:cancel", instanceID)
	return h.client.Publish(ctx, fmt.Sprintf("_wf_%s_tasks", h.name), subject, nil, WithMsgID(msgID))
}

// DLQStreamID returns the stream ID where failed instances land.
func (h *WorkflowHandle) DLQStreamID() uint32 {
	return h.dlqStreamID
}

// Stop gracefully shuts down the workflow processor.
func (h *WorkflowHandle) Stop(ctx context.Context) error {
	h.cancel()
	// Close all subs (trigger + source).
	for _, sub := range h.subs {
		sub.Close()
	}
	h.taskSub.Close()
	h.stateSub.Close()
	<-h.done
	return nil
}

// ── Processor config ──────────────────────────────────────────────────────

type processorConfig struct {
	client         *Client
	name           string
	taskStreamID   uint32
	dlqStreamID    uint32
	stateStreamID  uint32
	steps          []stepDef
	totalSteps     uint16
	maxRetries     uint8
	maxContextSize int
	suspended      *syncMap[string, *suspendedEntry]
	nackedSeqs     *syncMap[uint64, struct{}]
}

// ── State sync goroutine ──────────────────────────────────────────────────

func stateSync(ctx context.Context, cfg *processorConfig, sub *Subscription) {
	parkPrefix := fmt.Sprintf("_wf.%s.__state.park.", cfg.name)
	removePrefix := fmt.Sprintf("_wf.%s.__state.remove.", cfg.name)

	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-sub.Messages():
			if !ok {
				return
			}
			subject := msg.Subject()
			if strings.HasPrefix(subject, parkPrefix) {
				iid := subject[len(parkPrefix):]
				stepIdx, state, context, ok := decodePark(msg.Data())
				if ok {
					// Only insert if not already present (local insert may have won).
					cfg.suspended.LoadOrStore(iid, &suspendedEntry{
						stepIndex: stepIdx,
						state:     state,
						context:   context,
					})
				}
			} else if strings.HasPrefix(subject, removePrefix) {
				iid := subject[len(removePrefix):]
				cfg.suspended.Delete(iid)
			}
			msg.Ack()
		}
	}
}

// ── Trigger/Source loop ───────────────────────────────────────────────────

func triggerLoop(ctx context.Context, client *Client, sub *Subscription, taskStreamName, step0Subject string) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-sub.Messages():
			if !ok {
				return
			}
			instanceID := genInstanceID()
			msgID := fmt.Sprintf("wf:%s:0:0", instanceID)
			task := encodeTask(instanceID, 0, 0, msg.Data())
			_ = client.Publish(ctx, taskStreamName, step0Subject, task, WithMsgID(msgID))
			msg.Ack()
		}
	}
}

// ── Main dispatch loop ────────────────────────────────────────────────────

func processLoop(ctx context.Context, cfg *processorConfig, sub *Subscription) {
	resumePrefix := fmt.Sprintf("_wf.%s.resume.", cfg.name)
	timeoutPrefix := fmt.Sprintf("_wf.%s.timeout.", cfg.name)
	cancelPrefix := fmt.Sprintf("_wf.%s.cancel.", cfg.name)
	taskStreamName := fmt.Sprintf("_wf_%s_tasks", cfg.name)
	stateStreamName := fmt.Sprintf("_wf_%s_state", cfg.name)

	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-sub.Messages():
			if !ok {
				return
			}

			subject := msg.Subject()
			payload := msg.Data()

			// ── Resume event ──
			if strings.HasPrefix(subject, resumePrefix) {
				iid := subject[len(resumePrefix):]
				handleResume(ctx, cfg, msg, iid, payload, taskStreamName, stateStreamName)
				continue
			}

			// ── Timeout event ──
			if strings.HasPrefix(subject, timeoutPrefix) {
				iid := subject[len(timeoutPrefix):]
				handleTimeout(ctx, cfg, msg, iid, taskStreamName, stateStreamName)
				continue
			}

			// ── Cancel event ──
			if strings.HasPrefix(subject, cancelPrefix) {
				iid := subject[len(cancelPrefix):]
				handleCancel(ctx, cfg, msg, iid, stateStreamName)
				continue
			}

			// ── Normal task decode ──
			instanceID, stepIndex, attempt, taskCtx, ok := decodeTask(payload)
			if !ok {
				msg.Ack()
				continue
			}

			// ── Context overflow guard (incoming) ──
			if len(taskCtx) > cfg.maxContextSize {
				msg.Ack()
				continue
			}

			// ── Compensation task (high bit set) ──
			if stepIndex&compensationBit != 0 {
				origIdx := stepIndex & ^compensationBit
				if int(origIdx) < len(cfg.steps) {
					step := &cfg.steps[origIdx]
					if step.compensate != nil {
						sctx := StepContext{
							Context:    ctx,
							Name:       cfg.name,
							InstanceID: instanceID,
							StepIndex:  origIdx,
							Attempt:    attempt,
							Input:      taskCtx,
						}
						_ = step.compensate(sctx)
					}
				}
				msg.Ack()
				continue
			}

			// ── Step processing ──
			if int(stepIndex) >= len(cfg.steps) {
				msg.Ack()
				continue
			}

			step := &cfg.steps[stepIndex]
			sctx := StepContext{
				Context:    ctx,
				Name:       cfg.name,
				InstanceID: instanceID,
				StepIndex:  stepIndex,
				Attempt:    attempt,
				Input:      taskCtx,
			}

			// Unify: normal → Done outcome, suspend → StepOutcome.
			var outcome StepOutcome
			var stepErr error

			switch step.kind {
			case stepNormal:
				result, err := step.handler(sctx)
				if err != nil {
					stepErr = err
				} else {
					outcome = OutcomeDone(StepResult{Context: result})
				}
			case stepSuspend:
				outcome, stepErr = step.run(sctx)
			}

			if stepErr != nil {
				handleStepError(ctx, cfg, msg, instanceID, stepIndex, attempt, taskCtx, stepErr, taskStreamName)
				continue
			}

			if outcome.Done != nil {
				handleStepDone(ctx, cfg, msg, instanceID, stepIndex, outcome.Done, taskStreamName)
			} else if outcome.Suspend != nil {
				handleStepSuspend(ctx, cfg, msg, instanceID, stepIndex, step, taskCtx, outcome.Suspend, taskStreamName, stateStreamName)
			}
		}
	}
}

// ── Handler helpers ───────────────────────────────────────────────────────

func handleResume(ctx context.Context, cfg *processorConfig, msg *Msg, iid string, event []byte, taskStreamName, stateStreamName string) {
	entry, found := cfg.suspended.LoadAndDelete(iid)
	if !found {
		// Not found locally — cross-worker retry.
		_, loaded := cfg.nackedSeqs.LoadOrStore(msg.Seq(), struct{}{})
		if !loaded {
			msg.NackDelay(100 * time.Millisecond)
		} else {
			cfg.nackedSeqs.Delete(msg.Seq())
			msg.Ack() // stale
		}
		return
	}

	sidx := entry.stepIndex
	if int(sidx) >= len(cfg.steps) {
		msg.Ack()
		return
	}
	step := &cfg.steps[sidx]
	if step.kind != stepSuspend || step.onResume == nil {
		msg.Ack()
		return
	}

	rctx := ResumeContext{
		Name:       cfg.name,
		InstanceID: iid,
		StepIndex:  sidx,
		State:      entry.state,
		Event:      event,
	}
	result, err := step.onResume(rctx)
	if err != nil {
		msg.Nack()
		return
	}

	// Advance to next step.
	nextStep := sidx + 1
	if nextStep < cfg.totalSteps {
		publishNextStep(ctx, cfg.client, taskStreamName, cfg.name, iid, nextStep, result.Context)
	}

	// Publish remove to state stream.
	publishRemove(ctx, cfg.client, stateStreamName, cfg.name, iid)
	msg.Ack()
}

func handleTimeout(ctx context.Context, cfg *processorConfig, msg *Msg, iid string, taskStreamName, stateStreamName string) {
	entry, found := cfg.suspended.LoadAndDelete(iid)
	if !found {
		// Not found locally — cross-worker retry.
		_, loaded := cfg.nackedSeqs.LoadOrStore(msg.Seq(), struct{}{})
		if !loaded {
			msg.NackDelay(100 * time.Millisecond)
		} else {
			cfg.nackedSeqs.Delete(msg.Seq())
			msg.Ack() // already resumed — timeout is stale
		}
		return
	}

	sidx := entry.stepIndex
	if int(sidx) >= len(cfg.steps) {
		msg.Ack()
		return
	}
	step := &cfg.steps[sidx]
	if step.kind != stepSuspend {
		msg.Ack()
		return
	}

	if step.onTimeout != nil {
		tctx := TimeoutContext{
			Name:       cfg.name,
			InstanceID: iid,
			StepIndex:  sidx,
			State:      entry.state,
		}
		result, err := step.onTimeout(tctx)
		if err != nil {
			msg.Nack()
			return
		}
		nextStep := sidx + 1
		if nextStep < cfg.totalSteps {
			publishNextStep(ctx, cfg.client, taskStreamName, cfg.name, iid, nextStep, result.Context)
		}
	}
	// No timeout handler — discard.

	publishRemove(ctx, cfg.client, stateStreamName, cfg.name, iid)
	msg.Ack()
}

func handleCancel(_ context.Context, cfg *processorConfig, msg *Msg, iid string, stateStreamName string) {
	cfg.suspended.Delete(iid)
	// Always publish remove to state stream for cross-worker propagation.
	publishRemove(context.Background(), cfg.client, stateStreamName, cfg.name, iid)
	msg.Ack()
}

func handleStepDone(ctx context.Context, cfg *processorConfig, msg *Msg, instanceID string, stepIndex uint16, result *StepResult, taskStreamName string) {
	// Context overflow guard (outgoing).
	if len(result.Context) > cfg.maxContextSize {
		msg.Nack()
		return
	}

	nextStep := stepIndex + 1
	if nextStep < cfg.totalSteps {
		publishNextStep(ctx, cfg.client, taskStreamName, cfg.name, instanceID, nextStep, result.Context)
	}
	msg.Ack()
}

func handleStepSuspend(ctx context.Context, cfg *processorConfig, msg *Msg, instanceID string, stepIndex uint16, step *stepDef, taskCtx []byte, data *SuspendData, taskStreamName, stateStreamName string) {
	// Local cache — fast path for same-worker resume.
	cfg.suspended.Store(instanceID, &suspendedEntry{
		stepIndex: stepIndex,
		state:     data.State,
		context:   taskCtx,
	})

	// Publish park event to state stream for cross-worker visibility.
	publishPark(ctx, cfg.client, stateStreamName, cfg.name, instanceID, stepIndex, data.State, taskCtx)

	// Merge timeouts: handler can override the step default.
	effectiveTimeout := data.TimeoutMs
	if effectiveTimeout == 0 {
		effectiveTimeout = step.timeoutMs
	}

	if effectiveTimeout > 0 {
		// Schedule timeout via goroutine sleep + publish.
		go func() {
			time.Sleep(time.Duration(effectiveTimeout) * time.Millisecond)
			timeoutSubject := fmt.Sprintf("_wf.%s.timeout.%s", cfg.name, instanceID)
			timeoutMsgID := fmt.Sprintf("wf:%s:timeout:%d", instanceID, stepIndex)
			_ = cfg.client.Publish(context.Background(), taskStreamName, timeoutSubject, nil, WithMsgID(timeoutMsgID))
		}()
	}

	msg.Ack()
}

func handleStepError(ctx context.Context, cfg *processorConfig, msg *Msg, instanceID string, stepIndex uint16, attempt uint8, taskCtx []byte, stepErr error, taskStreamName string) {
	if attempt+1 >= cfg.maxRetries {
		// Publish to DLQ.
		dlqStreamName := fmt.Sprintf("_wf_%s_dlq", cfg.name)
		dlqSubject := fmt.Sprintf("_wf.%s.dlq.%d", cfg.name, stepIndex)
		idBytes := []byte(instanceID)
		dlqPayload := make([]byte, 0, 2+len(idBytes)+2+1+4+len(stepErr.Error())+len(taskCtx))
		idLenBuf := make([]byte, 2)
		binary.LittleEndian.PutUint16(idLenBuf, uint16(len(idBytes)))
		dlqPayload = append(dlqPayload, idLenBuf...)
		dlqPayload = append(dlqPayload, idBytes...)
		stepBuf := make([]byte, 2)
		binary.LittleEndian.PutUint16(stepBuf, stepIndex)
		dlqPayload = append(dlqPayload, stepBuf...)
		dlqPayload = append(dlqPayload, attempt)
		errBytes := []byte(stepErr.Error())
		errLenBuf := make([]byte, 4)
		binary.LittleEndian.PutUint32(errLenBuf, uint32(len(errBytes)))
		dlqPayload = append(dlqPayload, errLenBuf...)
		dlqPayload = append(dlqPayload, errBytes...)
		dlqPayload = append(dlqPayload, taskCtx...)

		dlqMsgID := fmt.Sprintf("wf:%s:dlq:%d", instanceID, stepIndex)
		_ = cfg.client.Publish(ctx, dlqStreamName, dlqSubject, dlqPayload, WithMsgID(dlqMsgID))

		// Trigger compensation in reverse for completed steps.
		if stepIndex > 0 {
			for compIdx := int(stepIndex) - 1; compIdx >= 0; compIdx-- {
				compStep := compensationBit | uint16(compIdx)
				compSubject := fmt.Sprintf("_wf.%s.compensate.%d", cfg.name, compIdx)
				compTask := encodeTask(instanceID, compStep, 0, taskCtx)
				compMsgID := fmt.Sprintf("wf:%s:comp:%d", instanceID, compIdx)
				_ = cfg.client.Publish(ctx, taskStreamName, compSubject, compTask, WithMsgID(compMsgID))
			}
		}

		msg.Ack()
	} else {
		// Re-publish with incremented attempt (ack current to avoid
		// nack-redelivery loop where the attempt field never changes).
		nextAttempt := attempt + 1
		retryMsgID := fmt.Sprintf("wf:%s:%d:%d", instanceID, stepIndex, nextAttempt)
		retrySubject := fmt.Sprintf("_wf.%s.step.%d", cfg.name, stepIndex)
		retryTask := encodeTask(instanceID, stepIndex, nextAttempt, taskCtx)
		_ = cfg.client.Publish(ctx, taskStreamName, retrySubject, retryTask, WithMsgID(retryMsgID))
		msg.Ack()
	}
}

// ── Publish helpers ───────────────────────────────────────────────────────

func publishNextStep(ctx context.Context, client *Client, taskStreamName, wfName, instanceID string, nextStep uint16, context []byte) {
	msgID := fmt.Sprintf("wf:%s:%d:0", instanceID, nextStep)
	subject := fmt.Sprintf("_wf.%s.step.%d", wfName, nextStep)
	task := encodeTask(instanceID, nextStep, 0, context)
	_ = client.Publish(ctx, taskStreamName, subject, task, WithMsgID(msgID))
}

func publishRemove(ctx context.Context, client *Client, stateStreamName, wfName, instanceID string) {
	rmSubject := fmt.Sprintf("_wf.%s.__state.remove.%s", wfName, instanceID)
	rmMsgID := fmt.Sprintf("wf:%s:remove", instanceID)
	_ = client.Publish(ctx, stateStreamName, rmSubject, nil, WithMsgID(rmMsgID))
}

func publishPark(ctx context.Context, client *Client, stateStreamName, wfName, instanceID string, stepIndex uint16, state, context []byte) {
	parkSubject := fmt.Sprintf("_wf.%s.__state.park.%s", wfName, instanceID)
	parkMsgID := fmt.Sprintf("wf:%s:park:%d", instanceID, stepIndex)
	parkPayload := encodePark(stepIndex, state, context)
	_ = client.Publish(ctx, stateStreamName, parkSubject, parkPayload, WithMsgID(parkMsgID))
}

// ── Generic sync.Map wrapper ──────────────────────────────────────────────

type syncMap[K comparable, V any] struct {
	m sync.Map
}

func (s *syncMap[K, V]) Store(key K, val V) {
	s.m.Store(key, val)
}

func (s *syncMap[K, V]) Load(key K) (V, bool) {
	v, ok := s.m.Load(key)
	if !ok {
		var zero V
		return zero, false
	}
	return v.(V), true
}

func (s *syncMap[K, V]) LoadOrStore(key K, val V) (V, bool) {
	v, loaded := s.m.LoadOrStore(key, val)
	return v.(V), loaded
}

func (s *syncMap[K, V]) LoadAndDelete(key K) (V, bool) {
	v, loaded := s.m.LoadAndDelete(key)
	if !loaded {
		var zero V
		return zero, false
	}
	return v.(V), true
}

func (s *syncMap[K, V]) Delete(key K) {
	s.m.Delete(key)
}
