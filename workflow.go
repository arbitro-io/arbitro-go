package arbitro

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// StepContext provides per-step execution context.
type StepContext struct {
	Context context.Context
	Input   []byte // payload from previous step or trigger
	State   []byte // accumulated saga state
}

// StepFunc is the function signature for a workflow step.
type StepFunc func(step StepContext) ([]byte, error)

// CompensateFunc is the function signature for a compensation (rollback).
type CompensateFunc func(step StepContext) error

// WorkflowBuilder constructs a workflow with fluent API.
type WorkflowBuilder struct {
	client         *Client
	name           string
	triggerSubject string
	triggerStream  string
	steps          []workflowStep
	maxRetries     int
	ackWait        time.Duration
	maxInflight    int
	maxContextSize int
}

type workflowStep struct {
	name       string
	handler    StepFunc
	compensate CompensateFunc
}

// WorkflowHandle is the live handle to a running workflow.
type WorkflowHandle struct {
	client      *Client
	name        string
	stream      string
	dlqStreamID uint32
	cancel      context.CancelFunc
	done        chan struct{}
	sub         *Subscription
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

// Step adds a named step to the workflow.
func (b *WorkflowBuilder) Step(name string, handler StepFunc) *WorkflowBuilder {
	b.steps = append(b.steps, workflowStep{name: name, handler: handler})
	return b
}

// Compensate attaches a compensation function to the last added step.
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
	b.maxRetries = n
	return b
}

// AckWait sets the timeout for each step execution.
func (b *WorkflowBuilder) AckWait(d time.Duration) *WorkflowBuilder {
	b.ackWait = d
	return b
}

// MaxInflight sets the maximum concurrent workflow instances.
func (b *WorkflowBuilder) MaxInflight(n int) *WorkflowBuilder {
	b.maxInflight = n
	return b
}

// MaxContextSize sets the maximum accumulated context bytes.
func (b *WorkflowBuilder) MaxContextSize(n int) *WorkflowBuilder {
	b.maxContextSize = n
	return b
}

// Start registers the workflow and begins processing trigger messages.
func (b *WorkflowBuilder) Start(ctx context.Context) (*WorkflowHandle, error) {
	if b.triggerStream == "" {
		return nil, fmt.Errorf("arbitro: workflow %q needs TriggerStream", b.name)
	}
	if len(b.steps) == 0 {
		return nil, fmt.Errorf("arbitro: workflow %q needs at least one step", b.name)
	}

	// Create DLQ stream for failed instances
	dlqName := b.name + ".dlq"
	_, _ = b.client.CreateStream(ctx, dlqName, StreamConfig{
		SubjectFilter: dlqName + ".>",
		MaxMsgs:       100000,
	})

	// Subscribe to trigger subject
	sub, err := b.client.Subscribe(ctx, b.triggerStream, ConsumerConfig{
		Name:        "wf-" + b.name,
		Filter:      b.triggerSubject,
		AckPolicy:   AckExplicit,
		MaxInflight: uint16(b.maxInflight),
		AckWait:     b.ackWait,
		MaxDeliver:  uint32(b.maxRetries),
	})
	if err != nil {
		return nil, fmt.Errorf("arbitro: workflow %q subscribe: %w", b.name, err)
	}

	childCtx, cancel := context.WithCancel(ctx)
	handle := &WorkflowHandle{
		client: b.client,
		name:   b.name,
		stream: b.triggerStream,
		cancel: cancel,
		done:   make(chan struct{}),
		sub:    sub,
	}

	// Start processor goroutine
	go b.processLoop(childCtx, handle, sub)

	return handle, nil
}

// Trigger manually fires the workflow with the given payload.
func (h *WorkflowHandle) Trigger(ctx context.Context, payload []byte) (string, error) {
	instanceID := fmt.Sprintf("%s-%d", h.name, time.Now().UnixNano())
	err := h.client.Publish(ctx, h.stream, h.name+".trigger", payload, WithMsgID(instanceID))
	return instanceID, err
}

// DLQStreamID returns the stream ID where failed instances land.
func (h *WorkflowHandle) DLQStreamID() uint32 {
	return h.dlqStreamID
}

// Stop gracefully shuts down the workflow processor.
func (h *WorkflowHandle) Stop(ctx context.Context) error {
	h.cancel()
	h.sub.Close()
	<-h.done
	return nil
}

func (b *WorkflowBuilder) processLoop(ctx context.Context, handle *WorkflowHandle, sub *Subscription) {
	defer close(handle.done)

	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-sub.Messages():
			if !ok {
				return
			}
			b.executeInstance(ctx, msg)
		}
	}
}

func (b *WorkflowBuilder) executeInstance(ctx context.Context, trigger *Msg) {
	var completedSteps []int
	input := trigger.Data()
	var state []byte

	for i, step := range b.steps {
		stepCtx := StepContext{
			Context: ctx,
			Input:   input,
			State:   state,
		}

		// Execute step with timeout
		execCtx, cancel := context.WithTimeout(ctx, b.ackWait)
		result, err := step.handler(StepContext{
			Context: execCtx,
			Input:   input,
			State:   state,
		})
		cancel()

		if err != nil {
			// Saga compensation: rollback completed steps in reverse
			b.compensate(ctx, completedSteps, stepCtx)

			// Send to DLQ
			dlqPayload, _ := json.Marshal(map[string]any{
				"workflow":    b.name,
				"failed_step": step.name,
				"error":       err.Error(),
				"input":       input,
			})
			_ = b.client.Publish(ctx, b.name+".dlq", b.name+".dlq.failed", dlqPayload)

			trigger.Nack()
			return
		}

		completedSteps = append(completedSteps, i)
		input = result

		// Accumulate state
		if len(state)+len(result) <= b.maxContextSize {
			state = append(state, result...)
		}
	}

	// All steps completed successfully
	trigger.Ack()
}

func (b *WorkflowBuilder) compensate(ctx context.Context, completedSteps []int, stepCtx StepContext) {
	// Compensate in reverse order
	var wg sync.WaitGroup
	for i := len(completedSteps) - 1; i >= 0; i-- {
		idx := completedSteps[i]
		step := b.steps[idx]
		if step.compensate == nil {
			continue
		}
		wg.Add(1)
		go func(compensate CompensateFunc) {
			defer wg.Done()
			_ = compensate(stepCtx)
		}(step.compensate)
	}
	wg.Wait()
}
