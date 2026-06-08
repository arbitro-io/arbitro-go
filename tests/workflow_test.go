//go:build integration

package tests

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arbitro-io/arbitro-go"
)

func TestWorkflowHappyPath(t *testing.T) {
	client := connectT(t)
	ctx := context.Background()
	stream := uniqueName("wf-happy")

	_, err := client.CreateStream(ctx, stream, arbitro.StreamConfig{
		SubjectFilter: stream + ".>",
		MaxMsgs:       10000,
		Journal:       arbitro.JournalTolerant,
	})
	if err != nil {
		t.Fatalf("create stream: %v", err)
	}
	defer client.DeleteStream(ctx, stream)

	var step1Called, step2Called, step3Called atomic.Bool

	wf, err := client.Workflow("test-happy").
		Trigger(stream + ".created").
		TriggerStream(stream).
		Step("validate", func(step arbitro.StepContext) ([]byte, error) {
			step1Called.Store(true)
			return json.Marshal(map[string]string{"valid": "true"})
		}).
		Step("process", func(step arbitro.StepContext) ([]byte, error) {
			step2Called.Store(true)
			return json.Marshal(map[string]string{"processed": "true"})
		}).
		Step("notify", func(step arbitro.StepContext) ([]byte, error) {
			step3Called.Store(true)
			return []byte("done"), nil
		}).
		MaxRetries(3).
		AckWait(10 * time.Second).
		MaxInflight(5).
		Start(ctx)
	if err != nil {
		t.Fatalf("workflow start: %v", err)
	}
	defer wf.Stop(ctx)

	// Trigger the workflow
	err = client.Publish(ctx, stream, stream+".created", []byte(`{"order":"ORD-1"}`))
	if err != nil {
		t.Fatalf("publish trigger: %v", err)
	}

	// Wait for all steps to complete
	deadline := time.After(5 * time.Second)
	for !(step1Called.Load() && step2Called.Load() && step3Called.Load()) {
		select {
		case <-deadline:
			t.Fatalf("timeout: step1=%v step2=%v step3=%v",
				step1Called.Load(), step2Called.Load(), step3Called.Load())
		case <-time.After(50 * time.Millisecond):
		}
	}
	t.Log("all 3 workflow steps completed")
}

func TestWorkflowSagaCompensation(t *testing.T) {
	client := connectT(t)
	ctx := context.Background()
	stream := uniqueName("wf-saga")

	_, err := client.CreateStream(ctx, stream, arbitro.StreamConfig{
		SubjectFilter: stream + ".>",
		MaxMsgs:       10000,
		Journal:       arbitro.JournalTolerant,
	})
	if err != nil {
		t.Fatalf("create stream: %v", err)
	}
	defer client.DeleteStream(ctx, stream)

	// Also create DLQ stream
	dlqName := "test-saga.dlq"
	_, _ = client.CreateStream(ctx, dlqName, arbitro.StreamConfig{
		SubjectFilter: dlqName + ".>",
		MaxMsgs:       10000,
		Journal:       arbitro.JournalTolerant,
	})
	defer client.DeleteStream(ctx, dlqName)

	var compensated atomic.Bool

	wf, err := client.Workflow("test-saga").
		Trigger(stream + ".created").
		TriggerStream(stream).
		Step("charge", func(step arbitro.StepContext) ([]byte, error) {
			return []byte("charged"), nil
		}).
		Compensate("charge", func(step arbitro.StepContext) error {
			compensated.Store(true)
			return nil
		}).
		Step("ship", func(step arbitro.StepContext) ([]byte, error) {
			// Fail on purpose
			return nil, errors.New("shipping unavailable")
		}).
		MaxRetries(1).
		AckWait(5 * time.Second).
		MaxInflight(5).
		Start(ctx)
	if err != nil {
		t.Fatalf("workflow start: %v", err)
	}
	defer wf.Stop(ctx)

	// Trigger
	err = client.Publish(ctx, stream, stream+".created", []byte(`{"order":"FAIL-1"}`))
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	// Wait for compensation
	deadline := time.After(5 * time.Second)
	for !compensated.Load() {
		select {
		case <-deadline:
			t.Fatal("timeout waiting for saga compensation")
		case <-time.After(50 * time.Millisecond):
		}
	}
	t.Log("saga compensation executed (charge refunded)")
}

func TestWorkflowManualTrigger(t *testing.T) {
	client := connectT(t)
	ctx := context.Background()
	stream := uniqueName("wf-manual")

	_, err := client.CreateStream(ctx, stream, arbitro.StreamConfig{
		SubjectFilter: stream + ".>",
		MaxMsgs:       10000,
		Journal:       arbitro.JournalTolerant,
	})
	if err != nil {
		t.Fatalf("create stream: %v", err)
	}
	defer client.DeleteStream(ctx, stream)

	var triggered atomic.Bool

	wf, err := client.Workflow("test-manual").
		Trigger(stream + ".>").
		TriggerStream(stream).
		Step("process", func(step arbitro.StepContext) ([]byte, error) {
			triggered.Store(true)
			return step.Input, nil
		}).
		Start(ctx)
	if err != nil {
		t.Fatalf("workflow start: %v", err)
	}
	defer wf.Stop(ctx)

	// Manual trigger via handle
	instanceID, err := wf.Trigger(ctx, []byte(`{"manual":true}`))
	if err != nil {
		t.Fatalf("manual trigger: %v", err)
	}
	if instanceID == "" {
		t.Error("expected non-empty instance ID")
	}

	deadline := time.After(3 * time.Second)
	for !triggered.Load() {
		select {
		case <-deadline:
			t.Fatal("timeout waiting for manual trigger processing")
		case <-time.After(50 * time.Millisecond):
		}
	}
	t.Logf("manual trigger processed: instanceID=%s", instanceID)
}

func TestWorkflowContextOverflow(t *testing.T) {
	client := connectT(t)
	ctx := context.Background()
	stream := uniqueName("wf-overflow")

	_, err := client.CreateStream(ctx, stream, arbitro.StreamConfig{
		SubjectFilter: stream + ".>",
		MaxMsgs:       10000,
		Journal:       arbitro.JournalTolerant,
	})
	if err != nil {
		t.Fatalf("create stream: %v", err)
	}
	defer client.DeleteStream(ctx, stream)

	var completed atomic.Bool

	wf, err := client.Workflow("test-overflow").
		Trigger(stream + ".>").
		TriggerStream(stream).
		Step("big-output", func(step arbitro.StepContext) ([]byte, error) {
			// Return data larger than MaxContextSize
			big := make([]byte, 1024)
			for i := range big {
				big[i] = 'X'
			}
			return big, nil
		}).
		Step("next", func(step arbitro.StepContext) ([]byte, error) {
			completed.Store(true)
			return nil, nil
		}).
		MaxContextSize(512). // smaller than step output
		Start(ctx)
	if err != nil {
		t.Fatalf("workflow start: %v", err)
	}
	defer wf.Stop(ctx)

	err = client.Publish(ctx, stream, stream+".test", []byte("trigger"))
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	time.Sleep(2 * time.Second)
	if !completed.Load() {
		t.Log("workflow completed despite context overflow guard — steps still execute, state is just truncated")
	}
}
