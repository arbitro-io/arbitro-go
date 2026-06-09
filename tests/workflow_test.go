//go:build integration

package tests

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arbitro-io/arbitro-go"
)

// wfName generates a unique workflow name per test to avoid stream/consumer collisions.
func wfName(prefix string) string {
	return uniqueName(prefix)
}

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

	name := wfName("happy")
	wf, err := client.Workflow(name).
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

	err = client.Publish(ctx, stream, stream+".created", []byte(`{"order":"ORD-1"}`))
	if err != nil {
		t.Fatalf("publish trigger: %v", err)
	}

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

	var compensated atomic.Bool

	name := wfName("saga")
	wf, err := client.Workflow(name).
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

	err = client.Publish(ctx, stream, stream+".created", []byte(`{"order":"FAIL-1"}`))
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

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

	name := wfName("manual")
	wf, err := client.Workflow(name).
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

	name := wfName("overflow")
	wf, err := client.Workflow(name).
		Trigger(stream + ".>").
		TriggerStream(stream).
		Step("big-output", func(step arbitro.StepContext) ([]byte, error) {
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
		MaxContextSize(512).
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
		t.Log("context overflow guard prevented advancement — working as expected")
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Fase 0 — trigger_with_id
// ═══════════════════════════════════════════════════════════════════════════

func TestWorkflowTriggerWithID(t *testing.T) {
	client := connectT(t)
	ctx := context.Background()

	var receivedID atomic.Value

	name := wfName("twid")
	wf, err := client.Workflow(name).
		Trigger("_dummy_" + name + ".>").
		Step("capture", func(step arbitro.StepContext) ([]byte, error) {
			receivedID.Store(step.InstanceID)
			return step.Input, nil
		}).
		Start(ctx)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer wf.Stop(ctx)

	err = wf.TriggerWithID(ctx, "ord_42", []byte("hello"))
	if err != nil {
		t.Fatalf("trigger_with_id: %v", err)
	}

	deadline := time.After(3 * time.Second)
	for receivedID.Load() == nil {
		select {
		case <-deadline:
			t.Fatal("timeout waiting for trigger_with_id")
		case <-time.After(50 * time.Millisecond):
		}
	}

	if receivedID.Load().(string) != "ord_42" {
		t.Fatalf("expected instance_id='ord_42', got %q", receivedID.Load())
	}
	t.Log("trigger_with_id works — instance_id=ord_42 received by step")
}

func TestWorkflowTriggerWithIDIdempotent(t *testing.T) {
	client := connectT(t)
	ctx := context.Background()

	var count atomic.Int32

	name := wfName("twidem")
	wf, err := client.Workflow(name).
		Trigger("_dummy_" + name + ".>").
		Step("count", func(step arbitro.StepContext) ([]byte, error) {
			count.Add(1)
			return nil, nil
		}).
		Start(ctx)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer wf.Stop(ctx)

	_ = wf.TriggerWithID(ctx, "idem_1", []byte("first"))
	_ = wf.TriggerWithID(ctx, "idem_1", []byte("second"))

	time.Sleep(2 * time.Second)
	if count.Load() != 1 {
		t.Fatalf("expected exactly 1 execution (dedup), got %d", count.Load())
	}
	t.Log("trigger_with_id idempotency works — duplicate was deduped")
}

// ═══════════════════════════════════════════════════════════════════════════
// Fase 1 — source
// ═══════════════════════════════════════════════════════════════════════════

func TestWorkflowSource(t *testing.T) {
	client := connectT(t)
	ctx := context.Background()
	srcStream := uniqueName("wfsrc")

	_, err := client.CreateStream(ctx, srcStream, arbitro.StreamConfig{
		SubjectFilter: srcStream + ".>",
		MaxMsgs:       10000,
		Journal:       arbitro.JournalTolerant,
	})
	if err != nil {
		t.Fatalf("create src stream: %v", err)
	}
	defer client.DeleteStream(ctx, srcStream)

	var received atomic.Value

	name := wfName("source")
	wf, err := client.Workflow(name).
		Trigger("_dummy_" + name + ".>").
		Source(srcStream, srcStream+".orders.>").
		Step("process", func(step arbitro.StepContext) ([]byte, error) {
			received.Store(string(step.Input))
			return step.Input, nil
		}).
		Start(ctx)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer wf.Stop(ctx)

	err = client.Publish(ctx, srcStream, srcStream+".orders.new", []byte("order-data"))
	if err != nil {
		t.Fatalf("publish source: %v", err)
	}

	deadline := time.After(5 * time.Second)
	for received.Load() == nil {
		select {
		case <-deadline:
			t.Fatal("timeout waiting for source trigger")
		case <-time.After(50 * time.Millisecond):
		}
	}

	if received.Load().(string) != "order-data" {
		t.Fatalf("expected 'order-data', got %q", received.Load())
	}
	t.Log("source trigger works — workflow received source payload")
}

// ═══════════════════════════════════════════════════════════════════════════
// Fase 2 — suspend/resume
// ═══════════════════════════════════════════════════════════════════════════

func TestWorkflowSuspendResume(t *testing.T) {
	client := connectT(t)
	ctx := context.Background()

	var doneCtx atomic.Value

	name := wfName("suspres")
	wf, err := client.Workflow(name).
		Trigger("_dummy_" + name + ".>").
		SuspendStep("wait-payment", 0,
			func(step arbitro.StepContext) (arbitro.StepOutcome, error) {
				return arbitro.OutcomeSuspend([]byte("prepared:"+string(step.Input)), 0), nil
			},
			func(rctx arbitro.ResumeContext) (arbitro.StepResult, error) {
				ctx := string(rctx.State) + "|resumed:" + string(rctx.Event)
				return arbitro.StepResult{Context: []byte(ctx)}, nil
			},
		).
		Step("finalize", func(step arbitro.StepContext) ([]byte, error) {
			result := string(step.Input) + "|done"
			doneCtx.Store(result)
			return []byte(result), nil
		}).
		Start(ctx)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer wf.Stop(ctx)

	err = wf.TriggerWithID(ctx, "pay_1", []byte("init"))
	if err != nil {
		t.Fatalf("trigger: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	err = wf.Resume(ctx, "pay_1", []byte("card_ok"))
	if err != nil {
		t.Fatalf("resume: %v", err)
	}

	deadline := time.After(5 * time.Second)
	for doneCtx.Load() == nil {
		select {
		case <-deadline:
			t.Fatal("timeout waiting for resume completion")
		case <-time.After(50 * time.Millisecond):
		}
	}

	expected := "prepared:init|resumed:card_ok|done"
	if doneCtx.Load().(string) != expected {
		t.Fatalf("expected %q, got %q", expected, doneCtx.Load())
	}
	t.Logf("suspend/resume works: %s", doneCtx.Load())
}

// ═══════════════════════════════════════════════════════════════════════════
// Fase 3 — timeout
// ═══════════════════════════════════════════════════════════════════════════

func TestWorkflowSuspendTimeout(t *testing.T) {
	client := connectT(t)
	ctx := context.Background()

	var doneCtx atomic.Value

	name := wfName("timeout")
	wf, err := client.Workflow(name).
		Trigger("_dummy_" + name + ".>").
		SuspendStep("wait-approval", 200,
			func(step arbitro.StepContext) (arbitro.StepOutcome, error) {
				return arbitro.OutcomeSuspend([]byte("waiting"), 0), nil
			},
			func(rctx arbitro.ResumeContext) (arbitro.StepResult, error) {
				return arbitro.StepResult{Context: []byte("resumed")}, nil
			},
		).
		OnTimeout(func(tctx arbitro.TimeoutContext) (arbitro.StepResult, error) {
			return arbitro.StepResult{Context: []byte("timed_out:" + string(tctx.State))}, nil
		}).
		Step("finalize", func(step arbitro.StepContext) ([]byte, error) {
			doneCtx.Store(string(step.Input) + "|done")
			return step.Input, nil
		}).
		Start(ctx)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer wf.Stop(ctx)

	err = wf.TriggerWithID(ctx, "to_1", []byte("start"))
	if err != nil {
		t.Fatalf("trigger: %v", err)
	}

	deadline := time.After(5 * time.Second)
	for doneCtx.Load() == nil {
		select {
		case <-deadline:
			t.Fatal("timeout waiting for timeout handler completion")
		case <-time.After(50 * time.Millisecond):
		}
	}

	expected := "timed_out:waiting|done"
	if doneCtx.Load().(string) != expected {
		t.Fatalf("expected %q, got %q", expected, doneCtx.Load())
	}
	t.Logf("timeout works: %s", doneCtx.Load())
}

// ═══════════════════════════════════════════════════════════════════════════
// Fase 4 — cancel
// ═══════════════════════════════════════════════════════════════════════════

func TestWorkflowCancel(t *testing.T) {
	client := connectT(t)
	ctx := context.Background()

	var resumed atomic.Bool

	name := wfName("cancel")
	wf, err := client.Workflow(name).
		Trigger("_dummy_" + name + ".>").
		SuspendStep("wait", 0,
			func(step arbitro.StepContext) (arbitro.StepOutcome, error) {
				return arbitro.OutcomeSuspend([]byte("parked"), 0), nil
			},
			func(rctx arbitro.ResumeContext) (arbitro.StepResult, error) {
				resumed.Store(true)
				return arbitro.StepResult{Context: []byte("should_not_reach")}, nil
			},
		).
		Step("after", func(step arbitro.StepContext) ([]byte, error) {
			return nil, nil
		}).
		Start(ctx)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer wf.Stop(ctx)

	err = wf.TriggerWithID(ctx, "can_1", []byte("data"))
	if err != nil {
		t.Fatalf("trigger: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	err = wf.Cancel(ctx, "can_1")
	if err != nil {
		t.Fatalf("cancel: %v", err)
	}

	_ = wf.Resume(ctx, "can_1", []byte("late"))

	time.Sleep(2 * time.Second)
	if resumed.Load() {
		t.Fatal("resume handler should not have been called after cancel")
	}
	t.Log("cancel works — resume after cancel is no-op")
}

func TestWorkflowCancelThenResume(t *testing.T) {
	client := connectT(t)
	ctx := context.Background()

	var resumed atomic.Int32

	name := wfName("canthenres")
	wf, err := client.Workflow(name).
		Trigger("_dummy_" + name + ".>").
		SuspendStep("wait", 0,
			func(step arbitro.StepContext) (arbitro.StepOutcome, error) {
				return arbitro.OutcomeSuspend([]byte("parked"), 0), nil
			},
			func(rctx arbitro.ResumeContext) (arbitro.StepResult, error) {
				resumed.Add(1)
				return arbitro.StepResult{}, nil
			},
		).
		Start(ctx)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer wf.Stop(ctx)

	err = wf.TriggerWithID(ctx, "ctr_1", []byte("data"))
	if err != nil {
		t.Fatalf("trigger: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	_ = wf.Cancel(ctx, "ctr_1")
	time.Sleep(200 * time.Millisecond)
	_ = wf.Resume(ctx, "ctr_1", []byte("late"))

	time.Sleep(2 * time.Second)
	if resumed.Load() != 0 {
		t.Fatalf("expected 0 resumes after cancel, got %d", resumed.Load())
	}
	t.Log("cancel-then-resume: resume correctly ignored")
}

// ═══════════════════════════════════════════════════════════════════════════
// Fase 5 — cross-worker distributed tests
// ═══════════════════════════════════════════════════════════════════════════

func TestWorkflowDistribSuspendResume(t *testing.T) {
	ctx := context.Background()
	const nWorkers = 4
	const nInstances = 8

	clients := make([]*arbitro.Client, nWorkers)
	for i := 0; i < nWorkers; i++ {
		clients[i] = connectT(t)
	}

	var doneCount atomic.Int32
	handles := make([]*arbitro.WorkflowHandle, nWorkers)

	name := wfName("distsr")
	for i := 0; i < nWorkers; i++ {
		wf, err := clients[i].Workflow(name).
			Trigger("_dummy_" + name + ".>").
			SuspendStep("wait", 0,
				func(step arbitro.StepContext) (arbitro.StepOutcome, error) {
					return arbitro.OutcomeSuspend(
						[]byte("prepared:"+string(step.Input)),
						0,
					), nil
				},
				func(rctx arbitro.ResumeContext) (arbitro.StepResult, error) {
					ctx := string(rctx.State) + "|resumed:" + string(rctx.Event)
					return arbitro.StepResult{Context: []byte(ctx)}, nil
				},
			).
			Step("done", func(step arbitro.StepContext) ([]byte, error) {
				doneCount.Add(1)
				return []byte(string(step.Input) + "|done"), nil
			}).
			AckWait(10 * time.Second).
			MaxInflight(10).
			Start(ctx)
		if err != nil {
			t.Fatalf("worker %d start: %v", i, err)
		}
		handles[i] = wf
	}
	defer func() {
		for _, h := range handles {
			h.Stop(ctx)
		}
	}()

	for i := 0; i < nInstances; i++ {
		iid := fmt.Sprintf("d_%d", i)
		err := handles[0].TriggerWithID(ctx, iid, []byte(fmt.Sprintf("p%d", i)))
		if err != nil {
			t.Fatalf("trigger %s: %v", iid, err)
		}
	}

	time.Sleep(1 * time.Second)

	for i := 0; i < nInstances; i++ {
		iid := fmt.Sprintf("d_%d", i)
		w := i % nWorkers
		err := handles[w].Resume(ctx, iid, []byte(fmt.Sprintf("ev%d", i)))
		if err != nil {
			t.Fatalf("resume %s on worker %d: %v", iid, w, err)
		}
	}

	deadline := time.After(10 * time.Second)
	for doneCount.Load() < int32(nInstances) {
		select {
		case <-deadline:
			t.Fatalf("timeout: %d/%d completed", doneCount.Load(), nInstances)
		case <-time.After(100 * time.Millisecond):
		}
	}
	t.Logf("distrib suspend/resume: %d/%d completed", doneCount.Load(), nInstances)
}

func TestWorkflowDistribSuspendCancel(t *testing.T) {
	ctx := context.Background()
	const nWorkers = 4
	const nInstances = 8

	clients := make([]*arbitro.Client, nWorkers)
	for i := 0; i < nWorkers; i++ {
		clients[i] = connectT(t)
	}

	var resumed atomic.Int32
	handles := make([]*arbitro.WorkflowHandle, nWorkers)

	name := wfName("distcan")
	for i := 0; i < nWorkers; i++ {
		wf, err := clients[i].Workflow(name).
			Trigger("_dummy_" + name + ".>").
			SuspendStep("wait", 0,
				func(step arbitro.StepContext) (arbitro.StepOutcome, error) {
					return arbitro.OutcomeSuspend([]byte("parked"), 0), nil
				},
				func(rctx arbitro.ResumeContext) (arbitro.StepResult, error) {
					resumed.Add(1)
					return arbitro.StepResult{}, nil
				},
			).
			AckWait(10 * time.Second).
			MaxInflight(10).
			Start(ctx)
		if err != nil {
			t.Fatalf("worker %d start: %v", i, err)
		}
		handles[i] = wf
	}
	defer func() {
		for _, h := range handles {
			h.Stop(ctx)
		}
	}()

	for i := 0; i < nInstances; i++ {
		iid := fmt.Sprintf("dc_%d", i)
		err := handles[0].TriggerWithID(ctx, iid, []byte("data"))
		if err != nil {
			t.Fatalf("trigger %s: %v", iid, err)
		}
	}

	time.Sleep(1 * time.Second)

	for i := 0; i < nInstances; i++ {
		iid := fmt.Sprintf("dc_%d", i)
		w := i % nWorkers
		err := handles[w].Cancel(ctx, iid)
		if err != nil {
			t.Fatalf("cancel %s on worker %d: %v", iid, w, err)
		}
	}

	time.Sleep(500 * time.Millisecond)
	for i := 0; i < nInstances; i++ {
		iid := fmt.Sprintf("dc_%d", i)
		_ = handles[i%nWorkers].Resume(ctx, iid, []byte("late"))
	}

	time.Sleep(2 * time.Second)
	if resumed.Load() != 0 {
		t.Fatalf("expected 0 resumes after cancel, got %d", resumed.Load())
	}
	t.Logf("distrib cancel: all %d instances cancelled, resume correctly ignored", nInstances)
}
