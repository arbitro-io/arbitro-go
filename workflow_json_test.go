package arbitro

import (
	"encoding/json"
	"testing"
)

type payload struct {
	Name  string `json:"name"`
	Count int    `json:"count"`
}

func ctxWith(b string) StepContext { return StepContext{Input: []byte(b)} }

func TestJSONDecodes(t *testing.T) {
	var p payload
	if err := ctxWith(`{"name":"ana","count":3}`).JSON(&p); err != nil {
		t.Fatalf("JSON: %v", err)
	}
	if p.Name != "ana" || p.Count != 3 {
		t.Errorf("got %+v", p)
	}
}

// JSON is the strict form: an empty context is a parse error. Callers who
// expect to arrive with no payload reach for JSONOrDefault instead, and the
// split only means something if this half actually fails.
func TestJSONErrorsOnEmptyContext(t *testing.T) {
	var p payload
	if err := ctxWith("").JSON(&p); err == nil {
		t.Error("JSON on an empty context should fail — that is what JSONOrDefault is for")
	}
}

func TestJSONOrDefaultLeavesZeroValueOnEmpty(t *testing.T) {
	p := payload{Name: "pre-existing", Count: 99}
	if err := ctxWith("").JSONOrDefault(&p); err != nil {
		t.Fatalf("JSONOrDefault: %v", err)
	}
	// Go has no Default trait; "default" is whatever the caller passed in
	// untouched. Asserting the value is unchanged pins that this does not
	// silently zero a struct the caller pre-filled.
	if p.Name != "pre-existing" || p.Count != 99 {
		t.Errorf("empty context must leave the destination alone, got %+v", p)
	}
}

// The empty-context shortcut must not become a blanket "errors are fine".
// A context that is present but malformed is a real failure and has to stay
// one, or a corrupted payload silently becomes an empty struct.
func TestJSONOrDefaultStillErrorsOnGarbage(t *testing.T) {
	var p payload
	if err := ctxWith("not json at all").JSONOrDefault(&p); err == nil {
		t.Error("a non-empty unparseable context must still error")
	}
}

func TestJSONMergeAddsAndOverwritesKeys(t *testing.T) {
	out, err := ctxWith(`{"name":"ana","count":1}`).
		JSONMerge(map[string]any{"count": 7, "extra": true})
	if err != nil {
		t.Fatalf("JSONMerge: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("result is not JSON: %v", err)
	}
	if got["name"] != "ana" {
		t.Errorf("untouched key lost: %v", got)
	}
	if got["count"] != float64(7) {
		t.Errorf("patch did not overwrite: %v", got)
	}
	if got["extra"] != true {
		t.Errorf("new key missing: %v", got)
	}
}

// Shallow is a decision, not an oversight: a deep merge has to guess whether
// arrays replace or concatenate, and guessing wrong is silent. This pins the
// choice so a later "improvement" to deep-merge fails here instead of in
// someone's workflow.
func TestJSONMergeIsShallow(t *testing.T) {
	out, err := ctxWith(`{"user":{"name":"ana","age":30}}`).
		JSONMerge(map[string]any{"user": map[string]any{"name": "bea"}})
	if err != nil {
		t.Fatalf("JSONMerge: %v", err)
	}
	var got map[string]map[string]any
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("result is not JSON: %v", err)
	}
	if _, ok := got["user"]["age"]; ok {
		t.Errorf("nested object was merged, not replaced — merge must stay shallow: %v", got)
	}
	if got["user"]["name"] != "bea" {
		t.Errorf("nested replace lost the patch: %v", got)
	}
}

func TestJSONMergeOnEmptyContextYieldsPatch(t *testing.T) {
	out, err := ctxWith("").JSONMerge(map[string]any{"first": "step"})
	if err != nil {
		t.Fatalf("JSONMerge: %v", err)
	}
	if string(out) != `{"first":"step"}` {
		t.Errorf("got %s", out)
	}
}

// Valid JSON that is not an object has nothing to merge into, so it is
// replaced. Malformed bytes are a different case and must error — collapsing
// the two would turn a corrupted context into a silent overwrite.
func TestJSONMergeReplacesNonObjectButErrorsOnGarbage(t *testing.T) {
	for _, ctx := range []string{`[1,2,3]`, `"a string"`, `null`, `42`} {
		out, err := ctxWith(ctx).JSONMerge(map[string]any{"k": "v"})
		if err != nil {
			t.Errorf("context %s: unexpected error %v", ctx, err)
			continue
		}
		if string(out) != `{"k":"v"}` {
			t.Errorf("context %s: expected outright replace, got %s", ctx, out)
		}
	}

	if _, err := ctxWith("{oops").JSONMerge(map[string]any{"k": "v"}); err == nil {
		t.Error("malformed context must error, not be silently replaced")
	}
}

// A non-object patch has nothing to merge with either.
func TestJSONMergeNonObjectPatchReplaces(t *testing.T) {
	out, err := ctxWith(`{"name":"ana"}`).JSONMerge([]int{1, 2})
	if err != nil {
		t.Fatalf("JSONMerge: %v", err)
	}
	if string(out) != `[1,2]` {
		t.Errorf("got %s", out)
	}
}

func TestJSONReplaceDiscardsPreviousContext(t *testing.T) {
	out, err := ctxWith(`{"name":"ana","count":9}`).
		JSONReplace(payload{Name: "bea", Count: 1})
	if err != nil {
		t.Fatalf("JSONReplace: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("result is not JSON: %v", err)
	}
	if len(got) != 2 || got["name"] != "bea" {
		t.Errorf("replace should not carry anything over: %v", got)
	}
}
