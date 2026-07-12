package proto

import (
	"encoding/binary"
	"encoding/json"
	"testing"
)

// TestEncodeCreateCronBody asserts the CreateCron JSON body matches the
// arbitro-proto canonical shape (wire/cron.rs CreateCronBody): field "every"
// (not "cron_expr"), name/every as JSON strings (not byte arrays), and a
// "timeout_ms" field always present.
func TestEncodeCreateCronBody(t *testing.T) {
	tests := []struct {
		name      string
		cronName  string
		every     string
		tz        string
		timeoutMs uint32
		overlap   bool
	}{
		{"with tz", "job-1", "0 * * * * *", "UTC", 5000, false},
		{"no tz", "job-2", "*/5 * * * * *", "", 30000, true},
		{"zero timeout", "job-3", "* * * * * *", "", 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			frame, err := EncodeCreateCron(1, []byte(tt.cronName), tt.every, tt.tz, tt.timeoutMs, tt.overlap)
			if err != nil {
				t.Fatalf("EncodeCreateCron: %v", err)
			}

			hdr := DecodeHeader(frame)
			if hdr.Action != ActionCreateCron {
				t.Errorf("action: got 0x%04X, want 0x%04X", hdr.Action, ActionCreateCron)
			}

			body := frame[HeaderSize:]

			var raw map[string]any
			if err := json.Unmarshal(body, &raw); err != nil {
				t.Fatalf("unmarshal body: %v", err)
			}

			if _, hasWrongField := raw["cron_expr"]; hasWrongField {
				t.Errorf("body must NOT contain legacy field %q: %s", "cron_expr", body)
			}
			everyVal, ok := raw["every"]
			if !ok {
				t.Fatalf("body missing required field %q: %s", "every", body)
			}
			if everyVal != tt.every {
				t.Errorf("every: got %v, want %q", everyVal, tt.every)
			}

			nameVal, ok := raw["name"]
			if !ok {
				t.Fatalf("body missing required field %q: %s", "name", body)
			}
			if _, isArray := nameVal.([]any); isArray {
				t.Errorf("name must be encoded as a JSON string, not an array: %s", body)
			}
			if nameVal != tt.cronName {
				t.Errorf("name: got %v, want %q", nameVal, tt.cronName)
			}

			timeoutVal, ok := raw["timeout_ms"]
			if !ok {
				t.Fatalf("body missing required field %q: %s", "timeout_ms", body)
			}
			if uint32(timeoutVal.(float64)) != tt.timeoutMs {
				t.Errorf("timeout_ms: got %v, want %d", timeoutVal, tt.timeoutMs)
			}

			if tt.tz == "" {
				if _, present := raw["tz"]; present {
					t.Errorf("tz should be omitted when empty: %s", body)
				}
			} else {
				if raw["tz"] != tt.tz {
					t.Errorf("tz: got %v, want %q", raw["tz"], tt.tz)
				}
			}

			overlapVal, ok := raw["overlap"]
			if !ok {
				t.Fatalf("body missing required field %q: %s", "overlap", body)
			}
			if overlapVal != tt.overlap {
				t.Errorf("overlap: got %v, want %v", overlapVal, tt.overlap)
			}
		})
	}
}

// TestCronFireRoundTrip verifies DecodeCronFireBody against the exact binary
// layout defined by arbitro-proto wire/cron.rs encode_cron_fire:
// [2 name_len LE][8 fire_time_ms LE][8 fire_count LE][name...].
func TestCronFireRoundTrip(t *testing.T) {
	name := "nightly-report"
	fireTimeMs := uint64(1_752_000_000_000)
	fireCount := uint64(42)

	// Build the body exactly as the Rust encoder would (manually, since the
	// Go client never encodes CronFire — only the broker does).
	body := make([]byte, cronFireFixed+len(name))
	binary.LittleEndian.PutUint16(body[0:2], uint16(len(name)))
	binary.LittleEndian.PutUint64(body[2:10], fireTimeMs)
	binary.LittleEndian.PutUint64(body[10:18], fireCount)
	copy(body[18:], name)

	gotName, gotFireTimeMs, gotFireCount, err := DecodeCronFireBody(body)
	if err != nil {
		t.Fatalf("DecodeCronFireBody: %v", err)
	}
	if gotName != name {
		t.Errorf("name: got %q, want %q", gotName, name)
	}
	if gotFireTimeMs != fireTimeMs {
		t.Errorf("fireTimeMs: got %d, want %d", gotFireTimeMs, fireTimeMs)
	}
	if gotFireCount != fireCount {
		t.Errorf("fireCount: got %d, want %d", gotFireCount, fireCount)
	}
}

func TestCronFireBodyTooShort(t *testing.T) {
	if _, _, _, err := DecodeCronFireBody([]byte{1, 2, 3}); err == nil {
		t.Fatal("expected error for truncated CronFire body")
	}
}

func TestCronFireBodyTruncatedName(t *testing.T) {
	body := make([]byte, cronFireFixed)
	binary.LittleEndian.PutUint16(body[0:2], 10) // name_len=10 but no name bytes follow
	if _, _, _, err := DecodeCronFireBody(body); err == nil {
		t.Fatal("expected error for truncated cron fire name")
	}
}

// TestCronAckEncoding verifies EncodeCronAck against the exact binary layout
// defined by arbitro-proto wire/cron.rs encode_cron_ack:
// [2 name_len LE][1 status (0=ok, 1=error)][name...].
func TestCronAckEncoding(t *testing.T) {
	tests := []struct {
		name       string
		cronName   string
		ok         bool
		wantStatus byte
	}{
		{"ok ack", "job-a", true, 0},
		{"error ack", "job-b", false, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			frame, err := EncodeCronAck(7, []byte(tt.cronName), tt.ok)
			if err != nil {
				t.Fatalf("EncodeCronAck: %v", err)
			}

			hdr := DecodeHeader(frame)
			if hdr.Action != ActionCronAck {
				t.Errorf("action: got 0x%04X, want 0x%04X", hdr.Action, ActionCronAck)
			}
			if hdr.Seq != 7 {
				t.Errorf("seq: got %d, want 7", hdr.Seq)
			}

			wantBodyLen := cronAckFixed + len(tt.cronName)
			if int(hdr.MsgLen) != wantBodyLen {
				t.Errorf("msg_len: got %d, want %d", hdr.MsgLen, wantBodyLen)
			}

			body := frame[HeaderSize:]
			nameLen := binary.LittleEndian.Uint16(body[0:2])
			if int(nameLen) != len(tt.cronName) {
				t.Errorf("name_len: got %d, want %d", nameLen, len(tt.cronName))
			}
			if body[2] != tt.wantStatus {
				t.Errorf("status byte: got %d, want %d", body[2], tt.wantStatus)
			}
			gotName := string(body[cronAckFixed : cronAckFixed+int(nameLen)])
			if gotName != tt.cronName {
				t.Errorf("name: got %q, want %q", gotName, tt.cronName)
			}
		})
	}
}
