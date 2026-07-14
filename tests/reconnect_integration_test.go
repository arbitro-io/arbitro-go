//go:build integration

package tests

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/arbitro-io/arbitro-go"
)

// findBrokerBinary locates the arbitro-server executable built from the
// sibling Rust workspace (arbitro/target/release/arbitro-server[.exe]).
// Returns "" if it cannot be found, in which case the test is skipped —
// spawning/killing a real broker process is what lets this test exercise
// the full G02 reconnect supervisor (redial + resubscribe) rather than just
// a local socket close.
func findBrokerBinary() string {
	if p := os.Getenv("ARBITRO_SERVER_BIN"); p != "" {
		return p
	}
	name := "arbitro-server"
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	// tests/ -> arbitro-go/ -> arbitro-io/ -> arbitro/target/release/<name>
	candidate := filepath.Join("..", "..", "arbitro", "target", "release", name)
	if _, err := os.Stat(candidate); err == nil {
		if abs, err := filepath.Abs(candidate); err == nil {
			return abs
		}
		return candidate
	}
	return ""
}

// startBroker launches a broker subprocess listening on addr, persisting
// under dataDir. The caller is responsible for killing/waiting on the
// returned command.
func startBroker(t *testing.T, bin, addr, dataDir string) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(bin)
	cmd.Env = append(os.Environ(),
		"ARBITRO_LISTEN="+addr,
		"ARBITRO_DATA_DIR="+dataDir,
		"ARBITRO_FSYNC_POLICY=none",
	)
	if err := cmd.Start(); err != nil {
		t.Fatalf("start broker %s: %v", bin, err)
	}
	return cmd
}

// killBrokerProcess terminates cmd's process abruptly (SIGKILL-equivalent) and
// waits for it to exit. On Windows, Process.Kill() (OpenProcess+TerminateProcess)
// occasionally races with the OS/AV holding a transient handle on a
// just-started executable and returns "Access is denied"; taskkill /F retries
// past that. On other platforms Process.Kill() is used directly.
func killBrokerProcess(t *testing.T, cmd *exec.Cmd) {
	t.Helper()
	if runtime.GOOS == "windows" {
		var lastErr error
		for i := 0; i < 10; i++ {
			out, err := exec.Command("taskkill", "/F", "/PID", fmt.Sprintf("%d", cmd.Process.Pid)).CombinedOutput()
			if err == nil || strings.Contains(string(out), "not found") {
				// Either taskkill succeeded, or the process is already gone
				// (a prior attempt landed even though it reported an error).
				lastErr = nil
				break
			}
			lastErr = fmt.Errorf("%v: %s", err, out)
			time.Sleep(300 * time.Millisecond)
		}
		if lastErr != nil {
			t.Fatalf("taskkill broker: %v", lastErr)
		}
	} else {
		if err := cmd.Process.Kill(); err != nil {
			t.Fatalf("kill broker: %v", err)
		}
	}
	_ = cmd.Wait()
}

// waitForBroker polls addr until a TCP connection succeeds or timeout elapses.
func waitForBroker(addr string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}

// TestReconnectSupervisorFullCycle is the real reconnect integration test
// requested for Wave 3 (G02): it kills the broker process out from under a
// connected client (simulating a crash, not just a graceful Close()),
// restarts it, and verifies:
//   - the client's reconnect supervisor notices the dead connection and
//     redials with backoff,
//   - Client.Metrics().Reconnects is bumped,
//   - the pre-existing Subscribe is replayed against the new connection and
//     the consumer keeps receiving deliveries,
//   - Publish works again once the new connection is in place.
//
// It is skipped (not failed) when the arbitro-server binary can't be found,
// since that binary lives in the sibling arbitro/ (Rust) workspace and may
// not be built in every environment running `go test`.
func TestReconnectSupervisorFullCycle(t *testing.T) {
	bin := findBrokerBinary()
	if bin == "" {
		t.Skip("arbitro-server binary not found (set ARBITRO_SERVER_BIN to override); skipping full reconnect integration test")
	}

	addr := "127.0.0.1:19898" // dedicated port so we don't collide with a shared broker
	dataDir := t.TempDir()

	cmd := startBroker(t, bin, addr, dataDir)
	brokerAlive := true
	defer func() {
		if brokerAlive && cmd.Process != nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	}()

	if !waitForBroker(addr, 5*time.Second) {
		t.Fatal("broker did not come up in time")
	}

	ctx := context.Background()
	client, err := arbitro.Connect(ctx, addr,
		arbitro.WithTimeout(2*time.Second),
		arbitro.WithReconnect(true, 200, 20*time.Millisecond),
		// A killed process doesn't always deliver a prompt FIN/RST to the
		// peer socket, so this test leans on the G09 heartbeat watchdog
		// (short interval/timeout) to positively detect the dead connection
		// rather than waiting on a passive read error that may never come.
		arbitro.WithKeepAlive(300*time.Millisecond, 900*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer client.Close()

	stream := "reconn-full-" + time.Now().Format("150405")
	if _, err := client.CreateStream(ctx, stream, arbitro.StreamConfig{
		SubjectFilter: stream + ".>",
		MaxMsgs:       10000,
		Journal:       arbitro.JournalTolerant,
	}); err != nil {
		t.Fatalf("create stream: %v", err)
	}

	sub, err := client.Subscribe(ctx, stream, arbitro.ConsumerConfig{
		Name:   "reconn-consumer",
		Filter: stream + ".>",
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	// Baseline: publish before the crash and confirm delivery works.
	if err := client.Publish(ctx, stream, stream+".before", []byte("before")); err != nil {
		t.Fatalf("publish before: %v", err)
	}
	select {
	case msg := <-sub.Messages():
		msg.Ack()
	case <-time.After(3 * time.Second):
		t.Fatal("did not receive message before crash")
	}

	// Kill the broker out from under the client (SIGKILL-equivalent via
	// Process.Kill — pkill isn't available on Windows CI, and this achieves
	// the same effect: an unclean, immediate socket reset with no FIN).
	killBrokerProcess(t, cmd)
	brokerAlive = false

	// Give the client a moment to notice the disconnect and start retrying.
	time.Sleep(300 * time.Millisecond)

	// Restart the broker on the same address with the same data dir so the
	// stream/consumer survive the restart.
	cmd2 := startBroker(t, bin, addr, dataDir)
	defer func() {
		if cmd2.Process != nil {
			_ = cmd2.Process.Kill()
			_ = cmd2.Wait()
		}
	}()
	if !waitForBroker(addr, 10*time.Second) {
		t.Fatal("broker did not come back up in time")
	}

	// Wait for the reconnect supervisor to complete a redial.
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if client.Metrics().Reconnects > 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if client.Metrics().Reconnects == 0 {
		t.Fatal("client never reconnected (Metrics().Reconnects stayed 0)")
	}
	t.Logf("reconnects observed: %d", client.Metrics().Reconnects)

	// Publish after reconnect — retry briefly since the very first publish
	// right after redial may race the resubscribe replay.
	published := false
	var lastErr error
	for i := 0; i < 30; i++ {
		if err := client.Publish(ctx, stream, stream+".after", []byte("after-reconnect")); err == nil {
			published = true
			break
		} else {
			lastErr = err
			time.Sleep(200 * time.Millisecond)
		}
	}
	if !published {
		t.Fatalf("publish after reconnect never succeeded: %v", lastErr)
	}

	// The resubscribed consumer should keep receiving deliveries on the new
	// connection.
	select {
	case msg := <-sub.Messages():
		if string(msg.Data()) != "after-reconnect" {
			t.Fatalf("unexpected payload after reconnect: %q", msg.Data())
		}
		msg.Ack()
	case <-time.After(8 * time.Second):
		t.Fatal("did not receive message after reconnect — resubscribe likely failed")
	}
}
