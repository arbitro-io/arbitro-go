package conn

import (
	"bufio"
	"net"
	"time"
)

// DefaultWriteQueueCap is the capacity of the write channel when Config
// leaves it at zero. Matches the Rust client's WRITE_QUEUE_CAP = 4096.
const DefaultWriteQueueCap = 4096

// DefaultMaxBlock caps how long Send waits for room when the caller's context
// carries no deadline of its own. Same role as Kafka's max.block.ms and the
// Rust client's ClientConfig::max_block: a peer that stopped draining has to
// surface as an error, not as a publish that never returns.
const DefaultMaxBlock = 5 * time.Second

// writerBufSize is the bufio.Writer buffer size — large enough to coalesce
// many small frames into a single syscall.
const writerBufSize = 65536

// writeLoop drains writeCh and flushes coalesced frames to the TCP socket.
// It implements the same pattern as the Rust client's writer_task:
//   recv_async (block for first frame) → write → try_recv loop (drain pending) → flush
//
// This eliminates the per-frame syscall overhead and global write mutex.
func writeLoop(conn net.Conn, writeCh <-chan []byte, done <-chan struct{}) {
	bw := bufio.NewWriterSize(conn, writerBufSize)

	for {
		// Block until at least one frame arrives (or shutdown).
		select {
		case frame, ok := <-writeCh:
			if !ok {
				_ = bw.Flush()
				return
			}
			_, _ = bw.Write(frame)

			// Drain all immediately available frames (write coalescing).
			// This is the key optimization: multiple publishes that arrived
			// while we were blocked get coalesced into a single flush.
		drain:
			for {
				select {
				case f, ok := <-writeCh:
					if !ok {
						_ = bw.Flush()
						return
					}
					_, _ = bw.Write(f)
				default:
					break drain
				}
			}

			// Single syscall flushes all coalesced frames.
			if err := bw.Flush(); err != nil {
				return
			}

		case <-done:
			_ = bw.Flush()
			return
		}
	}
}
