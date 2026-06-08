package pool

import "sync"

// DefaultFrameSize is the initial capacity for pooled frame buffers.
const DefaultFrameSize = 4096

var framePool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, DefaultFrameSize)
		return &b
	},
}

// Get returns a pooled byte slice (reset to length 0).
func Get() *[]byte {
	bp := framePool.Get().(*[]byte)
	*bp = (*bp)[:0]
	return bp
}

// Put returns a byte slice to the pool.
func Put(bp *[]byte) {
	if bp == nil {
		return
	}
	// Don't pool oversized buffers (> 64KB)
	if cap(*bp) > 65536 {
		return
	}
	framePool.Put(bp)
}
