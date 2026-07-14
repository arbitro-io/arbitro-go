package conn

import (
	"context"
	"math/rand"
	"time"
)

// ReconnectConfig controls reconnection behavior.
type ReconnectConfig struct {
	Enabled    bool
	MaxRetries int
	BaseDelay  time.Duration
	MaxDelay   time.Duration
}

// DefaultReconnectConfig returns sensible defaults for reconnection.
func DefaultReconnectConfig() ReconnectConfig {
	return ReconnectConfig{
		Enabled:    true,
		MaxRetries: 10,
		BaseDelay:  500 * time.Millisecond,
		MaxDelay:   30 * time.Second,
	}
}

// ReconnectLoop handles reconnection with exponential backoff + jitter.
// It calls onReconnect after each successful dial so the client can resubscribe.
type ReconnectLoop struct {
	cfg          ReconnectConfig
	connCfg      Config
	onReconnect  func(*Connection)
	onDisconnect func(error)
	prevDelay    time.Duration
}

// NewReconnectLoop creates a new reconnection manager.
func NewReconnectLoop(connCfg Config, cfg ReconnectConfig) *ReconnectLoop {
	return &ReconnectLoop{
		cfg:     cfg,
		connCfg: connCfg,
	}
}

// OnReconnect registers a callback invoked after each successful reconnect.
func (r *ReconnectLoop) OnReconnect(fn func(*Connection)) {
	r.onReconnect = fn
}

// OnDisconnect registers a callback invoked when a disconnect is detected.
func (r *ReconnectLoop) OnDisconnect(fn func(error)) {
	r.onDisconnect = fn
}

// Run attempts reconnection in a loop until ctx is cancelled, max retries
// exceeded, or a successful connection is made.
// Returns the new Connection or an error if all retries failed.
func (r *ReconnectLoop) Run(ctx context.Context) (*Connection, error) {
	var lastErr error

	for attempt := 0; r.cfg.MaxRetries <= 0 || attempt < r.cfg.MaxRetries; attempt++ {
		delay := r.backoff(attempt)

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
		}

		c, err := Dial(ctx, r.connCfg)
		if err != nil {
			lastErr = err
			continue
		}

		// Success
		if r.onReconnect != nil {
			r.onReconnect(c)
		}
		return c, nil
	}

	return nil, lastErr
}

// backoff calculates the delay for a given attempt using decorrelated
// jitter (AWS architecture blog / Rust conn/reconnect.rs Backoff):
//
//	next = min(cap, random_between(base, prev*3))
//
// This spreads out reconnect storms better than plain exponential backoff
// when many clients disconnect from the same broker simultaneously.
func (r *ReconnectLoop) backoff(attempt int) time.Duration {
	if attempt == 0 {
		r.prevDelay = 0
		return 0
	}

	base := r.cfg.BaseDelay
	if base <= 0 {
		base = time.Millisecond
	}
	maxDelay := r.cfg.MaxDelay
	if maxDelay <= 0 {
		maxDelay = base
	}

	prev := r.prevDelay
	if prev < base {
		prev = base
	}

	upper := prev * 3
	if upper <= base {
		upper = base + 1
	}

	span := upper - base
	delay := base + time.Duration(rand.Int63n(int64(span)))
	if delay > maxDelay {
		delay = maxDelay
	}

	r.prevDelay = delay
	return delay
}
