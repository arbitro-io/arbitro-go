package conn

import (
	"context"
	"math"
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

// backoff calculates the delay for a given attempt with exponential backoff + jitter.
func (r *ReconnectLoop) backoff(attempt int) time.Duration {
	if attempt == 0 {
		return 0
	}

	// Exponential: base * 2^attempt
	delay := float64(r.cfg.BaseDelay) * math.Pow(2, float64(attempt-1))
	if delay > float64(r.cfg.MaxDelay) {
		delay = float64(r.cfg.MaxDelay)
	}

	// Add jitter: ±25%
	jitter := delay * 0.25 * (rand.Float64()*2 - 1)
	delay += jitter

	return time.Duration(delay)
}
