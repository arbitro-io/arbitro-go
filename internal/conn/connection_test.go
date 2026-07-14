package conn

import (
	"context"
	"crypto/tls"
	"net"
	"testing"
	"time"
)

// fakeHelloListener accepts one connection and reads (and discards) the
// fixed-size Hello handshake bytes Dial() writes immediately after
// connecting, so Dial() doesn't block/error on the handshake write itself.
func fakeHelloListener(t *testing.T) (addr string, closeFn func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				buf := make([]byte, 64)
				_, _ = c.Read(buf) // discard Hello bytes; ignore errors on close
			}(c)
		}
	}()
	return ln.Addr().String(), func() { ln.Close() }
}

// TestDialPlainNoTLS confirms Dial() takes the plain net.Dialer path (not
// tls.DialWithDialer) when Config.TLS is nil — the pre-G15 behavior must be
// unchanged for existing non-TLS callers.
func TestDialPlainNoTLS(t *testing.T) {
	addr, closeFn := fakeHelloListener(t)
	defer closeFn()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	c, err := Dial(ctx, Config{Addr: addr, Timeout: time.Second})
	if err != nil {
		t.Fatalf("Dial (plain) unexpected error: %v", err)
	}
	defer c.Close()
}

// TestDialWithTLSAttemptsHandshake confirms Dial() takes the TLS path when
// Config.TLS is set (G15): dialing a plain (non-TLS) listener with a TLS
// config must fail with a TLS handshake error, proving
// tls.DialWithDialer was actually invoked instead of silently falling back
// to a plain connection (the pre-fix behavior, where WithTLS was a no-op).
func TestDialWithTLSAttemptsHandshake(t *testing.T) {
	addr, closeFn := fakeHelloListener(t)
	defer closeFn()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := Dial(ctx, Config{
		Addr:    addr,
		Timeout: time.Second,
		TLS:     &tls.Config{InsecureSkipVerify: true}, //nolint:gosec // test only
	})
	if err == nil {
		t.Fatal("expected a TLS handshake error when dialing a plain listener with TLS configured")
	}
	t.Logf("got expected TLS handshake error: %v", err)
}
