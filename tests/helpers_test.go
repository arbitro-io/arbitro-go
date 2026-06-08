//go:build integration

package tests

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/arbitro-io/arbitro-go"
)

const defaultAddr = "127.0.0.1:9898"

func brokerAddr() string {
	if addr := os.Getenv("ARBITRO_ADDR"); addr != "" {
		return addr
	}
	return defaultAddr
}

func connectT(t *testing.T) *arbitro.Client {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := arbitro.Connect(ctx, brokerAddr(), arbitro.WithTimeout(5*time.Second))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { client.Close() })
	return client
}

func uniqueName(prefix string) string {
	return prefix + "-" + time.Now().Format("150405-000")
}
