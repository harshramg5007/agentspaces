package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"testing"
	"time"
)

func TestValkeyServerHealthReportsBackend(t *testing.T) {
	if os.Getenv("RUN_INTEGRATION_TESTS") != "1" {
		t.Skip("Skipping integration test. Set RUN_INTEGRATION_TESTS=1 to run.")
	}

	valkeyAddr := os.Getenv("VALKEY_ADDR")
	if valkeyAddr == "" {
		t.Skip("Skipping Valkey server smoke: VALKEY_ADDR not set")
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to allocate local port: %v", err)
	}
	httpAddr := listener.Addr().String()
	_ = listener.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	cmd := exec.CommandContext(
		ctx,
		"go",
		"run",
		".",
		"--runtime-profile", "benchmark",
		"--store-type", "valkey",
		"--valkey-addr", valkeyAddr,
		"--http-addr", httpAddr,
		"--log-level", "error",
	)
	cmd.Env = os.Environ()
	cmd.Dir = "."

	if err := cmd.Start(); err != nil {
		t.Fatalf("failed to start valkey server smoke: %v", err)
	}
	defer func() {
		cancel()
		_ = cmd.Wait()
	}()

	healthURL := fmt.Sprintf("http://%s/health", httpAddr)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, healthURL, nil)
		if err != nil {
			t.Fatalf("failed to build health request: %v", err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			time.Sleep(200 * time.Millisecond)
			continue
		}
		var payload map[string]interface{}
		err = json.NewDecoder(resp.Body).Decode(&payload)
		_ = resp.Body.Close()
		if err != nil {
			t.Fatalf("failed to decode health response: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			time.Sleep(200 * time.Millisecond)
			continue
		}
		if backend, _ := payload["backend"].(string); backend != "valkey" {
			t.Fatalf("health backend = %q, want %q", backend, "valkey")
		}
		return
	}

	t.Fatalf("server did not become healthy at %s", healthURL)
}
