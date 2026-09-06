// internal/core/backoff_test.go
package core

import (
	"testing"
	"time"
)

func TestNextInitBackoffSequence(t *testing.T) {
	want := []time.Duration{
		time.Second,
		2 * time.Second,
		4 * time.Second,
		8 * time.Second,
		16 * time.Second,
		30 * time.Second,
		30 * time.Second,
		30 * time.Second,
	}
	for i, w := range want {
		got := nextInitBackoff(i + 1)
		if got != w {
			t.Fatalf("failures=%d: got %v, want %v", i+1, got, w)
		}
	}
}
