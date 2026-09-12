// Package trace carries a short, non-secret operation ID through a request.
// It is intentionally small so protocol frontends and the storage core can
// correlate debug events without coupling to a particular logger.
package trace

import (
	"context"
	"crypto/rand"
	"encoding/hex"
)

type contextKey struct{}

// NewID returns an opaque ID suitable for correlating log events. It never
// falls back to user-controlled data.
func NewID() string {
	var b [6]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "unknown"
	}
	return hex.EncodeToString(b[:])
}

func WithID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, contextKey{}, id)
}

func ID(ctx context.Context) string {
	id, _ := ctx.Value(contextKey{}).(string)
	return id
}
