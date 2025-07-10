package test

import (
	"context"
	"testing"

	"time"

	"github.com/dottedmag/limestone/llog"
)

// Context returns a new testing context.
//
// If your code relies on the values normally injected into the context by Tool
// or server, it's a good idea to test it with TestContext to provide adequate
// replacements.
func Context(t *testing.T) context.Context {
	ctx := context.Background()
	return llog.With(ctx, llog.NewForTesting(t))
}

// ContextWithTimeout is a version of TestContext with a timeout.
//
// If the timeout expires, the test context is closed with
// context.DeadlineExceeded.
func ContextWithTimeout(t *testing.T, timeout time.Duration) context.Context {
	ctx, cancel := context.WithTimeout(Context(t), timeout)
	t.Cleanup(cancel)
	return ctx
}
