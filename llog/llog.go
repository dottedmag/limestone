package llog

import (
	"context"
	"log/slog"
	"os"
	"testing"
)

var loggerContextKey struct{}

// MustGet returns the logger from the context, panic'ing if it does not exist
func MustGet(ctx context.Context) *slog.Logger {
	return ctx.Value(loggerContextKey).(*slog.Logger)
}

// Get returns the logger from the context, or nil if there is no logger there
func Get(ctx context.Context) *slog.Logger {
	logger := ctx.Value(loggerContextKey)
	if logger == nil {
		return nil
	}
	return logger.(*slog.Logger)
}

// With adds the logger to the context
func With(ctx context.Context, logger *slog.Logger) context.Context {
	return context.WithValue(ctx, loggerContextKey, logger)
}

// WithArgs is a shortcut that adds the logger args to the logger in the context
func WithArgs(ctx context.Context, args ...any) context.Context {
	return With(ctx, MustGet(ctx).With(args...))
}

// NewForTesting creates a logger for use in unit tests.
//
// The caller must call the returned cleanup function after using the logger.
func NewForTesting(t *testing.T) *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})).With("name", t.Name())
}

func Error(err error) slog.Attr {
	return slog.String("error", err.Error())
}
