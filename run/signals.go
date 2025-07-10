package run

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/dottedmag/limestone/llog"
)

func handleSignals(ctx context.Context) error {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)
	defer signal.Stop(signals)

	select {
	case sig := <-signals:
		llog.MustGet(ctx).Info("received signal, terminating", slog.Any("signal", sig))
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
