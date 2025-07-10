package tnet

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"syscall"

	"github.com/dottedmag/limestone/llog"
	"github.com/dottedmag/parallel"
)

// WriteOneWayCloser is a io.WriteCloser that can also be closed for writing (TCP stream, SSH channel)
type WriteOneWayCloser interface {
	io.WriteCloser
	CloseWrite() error
}

// CopyNetworkStream copies data from from to to, closing destination stream for writing when done
func CopyNetworkStream(ctx context.Context, to WriteOneWayCloser, from io.Reader) error {
	_, err := io.Copy(to, from)
	_ = to.CloseWrite() // ignore any errors from shutdown(2) - remote socket might not be connected anymore
	return stripIgnorableErrorForCopying(err)
}

var defaultDialer = &net.Dialer{}

type closableReaderWriter interface {
	io.Reader
	WriteOneWayCloser
}

func handleConn(ctx context.Context, remote closableReaderWriter, localPort int) error {
	defer remote.Close()

	local, err := defaultDialer.DialContext(ctx, "tcp", fmt.Sprintf("127.0.0.1:%d", localPort))
	if errors.Is(err, syscall.ECONNREFUSED) { // this is not a problem: local port is not being served
		llog.MustGet(ctx).Debug("Closing incoming remote connection, nothing listens locally", slog.Int("port", localPort))
		return nil
	}
	if errors.Is(err, syscall.ECONNRESET) { // this is not a problem: local process closed the connection
		llog.MustGet(ctx).Debug("Closing incoming remote connection, local connection reset", slog.Int("port", localPort))
		return nil
	}
	if err != nil {
		return err
	}

	defer local.Close()

	return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		spawn("data-remote-to-local", parallel.Continue, func(ctx context.Context) error {
			return CopyNetworkStream(ctx, local.(*net.TCPConn), remote)
		})
		spawn("data-local-to-remote", parallel.Continue, func(ctx context.Context) error {
			return CopyNetworkStream(ctx, remote, local)
		})
		return nil
	})
}

// TCPProxy proxies incoming connections to the local TCP server listening at localPort
func TCPProxy(ctx context.Context, listener net.Listener, localPort int) error {
	return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		spawn("closer", parallel.Continue, func(ctx context.Context) error {
			<-ctx.Done()
			_ = listener.Close()
			return nil
		})
		spawn("listener", parallel.Continue, func(ctx context.Context) error {
			for {
				// NB: this connection might be SSH channel. Do not try to change
				// handleConn parameter to *net.TCPConn
				conn, err := listener.Accept()
				llog.MustGet(ctx).Debug("Incoming connection", llog.Error(err))
				if err != nil {
					return StripClosedConnectionError(err)
				}
				spawn(conn.RemoteAddr().String(), parallel.Continue, func(ctx context.Context) error {
					return handleConn(ctx, conn.(closableReaderWriter), localPort)
				})
			}
		})
		return nil
	})
}
