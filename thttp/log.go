package thttp

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/dottedmag/limestone/llog"
)

// Log is a middleware that logs before and after handling of each request.
// Does not include logging of request and response bodies.
func Log(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		ctx := llog.WithArgs(r.Context(),
			slog.String("method", r.Method),
			slog.String("hostname", r.Host),
			slog.String("url", r.URL.String()),
		)
		logger := llog.MustGet(ctx)
		logger.Debug("HTTP request handling started")
		var status int
		next.ServeHTTP(CaptureStatus(w, &status), r.WithContext(ctx))
		logger.Debug("HTTP request handling ended", slog.Int("statusCode", status), slog.Duration("elapsed", time.Since(started)))
	})
}
