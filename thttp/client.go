package thttp

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"

	"github.com/dottedmag/limestone/llog"
)

// LoggingTransport is HTTP transport with logging
type LoggingTransport struct {
	Transport       http.RoundTripper
	SkipRequestBody bool
}

// WithRequestsLogging returns an http client with logging
func WithRequestsLogging(client *http.Client) *http.Client {
	transport := client.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}
	return &http.Client{
		Transport:     &LoggingTransport{Transport: transport},
		CheckRedirect: checkRedirect,
		// FIXME (eyal): client timeout and jar are ignored
	}
}

func checkRedirect(req *http.Request, via []*http.Request) error {
	if len(via) > 10 {
		return errors.New("request was terminated after 10 redirects")
	}
	// Go's http client removes Authorization from following request
	// https://github.com/golang/go/issues/35104
	for k, v := range via[0].Header {
		if _, exists := req.Header[k]; !exists {
			req.Header[k] = v
		}
	}
	return nil
}

// RoundTrip is an implementation of RoundTripper.
//
// RoundTripper is an interface representing the ability to execute a
// single HTTP transaction, obtaining the Response for a given Request.
//
// A RoundTripper must be safe for concurrent use by multiple
// goroutines.
func (t *LoggingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if !llog.MustGet(req.Context()).Enabled(req.Context(), slog.LevelDebug) {
		return t.Transport.RoundTrip(req)
	}

	logger := llog.MustGet(req.Context()).With(slog.String("method", req.Method), slog.Any("url", req.URL))

	req.Body = createReadCloserCapture(req.Body, func(p []byte, _ bool) {
		logFields := []any{slog.String("contentType", contentType(req.Header))}
		if !t.SkipRequestBody && shouldLogBody(req.Header) {
			logFields = append(logFields, slog.String("requestData", string(p)))
		}
		logger.Debug("HTTP request ended", logFields...)
	})

	logger.Debug("HTTP request started")
	resp, err := t.Transport.RoundTrip(req)
	if err != nil {
		logger.Debug("HTTP request failed", llog.Error(err))
		return resp, err
	}

	resp.Body = createReadCloserCapture(resp.Body, func(p []byte, eof bool) {
		logFields := []any{
			slog.String("status", resp.Status),
			slog.String("contentType", contentType(resp.Header)),
			slog.Bool("readAllBody", eof),
		}
		// requestID and correlationID are helpful to identify requests from third-parties, in case we receive an unexpected response
		if requestID, ok := resp.Header["X-Request-Id"]; ok && len(requestID) != 0 {
			logFields = append(logFields, slog.String("requestID", requestID[0]))
		}
		if correlationID, ok := resp.Header["X-Correlation-Id"]; ok && len(correlationID) != 0 {
			logFields = append(logFields, slog.String("correlationID", correlationID[0]))
		}
		if shouldLogBody(resp.Header) {
			logFields = append(logFields, slog.String("responseData", string(p)))
		}
		logger.Debug("HTTP response ended", logFields...)
	})

	return resp, err
}

// Test processes an http.Request (usually obtained from httptest.NewRequest)
// with the given handler as if it was received on the network. Only useful in
// tests.
//
// Does not require a running HTTP server to be running.
func Test(handler http.Handler, r *http.Request) *http.Response {
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	return w.Result()
}

// TestCtx is similar to Test, except that the given context is injected into
// the request
func TestCtx(ctx context.Context, handler http.Handler, r *http.Request) *http.Response {
	return Test(handler, r.WithContext(ctx))
}
