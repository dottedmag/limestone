package thttp

import (
	"bytes"
	"io"
	"log/slog"
	"net/http"
	"testing"

	"github.com/dottedmag/limestone/llog"
	"github.com/stretchr/testify/require"
)

// LogBodies is a middleware that logs request and response bodies.
//
// Only has an effect when debug logging is enabled.
func LogBodies(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		logger := llog.MustGet(req.Context())
		if !logger.Enabled(req.Context(), slog.LevelDebug) {
			next.ServeHTTP(w, req)
			return
		}

		if shouldLogBody(req.Header) {
			req.Body = createReadCloserCapture(req.Body, func(p []byte, _ bool) {
				logger.Debug("HTTP request body", slog.String("contentType", contentType(req.Header)), slog.String("requestData", string(p)))
			})
		}

		crw := &captureResponseWriter{ResponseWriter: w}
		// FIXME (eyal): flusher interface is ignored
		if h, ok := w.(http.Hijacker); ok {
			crw.Hijacker = h
		}
		next.ServeHTTP(crw, req)
		if shouldLogBody(crw.ResponseWriter.Header()) {
			logger.Debug("HTTP response body", slog.String("contentType", contentType(crw.ResponseWriter.Header())), slog.String("body", string(crw.buff.Bytes())))
		}
	})
}

type captureResponseWriter struct {
	http.ResponseWriter
	http.Hijacker
	buff bytes.Buffer
}

func (crw *captureResponseWriter) Write(p []byte) (int, error) {
	n, err := crw.ResponseWriter.Write(p)
	appendToBuffer(&crw.buff, p, n)
	return n, err
}

var testBytes = []byte("this is a body")

func TestCaptureReadCloserReadAll(t *testing.T) {
	var captured bool

	crc := createReadCloserCapture(newCaptureReadCloser(), func(readBytes []byte, eof bool) {
		require.False(t, captured)
		captured = true
		require.True(t, eof)
		require.Equal(t, testBytes, readBytes)
	})

	readBytes, err := io.ReadAll(crc)
	require.NoError(t, err)
	require.Equal(t, testBytes, readBytes)
	require.True(t, captured)

	require.NoError(t, crc.Close())
}

func TestCaptureReadCloserReadPart(t *testing.T) {
	var captured bool
	bytesToRead := 3

	crc := createReadCloserCapture(newCaptureReadCloser(), func(readBytes []byte, eof bool) {
		require.False(t, captured)
		captured = true
		require.False(t, eof)
		require.Equal(t, testBytes[:bytesToRead*2], readBytes)
	})

	readBytes := make([]byte, bytesToRead)
	n, err := crc.Read(readBytes)
	require.NoError(t, err)
	require.Equal(t, bytesToRead, n)
	require.Equal(t, testBytes[0:bytesToRead], readBytes)
	require.False(t, captured)

	n, err = crc.Read(readBytes)
	require.NoError(t, err)
	require.Equal(t, bytesToRead, n)
	require.Equal(t, testBytes[bytesToRead:bytesToRead*2], readBytes)
	require.False(t, captured)

	require.NoError(t, crc.Close())
	require.True(t, captured)
}

func TestCaptureReadCloserCloseWithoutReading(t *testing.T) {
	var captured bool

	crc := createReadCloserCapture(newCaptureReadCloser(), func(readBytes []byte, eof bool) {
		require.False(t, captured)
		captured = true
		require.Nil(t, readBytes)
		require.False(t, eof)
	})

	require.NoError(t, crc.Close())
	require.True(t, captured)
}

func newCaptureReadCloser() io.ReadCloser {
	return io.NopCloser(bytes.NewReader(testBytes))
}
