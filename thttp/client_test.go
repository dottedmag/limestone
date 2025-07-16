package thttp

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/dottedmag/limestone/test"
	"github.com/stretchr/testify/assert"
)

// RoundTrip processes an http.Request (usually obtained from httptest.NewRequest)
// with the given handler as if it was received on the network. Only useful in
// tests.
//
// Does not require a running HTTP server to be running.
func RoundTrip(handler http.Handler, r *http.Request) *http.Response {
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	return w.Result()
}

// RoundTripContext is similar to RoundTrip, except that the given context is injected into
// the request
func RoundTripContext(ctx context.Context, handler http.Handler, r *http.Request) *http.Response {
	return RoundTrip(handler, r.WithContext(ctx))
}

func TestTest(t *testing.T) {
	ctx := test.Context(t)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, err := w.Write([]byte("hello"))
		assert.NoError(t, err)
	})

	res := RoundTripContext(ctx, StandardMiddleware(handler), httptest.NewRequest(http.MethodGet, "/", nil))
	defer res.Body.Close()
	assert.Equal(t, http.StatusOK, res.StatusCode)
	body, err := io.ReadAll(res.Body)
	assert.NoError(t, err)
	assert.Equal(t, []byte("hello"), body)
}
