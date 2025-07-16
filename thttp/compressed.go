package thttp

import (
	"net/http"

	"github.com/kevinpollet/nego"
)

// ShouldGzip returns if gzip-compression is asked for in HTTP request
func ShouldGzip(r *http.Request) bool {
	// nego.NegotiateContentEncoding(r, "gzip") returns "gzip"
	// if there is no "Accept-Encoding" header there. Guard against it.
	return r.Header.Get("Accept-Encoding") != "" && nego.NegotiateContentEncoding(r, "gzip") == "gzip"
}
