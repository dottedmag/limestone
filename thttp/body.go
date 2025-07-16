package thttp

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/dottedmag/must"
)

const maxLogBodyLen = 1024 - 3 // make room for 3 dots

func contentType(header http.Header) string {
	return strings.TrimSpace(strings.ToLower(header.Get("Content-Type")))
}

func shouldLogBody(header http.Header) bool {
	return contentType(header) != "application/octet-stream"
}

type captureReadCloser struct {
	rc   io.ReadCloser
	buff bytes.Buffer
	done func([]byte, bool)
}

func createReadCloserCapture(rc io.ReadCloser, done func([]byte, bool)) *captureReadCloser {
	if rc == nil {
		rc = http.NoBody
	}

	var captured bool
	doneOnce := func(p []byte, eof bool) {
		if captured {
			return
		}
		captured = true
		done(p, eof)
	}

	return &captureReadCloser{rc: rc, done: doneOnce}
}
func appendToBuffer(buff *bytes.Buffer, p []byte, n int) {
	remaining := maxLogBodyLen - buff.Len()
	if n == 0 || remaining <= 0 {
		return
	}
	if n > remaining {
		must.OK1(buff.Write(p[:remaining])) // must is safe because buffer.Write() always returns nil
		must.OK1(buff.WriteString("..."))
	} else {
		must.OK1(buff.Write(p[:n]))
	}
}

func (crc *captureReadCloser) Read(p []byte) (int, error) {
	n, err := crc.rc.Read(p)
	appendToBuffer(&crc.buff, p, n)
	if errors.Is(err, io.EOF) {
		crc.done(crc.buff.Bytes(), true)
	}
	return n, err
}

func (crc *captureReadCloser) Close() error {
	crc.done(crc.buff.Bytes(), false)
	return crc.rc.Close()
}
