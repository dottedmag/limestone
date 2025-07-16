package server

import (
	"net/http"

	"github.com/dottedmag/limestone/kafka/api"
)

// Handler returns an HTTP handler that servers data from the given Kafka client
// in the same format as the file-based implementation in lib/kafka/local.
//
// Data is served as plain HTTP (read once to the end) or WebSocket (tailing),
// client's choice.
func Handler(client api.Client) http.Handler {
	h := handler{client: client}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /kafka", h.topics)
	mux.HandleFunc("GET /kafka/{topic}", h.topic)
	mux.HandleFunc("HEAD /kafka/{topic}", h.topicInfo)
	mux.HandleFunc("GET /kafka/{topic}/{offset}", h.record)
	return mux
}

type handler struct {
	client api.Client
}
