package api

import (
	"time"
)

// Message is an outgoing Kafka message
type Message struct {
	Topic, Key string
	Headers    map[string]string
	Value      []byte
}

// IncomingMessage is an incoming Kafka message
type IncomingMessage struct {
	Message
	Time   time.Time
	Offset int64
}
