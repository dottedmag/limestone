package nats

import (
	"github.com/dottedmag/limestone/kafka/api"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type client struct {
	nc *nats.Conn
	js jetstream.JetStream
}

// New creates a NATS JetStream client. The caller is responsible for managing
// the lifecycle of the provided nats.Conn.
func New(nc *nats.Conn) (api.Client, error) {
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, err
	}
	return &client{nc: nc, js: js}, nil
}
