package nats

import (
	"context"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
	"github.com/dottedmag/limestone/kafka/api"
	"github.com/dottedmag/limestone/test"
	"github.com/dottedmag/parallel"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

type testEnv struct {
	group  *parallel.Group
	client api.Client
}

func setupTest(t *testing.T) *testEnv {
	ns, err := server.NewServer(&server.Options{
		Host:      "127.0.0.1",
		Port:      -1,
		JetStream: true,
		StoreDir:  t.TempDir(),
		NoLog:     true,
	})
	assert.NoError(t, err)

	ns.Start()
	if !ns.ReadyForConnections(5 * time.Second) {
		t.Fatal("NATS server not ready")
	}
	t.Cleanup(ns.Shutdown)

	nc, err := nats.Connect(ns.ClientURL())
	assert.NoError(t, err)
	t.Cleanup(nc.Close)

	client, err := New(nc)
	assert.NoError(t, err)

	group := test.Group(t)

	return &testEnv{
		group:  group,
		client: client,
	}
}

// recv receives a message from the channel with a timeout.
func recv(t *testing.T, ch <-chan *api.IncomingMessage) *api.IncomingMessage {
	t.Helper()
	select {
	case msg := <-ch:
		return msg
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for message")
		return nil
	}
}

func TestTopics(t *testing.T) {
	env := setupTest(t)

	topics, err := env.client.Topics(env.group.Context())
	assert.NoError(t, err)
	assert.Equal(t, []string(nil), topics)

	assert.NoError(t, env.client.Write(env.group.Context(), "beta", []api.Message{
		{Topic: "beta", Value: []byte("x")},
	}))
	assert.NoError(t, env.client.Write(env.group.Context(), "alpha", []api.Message{
		{Topic: "alpha", Value: []byte("y")},
	}))

	topics, err = env.client.Topics(env.group.Context())
	assert.NoError(t, err)
	assert.Equal(t, []string{"alpha", "beta"}, topics)
}

func TestLastOffset(t *testing.T) {
	env := setupTest(t)

	offset, err := env.client.LastOffset(env.group.Context(), "nonexistent")
	assert.NoError(t, err)
	assert.Equal(t, int64(0), offset)

	assert.NoError(t, env.client.Write(env.group.Context(), "topic", []api.Message{
		{Topic: "topic", Value: []byte("a")},
		{Topic: "topic", Value: []byte("b")},
		{Topic: "topic", Value: []byte("c")},
	}))

	offset, err = env.client.LastOffset(env.group.Context(), "topic")
	assert.NoError(t, err)
	assert.Equal(t, int64(3), offset)
}

func TestReadExisting(t *testing.T) {
	env := setupTest(t)

	assert.NoError(t, env.client.Write(env.group.Context(), "topic", []api.Message{
		{Topic: "topic", Key: "k0", Value: []byte("msg-0")},
		{Topic: "topic", Key: "k1", Value: []byte("msg-1")},
	}))

	messages := make(chan *api.IncomingMessage)
	env.group.Spawn("reader", parallel.Fail, func(ctx context.Context) error {
		return env.client.Read(ctx, "topic", 0, messages)
	})

	msg := recv(t, messages)
	assert.Equal(t, int64(0), msg.Offset)
	assert.Equal(t, "k0", msg.Key)
	assert.Equal(t, []byte("msg-0"), msg.Value)

	msg = recv(t, messages)
	assert.Equal(t, int64(1), msg.Offset)
	assert.Equal(t, "k1", msg.Key)
	assert.Equal(t, []byte("msg-1"), msg.Value)

	// Hot end
	assert.Equal(t, (*api.IncomingMessage)(nil), recv(t, messages))
}

func TestReadFromOffset(t *testing.T) {
	env := setupTest(t)

	assert.NoError(t, env.client.Write(env.group.Context(), "topic", []api.Message{
		{Topic: "topic", Value: []byte("msg-0")},
		{Topic: "topic", Value: []byte("msg-1")},
		{Topic: "topic", Value: []byte("msg-2")},
	}))

	messages := make(chan *api.IncomingMessage)
	env.group.Spawn("reader", parallel.Fail, func(ctx context.Context) error {
		return env.client.Read(ctx, "topic", 1, messages)
	})

	msg := recv(t, messages)
	assert.Equal(t, int64(1), msg.Offset)
	assert.Equal(t, []byte("msg-1"), msg.Value)

	msg = recv(t, messages)
	assert.Equal(t, int64(2), msg.Offset)
	assert.Equal(t, []byte("msg-2"), msg.Value)

	assert.Equal(t, (*api.IncomingMessage)(nil), recv(t, messages))
}

func TestReadNonExistentStream(t *testing.T) {
	env := setupTest(t)

	messages := make(chan *api.IncomingMessage)
	env.group.Spawn("reader", parallel.Fail, func(ctx context.Context) error {
		return env.client.Read(ctx, "topic", 0, messages)
	})

	// Should get initial hot end for non-existent stream
	assert.Equal(t, (*api.IncomingMessage)(nil), recv(t, messages))

	// Write creates the stream
	assert.NoError(t, env.client.Write(env.group.Context(), "topic", []api.Message{
		{Topic: "topic", Value: []byte("hello")},
	}))

	msg := recv(t, messages)
	assert.Equal(t, int64(0), msg.Offset)
	assert.Equal(t, []byte("hello"), msg.Value)

	assert.Equal(t, (*api.IncomingMessage)(nil), recv(t, messages))
}

func TestReadTailing(t *testing.T) {
	env := setupTest(t)

	assert.NoError(t, env.client.Write(env.group.Context(), "topic", []api.Message{
		{Topic: "topic", Value: []byte("first")},
	}))

	messages := make(chan *api.IncomingMessage)
	env.group.Spawn("reader", parallel.Fail, func(ctx context.Context) error {
		return env.client.Read(ctx, "topic", 0, messages)
	})

	msg := recv(t, messages)
	assert.Equal(t, []byte("first"), msg.Value)
	assert.Equal(t, (*api.IncomingMessage)(nil), recv(t, messages))

	// Write more after reaching hot end
	assert.NoError(t, env.client.Write(env.group.Context(), "topic", []api.Message{
		{Topic: "topic", Value: []byte("second")},
	}))

	msg = recv(t, messages)
	assert.Equal(t, int64(1), msg.Offset)
	assert.Equal(t, []byte("second"), msg.Value)
	assert.Equal(t, (*api.IncomingMessage)(nil), recv(t, messages))
}

func TestReadHeaders(t *testing.T) {
	env := setupTest(t)

	assert.NoError(t, env.client.Write(env.group.Context(), "topic", []api.Message{
		{
			Topic:   "topic",
			Key:     "mykey",
			Headers: map[string]string{"a": "b", "c": "d"},
			Value:   []byte("data"),
		},
	}))

	messages := make(chan *api.IncomingMessage)
	env.group.Spawn("reader", parallel.Fail, func(ctx context.Context) error {
		return env.client.Read(ctx, "topic", 0, messages)
	})

	msg := recv(t, messages)
	assert.Equal(t, "mykey", msg.Key)
	assert.Equal(t, map[string]string{"a": "b", "c": "d"}, msg.Headers)
	assert.Equal(t, []byte("data"), msg.Value)
}
