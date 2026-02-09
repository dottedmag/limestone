package nats

import (
	"context"
	"fmt"

	"github.com/dottedmag/limestone/kafka/api"
	"github.com/dottedmag/limestone/kafka/names"
	"github.com/dottedmag/must"
	gonats "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const headerKey = "Limestone-Key"

func (c *client) ensureStream(ctx context.Context, topic string) (jetstream.Stream, error) {
	stream, err := c.js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     topic,
		Subjects: []string{topic},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to ensure stream %s: %w", topic, err)
	}
	return stream, nil
}

func (c *client) Write(ctx context.Context, topic string, messages []api.Message) error {
	must.OK(names.ValidateTopicName(topic))

	if len(messages) == 0 {
		return nil
	}

	if _, err := c.ensureStream(ctx, topic); err != nil {
		return err
	}

	for _, m := range messages {
		if m.Topic != topic {
			panic(fmt.Errorf("topic mismatch on write: %s (expected %s)", m.Topic, topic))
		}

		msg := &gonats.Msg{
			Subject: topic,
			Data:    m.Value,
		}
		if m.Key != "" || len(m.Headers) > 0 {
			msg.Header = gonats.Header{}
			if m.Key != "" {
				msg.Header.Set(headerKey, m.Key)
			}
			for k, v := range m.Headers {
				msg.Header.Set(k, v)
			}
		}

		if _, err := c.js.PublishMsg(ctx, msg); err != nil {
			return fmt.Errorf("failed to publish to %s: %w", topic, err)
		}
	}

	return nil
}
