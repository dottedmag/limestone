package nats

import (
	"context"
	"errors"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"
)

func (c *client) LastOffset(ctx context.Context, topic string) (int64, error) {
	stream, err := c.js.Stream(ctx, topic)
	if err != nil {
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			return 0, nil
		}
		return 0, fmt.Errorf("failed to get stream %s: %w", topic, err)
	}

	info, err := stream.Info(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to get stream info for %s: %w", topic, err)
	}

	if info.State.Msgs == 0 {
		return 0, nil
	}

	return int64(info.State.LastSeq), nil
}
