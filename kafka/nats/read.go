package nats

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/dottedmag/limestone/kafka/api"
	"github.com/dottedmag/limestone/kafka/names"
	"github.com/dottedmag/must"
	"github.com/nats-io/nats.go/jetstream"
)

const streamPollInterval = 500 * time.Millisecond

func (c *client) Read(ctx context.Context, topic string, offset int64, dest chan<- *api.IncomingMessage) error {
	must.OK(names.ValidateTopicName(topic))

	stream, err := c.waitForStream(ctx, topic, offset, dest)
	if err != nil {
		return err
	}

	cfg := jetstream.OrderedConsumerConfig{
		DeliverPolicy: jetstream.DeliverAllPolicy,
	}
	if offset > 0 {
		cfg.DeliverPolicy = jetstream.DeliverByStartSequencePolicy
		cfg.OptStartSeq = uint64(offset) + 1 // NATS is 1-based
	}

	cons, err := stream.OrderedConsumer(ctx, cfg)
	if err != nil {
		return fmt.Errorf("failed to create consumer on %s: %w", topic, err)
	}

	iter, err := cons.Messages()
	if err != nil {
		return fmt.Errorf("failed to start message iterator on %s: %w", topic, err)
	}

	// Stop the iterator when context is cancelled, so iter.Next() unblocks.
	go func() {
		<-ctx.Done()
		iter.Stop()
	}()
	defer iter.Stop()

	// Check for initial hot end: stream might be empty or fully consumed at
	// our starting offset.
	needsInitialHotEnd, err := c.isAtHotEnd(ctx, stream, offset)
	if err != nil {
		return err
	}
	if needsInitialHotEnd {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case dest <- nil:
		}
	}

	for {
		msg, err := iter.Next()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("failed to read from %s: %w", topic, err)
		}

		meta, err := msg.Metadata()
		if err != nil {
			return fmt.Errorf("failed to get message metadata on %s: %w", topic, err)
		}

		incoming := &api.IncomingMessage{
			Message: api.Message{
				Topic:   topic,
				Headers: map[string]string{},
				Value:   msg.Data(),
			},
			Time:   meta.Timestamp,
			Offset: int64(meta.Sequence.Stream) - 1, // NATS is 1-based, Limestone is 0-based
		}
		if h := msg.Headers(); h != nil {
			if key := h.Get(headerKey); key != "" {
				incoming.Key = key
			}
			for k := range h {
				if k == headerKey {
					continue
				}
				incoming.Headers[k] = h.Get(k)
			}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case dest <- incoming:
		}

		if meta.NumPending == 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case dest <- nil:
			}
		}
	}
}

func (c *client) isAtHotEnd(ctx context.Context, stream jetstream.Stream, offset int64) (bool, error) {
	info, err := stream.Info(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return false, ctx.Err()
		}
		return false, fmt.Errorf("failed to get stream info: %w", err)
	}
	return info.State.Msgs == 0 || int64(info.State.LastSeq) < offset+1, nil
}

// waitForStream waits for a stream to be created. If offset > 0 and the stream
// doesn't exist, it returns an error immediately. If offset == 0, it sends nil
// to dest (signaling hot end) and polls until the stream appears.
func (c *client) waitForStream(ctx context.Context, topic string, offset int64, dest chan<- *api.IncomingMessage) (jetstream.Stream, error) {
	stream, err := c.js.Stream(ctx, topic)
	if err == nil {
		return stream, nil
	}
	if !errors.Is(err, jetstream.ErrStreamNotFound) {
		return nil, fmt.Errorf("failed to get stream %s: %w", topic, err)
	}
	if offset != 0 {
		return nil, fmt.Errorf("stream %s does not exist", topic)
	}

	// Stream doesn't exist yet and offset is 0: signal hot end and wait.
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case dest <- nil:
	}

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(streamPollInterval):
		}

		stream, err := c.js.Stream(ctx, topic)
		if err == nil {
			return stream, nil
		}
		if !errors.Is(err, jetstream.ErrStreamNotFound) {
			return nil, fmt.Errorf("failed to get stream %s: %w", topic, err)
		}
	}
}
