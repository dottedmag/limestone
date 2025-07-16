package kafkago

import (
	"context"
	"fmt"
	"maps"
	"slices"

	"github.com/dottedmag/limestone/retry"
	"github.com/dottedmag/parallel"
)

func (c *client) Topics(ctx context.Context) (res []string, err error) {
	err = retry.Do(ctx, readerRetry, func() error {
		var err error
		res, err = c.listTopics(ctx)
		if err != nil {
			if shouldRetry(err) {
				return retry.Retriable(fmt.Errorf("failed to retrieve topic list: %w", err))
			}
			return err
		}
		return nil
	})
	return res, err
}

func (c *client) listTopics(ctx context.Context) (res []string, err error) {
	err = parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		conn, err := c.connectMaster(ctx)
		if err != nil {
			return err
		}
		spawn("partitions", parallel.Exit, func(ctx context.Context) error {
			partitions, err := conn.ReadPartitions()
			if err != nil {
				return err
			}
			topics := map[string]bool{}
			for _, p := range partitions {
				topics[p.Topic] = true
			}
			res = slices.Sorted(maps.Keys(topics))
			return nil
		})
		spawn("closer", parallel.Fail, func(ctx context.Context) error {
			<-ctx.Done()
			conn.Close()
			return ctx.Err()
		})
		return nil
	})
	return res, err
}
