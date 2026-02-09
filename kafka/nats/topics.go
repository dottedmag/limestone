package nats

import (
	"context"
	"fmt"
	"slices"
)

func (c *client) Topics(ctx context.Context) ([]string, error) {
	lister := c.js.StreamNames(ctx)
	var names []string
	for name := range lister.Name() {
		names = append(names, name)
	}
	if err := lister.Err(); err != nil {
		return nil, fmt.Errorf("failed to list streams: %w", err)
	}
	slices.Sort(names)
	return names, nil
}
