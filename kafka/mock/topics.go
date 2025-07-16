package mock

import (
	"context"
	"maps"
	"slices"
)

// Topics implements the Topics method of the kafka.Client interface (see
// documentation) by enumerating the topics in the simulated database
func (k *kafka) Topics(ctx context.Context) ([]string, error) {
	k.mu.Lock()
	defer k.mu.Unlock()

	return slices.Sorted(maps.Keys(k.topics)), nil
}
