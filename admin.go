package limestone

import (
	"context"
	"fmt"
	"log/slog"
	"reflect"

	"github.com/dottedmag/limestone/client"
	"github.com/dottedmag/limestone/kafka"
	"github.com/dottedmag/limestone/llog"
	"github.com/dottedmag/limestone/meta"
	"github.com/dottedmag/limestone/wire"
	"github.com/dottedmag/must"
)

// Bootstrap initializes a Kafka-based Limestone database:
//  1. writes an administrative transaction creating the given entities into the
//     transaction log topic by the given name,
//  2. publishes a manifest referencing that topic.
//
// The administrative transaction created by Bootstrap bypassing restrictions on
// what producer can write to what section.
func Bootstrap(ctx context.Context, c kafka.Client, dbVersion int, topic string, obj ...any) error {
	logger := llog.MustGet(ctx)

	if txn := adminTransaction(obj); txn != nil {
		logger.Debug("Publishing bootstrap transaction", slog.Any("txn", *txn))
		if err := client.PublishKafkaTransaction(ctx, c, topic, *txn); err != nil {
			return fmt.Errorf("failed to bootstrap Limestone: %w", err)
		}
	}

	manifest := wire.Manifest{
		Version: dbVersion,
		Topic:   topic,
	}
	logger.Debug("Publishing bootstrap manifest", slog.Any("manifest", manifest))
	if err := client.PublishKafkaManifest(ctx, c, manifest); err != nil {
		return fmt.Errorf("failed to bootstrap Limestone: %w", err)
	}
	return nil
}

func adminTransaction(obj []any) *wire.Transaction {
	cache := map[reflect.Type]meta.Struct{}
	changes := wire.Changes{}
	for _, o := range obj {
		t := reflect.TypeOf(o)
		s, ok := cache[t]
		if !ok {
			s = meta.Survey(t)
			cache[t] = s
		}
		diff := must.OK1(wire.Encode(s, nil, o, nil))
		byID := changes[s.DBName]
		if byID == nil {
			byID = wire.KindChanges{}
			changes[s.DBName] = byID
		}
		byID[reflect.ValueOf(o).FieldByIndex(s.Identity().Index).String()] = diff
	}
	if len(changes) == 0 {
		return nil
	}
	return &wire.Transaction{Changes: changes}
}
