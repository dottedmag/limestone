package converter

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"runtime/debug"

	"time"

	"github.com/dottedmag/limestone/client"
	"github.com/dottedmag/limestone/converter/xform"
	"github.com/dottedmag/limestone/kafka"
	"github.com/dottedmag/limestone/llog"
	"github.com/dottedmag/limestone/tcontext"
	"github.com/dottedmag/limestone/wire"
	"github.com/dottedmag/must"
	"github.com/dottedmag/parallel"
)

const batchSize = 1000

type step struct {
	name string
	run  xform.Transformation
}

func transform(ctx context.Context, config Config, lc client.KafkaClient, manifest wire.Manifest, steps []step) error {
	topicIsEmpty, err := kafka.TopicIsEmpty(ctx, config.DestKafka, config.NewTopic)
	if err != nil {
		return err
	}
	if !topicIsEmpty {
		return fmt.Errorf("destination topic %s is not empty", config.NewTopic)
	}

	names := make([]string, 0, len(steps))
	for _, step := range steps {
		names = append(names, step.name)
	}
	llog.MustGet(ctx).Info("Running upgrade steps", slog.Any("steps", names), slog.String("newTopic", config.NewTopic))

	var active activeSet
	if config.HotStartStorage != "" {
		active = activeSet{}
	}

	maintenance := false
	defer func() {
		if maintenance {
			llog.MustGet(ctx).Info("Canceling database maintenance", slog.Any("manifest", manifest))
			_ = client.PublishKafkaManifest(tcontext.Reopen(ctx), config.DestKafka, manifest)
		}
	}()

	err = parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		incoming := make(chan *wire.IncomingTransaction, batchSize)
		stream := make(chan wire.Transaction, batchSize)
		spawn("reader", parallel.Fail, func(ctx context.Context) error {
			return lc.ConnectMaintenance(manifest, wire.Beginning, nil).Run(ctx, incoming)
		})
		spawn("preprocessor", parallel.Continue, func(ctx context.Context) error {
			defer close(stream)
			if err := preprocess(ctx, incoming, stream); err != nil {
				return err
			}

			if !config.DryRun {
				maintenanceManifest := manifest
				maintenanceManifest.Maintenance = true
				llog.MustGet(ctx).Info("Hot end reached, announcing database maintenance", slog.Any("manifest", maintenanceManifest))

				maintenance = true
				if err := client.PublishKafkaManifest(tcontext.Reopen(ctx), config.DestKafka, maintenanceManifest); err != nil {
					return err
				}

				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(config.SafetyInterval):
				}

				if len(incoming) != 0 {
					if err := preprocess(ctx, incoming, stream); err != nil {
						return err
					}
				}
			}

			llog.MustGet(ctx).Info("Reading complete")
			return nil
		})

		lastStream := stream
		for _, step := range steps {
			step := step
			from := lastStream
			to := make(chan wire.Transaction, batchSize)
			spawn(step.name, parallel.Continue, func(ctx context.Context) (err error) {
				defer func() {
					if e := recover(); e != nil {
						// Prevent previous step from deadlocking on writing
						spawn(step.name+":drain", parallel.Continue, func(ctx context.Context) error {
							for range from {
							}
							return nil
						})
						err = fmt.Errorf("panic in conversion step %s: %s\n\n%s", step.name, e, debug.Stack())
					}
					close(to)
				}()

				step.run(llog.MustGet(ctx), from, to)

				// Safety check: all transactions must be consumed
				select {
				case <-ctx.Done():
					return ctx.Err()
				case _, ok := <-from:
					if ok {
						return fmt.Errorf("conversion step %s failed to consume all transactions", step.name)
					}
				}

				return nil
			})
			lastStream = to
		}

		output := make(chan kafka.Message, batchSize)
		spawn("postprocessor", parallel.Continue, func(ctx context.Context) error {
			// Prevent previous step from deadlocking on writing
			defer func() {
				spawn("postprocessor:drain", parallel.Continue, func(ctx context.Context) error {
					for range lastStream {
					}
					return nil
				})
				close(output)
			}()

			now := time.Now()
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case txn, ok := <-lastStream:
					if !ok {
						return nil
					}
					if active != nil {
						active.applyTxn(txn, now, config.Survive)
					}
					select {
					case <-ctx.Done():
						return ctx.Err()
					case output <- kafka.Message{Topic: config.NewTopic, Value: must.OK1(json.Marshal(txn))}:
					}
				}
			}
		})

		spawn("writer", parallel.Exit, func(ctx context.Context) error {
			batch := make([]kafka.Message, 0, batchSize)
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case msg, ok := <-output:
					// batch size or end of stream reached
					if !ok || len(batch) >= batchSize {
						if err := config.DestKafka.Write(ctx, config.NewTopic, batch); err != nil {
							return err
						}
						batch = batch[:0] // truncate while keeping the underlying capacity
					}
					if !ok {
						return nil
					}
					batch = append(batch, msg)
				}
			}
		})

		return nil
	})
	if err != nil {
		return err
	}

	newManifest := wire.Manifest{
		Version: len(config.DBVersionHistory),
		Topic:   config.NewTopic,
	}
	if active != nil {
		header := wire.ActiveSetHeader{Version: newManifest.Version}
		header.Position, err = client.PredictPosition(ctx, config.DestKafka, newManifest)
		if err != nil {
			return err
		}
		gsURL := fmt.Sprintf("%s%v", config.HotStartStorage, newManifest.Version)
		llog.MustGet(ctx).Info("Uploading hot start data", slog.String("url", gsURL), slog.Any("header", header))
		// FIXME (alexey): implmenet Google retry policy
		if err := uploadActiveSet(ctx, gsURL, header, active); err != nil {
			llog.MustGet(ctx).Warn("Hot start data failed to upload", slog.String("url", gsURL), llog.Error(err))
		} else {
			llog.MustGet(ctx).Info("Hot start data uploaded", slog.String("url", gsURL))
		}
	}
	if config.DryRun {
		llog.MustGet(ctx).Info("Not publishing new manifest becase of --dry-run", slog.Any("manifest", newManifest))
	} else {
		llog.MustGet(ctx).Info("Publishing new manifest", slog.Any("manifest", newManifest))
		if err := client.PublishKafkaManifest(ctx, config.DestKafka, newManifest); err != nil {
			return err
		}
		maintenance = false
	}
	return nil
}

func preprocess(ctx context.Context, incoming <-chan *wire.IncomingTransaction, stream chan<- wire.Transaction) error {
	for {
		var txn *wire.IncomingTransaction
		select {
		case <-ctx.Done():
			return ctx.Err()
		case txn = <-incoming:
		}
		if txn == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case stream <- txn.Transaction:
		}
	}
}

func refreshHotStart(ctx context.Context, config Config, lc client.KafkaClient, manifest wire.Manifest) error {
	gsURL := fmt.Sprintf("%s%v", config.HotStartStorage, manifest.Version)
	now := time.Now()

	llog.MustGet(ctx).Info("Downloading hot start data", slog.String("url", gsURL))
	header, active, err := downloadActiveSet(ctx, gsURL, config.Survive)
	if err != nil {
		llog.MustGet(ctx).Warn("Hot start data failed to download", slog.String("url", gsURL), llog.Error(err))
		active = nil
	} else {
		llog.MustGet(ctx).Info("Hot start data downloaded", slog.String("url", gsURL))
		if header.Version != manifest.Version {
			llog.MustGet(ctx).Warn("Hot start data discarded: version mismatch", slog.Int("expected", manifest.Version), slog.Int("actual", header.Version))
			active = nil
		}
	}
	if active == nil {
		active = activeSet{}
		header = wire.ActiveSetHeader{
			Version:  manifest.Version,
			Position: wire.Beginning,
		}
	}

	err = parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		incoming := make(chan *wire.IncomingTransaction, batchSize)
		spawn("reader", parallel.Fail, func(ctx context.Context) error {
			return lc.Connect(manifest.Version, header.Position, nil, false).Run(ctx, incoming)
		})
		spawn("consumer", parallel.Exit, func(ctx context.Context) error {
			for {
				var txn *wire.IncomingTransaction
				select {
				case <-ctx.Done():
					return ctx.Err()
				case txn = <-incoming:
				}
				if txn == nil {
					return nil
				}
				header.Position = txn.Position
				active.applyTxn(txn.Transaction, now, config.Survive)
			}
		})
		return nil
	})
	if err != nil {
		return err
	}

	llog.MustGet(ctx).Info("Uploading hot start data", slog.String("url", gsURL), slog.Any("header", header))
	if err := uploadActiveSet(ctx, gsURL, header, active); err != nil {
		llog.MustGet(ctx).Warn("Hot start data failed to upload", slog.String("url", gsURL), llog.Error(err))
	} else {
		llog.MustGet(ctx).Info("Hot start data uploaded", slog.String("url", gsURL))
	}
	return nil
}
