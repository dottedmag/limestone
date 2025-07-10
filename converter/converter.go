package converter

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"time"

	"github.com/dottedmag/limestone/client"
	"github.com/dottedmag/limestone/converter/xform"
	"github.com/dottedmag/limestone/kafka"
	"github.com/dottedmag/limestone/llog"
	"github.com/dottedmag/limestone/wire"
)

// Config describes the converter configuration
type Config struct {
	SourceKafka      kafka.Client
	DestKafka        kafka.Client
	NewTopic         string // output topic name
	Force            bool   // perform a null conversion even if the database version is current
	DryRun           bool   // do not publish new manifest
	SafetyInterval   time.Duration
	HotStartStorage  string
	DBVersionHistory []string
	UpgradeSteps     map[string]xform.Transformation
	Survive          func(object wire.ActiveObject, now time.Time) bool
}

// DatabaseIsEmpty returns true if database is empty
func DatabaseIsEmpty(ctx context.Context, config Config) (bool, error) {
	lc := client.NewKafkaClient(config.SourceKafka)

	_, err := lc.RetrieveManifest(ctx, false)
	if errors.Is(err, client.ErrNoManifest) {
		return true, nil
	}
	if err != nil {
		return false, err
	}

	return false, nil
}

// Run runs the converter
func Run(ctx context.Context, config Config) error {
	logger := llog.MustGet(ctx)
	lc := client.NewKafkaClient(config.SourceKafka)

	manifest, err := lc.RetrieveManifest(ctx, true)
	if err != nil {
		return err
	}
	logger.Info("Retrieved manifest", slog.Any("manifest", manifest))

	if manifest.Version > len(config.DBVersionHistory) {
		panic(fmt.Errorf("actual database version %d is later than target %d", manifest.Version, len(config.DBVersionHistory)))
	}

	if manifest.Version == len(config.DBVersionHistory) && !config.Force {
		if config.HotStartStorage != "" {
			logger.Info("Database already at target version, refreshing hot start data", slog.Int("manifestVersion", manifest.Version))
			return refreshHotStart(ctx, config, lc, manifest)
		}
		logger.Info("Database already at target version, nothing to do", slog.Int("manifestVersion", manifest.Version))
		return nil
	}

	logger.Info("Preparing to upgrade", slog.Int("fromVersion", manifest.Version), slog.Int("toVersion", len(config.DBVersionHistory)))
	var steps []step
	for ver := manifest.Version; ver < len(config.DBVersionHistory); ver++ {
		name := config.DBVersionHistory[ver]
		run := config.UpgradeSteps[name]
		if run == nil {
			panic(fmt.Errorf("upgrade step %s from version %d to %d is not available", name, ver, ver+1))
		}
		steps = append(steps, step{name: name, run: run})
	}

	return transform(ctx, config, lc, manifest, steps)
}
