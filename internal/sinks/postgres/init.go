package postgres

import (
	"context"
	"fmt"
	"time"

	"my-cdc/internal/config"
	"my-cdc/internal/models"
	"my-cdc/internal/sinks"
	"my-cdc/internal/utils"
)

// init automatically registers the PostgreSQL Consumer into the Sinks Registry when this package is imported.
func init() {
	sinks.Register(
		"postgres",
		sinks.Metadata{
			DisplayName: "PostgreSQL",
			URLTemplate: "postgres://user:password@host:5432/dbname?sslmode=disable",
		},
		func(ctx context.Context, consumerName string, cfg *config.AppConfig, consumerURL string, state *models.GlobalState, multiSink *sinks.MultiSink) error {
			builder := &Builder{}
			executor := &Executor{}

			// Attempt to connect to the destination DB with a retry mechanism
			err := utils.DoWithRetry(
				cfg.Retry.MaxRetries,
				time.Duration(cfg.Retry.BaseDelayMs)*time.Millisecond,
				time.Duration(cfg.Retry.MaxDelayTimeMs)*time.Millisecond,
				func() error {
					return executor.Init(ctx, consumerURL)
				},
			)
			if err != nil {
				return fmt.Errorf("failed to initialize connection: %w", err)
			}

			pgPipeline := sinks.NewDataProcessor(consumerName, cfg, builder, executor, state)
			multiSink.AddPipeline(pgPipeline)

			return nil
		},
	)
}