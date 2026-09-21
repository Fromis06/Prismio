package sqlserver

import (
	"context"
	"fmt"
	"time"

	"my-cdc/internal/config"
	"my-cdc/internal/models"
	"my-cdc/internal/sinks"
	"my-cdc/internal/utils"
)

func init() {
	sinks.Register(
		"sqlserver",
		sinks.Metadata{
			DisplayName: "SQL Server",
			URLTemplate: "sqlserver://user:password@host:1433?database=dbname",
		},
		func(ctx context.Context, consumerName string, cfg *config.AppConfig, consumerURL string, state *models.GlobalState, multiSink *sinks.MultiSink) error {
			builder := &Builder{}
			executor := &Executor{}

			err := utils.DoWithRetry(
				cfg.Retry.MaxRetries,
				time.Duration(cfg.Retry.BaseDelayMs)*time.Millisecond,
				time.Duration(cfg.Retry.MaxDelayTimeMs)*time.Millisecond,
				func() error { return executor.Init(ctx, consumerURL) },
			)
			if err != nil {
				return fmt.Errorf("failed to initialize connection: %w", err)
			}

			pipeline := sinks.NewDataProcessor(consumerName, cfg, builder, executor, state)
			multiSink.AddPipeline(pipeline)
			return nil
		},
	)
}
