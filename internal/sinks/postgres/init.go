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

// init tự động đăng ký PostgreSQL Consumer vào Sinks Registry khi package này được import.
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

			// Thử kết nối đến DB đích với cơ chế retry
			err := utils.DoWithRetry(
				cfg.Retry.MaxRetries,
				time.Duration(cfg.Retry.BaseDelayMs)*time.Millisecond,
				time.Duration(cfg.Retry.MaxDelayTimeMs)*time.Millisecond,
				func() error {
					return executor.Init(ctx, consumerURL)
				},
			)
			if err != nil {
				return fmt.Errorf("khởi tạo kết nối thất bại: %w", err)
			}

			pgPipeline := sinks.NewDataProcessor(consumerName, cfg, builder, executor, state)
			multiSink.AddPipeline(pgPipeline)

			return nil
		},
	)
}