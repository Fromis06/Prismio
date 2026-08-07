package postgres

import (
	"my-cdc/internal/capture"
	"my-cdc/internal/config"
	"my-cdc/internal/models"
	"my-cdc/internal/sinks"
)

// init tự động đăng ký PostgreSQL Provider vào Capture Registry khi package này được import.
func init() {
	capture.Register(
		"postgres",
		capture.Metadata{
			DisplayName: "PostgreSQL",
			URLTemplate: "postgres://user:password@host:5432/dbname?sslmode=disable&slot_name=my_slot&publication_names=my_pub",
		},
		func(cfg *config.AppConfig, multiSink *sinks.MultiSink, eventsCount *models.EventsCount) capture.Listener {
			return NewListener(cfg, multiSink, eventsCount)
		},
	)
}