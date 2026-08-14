package postgres

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"my-cdc/internal/config"
	"my-cdc/internal/models"
	"my-cdc/internal/pb"
	"my-cdc/internal/sinks"
	"my-cdc/internal/utils"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

type Listener struct {
	Config    *config.AppConfig
	Processor *Processor
}

func NewListener(cfg *config.AppConfig, targetSink sinks.Pipeline, counts *models.EventsCount) *Listener {
	return &Listener{
		Config:    cfg,
		Processor: NewProcessor(cfg, targetSink, counts),
	}
}

func (l *Listener) Start(ctx context.Context, sourceURL string, globalState *models.GlobalState) error {
	slog.Info("CAPTURE: Connecting to the source database...")

	connConfig, err := pgconn.ParseConfig(sourceURL)
	if err != nil {
		return fmt.Errorf("could not parse source URL: %w", err)
	}

	slotName := connConfig.RuntimeParams["slot_name"]
	if slotName == "" {
		return fmt.Errorf("replication slot_name not specified in source URL")
	}

	publicationNames := connConfig.RuntimeParams["publication_names"]
	if publicationNames == "" {
		return fmt.Errorf("publication_names not specified in source URL")
	}
	delete(connConfig.RuntimeParams, "slot_name")
	delete(connConfig.RuntimeParams, "publication_names")

	var conn *pgconn.PgConn
	err = utils.DoWithRetry(
		l.Config.Retry.MaxRetries,
		time.Duration(l.Config.Retry.BaseDelayMs)*time.Millisecond,
		time.Duration(l.Config.Retry.MaxDelayTimeMs)*time.Millisecond,
		func() error {
			var connErr error
			conn, connErr = pgconn.ConnectConfig(ctx, connConfig)
			return connErr
		},
	)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	sysident, err := pglogrepl.IdentifySystem(ctx, conn)
	if err != nil {
		return err
	}
	slog.Info("CAPTURE: Connection successful", "system_lsn", sysident.XLogPos.String())

	_, err = pglogrepl.CreateReplicationSlot(ctx, conn, slotName, "pgoutput", pglogrepl.CreateReplicationSlotOptions{
		Mode: pglogrepl.LogicalReplication,
	})
	if err != nil && !strings.Contains(err.Error(), "already exists") {
		return err
	}

	// Get the Checkpoint loaded from disk (if any) to request Postgres to start from this exact point
	startLSN := globalState.GetMinCheckpoint()
	if startLSN > 0 {
		slog.Info("CAPTURE: Requesting Postgres to start sending data from LSN", "lsn", startLSN)
	}

	pluginArgs := []string{
		"proto_version '1'",
		fmt.Sprintf("publication_names '%s'", publicationNames),
	}
	err = pglogrepl.StartReplication(ctx, conn, slotName, pglogrepl.LSN(startLSN), pglogrepl.StartReplicationOptions{
		PluginArgs: pluginArgs,
	})
	if err != nil {
		return err
	}

	slog.Info("CAPTURE: Starting to listen for changes from PostgreSQL...")

	feedbackInterval := time.Duration(l.Config.Capture.FeedbackInterval.Load()) * time.Second
	ticker := time.NewTicker(feedbackInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done(): // Receive stop signal from the main context
			return nil

		case <-ticker.C:
			// Periodically send StandbyStatusUpdate to inform Postgres of the processed LSN,
			// which helps Postgres clean up WAL files and avoid disk space issues.
			confirmedLSN := globalState.GetMinCheckpoint()

			if confirmedLSN > 0 {
				errUpdate := pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{
					WALWritePosition: pglogrepl.LSN(confirmedLSN),
					WALFlushPosition: pglogrepl.LSN(confirmedLSN),
					WALApplyPosition: pglogrepl.LSN(confirmedLSN),
					ReplyRequested:   true,
				})

				if errUpdate == nil {
					// At the same time, periodically save this checkpoint to disk.
					// This prevents data loss in case the app is suddenly terminated (kill -9) without a graceful shutdown.
					ckptData := models.CheckpointFileData{
						InstanceName: l.Config.Provider.Source.Name,
						SourceType:   sourceTypeName,
						CheckpointData: &pb.Checkpoint{
							Offset: &pb.Checkpoint_Lsn{Lsn: confirmedLSN},
						},
					}
					errSave := utils.SaveProviderCheckpoint(l.Config.SaveDestination, ckptData)
					if errSave != nil {
						slog.Warn("CAPTURE: Error saving periodic checkpoint to disk", "error", errSave)
					}
				} else {
					slog.Warn("CAPTURE: Error sending StandbyStatusUpdate", "error", errUpdate)
				}
			}
		default:
			ctxTimeout, cancel := context.WithTimeout(ctx, 1*time.Second)
			msg, err := conn.ReceiveMessage(ctxTimeout)
			cancel()
			if err != nil {
				// If it's a timeout error, ignore it and continue the loop.
				if pgconn.Timeout(err) {
					continue
				}
				continue
			}

			if cd, ok := msg.(*pgproto3.CopyData); ok {
				switch cd.Data[0] {
				case pglogrepl.PrimaryKeepaliveMessageByteID:
					pkm, _ := pglogrepl.ParsePrimaryKeepaliveMessage(cd.Data[1:])
					// Postgres requests a response to check if the connection is still alive.
					if pkm.ReplyRequested {
						confirmedLSN := globalState.GetMinCheckpoint()
						if confirmedLSN == 0 {
							confirmedLSN = uint64(pkm.ServerWALEnd)
						}
						pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{
							WALWritePosition: pglogrepl.LSN(confirmedLSN),
							WALFlushPosition: pglogrepl.LSN(confirmedLSN),
							WALApplyPosition: pglogrepl.LSN(confirmedLSN),
						})
					}
				case pglogrepl.XLogDataByteID:
					xld, err := pglogrepl.ParseXLogData(cd.Data[1:])
					// This is the packet containing change data (INSERT, UPDATE, DELETE...).
					if err != nil {
						continue
					}

					// Backpressure: if RAM pressure is high (AutoTuner's RAM
					// guard, see internal/tuning/auto_tuner.go), pause here
					// instead of pushing more data into the pipeline. This
					// stalls the read loop, so we stop calling
					// conn.ReceiveMessage — TCP flow control then holds
					// data back at Postgres itself, instead of it piling up
					// as unbounded backlog on our side. Unlike the old
					// approach (cutting BatchMaxSize), this never touches
					// flush throughput at the sink.
					if l.waitForRAMRecovery(ctx, globalState) {
						return nil // ctx canceled while waiting
					}

					currentLSN := xld.WALStart + pglogrepl.LSN(len(xld.WALData))
					l.Processor.ProcessRawBytes(xld.WALData, currentLSN)
				}
			}
		}
	}
}

// waitForRAMRecovery blocks while the system is under RAM pressure (see
// AutoTuner's RAM guard / GlobalState.SetRAMThrottled), polling every
// 200ms. Returns true if ctx was canceled while waiting (caller should
// stop), false once it's safe to continue.
//
// KNOWN LIMITATION: while paused here, we also stop sending
// StandbyStatusUpdate feedback (normally sent from the ticker case in the
// outer select loop above), since we're blocked inside this same loop
// iteration. If the pause outlasts Postgres's replication timeout, the
// server may drop the connection. Acceptable for now since RAM pressure is
// expected to be transient (draining an existing backlog, not a permanently
// under-provisioned sink); if long pauses turn out to be common in
// practice, move status-update sending to its own goroutine/ticker
// independent of this loop instead of patching around it here.
func (l *Listener) waitForRAMRecovery(ctx context.Context, globalState *models.GlobalState) bool {
	if !globalState.IsRAMThrottled() {
		return false
	}
	slog.Warn("CAPTURE: RAM pressure detected, pausing WAL intake until it recovers")
	for globalState.IsRAMThrottled() {
		select {
		case <-ctx.Done():
			return true
		case <-time.After(200 * time.Millisecond):
		}
	}
	slog.Info("CAPTURE: RAM recovered, resuming WAL intake")
	return false
}