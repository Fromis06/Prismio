package sinks

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"my-cdc/internal/config"
	"my-cdc/internal/models"
	"my-cdc/internal/pb"
	"my-cdc/internal/utils"
)

// DataProcessor quản lý toàn bộ luồng xử lý dữ liệu cho một đích (destination) cụ thể.
type DataProcessor struct {
	Name        string                      // Tên định danh của Sink.
	Config      *config.AppConfig           // Cấu hình của ứng dụng.
	Builder     QueryBuilder                // Khối chuyển đổi ChangeEvent sang SQL.
	Executor    DatabaseExecutor            // Khối thực thi lệnh SQL.
	EventChan   chan *models.SharedEventBag // Kênh giao tiếp nhận túi sự kiện được đóng gói.
	stopChan    chan struct{}               // Kênh tín hiệu yêu cầu dừng.
	ctx         context.Context             // Context chính của processor, bị hủy khi Stop().
	cancel      context.CancelFunc          // Hàm để hủy context trên.
	GlobalState *models.GlobalState         // Tham chiếu state tổng để báo cáo Checkpoint.
	wg          sync.WaitGroup              // Khóa chờ worker kết thúc trước khi tắt.
	isActive    atomic.Bool                 // Trạng thái sống/chết của Sink.
}

func NewDataProcessor(name string, cfg *config.AppConfig, builder QueryBuilder, executor DatabaseExecutor, globalState *models.GlobalState) *DataProcessor {
	ctx, cancel := context.WithCancel(context.Background())
	dp := &DataProcessor{
		Name:        name,
		Config:      cfg,
		Builder:     builder,
		Executor:    executor,
		EventChan:   make(chan *models.SharedEventBag, cfg.Pipeline.PipelineMaxSize.Load()),
		stopChan:    make(chan struct{}),
		ctx:         ctx,
		cancel:      cancel,
		GlobalState: globalState,
	}
	dp.isActive.Store(true)
	return dp
}

// WriteBatch là phương thức của interface Pipeline. Nó được dùng trong kịch bản
// một-một (1 producer -> 1 consumer). Nó sẽ tự đóng gói túi sự kiện với
// bộ đếm tham chiếu là 1 và gửi vào kênh xử lý.
func (dp *DataProcessor) WriteBatch(events []*pb.ChangeEvent) error {
	if len(events) > 0 {
		// Tự đóng gói với bộ đếm là 1, vì đây là consumer duy nhất.
		dp.WriteShared(models.NewSharedEventBag(events, 1))
	}
	return nil
}

// WriteShared là phương thức được gọi bởi MultiSink để gửi một túi sự kiện
// đã được đóng gói (với bộ đếm tham chiếu) vào kênh xử lý.
func (dp *DataProcessor) WriteShared(bag *models.SharedEventBag) { // Implements Pipeline interface
	// Không cần kiểm tra len, vì MultiSink đã làm.
	dp.EventChan <- bag
}

// Start khởi động goroutine xử lý chính (workerLoop).
func (dp *DataProcessor) Start() error {
	dp.wg.Add(1)
	go dp.workerLoop()
	return nil
}

func (dp *DataProcessor) Stop() error {
	dp.cancel()        // Hủy context, báo hiệu cho các hoạt động con (như flush) nên dừng lại.
	close(dp.stopChan) // Gửi tín hiệu dừng cho workerLoop.
	dp.wg.Wait()       // Chờ workerLoop xả nốt dữ liệu và kết thúc.

	// [Pattern: Graceful Shutdown] Xả nốt các túi sự kiện còn trong kênh để tránh rò rỉ bộ nhớ.
	// Điều này đặc biệt quan trọng nếu có kịch bản hot-restart một sink.
	close(dp.EventChan) // Đóng kênh để vòng lặp for..range bên dưới có thể kết thúc.
	for sharedBag := range dp.EventChan {
		sharedBag.Done() // Gọi Done() để giảm refCount và có thể trả bag về pool.
	}

	return dp.Executor.Close() // Cuối cùng, đóng kết nối vật lý.
}

// IsActive trả về trạng thái hoạt động của DataProcessor.
func (dp *DataProcessor) IsActive() bool { // Implements Pipeline interface
	return dp.isActive.Load()
}

// [Pattern: Worker Loop & Batching] workerLoop thực hiện xử lý sự kiện liên tục và gom lô.
func (dp *DataProcessor) workerLoop() {
	var currentQueries []string
	var currentArgs [][]any

	defer dp.wg.Done() // Báo cho WaitGroup biết là goroutine đã kết thúc.

	// ✅ Theo dõi Checkpoint lớn nhất trong lô để cập nhật GlobalState
	var CurrentLastCheckpoint uint64

	initialTimeout := dp.Config.Batch.BatchTimeout.Load()
	ticker := time.NewTicker(time.Duration(initialTimeout) * time.Millisecond)
	defer ticker.Stop()

	// flush là một hàm nội bộ để thực hiện việc ghi lô dữ liệu xuống DB đích.
	flush := func(reason string) {
		if len(currentQueries) == 0 {
			// Ngay cả khi không có câu lệnh SQL nào (VD: chỉ có dummy event của COMMIT),
			// ta vẫn cần cập nhật checkpoint nếu có.
			if CurrentLastCheckpoint > 0 {
				dp.GlobalState.UpdateCheckpoint(dp.Name, CurrentLastCheckpoint)
				CurrentLastCheckpoint = 0
			}
			return
		}
		slog.Info("Flushing batch to destination",
			"sink", dp.Name,
			"reason", reason,
			"query_count", len(currentQueries),
			"checkpoint_lsn", CurrentLastCheckpoint,
		)
		// Sử dụng context của processor (dp.ctx) làm parent.
		// Điều này đảm bảo nếu Stop() được gọi, context này sẽ bị hủy,
		// giúp ngắt các lệnh ExecuteBatch đang chạy hoặc đang chờ retry.
		flushTimeout := time.Duration(dp.Config.Batch.FlushTimeoutMs.Load()) * time.Millisecond
		execCtx, execCancel := context.WithTimeout(dp.ctx, flushTimeout)
		defer execCancel()

		err := utils.DoWithRetry(
			dp.Config.Retry.MaxRetries,
			time.Duration(dp.Config.Retry.BaseDelayMs)*time.Millisecond,
			time.Duration(dp.Config.Retry.MaxDelayTimeMs)*time.Millisecond,
			func() error {
				return dp.Executor.ExecuteBatch(execCtx, currentQueries, currentArgs)
			},
		)
		if err != nil {
			// [Pattern: Graceful Degradation] Ngắt bỏ kết nối lỗi thay vì panic để bảo vệ các đích khác.
			slog.Error("Disconnecting sink due to critical error", "sink", dp.Name, "error", err)
			dp.isActive.Store(false)
			dp.GlobalState.RemoveSink(dp.Name)
		} else {
			// Ghi thành công, cập nhật checkpoint trong GlobalState.
			if CurrentLastCheckpoint > 0 {
				dp.GlobalState.UpdateCheckpoint(dp.Name, CurrentLastCheckpoint)
			}
		}

		// Reset buffers và biến cho mẻ tiếp theo
		currentQueries = currentQueries[:0]
		currentArgs = currentArgs[:0]
		CurrentLastCheckpoint = 0
	}

	for {
		select {
		case <-dp.stopChan:
			if dp.isActive.Load() {
				flush("Shutdown") // Trước khi thoát, xả nốt dữ liệu còn lại.
			}
			return

		case sharedBag := <-dp.EventChan:
			// Nếu sink đã chết, vứt bỏ sự kiện và báo cho bộ đếm biết là đã "xong"
			// để không block các sink khác và không làm rò rỉ bộ nhớ.
			if !dp.isActive.Load() {
				sharedBag.Done()
				continue
			}

			eventsBuffer := sharedBag.Events

			activeMaxSize := dp.Config.Batch.BatchMaxSize.Load()
			numWorkers := int(dp.Config.DataProcessing.DataProcessingWorkerCount.Load())

			// Lấy Checkpoint và SourceType từ sự kiện cuối cùng trong túi.
			if len(eventsBuffer) > 0 {
				lastEvent := eventsBuffer[len(eventsBuffer)-1]
				// Sử dụng GetOffset() sẽ tự động an toàn kể cả khi lastEvent.Offset bị nil
				LastCheckPoint := lastEvent.GetOffset().GetLsn()

				if LastCheckPoint > CurrentLastCheckpoint {
					CurrentLastCheckpoint = LastCheckPoint
				}
			}

			var wg sync.WaitGroup
			workerQueries := make([][]string, numWorkers)
			workerArgs := make([][][]any, numWorkers)
			chunkSize := (len(eventsBuffer) + numWorkers - 1) / numWorkers

			// [Pattern: Fan-Out] Chia túi sự kiện thành các phần nhỏ cho worker xử lý song song.
			for w := 0; w < numWorkers; w++ {
				wg.Add(1)
				go func(workerID int) {
					defer wg.Done()
					wStart := workerID * chunkSize
					wEnd := wStart + chunkSize
					if wStart >= len(eventsBuffer) {
						return
					}
					if wEnd > len(eventsBuffer) {
						wEnd = len(eventsBuffer)
					}
					subChunk := eventsBuffer[wStart:wEnd]

					// [Pattern: Object Pool] Xin lại vùng RAM cũ từ Pool, đưa len về 0 để ghi đè dữ liệu.
					localQueries := models.QueryPool.Get().([]string)[:0]
					localArgs := models.ArgsPool.Get().([][]any)[:0]

					for _, e := range subChunk {
						// Mỗi worker gọi Builder để chuyển đổi ChangeEvent thành câu lệnh SQL.
						q, a := dp.Builder.BuildQuery(e)

						if q != "" {
							localQueries = append(localQueries, q)
							localArgs = append(localArgs, a)
						}
					}
					workerQueries[workerID] = localQueries
					workerArgs[workerID] = localArgs
				}(w)
			}

			wg.Wait()

			// [Pattern: Fan-In] Tổng hợp kết quả SQL từ các worker về mảng chính.
			for i := 0; i < numWorkers; i++ {
				currentQueries = append(currentQueries, workerQueries[i]...)
				currentArgs = append(currentArgs, workerArgs[i]...)

				// Trả các mảng trung gian của worker về Pool để mẻ sau dùng tiếp
				models.QueryPool.Put(workerQueries[i])
				models.ArgsPool.Put(workerArgs[i])
			}
			if int64(len(currentQueries)) >= activeMaxSize {
				flush("Batch đầy")
				ticker.Reset(time.Duration(dp.Config.Batch.BatchTimeout.Load()) * time.Millisecond)
			}

			// Báo cho bộ đếm tham chiếu biết là worker này đã xử lý xong.
			sharedBag.Done()

		case <-ticker.C:
			if dp.isActive.Load() {
				flush("Hết thời gian chờ")
			}
			ticker.Reset(time.Duration(dp.Config.Batch.BatchTimeout.Load()) * time.Millisecond)
		}
	}
}
