package app

import (
	"context"
	"log"
	"time"

	"my-cdc/internal/capture"
	"my-cdc/internal/config"
	"my-cdc/internal/models"
	"my-cdc/internal/pb"
	"my-cdc/internal/sinks"
	"my-cdc/internal/tuning"
)

// Application chứa tất cả các thành phần cốt lõi của hệ thống CDC.
// Việc đóng gói chúng vào struct này giúp dễ quản lý trạng thái toàn cục.
type Application struct {
	Config      *config.AppConfig
	GlobalState *models.GlobalState
	EventsCount *models.EventsCount
	MultiSink   *sinks.MultiSink
	Listener    capture.Listener
	AutoTuner   *tuning.AutoTuner
}

// Initialize khởi tạo tất cả các thành phần (Config, Pool, Checkpoint, Sinks, Listener).
func Initialize(ctx context.Context) *Application {
	// 1. Khởi tạo Cấu hình mặc định
	cfg := config.NewDefaultConfig()

	// Khởi tạo sức chứa cho pool túi sự kiện
	poolCapacity := int(cfg.Bag.BagMaxSize.Load() * int64(cfg.Bag.BagMaxMultiple.Load()))
	models.InitBagPool(poolCapacity)

	eventsCount := &models.EventsCount{}
	globalState := models.NewGlobalState()

	sourceType := models.ParseSourceType(cfg.Provider.Source.Type)
	instanceName := cfg.Provider.Source.Name

	// 2. Phục hồi trạng thái (Load Checkpoint)
	log.Println("CHECKPOINT: Đang kiểm tra lịch sử...")
	ckptData, err := utils.LoadProviderCheckpoint(cfg.SaveDestination, sourceType, instanceName)
	if err != nil {
		log.Fatalf("CHECKPOINT: Lỗi nghiêm trọng khi đọc file checkpoint: %v", err)
	}

	recoveredLSN := uint64(0)
	if ckptData != nil && ckptData.CheckpointData != nil {
		if lsn, ok := ckptData.CheckpointData.Offset.(*pb.Checkpoint_Lsn); ok && lsn.Lsn > 0 {
			recoveredLSN = lsn.Lsn
			lastSaved := time.Unix(ckptData.UpdatedAt, 0).Format("2006-01-02 15:04:05")
			log.Printf("CHECKPOINT: Phục hồi thành công LSN %d từ lần lưu lúc %s", recoveredLSN, lastSaved)
		}
	}
	if recoveredLSN == 0 {
		log.Println("CHECKPOINT: Không tìm thấy checkpoint cũ, sẽ bắt đầu từ LSN mới nhất.")
	}

	// Khởi tạo mốc Checkpoint ban đầu cho TẤT CẢ các đích đang hoạt động
	for _, consumer := range cfg.Consumers.List {
		if consumer.IsActive {
			globalState.InitSink(consumer.Name, recoveredLSN)
		}
	}

	// 3. Khởi tạo các Đích (Sinks)
	multiSink := sinks.NewMultiSink()

	for _, consumer := range cfg.Consumers.List {
		if !consumer.IsActive {
			log.Printf("SINK: Bỏ qua đích không hoạt động: %s", consumer.Name)
			continue
		}

		// Khởi tạo và cắm pipeline đích dựa vào loại (type)
		if err := sinks.BuildAndAddPipeline(ctx, consumer.Type, consumer.Name, cfg, consumer.URL, globalState, multiSink); err != nil {
			log.Fatalf("SINK: Khởi tạo đích [%s] thất bại: %v", consumer.Name, err)
		}
		log.Printf("SINK: Đã khởi tạo pipeline cho đích: %s", consumer.Name)
	}

	// 4. Khởi tạo Nguồn (Capture)
	listener, err := capture.CreateListener(cfg.Provider.Source.Type, cfg, multiSink, eventsCount)
	if err != nil {
		log.Fatalf("CAPTURE: Lỗi khởi tạo nguồn: %v", err)
	}

	// 5. Khởi tạo bộ tự động điều chỉnh (Auto-Tuner)
	autoTuner := tuning.NewAutoTuner(cfg, eventsCount)

	return &Application{
		Config:      cfg,
		GlobalState: globalState,
		EventsCount: eventsCount,
		MultiSink:   multiSink,
		Listener:    listener,
		AutoTuner:   autoTuner,
	}
}

// Shutdown thực hiện công việc lưu trạng thái (checkpoint) trước khi tắt ứng dụng.
func (a *Application) Shutdown() {
	sourceType := models.ParseSourceType(a.Config.Provider.Source.Type)
	finalLSN := a.GlobalState.GetMinCheckpoint()
	if finalLSN > 0 {
		finalData := models.CheckpointFileData{
			InstanceName:   a.Config.Provider.Source.Name,
			SourceType:     sourceType,
			CheckpointData: &pb.Checkpoint{Offset: &pb.Checkpoint_Lsn{Lsn: finalLSN}},
		}
		if errSave := utils.SaveProviderCheckpoint(a.Config.SaveDestination, finalData); errSave != nil {
			log.Printf("CHECKPOINT: Lỗi khi lưu checkpoint cuối cùng: %v", errSave)
		} else {
			log.Printf("CHECKPOINT: Đã lưu thành công LSN cuối cùng là %d.", finalLSN)
		}
	}
}
