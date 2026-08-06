package app

import (
	"context"
	"fmt"
	"os"
	"time"

	"log/slog"
	"my-cdc/internal/capture"
	"my-cdc/internal/config"
	"my-cdc/internal/models"
	"my-cdc/internal/pb"
	"my-cdc/internal/sinks"
	"my-cdc/internal/tuning"
	"my-cdc/internal/utils"
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

// Bootstrap khởi tạo tất cả các thành phần có side-effect nặng của ứng dụng.
// Hàm này chỉ nên được gọi sau khi cấu hình đã được xác nhận.
func Bootstrap(ctx context.Context, cfg *config.AppConfig) (*Application, error) {
	poolCapacity := int(cfg.Bag.BagMaxSize.Load() * int64(cfg.Bag.BagMaxMultiple.Load()))
	models.InitBagPool(poolCapacity)

	eventsCount := &models.EventsCount{}
	globalState := models.NewGlobalState()

	// 0. Đảm bảo thư mục lưu checkpoint tồn tại NGAY LÚC KHỞI ĐỘNG, độc lập với việc
	// có dữ liệu để lưu hay chưa (SaveProviderCheckpoint chỉ được gọi khi đã có
	// checkpoint LSN > 0, tức là sau khi có ít nhất 1 transaction đi qua trót lọt).
	// Nếu để tới lúc đó mới tạo thư mục, người dùng sẽ không biết được liệu tiến trình
	// có quyền ghi đĩa hay không cho tới khi có dữ liệu thật — vừa trễ, vừa dễ gây
	// nhầm lẫn với lỗi kết nối. Tạo và log ngay ở đây giúp fail-fast (VD: sai quyền
	// thư mục) và cho người dùng biết checkpoint sẽ được lưu ở đâu.
	if err := os.MkdirAll(cfg.SaveDestination.Path, 0755); err != nil {
		return nil, fmt.Errorf("không thể tạo thư mục lưu checkpoint [%s]: %w", cfg.SaveDestination.Path, err)
	}
	slog.Info("CHECKPOINT: Thư mục lưu trữ đã sẵn sàng", "path", cfg.SaveDestination.Path)

	// sourceType là chuỗi tự do lấy thẳng từ config (VD: "postgres"), khớp với
	// tên driver đã Register() trong internal/drivers — không còn qua bước parse enum.
	sourceType := cfg.Provider.Source.Type
	instanceName := cfg.Provider.Source.Name

	// 1. Phục hồi trạng thái (Load Checkpoint)
	slog.Info("CHECKPOINT: Đang kiểm tra lịch sử...")
	ckptData, err := utils.LoadProviderCheckpoint(cfg.SaveDestination, sourceType, instanceName)
	if err != nil {
		return nil, fmt.Errorf("lỗi nghiêm trọng khi đọc file checkpoint: %w", err)
	}

	recoveredLSN := uint64(0)
	if ckptData != nil && ckptData.CheckpointData != nil {
		if lsn, ok := ckptData.CheckpointData.Offset.(*pb.Checkpoint_Lsn); ok && lsn.Lsn > 0 {
			recoveredLSN = lsn.Lsn
			lastSaved := time.Unix(ckptData.UpdatedAt, 0).Format("2006-01-02 15:04:05")
			slog.Info("CHECKPOINT: Phục hồi thành công", "lsn", recoveredLSN, "last_saved", lastSaved)
		}
	}
	if recoveredLSN == 0 {
		slog.Info("CHECKPOINT: Không tìm thấy checkpoint cũ, sẽ bắt đầu từ LSN mới nhất.")
	}

	// Khởi tạo mốc Checkpoint ban đầu cho TẤT CẢ các đích đang hoạt động
	for _, consumer := range cfg.Consumers.List {
		if consumer.IsActive {
			globalState.InitSink(consumer.Name, recoveredLSN)
		}
	}

	// 2. Khởi tạo các Đích (Sinks)
	multiSink := sinks.NewMultiSink()

	for _, consumer := range cfg.Consumers.List {
		if !consumer.IsActive {
			slog.Info("SINK: Bỏ qua đích không hoạt động", "sink_name", consumer.Name)
			continue
		}

		// Khởi tạo và cắm pipeline đích dựa vào loại (type)
		if err := sinks.BuildAndAddPipeline(ctx, consumer.Type, consumer.Name, cfg, consumer.URL, globalState, multiSink); err != nil {
			return nil, fmt.Errorf("khởi tạo đích [%s] thất bại: %w", consumer.Name, err)
		}
		slog.Info("SINK: Đã khởi tạo pipeline cho đích", "sink_name", consumer.Name)
	}

	// 3. Khởi tạo Nguồn (Capture)
	listener, err := capture.CreateListener(cfg.Provider.Source.Type, cfg, multiSink, eventsCount)
	if err != nil {
		return nil, fmt.Errorf("lỗi khởi tạo nguồn: %w", err)
	}

	// 4. Khởi tạo bộ tự động điều chỉnh (Auto-Tuner)
	autoTuner := tuning.NewAutoTuner(cfg, eventsCount)

	return &Application{
		Config:      cfg,
		GlobalState: globalState,
		EventsCount: eventsCount,
		MultiSink:   multiSink,
		Listener:    listener,
		AutoTuner:   autoTuner,
	}, nil
}

// Shutdown thực hiện công việc lưu trạng thái (checkpoint) trước khi tắt ứng dụng.
func (a *Application) Shutdown() {
	sourceType := a.Config.Provider.Source.Type
	finalLSN := a.GlobalState.GetMinCheckpoint()
	if finalLSN > 0 {
		finalData := models.CheckpointFileData{
			InstanceName:   a.Config.Provider.Source.Name,
			SourceType:     sourceType,
			CheckpointData: &pb.Checkpoint{Offset: &pb.Checkpoint_Lsn{Lsn: finalLSN}},
		}
		if errSave := utils.SaveProviderCheckpoint(a.Config.SaveDestination, finalData); errSave != nil {
			slog.Error("CHECKPOINT: Lỗi khi lưu checkpoint cuối cùng", "error", errSave)
		} else {
			slog.Info("CHECKPOINT: Đã lưu thành công LSN cuối cùng.", "lsn", finalLSN)
		}
	} else {
		slog.Info("CHECKPOINT: Chưa có dữ liệu để lưu (chưa xử lý transaction nào), bỏ qua lúc shutdown.")
	}
}