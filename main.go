package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"my-cdc/internal/app"
	"my-cdc/internal/logger"
	"my-cdc/internal/utils"

	// Đăng ký các Driver (Provider và Consumer)
	_ "my-cdc/internal/capture/postgres"
	_ "my-cdc/internal/sinks/postgres"
	// _ "my-cdc/internal/sinks/mysql" // Bỏ comment dòng này khi bạn đã viết xong
)

func main() {
	logger.Initialize()
	slog.Info("System starting up")

	// Tạo context chính cho toàn bộ ứng dụng, cho phép hủy đồng loạt
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// ==========================================
	// 1. KHỞI TẠO ỨNG DỤNG (INITIALIZATION)
	// ==========================================
	cdcApp := app.Initialize(ctx)

	// ==========================================
	// 2. BẮT ĐẦU CÁC TIẾN TRÌNH VÀ WORKER
	// ==========================================
	cdcApp.MultiSink.Start()      // Khởi động tất cả các pipeline (worker loop)
	defer cdcApp.MultiSink.Stop() // Đảm bảo tất cả pipeline được dừng an toàn khi main thoát

	// Khởi động bộ giám sát (monitor)
	go utils.StartAdaptiveMonitor(cdcApp.Config, cdcApp.EventsCount, 5*time.Second)

	// Kênh để bắt tín hiệu dừng từ OS (Ctrl+C) hoặc từ các goroutine khác
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		// Bắt đầu lắng nghe thay đổi từ DB nguồn. Đây là một lời gọi blocking.
		if err := cdcApp.Listener.Start(ctx, cdcApp.Config.Provider.Source.URL, cdcApp.GlobalState); err != nil && err != context.Canceled {
			slog.Error("Capture stream unexpectedly interrupted", "error", err)
			// Nếu listener gặp lỗi nghiêm trọng, tự gửi tín hiệu để shutdown chương trình
			sigChan <- syscall.SIGINT
		}
	}()

	// [Pattern: Graceful Shutdown] Chặn luồng main để chờ tín hiệu hệ điều hành trước khi dọn dẹp.
	<-sigChan // Block chương trình chính tại đây cho đến khi có tín hiệu

	slog.Info("Received stop signal, starting shutdown process")
	cancel() // Gửi tín hiệu dừng cho các goroutine con đang lắng nghe context

	// Gọi hàm xả dữ liệu và lưu checkpoint
	cdcApp.Shutdown()

	slog.Info("Shutdown complete. Goodbye!")
}
