package postgres

import (
	"context"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// [Pattern: Connection Pool] Executor duy trì hồ chứa kết nối để tăng tốc độ ghi.
type Executor struct {
	Pool *pgxpool.Pool
}

// Init mở kết nối bằng connection pool thay vì 1 connection đơn lẻ
func (pe *Executor) Init(ctx context.Context, url string) error {
	slog.Info("SINK (Postgres): Đang khởi tạo connection pool")
	var err error

	// Phân tích chuỗi URL để có thể tùy chỉnh thêm cấu hình pool nếu cần.
	config, err := pgxpool.ParseConfig(url)
	if err != nil {
		return err
	}

	pe.Pool, err = pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return err
	}

	// Kiểm tra kết nối thực tế
	if err := pe.Pool.Ping(ctx); err != nil {
		return err
	}

	slog.Info("SINK (Postgres): Connection pool đã sẵn sàng")
	return nil
}

// [Pattern: Batch Execution] ExecuteBatch gửi nhiều lệnh SQL trong 1 lần round-trip mạng để tối ưu I/O.
func (pe *Executor) ExecuteBatch(ctx context.Context, queries []string, argsList [][]any) error {
	if len(queries) == 0 {
		return nil
	}

	batch := &pgx.Batch{}
	for i, q := range queries {
		batch.Queue(q, argsList[i]...)
	}

	br := pe.Pool.SendBatch(ctx, batch)
	defer br.Close()

	for i := 0; i < len(queries); i++ {
		if _, err := br.Exec(); err != nil {
			// Ghi log chi tiết về câu lệnh gây lỗi để dễ dàng debug.
			return err // Lỗi đã được ghi trong DataProcessor, ở đây chỉ cần trả về
		}
	}

	return nil
}

// Close dọn dẹp tài nguyên khi hệ thống tắt
func (pe *Executor) Close() error {
	if pe.Pool != nil {
		slog.Info("SINK (Postgres): Đang đóng connection pool")
		pe.Pool.Close()
	}
	return nil
}
