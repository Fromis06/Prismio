package sinks

import (
	"context"
	"my-cdc/internal/models"
	"my-cdc/internal/pb"
)

// QueryBuilder là interface định nghĩa hành vi chuyển đổi một ChangeEvent chuẩn hóa
// thành một câu lệnh SQL cụ thể cho database đích.
type QueryBuilder interface {
	// BuildQuery nhận vào một sự kiện và trả về chuỗi câu lệnh cùng mảng tham số,
	// giúp chống lại tấn công SQL Injection.
	BuildQuery(event *pb.ChangeEvent) (query string, args []any)
}

// DatabaseExecutor là interface chịu trách nhiệm quản lý kết nối vật lý
// và thực thi các câu lệnh SQL lên database đích.
type DatabaseExecutor interface {
	Init(ctx context.Context, url string) error                                 // Khởi tạo kết nối (VD: tạo connection pool).
	ExecuteBatch(ctx context.Context, queries []string, argsList [][]any) error // Thực thi một lô câu lệnh.
	Close() error                                                               // Đóng kết nối và giải phóng tài nguyên.
}

// Pipeline đại diện cho một luồng xử lý dữ liệu hoàn chỉnh cho một database đích.
// Nó bao gồm việc nhận dữ liệu, xử lý và ghi xuống đích.
type Pipeline interface {
	Start() error                              // Khởi động pipeline (VD: chạy các worker goroutine).
	WriteBatch(events []*pb.ChangeEvent) error // Nhận một "túi" sự kiện (dùng cho kịch bản 1-1).
	WriteShared(bag *models.SharedEventBag)    // Nhận một "túi" sự kiện đã được chia sẻ (dùng cho kịch bản 1-nhiều).
	Stop() error                               // Dừng pipeline một cách an toàn (graceful shutdown).
	IsActive() bool                            // Kiểm tra xem pipeline có còn hoạt động không.
}
