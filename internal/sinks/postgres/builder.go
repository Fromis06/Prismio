package postgres

import (
	"encoding/json"
	"fmt"
	"strings"

	"my-cdc/internal/pb"
)

// Builder chuyển đổi đối tượng ChangeEvent thành lệnh SQL thô dành cho PostgreSQL.
type Builder struct{}

// BuildQuery xây dựng câu lệnh SQL từ ChangeEvent chuẩn hóa
func (b *Builder) BuildQuery(e *pb.ChangeEvent) (string, []any) {
	// Bỏ qua các sự kiện không có tên bảng (VD: dummy event "COMMIT").
	if e.Table == "" {
		return "", nil
	}

	var query strings.Builder
	var args []any
	paramIndex := 1 // Đếm số thứ tự của tham số ($1, $2,...)

	var before, after map[string]any
	if len(e.Before) > 0 {
		json.Unmarshal(e.Before, &before)
	}
	if len(e.After) > 0 {
		json.Unmarshal(e.After, &after)
	}

	switch e.Action {
	case pb.Action_INSERT:
		// Xây dựng câu lệnh INSERT INTO ... VALUES ...
		if len(after) == 0 {
			return "", nil
		}

		query.WriteString(fmt.Sprintf("INSERT INTO %s (", e.Table))

		var colNames []string
		var placeholders []string

		// Duyệt qua Map để lấy Tên cột và Giá trị
		for colName, val := range after {
			colNames = append(colNames, colName)
			placeholders = append(placeholders, fmt.Sprintf("$%d", paramIndex))
			args = append(args, val)
			paramIndex++
		}

		query.WriteString(strings.Join(colNames, ", "))
		query.WriteString(") VALUES (")
		query.WriteString(strings.Join(placeholders, ", "))
		query.WriteString(")")

		// Sử dụng ON CONFLICT (Upsert) để đảm bảo tính idempotent.
		// Nếu một sự kiện được xử lý lại, nó sẽ không tạo ra bản ghi trùng lặp.
		if len(e.KeyNames) > 0 {
			query.WriteString(" ON CONFLICT (")
			query.WriteString(strings.Join(e.KeyNames, ", "))
			query.WriteString(")")

			var setClauses []string
			pkMap := make(map[string]bool)
			for _, pk := range e.KeyNames {
				pkMap[pk] = true
			}

			for _, colName := range colNames {
				// Chỉ UPDATE các cột không phải là khóa chính.
				if !pkMap[colName] {
					setClauses = append(setClauses, fmt.Sprintf("%s = EXCLUDED.%s", colName, colName))
				}
			}

			if len(setClauses) > 0 {
				query.WriteString(" DO UPDATE SET ")
				query.WriteString(strings.Join(setClauses, ", "))
			} else {
				query.WriteString(" DO NOTHING")
			}
		}
		query.WriteString(";")

	case pb.Action_UPDATE:
		// Xây dựng câu lệnh UPDATE ... SET ... WHERE ...
		if len(after) == 0 || len(e.KeyNames) == 0 {
			return "", nil
		}

		query.WriteString(fmt.Sprintf("UPDATE %s SET ", e.Table))

		var setClauses []string
		for colName, val := range after {
			setClauses = append(setClauses, fmt.Sprintf("%s = $%d", colName, paramIndex))
			args = append(args, val)
			paramIndex++
		}
		query.WriteString(strings.Join(setClauses, ", "))

		// Thêm điều kiện WHERE dựa trên giá trị của khóa chính.
		query.WriteString(" WHERE ")
		b.appendWhereClause(&query, &args, &paramIndex, e.KeyNames, before, after)

	case pb.Action_DELETE:
		// Xây dựng câu lệnh DELETE FROM ... WHERE ...
		if len(before) == 0 || len(e.KeyNames) == 0 {
			return "", nil
		}

		query.WriteString(fmt.Sprintf("DELETE FROM %s WHERE ", e.Table))
		b.appendWhereClause(&query, &args, &paramIndex, e.KeyNames, before, nil)
	}

	return query.String(), args
}

// appendWhereClause là hàm trợ giúp để xây dựng mệnh đề WHERE
// dựa trên các cột khóa chính một cách an toàn.
func (b *Builder) appendWhereClause(query *strings.Builder, args *[]any, paramIndex *int, keyNames []string, before map[string]any, after map[string]any) {
	var whereClauses []string

	for _, pkName := range keyNames {
		// Lấy giá trị khóa chính từ Before (Ưu tiên cho DELETE/UPDATE).
		// Nếu Before không có (ít xảy ra), lấy fallback từ After.
		val, exists := before[pkName]
		if !exists && after != nil {
			val = after[pkName]
		}

		whereClauses = append(whereClauses, fmt.Sprintf("%s = $%d", pkName, *paramIndex))
		*args = append(*args, val)
		(*paramIndex)++
	}

	query.WriteString(strings.Join(whereClauses, " AND "))
	query.WriteString(";")
}
