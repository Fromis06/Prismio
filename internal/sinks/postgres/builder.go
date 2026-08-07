package postgres

import (
	"encoding/json"
	"fmt"
	"strings"

	"my-cdc/internal/pb"
)

// Builder converts a standardized ChangeEvent object into a PostgreSQL-specific SQL command.
type Builder struct{}

// BuildQuery constructs a SQL statement from a standardized ChangeEvent.
func (b *Builder) BuildQuery(e *pb.ChangeEvent) (string, []any) {
	// Ignore events without a table name, such as logical replication "COMMIT" messages.
	if e.Table == "" {
		return "", nil
	}

	var query strings.Builder
	var args []any
	paramIndex := 1 // Counter for placeholder parameters ($1, $2, ...).

	var before, after map[string]any
	if len(e.Before) > 0 {
		json.Unmarshal(e.Before, &before)
	}
	if len(e.After) > 0 {
		json.Unmarshal(e.After, &after)
	}

	switch e.Action {
	case pb.Action_INSERT:
		if len(after) == 0 {
			return "", nil
		}

		query.WriteString(fmt.Sprintf("INSERT INTO %s (", e.Table))

		var colNames []string
		var placeholders []string

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

		// Use ON CONFLICT (Upsert) to ensure idempotency.
		// If an event is re-processed, it will not create a duplicate record.
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
				// Only UPDATE columns that are not part of the primary key.
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

		query.WriteString(" WHERE ")
		b.appendWhereClause(&query, &args, &paramIndex, e.KeyNames, before, after)

	case pb.Action_DELETE:
		if len(before) == 0 || len(e.KeyNames) == 0 {
			return "", nil
		}

		query.WriteString(fmt.Sprintf("DELETE FROM %s WHERE ", e.Table))
		b.appendWhereClause(&query, &args, &paramIndex, e.KeyNames, before, nil)
	}

	return query.String(), args
}

// appendWhereClause is a helper function to safely build a WHERE clause
// based on primary key columns.
func (b *Builder) appendWhereClause(query *strings.Builder, args *[]any, paramIndex *int, keyNames []string, before map[string]any, after map[string]any) {
	var whereClauses []string

	for _, pkName := range keyNames {
		// Prioritize the primary key value from 'before' for DELETE/UPDATE operations.
		// Fallback to 'after' if 'before' is not available (which is rare).
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
