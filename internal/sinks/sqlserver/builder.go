package sqlserver

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"my-cdc/internal/pb"
)

// Builder converts a standardized ChangeEvent into a SQL Server statement.
type Builder struct{}

// BuildQuery constructs a parameterized SQL Server statement. Inserts with a
// primary key use MERGE so replaying an event after a restart is idempotent.
func (b *Builder) BuildQuery(e *pb.ChangeEvent) (string, []any) {
	if e == nil || e.Table == "" {
		return "", nil
	}

	before := decodeRow(e.Before)
	after := decodeRow(e.After)
	table := quoteIdentifier(e.Table)

	switch e.Action {
	case pb.Action_INSERT:
		return b.buildInsert(table, e.KeyNames, after)
	case pb.Action_UPDATE:
		return b.buildUpdate(table, e.KeyNames, before, after)
	case pb.Action_DELETE:
		return b.buildDelete(table, e.KeyNames, before)
	default:
		return "", nil
	}
}

func (b *Builder) buildInsert(table string, keyNames []string, after map[string]any) (string, []any) {
	if len(after) == 0 {
		return "", nil
	}

	columns := sortedKeys(after)
	args := valuesForColumns(after, columns)
	placeholders := make([]string, len(columns))
	quotedColumns := make([]string, len(columns))
	for i, column := range columns {
		placeholders[i] = placeholder(i + 1)
		quotedColumns[i] = quoteIdentifier(column)
	}

	if len(keyNames) == 0 {
		return fmt.Sprintf(
			"INSERT INTO %s (%s) VALUES (%s);",
			table,
			strings.Join(quotedColumns, ", "),
			strings.Join(placeholders, ", "),
		), args
	}

	// A key absent from the row would produce an invalid MERGE source. Such an
	// event cannot be applied safely, so let the caller skip it.
	for _, key := range keyNames {
		if _, ok := after[key]; !ok {
			return "", nil
		}
	}

	var query strings.Builder
	query.WriteString("MERGE INTO ")
	query.WriteString(table)
	query.WriteString(" WITH (HOLDLOCK) AS target USING (VALUES (")
	query.WriteString(strings.Join(placeholders, ", "))
	query.WriteString(")) AS source (")
	query.WriteString(strings.Join(quotedColumns, ", "))
	query.WriteString(") ON ")

	onClauses := make([]string, 0, len(keyNames))
	keySet := make(map[string]struct{}, len(keyNames))
	for _, key := range keyNames {
		quotedKey := quoteIdentifier(key)
		onClauses = append(onClauses, "target."+quotedKey+" = source."+quotedKey)
		keySet[key] = struct{}{}
	}
	query.WriteString(strings.Join(onClauses, " AND "))

	setClauses := make([]string, 0, len(columns))
	for _, column := range columns {
		if _, isKey := keySet[column]; isKey {
			continue
		}
		quotedColumn := quoteIdentifier(column)
		setClauses = append(setClauses, quotedColumn+" = source."+quotedColumn)
	}
	if len(setClauses) > 0 {
		query.WriteString(" WHEN MATCHED THEN UPDATE SET ")
		query.WriteString(strings.Join(setClauses, ", "))
	}

	query.WriteString(" WHEN NOT MATCHED THEN INSERT (")
	query.WriteString(strings.Join(quotedColumns, ", "))
	query.WriteString(") VALUES (")
	for i, column := range columns {
		if i > 0 {
			query.WriteString(", ")
		}
		query.WriteString("source.")
		query.WriteString(quoteIdentifier(column))
	}
	query.WriteString(");")

	return query.String(), args
}

func (b *Builder) buildUpdate(table string, keyNames []string, before, after map[string]any) (string, []any) {
	if len(after) == 0 || len(keyNames) == 0 {
		return "", nil
	}

	columns := sortedKeys(after)
	args := valuesForColumns(after, columns)
	setClauses := make([]string, len(columns))
	for i, column := range columns {
		setClauses[i] = fmt.Sprintf("%s = %s", quoteIdentifier(column), placeholder(i+1))
	}

	whereClauses, keyArgs, ok := buildWhereClause(keyNames, before, after, len(args)+1)
	if !ok {
		return "", nil
	}
	args = append(args, keyArgs...)

	return fmt.Sprintf(
		"UPDATE %s SET %s WHERE %s;",
		table,
		strings.Join(setClauses, ", "),
		strings.Join(whereClauses, " AND "),
	), args
}

func (b *Builder) buildDelete(table string, keyNames []string, before map[string]any) (string, []any) {
	if len(before) == 0 || len(keyNames) == 0 {
		return "", nil
	}

	whereClauses, args, ok := buildWhereClause(keyNames, before, nil, 1)
	if !ok {
		return "", nil
	}

	return fmt.Sprintf(
		"DELETE FROM %s WHERE %s;",
		table,
		strings.Join(whereClauses, " AND "),
	), args
}

func buildWhereClause(keyNames []string, before, after map[string]any, firstParameter int) ([]string, []any, bool) {
	clauses := make([]string, 0, len(keyNames))
	args := make([]any, 0, len(keyNames))
	for i, key := range keyNames {
		value, ok := before[key]
		if !ok && after != nil {
			value, ok = after[key]
		}
		if !ok {
			return nil, nil, false
		}
		clauses = append(clauses, fmt.Sprintf("%s = %s", quoteIdentifier(key), placeholder(firstParameter+i)))
		args = append(args, value)
	}
	return clauses, args, true
}

func decodeRow(data []byte) map[string]any {
	if len(data) == 0 {
		return nil
	}
	var row map[string]any
	if err := json.Unmarshal(data, &row); err != nil {
		return nil
	}
	return row
}

func sortedKeys(row map[string]any) []string {
	columns := make([]string, 0, len(row))
	for column := range row {
		columns = append(columns, column)
	}
	sort.Strings(columns)
	return columns
}

func valuesForColumns(row map[string]any, columns []string) []any {
	args := make([]any, len(columns))
	for i, column := range columns {
		args[i] = row[column]
	}
	return args
}

func placeholder(index int) string {
	return fmt.Sprintf("@p%d", index)
}

func quoteIdentifier(identifier string) string {
	return "[" + strings.ReplaceAll(identifier, "]", "]]") + "]"
}
