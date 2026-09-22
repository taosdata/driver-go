package unified

import (
	"database/sql/driver"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/taosdata/driver-go/v3/common"
	commonstmt "github.com/taosdata/driver-go/v3/common/stmt"
)

// Reuse the driver's interpolation and length validation. The remaining code
// only maps stmt2's column-oriented batches to one VALUES tuple per row.
func buildStmtSQL(sql string, insert bool, fields []*commonstmt.Stmt2AllField, data []*commonstmt.TaosStmt2BindData) ([]string, error) {
	if !insert {
		if len(data) != 1 || data[0] == nil {
			return nil, newInvalidStateErrorf("query bind data is missing")
		}
		args, err := stmtQueryArgs(data[0].Cols, fields)
		if err != nil {
			return nil, err
		}
		out, err := common.InterpolateParams(sql, common.ValueArgsToNamedValueArgs(args))
		if err != nil {
			return nil, err
		}
		return []string{out}, nil
	}

	if len(fields) != strings.Count(sql, "?") {
		return nil, newInvalidStateErrorf("prepared SQL marker count does not match field count")
	}
	prefix, tuple, suffix, err := stmtInsertTemplate(sql)
	if err != nil {
		return nil, err
	}
	prefixCount := strings.Count(prefix, "?")
	if strings.Count(suffix, "?") != 0 || prefixCount+strings.Count(tuple, "?") != len(fields) {
		return nil, newInvalidStateErrorf("unsupported insert SQL parameter layout")
	}
	prefixCols := 0
	for _, field := range fields[:prefixCount] {
		if field != nil && field.BindType == commonstmt.TAOS_FIELD_COL {
			prefixCols++
		}
	}

	out := make([]string, 0, len(data))
	for _, item := range data {
		if item == nil || len(item.Cols) == 0 || len(item.Cols[0]) == 0 {
			return nil, ErrStmtNoRowsToAdd
		}

		prefixArgs, err := stmtInsertArgs(item, fields[:prefixCount], 0, 0)
		if err != nil {
			return nil, err
		}

		var statement strings.Builder
		statement.Grow(len(prefix) + len(tuple)*len(item.Cols[0]) + len(suffix))
		if err := appendStmtSQLArgs(&statement, prefix, prefixArgs); err != nil {
			return nil, err
		}
		for row := range item.Cols[0] {
			if row > 0 {
				statement.WriteByte(' ')
			}
			args, err := stmtInsertArgs(item, fields[prefixCount:], row, prefixCols)
			if err != nil {
				return nil, err
			}
			if err := appendStmtSQLArgs(&statement, tuple, args); err != nil {
				return nil, err
			}
		}
		statement.WriteString(suffix)
		statementSQL := statement.String()
		if len(statementSQL) > common.MaxTaosSqlLen {
			return nil, fmt.Errorf("sql statement exceeds the maximum length")
		}
		out = append(out, statementSQL)
	}
	return out, nil
}

// appendStmtSQLArgs writes an interpolated fragment into the final SQL buffer.
// stmtInsertArgs still performs all value conversion and validation.
func appendStmtSQLArgs(dst *strings.Builder, template string, args []driver.Value) error {
	if strings.Count(template, "?") != len(args) {
		return driver.ErrSkip
	}
	for start, argIndex := 0, 0; ; {
		marker := strings.IndexByte(template[start:], '?')
		if marker < 0 {
			dst.WriteString(template[start:])
			return nil
		}
		marker += start
		dst.WriteString(template[start:marker])
		if err := appendStmtSQLArg(dst, args[argIndex]); err != nil {
			return err
		}
		start = marker + 1
		argIndex++
	}
}

// appendStmtSQLArg preserves common.InterpolateParams' supported value forms,
// but writes directly to the final result rather than allocating a row string.
func appendStmtSQLArg(dst *strings.Builder, arg driver.Value) error {
	switch v := arg.(type) {
	case nil:
		dst.WriteString("NULL")
	case int8:
		dst.WriteString(strconv.FormatInt(int64(v), 10))
	case int16:
		dst.WriteString(strconv.FormatInt(int64(v), 10))
	case int32:
		dst.WriteString(strconv.FormatInt(int64(v), 10))
	case int64:
		dst.WriteString(strconv.FormatInt(v, 10))
	case uint8:
		dst.WriteString(strconv.FormatUint(uint64(v), 10))
	case uint16:
		dst.WriteString(strconv.FormatUint(uint64(v), 10))
	case uint32:
		dst.WriteString(strconv.FormatUint(uint64(v), 10))
	case uint64:
		dst.WriteString(strconv.FormatUint(v, 10))
	case float32:
		dst.WriteString(strconv.FormatFloat(float64(v), 'f', 6, 32))
	case float64:
		dst.WriteString(strconv.FormatFloat(v, 'f', 6, 64))
	case int:
		dst.WriteString(strconv.Itoa(v))
	case uint:
		dst.WriteString(strconv.FormatUint(uint64(v), 10))
	case bool:
		if v {
			dst.WriteByte('1')
		} else {
			dst.WriteByte('0')
		}
	case time.Time:
		dst.WriteByte('\'')
		dst.WriteString(v.Format(time.RFC3339Nano))
		dst.WriteByte('\'')
	case []byte:
		dst.Write(v)
	case string:
		dst.WriteString(v)
	default:
		return driver.ErrSkip
	}
	return nil
}

func stmtQueryArgs(cols [][]driver.Value, fields []*commonstmt.Stmt2AllField) ([]driver.Value, error) {
	args := make([]driver.Value, len(cols))
	for i, col := range cols {
		if len(col) != 1 {
			return nil, newInvalidStateErrorf("query parameter %d must contain exactly one value", i)
		}
		var field *commonstmt.Stmt2AllField
		if i < len(fields) {
			field = fields[i]
		}
		value, err := stmtSQLArg(col[0], field)
		if err != nil {
			return nil, err
		}
		args[i] = value
	}
	return args, nil
}

func stmtInsertArgs(item *commonstmt.TaosStmt2BindData, fields []*commonstmt.Stmt2AllField, row, colBase int) ([]driver.Value, error) {
	args := make([]driver.Value, len(fields))
	tagIndex, colIndex := 0, colBase
	for i, field := range fields {
		if field == nil {
			return nil, newInvalidStateErrorf("nil prepared field")
		}
		var value driver.Value
		switch field.BindType {
		case commonstmt.TAOS_FIELD_TBNAME:
			name, err := stmtSQLIdentifier(item.TableName)
			if err != nil {
				return nil, err
			}
			args[i] = name
			continue
		case commonstmt.TAOS_FIELD_TAG:
			if tagIndex >= len(item.Tags) {
				return nil, newInvalidStateErrorf("missing tag value %d", tagIndex)
			}
			value = item.Tags[tagIndex]
			tagIndex++
		case commonstmt.TAOS_FIELD_COL:
			if colIndex >= len(item.Cols) || row >= len(item.Cols[colIndex]) {
				return nil, newInvalidStateErrorf("missing column value %d row %d", colIndex, row)
			}
			value = item.Cols[colIndex][row]
			colIndex++
		default:
			return nil, newInvalidStateErrorf("unsupported bind type %d", field.BindType)
		}
		arg, err := stmtSQLArg(value, field)
		if err != nil {
			return nil, err
		}
		args[i] = arg
	}
	return args, nil
}

func stmtInsertTemplate(sql string) (prefix, tuple, suffix string, err error) {
	at := strings.Index(strings.ToLower(sql), "values")
	if at < 0 {
		return "", "", "", newInvalidStateErrorf("insert SQL has no VALUES clause")
	}
	start := at + len("values")
	for start < len(sql) && strings.ContainsRune(" \t\r\n", rune(sql[start])) {
		start++
	}
	if start == len(sql) || sql[start] != '(' {
		return "", "", "", newInvalidStateErrorf("VALUES clause has no row tuple")
	}
	depth := 0
	for end := start; end < len(sql); end++ {
		switch sql[end] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return sql[:start], sql[start : end+1], sql[end+1:], nil
			}
		}
	}
	return "", "", "", newInvalidStateErrorf("unterminated VALUES tuple")
}

// InterpolateParams writes strings verbatim, so text and binary values must be
// converted to SQL literals before they are handed to it.
func stmtSQLArg(value driver.Value, field *commonstmt.Stmt2AllField) (driver.Value, error) {
	if value == nil {
		return nil, nil
	}
	typ := int8(common.TSDB_DATA_TYPE_NULL)
	if field != nil {
		typ = field.FieldType
	}
	switch typ {
	case common.TSDB_DATA_TYPE_DECIMAL, common.TSDB_DATA_TYPE_DECIMAL64:
		s, ok := value.(string)
		if !ok {
			return nil, newInvalidStateErrorf("decimal value must be string")
		}
		if _, err := strconv.ParseFloat(s, 64); err != nil {
			return nil, newInvalidStateErrorf("invalid decimal value %q", s)
		}
		return s, nil
	case common.TSDB_DATA_TYPE_VARBINARY, common.TSDB_DATA_TYPE_BLOB:
		b, ok := value.([]byte)
		if !ok {
			return nil, newInvalidStateErrorf("binary value must be []byte")
		}
		return "'\\x" + strings.ToUpper(hex.EncodeToString(b)) + "'", nil
	case common.TSDB_DATA_TYPE_GEOMETRY:
		return nil, newInvalidStateErrorf("ToSQL does not support GEOMETRY WKB")
	}
	switch v := value.(type) {
	case string:
		return stmtSQLQuote(v)
	case []byte:
		return stmtSQLQuote(string(v))
	default:
		return value, nil
	}
}

func stmtSQLQuote(s string) (string, error) {
	if strings.IndexByte(s, 0) >= 0 {
		return "", newInvalidStateErrorf("string value contains NUL")
	}
	return "'" + strings.ReplaceAll(strings.ReplaceAll(s, "\\", "\\\\"), "'", "\\'") + "'", nil
}

func stmtSQLIdentifier(name string) (string, error) {
	if name == "" {
		return "", ErrStmtTableNameNotSet
	}
	for _, r := range name {
		if r < 0x20 || r == ';' {
			return "", newInvalidStateErrorf("invalid table name")
		}
	}
	parts := strings.Split(name, ".")
	for i, part := range parts {
		if part == "" {
			return "", newInvalidStateErrorf("invalid table name")
		}
		parts[i] = "`" + strings.ReplaceAll(part, "`", "``") + "`"
	}
	return strings.Join(parts, "."), nil
}
