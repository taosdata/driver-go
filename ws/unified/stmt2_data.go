package unified

import (
	"database/sql/driver"
	"time"

	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/common/param"
	commonstmt "github.com/taosdata/driver-go/v3/common/stmt"
	"github.com/taosdata/driver-go/v3/types"
)

// normalizeStmt2Value converts one compatibility-layer value into stmt2 bind value.
// queryMode controls timestamp encoding:
//   - query mode: RFC3339Nano string
//   - insert mode: integer timestamp by precision
func normalizeStmt2Value(v driver.Value, queryMode bool) (driver.Value, error) {
	switch typed := v.(type) {
	case nil:
		return nil, nil
	case bool, int8, int16, int32, int64, uint8, uint16, uint32, uint64, float32, float64, string, []byte, time.Time:
		return typed, nil
	case types.TaosBool:
		return bool(typed), nil
	case types.TaosTinyint:
		return int8(typed), nil
	case types.TaosSmallint:
		return int16(typed), nil
	case types.TaosInt:
		return int32(typed), nil
	case types.TaosBigint:
		return int64(typed), nil
	case types.TaosUTinyint:
		return uint8(typed), nil
	case types.TaosUSmallint:
		return uint16(typed), nil
	case types.TaosUInt:
		return uint32(typed), nil
	case types.TaosUBigint:
		return uint64(typed), nil
	case types.TaosFloat:
		return float32(typed), nil
	case types.TaosDouble:
		return float64(typed), nil
	case types.TaosBinary:
		return []byte(typed), nil
	case types.TaosVarBinary:
		return []byte(typed), nil
	case types.TaosNchar:
		return string(typed), nil
	case types.TaosJson:
		return []byte(typed), nil
	case types.TaosGeometry:
		return []byte(typed), nil
	case types.TaosBlob:
		return []byte(typed), nil
	case types.TaosTimestamp:
		if queryMode {
			return typed.T.Format(time.RFC3339Nano), nil
		}
		return common.TimeToTimestamp(typed.T, typed.Precision), nil
	default:
		return nil, newInvalidStateErrorf("unsupported stmt2 value type %T", v)
	}
}

// normalizeStmt2Column converts one Param column into stmt2 bind column data.
func normalizeStmt2Column(paramColumn *param.Param, queryMode bool) ([]driver.Value, error) {
	values := paramColumn.GetValues()
	normalized := make([]driver.Value, len(values))
	for i := 0; i < len(values); i++ {
		v, err := normalizeStmt2Value(values[i], queryMode)
		if err != nil {
			return nil, err
		}
		normalized[i] = v
	}
	return normalized, nil
}

// normalizeStmt2Columns converts all Param columns into stmt2 bind columns.
func normalizeStmt2Columns(columns []*param.Param, queryMode bool) ([][]driver.Value, error) {
	normalized := make([][]driver.Value, len(columns))
	for i := 0; i < len(columns); i++ {
		col, err := normalizeStmt2Column(columns[i], queryMode)
		if err != nil {
			return nil, err
		}
		normalized[i] = col
	}
	return normalized, nil
}

// buildStmt2InsertBindData builds one stmt2 bind block for insert path.
func buildStmt2InsertBindData(tableName string, tags *param.Param, params []*param.Param) (*commonstmt.TaosStmt2BindData, error) {
	item := &commonstmt.TaosStmt2BindData{
		TableName: tableName,
	}
	if tags != nil {
		normalizedTags, err := normalizeStmt2Column(tags, false)
		if err != nil {
			return nil, err
		}
		item.Tags = normalizedTags
	}
	if len(params) > 0 {
		normalizedCols, err := normalizeStmt2Columns(params, false)
		if err != nil {
			return nil, err
		}
		item.Cols = normalizedCols
	}
	return item, nil
}

// buildStmt2QueryBindData builds stmt2 bind data for query path.
// Query supports exactly one bind block.
func buildStmt2QueryBindData(params []*param.Param) ([]*commonstmt.TaosStmt2BindData, error) {
	if len(params) == 0 {
		return nil, newInvalidStateErrorf("no query params")
	}
	cols, err := normalizeStmt2Columns(params, true)
	if err != nil {
		return nil, err
	}
	return []*commonstmt.TaosStmt2BindData{
		{
			Cols: cols,
		},
	}, nil
}
