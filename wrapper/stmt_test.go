package wrapper

import (
	"database/sql/driver"
	"fmt"
	"testing"
	"time"
	"unsafe"

	"github.com/taosdata/driver-go/v2/common"
	taosError "github.com/taosdata/driver-go/v2/errors"
	taosTypes "github.com/taosdata/driver-go/v2/types"
)

// @author: xftan
// @date: 2022/1/27 17:27
// @description: test stmt with taos_stmt_bind_param_batch
func TestStmt(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer TaosClose(conn)
	err = exec(conn, "create database if not exists test_wrapper precision 'us' update 1 keep 36500")
	if err != nil {
		t.Error(err)
		return
	}
	err = exec(conn, "use test_wrapper")
	if err != nil {
		t.Error(err)
		return
	}
	now := time.Now()
	for i, tc := range []struct {
		tbType      string
		pos         string
		params      [][]interface{}
		bindType    []*taosTypes.ColumnType
		expectValue interface{}
	}{
		{
			tbType:      "ts timestamp, value int",
			pos:         "?, ?",
			params:      [][]interface{}{{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMicroSecond}}, {taosTypes.TaosInt(1)}},
			bindType:    []*taosTypes.ColumnType{{Type: taosTypes.TaosTimestampType}, {Type: taosTypes.TaosIntType}},
			expectValue: int32(1),
		},
		{
			tbType:      "ts timestamp, value bool",
			pos:         "?, ?",
			params:      [][]interface{}{{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMicroSecond}}, {taosTypes.TaosBool(true)}},
			bindType:    []*taosTypes.ColumnType{{Type: taosTypes.TaosTimestampType}, {Type: taosTypes.TaosBoolType}},
			expectValue: true,
		},
		{
			tbType:      "ts timestamp, value tinyint",
			pos:         "?, ?",
			params:      [][]interface{}{{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMicroSecond}}, {taosTypes.TaosTinyint(1)}},
			bindType:    []*taosTypes.ColumnType{{Type: taosTypes.TaosTimestampType}, {Type: taosTypes.TaosTinyintType}},
			expectValue: int8(1),
		},
		{
			tbType:      "ts timestamp, value smallint",
			pos:         "?, ?",
			params:      [][]interface{}{{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMicroSecond}}, {taosTypes.TaosSmallint(1)}},
			bindType:    []*taosTypes.ColumnType{{Type: taosTypes.TaosTimestampType}, {Type: taosTypes.TaosSmallintType}},
			expectValue: int16(1),
		},
		{
			tbType:      "ts timestamp, value bigint",
			pos:         "?, ?",
			params:      [][]interface{}{{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMicroSecond}}, {taosTypes.TaosBigint(1)}},
			bindType:    []*taosTypes.ColumnType{{Type: taosTypes.TaosTimestampType}, {Type: taosTypes.TaosBigintType}},
			expectValue: int64(1),
		},
		{
			tbType:      "ts timestamp, value tinyint unsigned",
			pos:         "?, ?",
			params:      [][]interface{}{{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMicroSecond}}, {taosTypes.TaosUTinyint(1)}},
			bindType:    []*taosTypes.ColumnType{{Type: taosTypes.TaosTimestampType}, {Type: taosTypes.TaosUTinyintType}},
			expectValue: uint8(1),
		},
		{
			tbType:      "ts timestamp, value smallint unsigned",
			pos:         "?, ?",
			params:      [][]interface{}{{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMicroSecond}}, {taosTypes.TaosUSmallint(1)}},
			bindType:    []*taosTypes.ColumnType{{Type: taosTypes.TaosTimestampType}, {Type: taosTypes.TaosUSmallintType}},
			expectValue: uint16(1),
		},
		{
			tbType:      "ts timestamp, value int unsigned",
			pos:         "?, ?",
			params:      [][]interface{}{{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMicroSecond}}, {taosTypes.TaosUInt(1)}},
			bindType:    []*taosTypes.ColumnType{{Type: taosTypes.TaosTimestampType}, {Type: taosTypes.TaosUIntType}},
			expectValue: uint32(1),
		},
		{
			tbType:      "ts timestamp, value bigint unsigned",
			pos:         "?, ?",
			params:      [][]interface{}{{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMicroSecond}}, {taosTypes.TaosUBigint(1)}},
			bindType:    []*taosTypes.ColumnType{{Type: taosTypes.TaosTimestampType}, {Type: taosTypes.TaosUBigintType}},
			expectValue: uint64(1),
		},
		{
			tbType:      "ts timestamp, value float",
			pos:         "?, ?",
			params:      [][]interface{}{{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMicroSecond}}, {taosTypes.TaosFloat(1.2)}},
			bindType:    []*taosTypes.ColumnType{{Type: taosTypes.TaosTimestampType}, {Type: taosTypes.TaosFloatType}},
			expectValue: float32(1.2),
		},
		{
			tbType:      "ts timestamp, value double",
			pos:         "?, ?",
			params:      [][]interface{}{{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMicroSecond}}, {taosTypes.TaosDouble(1.2)}},
			bindType:    []*taosTypes.ColumnType{{Type: taosTypes.TaosTimestampType}, {Type: taosTypes.TaosDoubleType}},
			expectValue: 1.2,
		},
		{
			tbType: "ts timestamp, value binary(8)",
			pos:    "?, ?",
			params: [][]interface{}{{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMicroSecond}}, {taosTypes.TaosBinary("yes")}},
			bindType: []*taosTypes.ColumnType{{Type: taosTypes.TaosTimestampType}, {
				Type:   taosTypes.TaosBinaryType,
				MaxLen: 3,
			}},
			expectValue: "yes",
		}, //3
		{
			tbType: "ts timestamp, value nchar(8)",
			pos:    "?, ?",
			params: [][]interface{}{{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMicroSecond}}, {taosTypes.TaosNchar("yes")}},
			bindType: []*taosTypes.ColumnType{{Type: taosTypes.TaosTimestampType}, {
				Type:   taosTypes.TaosNcharType,
				MaxLen: 3,
			}},
			expectValue: "yes",
		}, //3
		{
			tbType: "ts timestamp, value nchar(8)",
			pos:    "?, ?",
			params: [][]interface{}{{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMicroSecond}}, {nil}},
			bindType: []*taosTypes.ColumnType{{Type: taosTypes.TaosTimestampType}, {
				Type:   taosTypes.TaosNcharType,
				MaxLen: 1,
			}},
			expectValue: nil,
		}, //1
	} {
		tbName := fmt.Sprintf("test_fast_insert_%02d", i)
		tbType := tc.tbType
		drop := fmt.Sprintf("drop table if exists %s", tbName)
		create := fmt.Sprintf("create table if not exists %s(%s)", tbName, tbType)
		name := fmt.Sprintf("%02d-%s", i, tbType)
		pos := tc.pos
		sql := fmt.Sprintf("insert into %s values(%s)", tbName, pos)
		var err error
		t.Run(name, func(t *testing.T) {
			if err = exec(conn, drop); err != nil {
				t.Error(err)
				return
			}
			if err = exec(conn, create); err != nil {
				t.Error(err)
				return
			}
			insertStmt := TaosStmtInit(conn)
			code := TaosStmtPrepare(insertStmt, sql)
			err = taosError.GetError(code)
			if err != nil {
				t.Error(err)
				return
			}
			code = TaosStmtBindParamBatch(insertStmt, tc.params, tc.bindType)
			err = taosError.GetError(code)
			if err != nil {
				t.Error(err)
				return
			}
			code = TaosStmtAddBatch(insertStmt)
			err = taosError.GetError(code)
			if err != nil {
				t.Error(err)
				return
			}
			code = TaosStmtExecute(insertStmt)
			err = taosError.GetError(code)
			if err != nil {
				t.Error(err)
				return
			}
			code = TaosStmtClose(insertStmt)
			err = taosError.GetError(code)
			if err != nil {
				t.Error(err)
				return
			}
			result, err := query(conn, fmt.Sprintf("select value from %s", tbName))
			if err != nil {
				t.Error(err)
				return
			}
			if len(result) != 1 {
				t.Errorf("expect %d got %d", 1, len(result))
				return
			}
			if result[0][0] != tc.expectValue {
				t.Errorf("expect %v got %v", tc.expectValue, result[0][0])
				return
			}
		})
	}

}

// @author: xftan
// @date: 2022/1/27 17:27
// @description: test stmt insert with taos_stmt_bind_param
func TestStmtExec(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer TaosClose(conn)
	err = exec(conn, "create database if not exists test_wrapper precision 'us' update 1 keep 36500")
	if err != nil {
		t.Error(err)
		return
	}
	err = exec(conn, "use test_wrapper")
	if err != nil {
		t.Error(err)
		return
	}
	now := time.Now()
	for i, tc := range []struct {
		tbType      string
		pos         string
		params      []interface{}
		expectValue interface{}
	}{
		{
			tbType:      "ts timestamp, value int",
			pos:         "?, ?",
			params:      []interface{}{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMicroSecond}, taosTypes.TaosInt(1)},
			expectValue: int32(1),
		},
		{
			tbType:      "ts timestamp, value bool",
			pos:         "?, ?",
			params:      []interface{}{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMicroSecond}, taosTypes.TaosBool(true)},
			expectValue: true,
		},
		{
			tbType:      "ts timestamp, value tinyint",
			pos:         "?, ?",
			params:      []interface{}{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMicroSecond}, taosTypes.TaosTinyint(1)},
			expectValue: int8(1),
		},
		{
			tbType:      "ts timestamp, value smallint",
			pos:         "?, ?",
			params:      []interface{}{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMicroSecond}, taosTypes.TaosSmallint(1)},
			expectValue: int16(1),
		},
		{
			tbType:      "ts timestamp, value bigint",
			pos:         "?, ?",
			params:      []interface{}{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMicroSecond}, taosTypes.TaosBigint(1)},
			expectValue: int64(1),
		},
		{
			tbType:      "ts timestamp, value tinyint unsigned",
			pos:         "?, ?",
			params:      []interface{}{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMicroSecond}, taosTypes.TaosUTinyint(1)},
			expectValue: uint8(1),
		},
		{
			tbType:      "ts timestamp, value smallint unsigned",
			pos:         "?, ?",
			params:      []interface{}{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMicroSecond}, taosTypes.TaosUSmallint(1)},
			expectValue: uint16(1),
		},
		{
			tbType:      "ts timestamp, value int unsigned",
			pos:         "?, ?",
			params:      []interface{}{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMicroSecond}, taosTypes.TaosUInt(1)},
			expectValue: uint32(1),
		},
		{
			tbType:      "ts timestamp, value bigint unsigned",
			pos:         "?, ?",
			params:      []interface{}{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMicroSecond}, taosTypes.TaosUBigint(1)},
			expectValue: uint64(1),
		},
		{
			tbType:      "ts timestamp, value float",
			pos:         "?, ?",
			params:      []interface{}{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMicroSecond}, taosTypes.TaosFloat(1.2)},
			expectValue: float32(1.2),
		},
		{
			tbType:      "ts timestamp, value double",
			pos:         "?, ?",
			params:      []interface{}{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMicroSecond}, taosTypes.TaosDouble(1.2)},
			expectValue: 1.2,
		},
		{
			tbType:      "ts timestamp, value binary(8)",
			pos:         "?, ?",
			params:      []interface{}{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMicroSecond}, taosTypes.TaosBinary("yes")},
			expectValue: "yes",
		}, //3
		{
			tbType:      "ts timestamp, value nchar(8)",
			pos:         "?, ?",
			params:      []interface{}{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMicroSecond}, taosTypes.TaosNchar("yes")},
			expectValue: "yes",
		}, //3
		{
			tbType:      "ts timestamp, value nchar(8)",
			pos:         "?, ?",
			params:      []interface{}{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMicroSecond}, nil},
			expectValue: nil,
		}, //1
	} {
		tbName := fmt.Sprintf("test_fast_insert_2_%02d", i)
		tbType := tc.tbType
		drop := fmt.Sprintf("drop table if exists %s", tbName)
		create := fmt.Sprintf("create table if not exists %s(%s)", tbName, tbType)
		name := fmt.Sprintf("%02d-%s", i, tbType)
		pos := tc.pos
		sql := fmt.Sprintf("insert into %s values(%s)", tbName, pos)
		var err error
		t.Run(name, func(t *testing.T) {
			if err = exec(conn, drop); err != nil {
				t.Error(err)
				return
			}
			if err = exec(conn, create); err != nil {
				t.Error(err)
				return
			}
			insertStmt := TaosStmtInit(conn)
			code := TaosStmtPrepare(insertStmt, sql)
			err = taosError.GetError(code)
			if err != nil {
				t.Error(err)
				return
			}
			code = TaosStmtBindParam(insertStmt, tc.params)
			err = taosError.GetError(code)
			if err != nil {
				t.Error(err)
				return
			}
			code = TaosStmtAddBatch(insertStmt)
			err = taosError.GetError(code)
			if err != nil {
				t.Error(err)
				return
			}
			code = TaosStmtExecute(insertStmt)
			err = taosError.GetError(code)
			if err != nil {
				t.Error(err)
				return
			}
			insertResult := TaosStmtUseResult(insertStmt)
			affectedRows := TaosAffectedRows(insertResult)
			if affectedRows != 1 {
				t.Errorf("expect 1 got %d", affectedRows)
				return
			}
			TaosFreeResult(insertResult)
			code = TaosStmtClose(insertStmt)
			err = taosError.GetError(code)
			if err != nil {
				t.Error(err)
				return
			}
			result, err := query(conn, fmt.Sprintf("select value from %s", tbName))
			if err != nil {
				t.Error(err)
				return
			}
			if len(result) != 1 {
				t.Errorf("expect %d got %d", 1, len(result))
				return
			}
			if result[0][0] != tc.expectValue {
				t.Errorf("expect %v got %v", tc.expectValue, result[0][0])
				return
			}
		})
	}
}

func query(conn unsafe.Pointer, sql string) ([][]driver.Value, error) {
	res := TaosQuery(conn, sql)
	defer TaosFreeResult(res)
	code := TaosError(res)
	if code != 0 {
		errStr := TaosErrorStr(res)
		return nil, taosError.NewError(code, errStr)
	}
	fileCount := TaosNumFields(res)
	rh, err := ReadColumn(res, fileCount)
	if err != nil {
		return nil, err
	}
	precision := TaosResultPrecision(res)
	var result [][]driver.Value
	for {
		columns, block := TaosFetchBlock(res)
		if columns == 0 {
			break
		}
		r := ReadBlock(block, columns, rh.ColTypes, precision)
		result = append(result, r...)
	}
	return result, nil
}
