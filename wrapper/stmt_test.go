package wrapper

import (
	"database/sql/driver"
	"fmt"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/common/param"
	"github.com/taosdata/driver-go/v3/common/parser"
	taosError "github.com/taosdata/driver-go/v3/errors"
	taosTypes "github.com/taosdata/driver-go/v3/types"
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
	defer func() {
		err = exec(conn, "drop database if exists test_wrapper")
		if err != nil {
			t.Error(err)
			return
		}
	}()
	err = exec(conn, "create database if not exists test_wrapper precision 'us' keep 36500")
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
		params      [][]driver.Value
		bindType    []*taosTypes.ColumnType
		expectValue interface{}
	}{
		{
			tbType:      "ts timestamp, v int",
			pos:         "?, ?",
			params:      [][]driver.Value{{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMilliSecond}}, {taosTypes.TaosInt(1)}},
			bindType:    []*taosTypes.ColumnType{{Type: taosTypes.TaosTimestampType}, {Type: taosTypes.TaosIntType}},
			expectValue: int32(1),
		},
		{
			tbType:      "ts timestamp, v bool",
			pos:         "?, ?",
			params:      [][]driver.Value{{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMilliSecond}}, {taosTypes.TaosBool(true)}},
			bindType:    []*taosTypes.ColumnType{{Type: taosTypes.TaosTimestampType}, {Type: taosTypes.TaosBoolType}},
			expectValue: true,
		},
		{
			tbType:      "ts timestamp, v tinyint",
			pos:         "?, ?",
			params:      [][]driver.Value{{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMilliSecond}}, {taosTypes.TaosTinyint(1)}},
			bindType:    []*taosTypes.ColumnType{{Type: taosTypes.TaosTimestampType}, {Type: taosTypes.TaosTinyintType}},
			expectValue: int8(1),
		},
		{
			tbType:      "ts timestamp, v smallint",
			pos:         "?, ?",
			params:      [][]driver.Value{{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMilliSecond}}, {taosTypes.TaosSmallint(1)}},
			bindType:    []*taosTypes.ColumnType{{Type: taosTypes.TaosTimestampType}, {Type: taosTypes.TaosSmallintType}},
			expectValue: int16(1),
		},
		{
			tbType:      "ts timestamp, v bigint",
			pos:         "?, ?",
			params:      [][]driver.Value{{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMilliSecond}}, {taosTypes.TaosBigint(1)}},
			bindType:    []*taosTypes.ColumnType{{Type: taosTypes.TaosTimestampType}, {Type: taosTypes.TaosBigintType}},
			expectValue: int64(1),
		},
		{
			tbType:      "ts timestamp, v tinyint unsigned",
			pos:         "?, ?",
			params:      [][]driver.Value{{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMilliSecond}}, {taosTypes.TaosUTinyint(1)}},
			bindType:    []*taosTypes.ColumnType{{Type: taosTypes.TaosTimestampType}, {Type: taosTypes.TaosUTinyintType}},
			expectValue: uint8(1),
		},
		{
			tbType:      "ts timestamp, v smallint unsigned",
			pos:         "?, ?",
			params:      [][]driver.Value{{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMilliSecond}}, {taosTypes.TaosUSmallint(1)}},
			bindType:    []*taosTypes.ColumnType{{Type: taosTypes.TaosTimestampType}, {Type: taosTypes.TaosUSmallintType}},
			expectValue: uint16(1),
		},
		{
			tbType:      "ts timestamp, v int unsigned",
			pos:         "?, ?",
			params:      [][]driver.Value{{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMilliSecond}}, {taosTypes.TaosUInt(1)}},
			bindType:    []*taosTypes.ColumnType{{Type: taosTypes.TaosTimestampType}, {Type: taosTypes.TaosUIntType}},
			expectValue: uint32(1),
		},
		{
			tbType:      "ts timestamp, v bigint unsigned",
			pos:         "?, ?",
			params:      [][]driver.Value{{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMilliSecond}}, {taosTypes.TaosUBigint(1)}},
			bindType:    []*taosTypes.ColumnType{{Type: taosTypes.TaosTimestampType}, {Type: taosTypes.TaosUBigintType}},
			expectValue: uint64(1),
		},
		{
			tbType:      "ts timestamp, v float",
			pos:         "?, ?",
			params:      [][]driver.Value{{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMilliSecond}}, {taosTypes.TaosFloat(1.2)}},
			bindType:    []*taosTypes.ColumnType{{Type: taosTypes.TaosTimestampType}, {Type: taosTypes.TaosFloatType}},
			expectValue: float32(1.2),
		},
		{
			tbType:      "ts timestamp, v double",
			pos:         "?, ?",
			params:      [][]driver.Value{{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMilliSecond}}, {taosTypes.TaosDouble(1.2)}},
			bindType:    []*taosTypes.ColumnType{{Type: taosTypes.TaosTimestampType}, {Type: taosTypes.TaosDoubleType}},
			expectValue: 1.2,
		},
		{
			tbType: "ts timestamp, v binary(8)",
			pos:    "?, ?",
			params: [][]driver.Value{{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMilliSecond}}, {taosTypes.TaosBinary("yes")}},
			bindType: []*taosTypes.ColumnType{{Type: taosTypes.TaosTimestampType}, {
				Type:   taosTypes.TaosBinaryType,
				MaxLen: 3,
			}},
			expectValue: "yes",
		}, //3
		{
			tbType: "ts timestamp, v nchar(8)",
			pos:    "?, ?",
			params: [][]driver.Value{{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMilliSecond}}, {taosTypes.TaosNchar("yes")}},
			bindType: []*taosTypes.ColumnType{{Type: taosTypes.TaosTimestampType}, {
				Type:   taosTypes.TaosNcharType,
				MaxLen: 3,
			}},
			expectValue: "yes",
		}, //3
		{
			tbType: "ts timestamp, v nchar(8)",
			pos:    "?, ?",
			params: [][]driver.Value{{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMilliSecond}}, {nil}},
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
			if code != 0 {
				errStr := TaosStmtErrStr(insertStmt)
				err = taosError.NewError(code, errStr)
				t.Error(err)
				return
			}
			code = TaosStmtBindParamBatch(insertStmt, tc.params, tc.bindType)
			if code != 0 {
				errStr := TaosStmtErrStr(insertStmt)
				err = taosError.NewError(code, errStr)
				t.Error(err)
				return
			}
			code = TaosStmtAddBatch(insertStmt)
			if code != 0 {
				errStr := TaosStmtErrStr(insertStmt)
				err = taosError.NewError(code, errStr)
				t.Error(err)
				return
			}
			code = TaosStmtExecute(insertStmt)
			if code != 0 {
				errStr := TaosStmtErrStr(insertStmt)
				err = taosError.NewError(code, errStr)
				t.Error(err)
				return
			}
			code = TaosStmtClose(insertStmt)
			if code != 0 {
				errStr := TaosStmtErrStr(insertStmt)
				err = taosError.NewError(code, errStr)
				t.Error(err)
				return
			}
			result, err := query(conn, fmt.Sprintf("select v from %s", tbName))
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
	defer func() {
		err = exec(conn, "drop database if exists test_wrapper")
		if err != nil {
			t.Error(err)
			return
		}
	}()
	err = exec(conn, "create database if not exists test_wrapper precision 'us' keep 36500")
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
		params      []driver.Value
		expectValue interface{}
	}{
		{
			tbType:      "ts timestamp, v int",
			pos:         "?, ?",
			params:      []driver.Value{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMilliSecond}, taosTypes.TaosInt(1)},
			expectValue: int32(1),
		},
		{
			tbType:      "ts timestamp, v bool",
			pos:         "?, ?",
			params:      []driver.Value{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMilliSecond}, taosTypes.TaosBool(true)},
			expectValue: true,
		},
		{
			tbType:      "ts timestamp, v tinyint",
			pos:         "?, ?",
			params:      []driver.Value{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMilliSecond}, taosTypes.TaosTinyint(1)},
			expectValue: int8(1),
		},
		{
			tbType:      "ts timestamp, v smallint",
			pos:         "?, ?",
			params:      []driver.Value{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMilliSecond}, taosTypes.TaosSmallint(1)},
			expectValue: int16(1),
		},
		{
			tbType:      "ts timestamp, v bigint",
			pos:         "?, ?",
			params:      []driver.Value{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMilliSecond}, taosTypes.TaosBigint(1)},
			expectValue: int64(1),
		},
		{
			tbType:      "ts timestamp, v tinyint unsigned",
			pos:         "?, ?",
			params:      []driver.Value{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMilliSecond}, taosTypes.TaosUTinyint(1)},
			expectValue: uint8(1),
		},
		{
			tbType:      "ts timestamp, v smallint unsigned",
			pos:         "?, ?",
			params:      []driver.Value{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMilliSecond}, taosTypes.TaosUSmallint(1)},
			expectValue: uint16(1),
		},
		{
			tbType:      "ts timestamp, v int unsigned",
			pos:         "?, ?",
			params:      []driver.Value{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMilliSecond}, taosTypes.TaosUInt(1)},
			expectValue: uint32(1),
		},
		{
			tbType:      "ts timestamp, v bigint unsigned",
			pos:         "?, ?",
			params:      []driver.Value{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMilliSecond}, taosTypes.TaosUBigint(1)},
			expectValue: uint64(1),
		},
		{
			tbType:      "ts timestamp, v float",
			pos:         "?, ?",
			params:      []driver.Value{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMilliSecond}, taosTypes.TaosFloat(1.2)},
			expectValue: float32(1.2),
		},
		{
			tbType:      "ts timestamp, v double",
			pos:         "?, ?",
			params:      []driver.Value{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMilliSecond}, taosTypes.TaosDouble(1.2)},
			expectValue: 1.2,
		},
		{
			tbType:      "ts timestamp, v binary(8)",
			pos:         "?, ?",
			params:      []driver.Value{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMilliSecond}, taosTypes.TaosBinary("yes")},
			expectValue: "yes",
		}, //3
		{
			tbType:      "ts timestamp, v nchar(8)",
			pos:         "?, ?",
			params:      []driver.Value{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMilliSecond}, taosTypes.TaosNchar("yes")},
			expectValue: "yes",
		}, //3
		{
			tbType:      "ts timestamp, v nchar(8)",
			pos:         "?, ?",
			params:      []driver.Value{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMilliSecond}, nil},
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
			if code != 0 {
				errStr := TaosStmtErrStr(insertStmt)
				err = taosError.NewError(code, errStr)
				t.Error(err)
				return
			}
			code = TaosStmtBindParam(insertStmt, tc.params)
			if code != 0 {
				errStr := TaosStmtErrStr(insertStmt)
				err = taosError.NewError(code, errStr)
				t.Error(err)
				return
			}
			code = TaosStmtAddBatch(insertStmt)
			if code != 0 {
				errStr := TaosStmtErrStr(insertStmt)
				err = taosError.NewError(code, errStr)
				t.Error(err)
				return
			}
			code = TaosStmtExecute(insertStmt)
			if code != 0 {
				errStr := TaosStmtErrStr(insertStmt)
				err = taosError.NewError(code, errStr)
				t.Error(err)
				return
			}
			affectedRows := TaosStmtAffectedRowsOnce(insertStmt)
			if affectedRows != 1 {
				t.Errorf("expect 1 got %d", affectedRows)
				return
			}
			code = TaosStmtClose(insertStmt)
			if code != 0 {
				errStr := TaosStmtErrStr(insertStmt)
				err = taosError.NewError(code, errStr)
				t.Error(err)
				return
			}
			result, err := query(conn, fmt.Sprintf("select v from %s", tbName))
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

func TestStmtQuery(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer TaosClose(conn)
	err = exec(conn, "create database if not exists test_wrapper precision 'us' keep 36500")
	if err != nil {
		t.Error(err)
		return
	}
	err = exec(conn, "use test_wrapper")
	if err != nil {
		t.Error(err)
		return
	}
	for i, tc := range []struct {
		tbType string
		data   string
		clause string
		params *param.Param
		skip   bool
	}{
		{
			tbType: "ts timestamp, v int",
			data:   "0, 1",
			clause: "v = ?",
			params: param.NewParam(1).AddInt(1),
		},
		{
			tbType: "ts timestamp, v bool",
			data:   "now, true",
			clause: "v = ?",
			params: param.NewParam(1).AddBool(true),
		},
		{
			tbType: "ts timestamp, v tinyint",
			data:   "now, 3",
			clause: "v = ?",
			params: param.NewParam(1).AddTinyint(3),
		},
		{
			tbType: "ts timestamp, v smallint",
			data:   "now, 5",
			clause: "v = ?",
			params: param.NewParam(1).AddSmallint(5),
		},
		{
			tbType: "ts timestamp, v int",
			data:   "now, 6",
			clause: "v = ?",
			params: param.NewParam(1).AddInt(6),
		},
		{
			tbType: "ts timestamp, v bigint",
			data:   "now, 7",
			clause: "v = ?",
			params: param.NewParam(1).AddBigint(7),
		},
		{
			tbType: "ts timestamp, v tinyint unsigned",
			data:   "now, 1",
			clause: "v = ?",
			params: param.NewParam(1).AddUTinyint(1),
		},
		{
			tbType: "ts timestamp, v smallint unsigned",
			data:   "now, 2",
			clause: "v = ?",
			params: param.NewParam(1).AddUSmallint(2),
		},
		{
			tbType: "ts timestamp, v int unsigned",
			data:   "now, 3",
			clause: "v = ?",
			params: param.NewParam(1).AddUInt(3),
		},
		{
			tbType: "ts timestamp, v bigint unsigned",
			data:   "now, 4",
			clause: "v = ?",
			params: param.NewParam(1).AddUBigint(4),
		},
		{
			tbType: "ts timestamp, v tinyint unsigned",
			data:   "now, 1",
			clause: "v = ?",
			params: param.NewParam(1).AddUTinyint(1),
		},
		{
			tbType: "ts timestamp, v smallint unsigned",
			data:   "now, 2",
			clause: "v = ?",
			params: param.NewParam(1).AddUSmallint(2),
		},
		{
			tbType: "ts timestamp, v int unsigned",
			data:   "now, 3",
			clause: "v = ?",
			params: param.NewParam(1).AddUInt(3),
		},
		{
			tbType: "ts timestamp, v bigint unsigned",
			data:   "now, 4",
			clause: "v = ?",
			params: param.NewParam(1).AddUBigint(4),
		},
		{
			tbType: "ts timestamp, v float",
			data:   "now, 1.2",
			clause: "v = ?",
			params: param.NewParam(1).AddFloat(1.2),
		},
		{
			tbType: "ts timestamp, v double",
			data:   "now, 1.3",
			clause: "v = ?",
			params: param.NewParam(1).AddDouble(1.3),
		},
		{
			tbType: "ts timestamp, v double",
			data:   "now, 1.4",
			clause: "v = ?",
			params: param.NewParam(1).AddDouble(1.4),
		},
		{
			tbType: "ts timestamp, v binary(8)",
			data:   "now, 'yes'",
			clause: "v = ?",
			params: param.NewParam(1).AddBinary([]byte("yes")),
		},
		{
			tbType: "ts timestamp, v nchar(8)",
			data:   "now, 'OK'",
			clause: "v = ?",
			params: param.NewParam(1).AddNchar("OK"),
		},
		{
			tbType: "ts timestamp, v nchar(8)",
			data:   "1622282105000000, 'NOW'",
			clause: "ts = ? and v = ?",
			params: param.NewParam(2).AddTimestamp(time.Unix(1622282105, 0), common.PrecisionMicroSecond).AddBinary([]byte("NOW")),
		},
		{
			tbType: "ts timestamp, v nchar(8)",
			data:   "1622282105000000, 'NOW'",
			clause: "ts = ? and v = ?",
			params: param.NewParam(2).AddBigint(1622282105000000).AddBinary([]byte("NOW")),
		},
	} {
		tbName := fmt.Sprintf("test_stmt_query%02d", i)
		tbType := tc.tbType
		create := fmt.Sprintf("create table if not exists %s(%s)", tbName, tbType)
		insert := fmt.Sprintf("insert into %s values(%s)", tbName, tc.data)
		params := tc.params
		sql := fmt.Sprintf("select * from %s where %s", tbName, tc.clause)
		name := fmt.Sprintf("%02d-%s", i, tbType)
		var err error
		t.Run(name, func(t *testing.T) {
			if tc.skip {
				t.Skip("Skip, not support yet")
			}
			if err = exec(conn, create); err != nil {
				t.Error(err)
				return
			}
			if err = exec(conn, insert); err != nil {
				t.Error(err)
				return
			}

			rows, err := StmtQuery(t, conn, sql, params)
			if err != nil {
				t.Error(err)
				return
			}
			t.Log(rows)
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
		columns, errCode, block := TaosFetchRawBlock(res)
		if errCode != 0 {
			errStr := TaosErrorStr(res)
			return nil, taosError.NewError(errCode, errStr)
		}
		if columns == 0 {
			break
		}
		r := parser.ReadBlock(block, columns, rh.ColTypes, precision)
		result = append(result, r...)
	}
	return result, nil
}

func StmtQuery(t *testing.T, conn unsafe.Pointer, sql string, params *param.Param) (rows [][]driver.Value, err error) {
	stmt := TaosStmtInit(conn)
	if stmt == nil {
		err = taosError.NewError(0xffff, "failed to init stmt")
		return
	}
	defer TaosStmtClose(stmt)
	code := TaosStmtPrepare(stmt, sql)
	if code != 0 {
		errStr := TaosStmtErrStr(stmt)
		return nil, taosError.NewError(code, errStr)
	}
	value := params.GetValues()
	code = TaosStmtBindParam(stmt, value)
	if code != 0 {
		errStr := TaosStmtErrStr(stmt)
		return nil, taosError.NewError(code, errStr)
	}
	code = TaosStmtExecute(stmt)
	if code != 0 {
		errStr := TaosStmtErrStr(stmt)
		return nil, taosError.NewError(code, errStr)
	}
	res := TaosStmtUseResult(stmt)
	numFields := TaosFieldCount(res)
	rowsHeader, err := ReadColumn(res, numFields)
	t.Log(rowsHeader)
	if err != nil {
		return nil, err
	}
	precision := TaosResultPrecision(res)
	var data [][]driver.Value
	for {
		blockSize, errCode, block := TaosFetchRawBlock(res)
		if errCode != int(taosError.SUCCESS) {
			errStr := TaosErrorStr(res)
			err := taosError.NewError(code, errStr)
			TaosFreeResult(res)
			return nil, err
		}
		if blockSize == 0 {
			break
		}
		d := parser.ReadBlock(block, blockSize, rowsHeader.ColTypes, precision)
		data = append(data, d...)
	}
	TaosFreeResult(res)
	return data, nil
}

func TestGetFields(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer TaosClose(conn)
	stmt := TaosStmtInit(conn)
	defer func() {
		err = exec(conn, "drop database if exists test_stmt_field")
		if err != nil {
			t.Error(err)
			return
		}
	}()
	err = exec(conn, "create database if not exists test_stmt_field")
	if err != nil {
		t.Error(err)
		return
	}
	err = exec(conn, "create table if not exists test_stmt_field.all_type(ts timestamp,"+
		"c1 bool,"+
		"c2 tinyint,"+
		"c3 smallint,"+
		"c4 int,"+
		"c5 bigint,"+
		"c6 tinyint unsigned,"+
		"c7 smallint unsigned,"+
		"c8 int unsigned,"+
		"c9 bigint unsigned,"+
		"c10 float,"+
		"c11 double,"+
		"c12 binary(20),"+
		"c13 nchar(20)"+
		")"+
		"tags(tts timestamp,"+
		"tc1 bool,"+
		"tc2 tinyint,"+
		"tc3 smallint,"+
		"tc4 int,"+
		"tc5 bigint,"+
		"tc6 tinyint unsigned,"+
		"tc7 smallint unsigned,"+
		"tc8 int unsigned,"+
		"tc9 bigint unsigned,"+
		"tc10 float,"+
		"tc11 double,"+
		"tc12 binary(20),"+
		"tc13 nchar(20)"+
		")")
	if err != nil {
		t.Error(err)
		return
	}
	code := TaosStmtPrepare(stmt, "insert into ? using test_stmt_field.all_type tags(?,?,?,?,?,?,?,?,?,?,?,?,?,?) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
	if code != 0 {
		errStr := TaosStmtErrStr(stmt)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	code = TaosStmtSetTBName(stmt, "test_stmt_field.ct2")
	if code != 0 {
		errStr := TaosStmtErrStr(stmt)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	code, tagCount, tagsP := TaosStmtGetTagFields(stmt)
	if code != 0 {
		errStr := TaosStmtErrStr(stmt)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	code, columnCount, columnsP := TaosStmtGetColFields(stmt)
	if code != 0 {
		errStr := TaosStmtErrStr(stmt)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	columns := StmtParseFields(columnCount, columnsP)
	tags := StmtParseFields(tagCount, tagsP)
	assert.Equal(t, []*StmtField{
		{"ts", 9, 0, 0, 8},
		{"c1", 1, 0, 0, 1},
		{"c2", 2, 0, 0, 1},
		{"c3", 3, 0, 0, 2},
		{"c4", 4, 0, 0, 4},
		{"c5", 5, 0, 0, 8},
		{"c6", 11, 0, 0, 1},
		{"c7", 12, 0, 0, 2},
		{"c8", 13, 0, 0, 4},
		{"c9", 14, 0, 0, 8},
		{"c10", 6, 0, 0, 4},
		{"c11", 7, 0, 0, 8},
		{"c12", 8, 0, 0, 22},
		{"c13", 10, 0, 0, 82},
	}, columns)
	assert.Equal(t, []*StmtField{
		{"tts", 9, 0, 0, 8},
		{"tc1", 1, 0, 0, 1},
		{"tc2", 2, 0, 0, 1},
		{"tc3", 3, 0, 0, 2},
		{"tc4", 4, 0, 0, 4},
		{"tc5", 5, 0, 0, 8},
		{"tc6", 11, 0, 0, 1},
		{"tc7", 12, 0, 0, 2},
		{"tc8", 13, 0, 0, 4},
		{"tc9", 14, 0, 0, 8},
		{"tc10", 6, 0, 0, 4},
		{"tc11", 7, 0, 0, 8},
		{"tc12", 8, 0, 0, 22},
		{"tc13", 10, 0, 0, 82},
	}, tags)
}

func TestGetFieldsCommonTable(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer TaosClose(conn)
	stmt := TaosStmtInit(conn)
	defer func() {
		err = exec(conn, "drop database if exists test_stmt_field")
		if err != nil {
			t.Error(err)
			return
		}
	}()
	err = exec(conn, "create database if not exists test_stmt_field")
	if err != nil {
		t.Error(err)
		return
	}
	TaosSelectDB(conn, "test_stmt_field")
	err = exec(conn, "create table if not exists ct(ts timestamp,"+
		"c1 bool,"+
		"c2 tinyint,"+
		"c3 smallint,"+
		"c4 int,"+
		"c5 bigint,"+
		"c6 tinyint unsigned,"+
		"c7 smallint unsigned,"+
		"c8 int unsigned,"+
		"c9 bigint unsigned,"+
		"c10 float,"+
		"c11 double,"+
		"c12 binary(20),"+
		"c13 nchar(20)"+
		")")
	if err != nil {
		t.Error(err)
		return
	}
	code := TaosStmtPrepare(stmt, "insert into ct values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
	if code != 0 {
		errStr := TaosStmtErrStr(stmt)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	code, _, _ = TaosStmtGetTagFields(stmt)
	assert.Equal(t, 0x22A, code&0xffff)
	code, columnCount, columnsP := TaosStmtGetColFields(stmt)
	if code != 0 {
		errStr := TaosStmtErrStr(stmt)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	columns := StmtParseFields(columnCount, columnsP)
	assert.Equal(t, []*StmtField{
		{"ts", 9, 0, 0, 8},
		{"c1", 1, 0, 0, 1},
		{"c2", 2, 0, 0, 1},
		{"c3", 3, 0, 0, 2},
		{"c4", 4, 0, 0, 4},
		{"c5", 5, 0, 0, 8},
		{"c6", 11, 0, 0, 1},
		{"c7", 12, 0, 0, 2},
		{"c8", 13, 0, 0, 4},
		{"c9", 14, 0, 0, 8},
		{"c10", 6, 0, 0, 4},
		{"c11", 7, 0, 0, 8},
		{"c12", 8, 0, 0, 22},
		{"c13", 10, 0, 0, 82},
	}, columns)
}

func exec(conn unsafe.Pointer, sql string) error {
	res := TaosQuery(conn, sql)
	defer TaosFreeResult(res)
	code := TaosError(res)
	if code != 0 {
		errStr := TaosErrorStr(res)
		return taosError.NewError(code, errStr)
	}
	return nil
}

func TestTaosStmtSetTags(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer TaosClose(conn)
	err = exec(conn, "drop database if exists test_wrapper")
	if err != nil {
		t.Error(err)
		return
	}
	err = exec(conn, "create database if not exists test_wrapper precision 'us' keep 36500")
	if err != nil {
		t.Error(err)
		return
	}
	defer exec(conn, "drop database if exists test_wrapper")
	err = exec(conn, "create table if not exists test_wrapper.tgs(ts timestamp,v int) tags (tts timestamp,"+
		"t1 bool,"+
		"t2 tinyint,"+
		"t3 smallint,"+
		"t4 int,"+
		"t5 bigint,"+
		"t6 tinyint unsigned,"+
		"t7 smallint unsigned,"+
		"t8 int unsigned,"+
		"t9 bigint unsigned,"+
		"t10 float,"+
		"t11 double,"+
		"t12 binary(20),"+
		"t13 nchar(20))")
	if err != nil {
		t.Error(err)
		return
	}
	err = exec(conn, "create table if not exists test_wrapper.json_tag (ts timestamp,v int) tags (info json)")
	if err != nil {
		t.Error(err)
		return
	}
	stmt := TaosStmtInit(conn)
	if stmt == nil {
		err = taosError.NewError(0xffff, "failed to init stmt")
		t.Error(err)
		return
	}
	//defer TaosStmtClose(stmt)
	code := TaosStmtPrepare(stmt, "insert into ? using test_wrapper.tgs tags(?,?,?,?,?,?,?,?,?,?,?,?,?,?) values (?,?)")
	if code != 0 {
		errStr := TaosStmtErrStr(stmt)
		t.Error(taosError.NewError(code, errStr))
		return
	}

	code = TaosStmtSetTBName(stmt, "test_wrapper.t0")
	if code != 0 {
		errStr := TaosStmtErrStr(stmt)
		t.Error(taosError.NewError(code, errStr))
		return
	}
	now := time.Now()
	code = TaosStmtSetTags(stmt, param.NewParam(14).
		AddTimestamp(now, common.PrecisionMicroSecond).
		AddBool(true).
		AddTinyint(2).
		AddSmallint(3).
		AddInt(4).
		AddBigint(5).
		AddUTinyint(6).
		AddUSmallint(7).
		AddUInt(8).
		AddUBigint(9).
		AddFloat(10).
		AddDouble(11).
		AddBinary([]byte("binary")).
		AddNchar("nchar").
		GetValues())
	if code != 0 {
		errStr := TaosStmtErrStr(stmt)
		t.Error(taosError.NewError(code, errStr))
		return
	}
	code = TaosStmtBindParam(stmt, param.NewParam(2).AddTimestamp(now, common.PrecisionMicroSecond).AddInt(100).GetValues())
	if code != 0 {
		errStr := TaosStmtErrStr(stmt)
		t.Error(taosError.NewError(code, errStr))
		return
	}
	code = TaosStmtAddBatch(stmt)
	if code != 0 {
		errStr := TaosStmtErrStr(stmt)
		t.Error(taosError.NewError(code, errStr))
		return
	}
	code = TaosStmtExecute(stmt)
	if code != 0 {
		errStr := TaosStmtErrStr(stmt)
		t.Error(taosError.NewError(code, errStr))
		return
	}
	code = TaosStmtSetTBName(stmt, "test_wrapper.t1")
	if code != 0 {
		errStr := TaosStmtErrStr(stmt)
		t.Error(taosError.NewError(code, errStr))
		return
	}
	code = TaosStmtSetTags(stmt, param.NewParam(14).
		AddNull().
		AddNull().
		AddNull().
		AddNull().
		AddNull().
		AddNull().
		AddNull().
		AddNull().
		AddNull().
		AddNull().
		AddNull().
		AddNull().
		AddNull().
		AddNull().
		GetValues())
	if code != 0 {
		errStr := TaosStmtErrStr(stmt)
		t.Error(taosError.NewError(code, errStr))
		return
	}
	code = TaosStmtBindParam(stmt, param.NewParam(2).AddTimestamp(now, common.PrecisionMicroSecond).AddInt(101).GetValues())
	if code != 0 {
		errStr := TaosStmtErrStr(stmt)
		t.Error(taosError.NewError(code, errStr))
		return
	}
	code = TaosStmtAddBatch(stmt)
	if code != 0 {
		errStr := TaosStmtErrStr(stmt)
		t.Error(taosError.NewError(code, errStr))
		return
	}
	code = TaosStmtExecute(stmt)
	if code != 0 {
		errStr := TaosStmtErrStr(stmt)
		t.Error(taosError.NewError(code, errStr))
		return
	}

	code = TaosStmtPrepare(stmt, "insert into ? using test_wrapper.json_tag tags(?) values (?,?)")
	if code != 0 {
		errStr := TaosStmtErrStr(stmt)
		t.Error(taosError.NewError(code, errStr))
		return
	}
	code = TaosStmtSetTBName(stmt, "test_wrapper.t2")
	if code != 0 {
		errStr := TaosStmtErrStr(stmt)
		t.Error(taosError.NewError(code, errStr))
		return
	}
	code = TaosStmtSetTags(stmt, param.NewParam(1).AddJson([]byte(`{"a":"b"}`)).GetValues())
	if code != 0 {
		errStr := TaosStmtErrStr(stmt)
		t.Error(taosError.NewError(code, errStr))
		return
	}
	code = TaosStmtBindParam(stmt, param.NewParam(2).AddTimestamp(now, common.PrecisionMicroSecond).AddInt(102).GetValues())
	if code != 0 {
		errStr := TaosStmtErrStr(stmt)
		t.Error(taosError.NewError(code, errStr))
		return
	}
	code = TaosStmtAddBatch(stmt)
	if code != 0 {
		errStr := TaosStmtErrStr(stmt)
		t.Error(taosError.NewError(code, errStr))
		return
	}
	code = TaosStmtExecute(stmt)
	if code != 0 {
		errStr := TaosStmtErrStr(stmt)
		t.Error(taosError.NewError(code, errStr))
		return
	}
	code = TaosStmtClose(stmt)
	if code != 0 {
		errStr := TaosStmtErrStr(stmt)
		t.Error(taosError.NewError(code, errStr))
		return
	}
	data, err := query(conn, "select tbname,tgs.* from test_wrapper.tgs where v >= 100")
	if err != nil {
		t.Error(err)
		return
	}

	assert.Equal(t, 2, len(data))
	for i := 0; i < 2; i++ {

		switch data[i][0] {
		case "t0":
			assert.Equal(t, now.UTC().UnixNano()/1e3, data[i][1].(time.Time).UTC().UnixNano()/1e3)
			assert.Equal(t, int32(100), data[i][2].(int32))
			assert.Equal(t, now.UTC().UnixNano()/1e3, data[i][3].(time.Time).UTC().UnixNano()/1e3)
			assert.Equal(t, true, data[i][4].(bool))
			assert.Equal(t, int8(2), data[i][5].(int8))
			assert.Equal(t, int16(3), data[i][6].(int16))
			assert.Equal(t, int32(4), data[i][7].(int32))
			assert.Equal(t, int64(5), data[i][8].(int64))
			assert.Equal(t, uint8(6), data[i][9].(uint8))
			assert.Equal(t, uint16(7), data[i][10].(uint16))
			assert.Equal(t, uint32(8), data[i][11].(uint32))
			assert.Equal(t, uint64(9), data[i][12].(uint64))
			assert.Equal(t, float32(10), data[i][13].(float32))
			assert.Equal(t, float64(11), data[i][14].(float64))
			assert.Equal(t, "binary", data[i][15].(string))
			assert.Equal(t, "nchar", data[i][16].(string))
		case "t1":
			assert.Equal(t, now.UTC().UnixNano()/1e3, data[i][1].(time.Time).UTC().UnixNano()/1e3)
			assert.Equal(t, int32(101), data[i][2].(int32))
			for j := 0; j < 14; j++ {
				assert.Nil(t, data[i][3+j])
			}
		}
	}

	data, err = query(conn, "select tbname,json_tag.* from test_wrapper.json_tag where v >= 100")
	if err != nil {
		t.Error(err)
		return
	}
	assert.Equal(t, 1, len(data))
	assert.Equal(t, "t2", data[0][0].(string))
	assert.Equal(t, now.UTC().UnixNano()/1e3, data[0][1].(time.Time).UTC().UnixNano()/1e3)
	assert.Equal(t, int32(102), data[0][2].(int32))
	assert.Equal(t, []byte(`{"a":"b"}`), data[0][3].([]byte))
}
