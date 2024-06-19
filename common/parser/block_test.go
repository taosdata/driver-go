package parser

import (
	"database/sql/driver"
	"fmt"
	"math"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/common/pointer"
	"github.com/taosdata/driver-go/v3/errors"
	"github.com/taosdata/driver-go/v3/wrapper"
)

// @author: xftan
// @date: 2023/10/13 11:13
// @description: test block
func TestReadBlock(t *testing.T) {
	conn, err := wrapper.TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}

	defer wrapper.TaosClose(conn)
	defer func() {
		res := wrapper.TaosQuery(conn, "drop database if exists test_block_raw_parser")
		code := wrapper.TaosError(res)
		if code != 0 {
			errStr := wrapper.TaosErrorStr(res)
			wrapper.TaosFreeResult(res)
			t.Error(errors.NewError(code, errStr))
			return
		}
		wrapper.TaosFreeResult(res)
	}()
	res := wrapper.TaosQuery(conn, "create database if not exists test_block_raw_parser")
	code := wrapper.TaosError(res)
	if code != 0 {
		errStr := wrapper.TaosErrorStr(res)
		wrapper.TaosFreeResult(res)
		t.Error(errors.NewError(code, errStr))
		return
	}
	wrapper.TaosFreeResult(res)
	res = wrapper.TaosQuery(conn, "drop table if exists test_block_raw_parser.all_type2")
	code = wrapper.TaosError(res)
	if code != 0 {
		errStr := wrapper.TaosErrorStr(res)
		wrapper.TaosFreeResult(res)
		t.Error(errors.NewError(code, errStr))
		return
	}
	wrapper.TaosFreeResult(res)
	res = wrapper.TaosQuery(conn, "create table if not exists test_block_raw_parser.all_type2 (ts timestamp,"+
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
	code = wrapper.TaosError(res)
	if code != 0 {
		errStr := wrapper.TaosErrorStr(res)
		wrapper.TaosFreeResult(res)
		t.Error(errors.NewError(code, errStr))
		return
	}
	wrapper.TaosFreeResult(res)
	now := time.Now()
	after1s := now.Add(time.Second)
	sql := fmt.Sprintf("insert into test_block_raw_parser.all_type2 values('%s',1,1,1,1,1,1,1,1,1,1,1,'test_binary','test_nchar')('%s',null,null,null,null,null,null,null,null,null,null,null,null,null)", now.Format(time.RFC3339Nano), after1s.Format(time.RFC3339Nano))
	res = wrapper.TaosQuery(conn, sql)
	code = wrapper.TaosError(res)
	if code != 0 {
		errStr := wrapper.TaosErrorStr(res)
		wrapper.TaosFreeResult(res)
		t.Error(errors.NewError(code, errStr))
		return
	}
	wrapper.TaosFreeResult(res)

	sql = "select * from test_block_raw_parser.all_type2"
	res = wrapper.TaosQuery(conn, sql)
	code = wrapper.TaosError(res)
	if code != 0 {
		errStr := wrapper.TaosErrorStr(res)
		wrapper.TaosFreeResult(res)
		t.Error(errors.NewError(code, errStr))
		return
	}
	fileCount := wrapper.TaosNumFields(res)
	rh, err := wrapper.ReadColumn(res, fileCount)
	if err != nil {
		t.Error(err)
		return
	}
	precision := wrapper.TaosResultPrecision(res)
	pHeaderList := make([]unsafe.Pointer, fileCount)
	pStartList := make([]unsafe.Pointer, fileCount)
	var data [][]driver.Value
	for {
		blockSize, errCode, block := wrapper.TaosFetchRawBlock(res)
		if errCode != int(errors.SUCCESS) {
			errStr := wrapper.TaosErrorStr(res)
			err := errors.NewError(code, errStr)
			t.Error(err)
			wrapper.TaosFreeResult(res)
			return
		}
		if blockSize == 0 {
			break
		}
		nullBitMapOffset := uintptr(BitmapLen(blockSize))
		lengthOffset := RawBlockGetColumnLengthOffset(fileCount)
		tmpPHeader := pointer.AddUintptr(block, RawBlockGetColDataOffset(fileCount))
		version := RawBlockGetVersion(block)
		var tmpPStart unsafe.Pointer
		for column := 0; column < fileCount; column++ {
			colLength := *((*int32)(pointer.AddUintptr(block, lengthOffset+uintptr(column)*Int32Size)))
			if IsVarDataType(rh.ColTypes[column]) {
				pHeaderList[column] = tmpPHeader
				tmpPStart = pointer.AddUintptr(tmpPHeader, Int32Size*uintptr(blockSize))
				pStartList[column] = tmpPStart
			} else {
				pHeaderList[column] = tmpPHeader
				tmpPStart = pointer.AddUintptr(tmpPHeader, nullBitMapOffset)
				pStartList[column] = tmpPStart
			}
			tmpPHeader = pointer.AddUintptr(tmpPStart, uintptr(colLength))
		}
		for row := 0; row < blockSize; row++ {
			rowV := make([]driver.Value, fileCount)
			for column := 0; column < fileCount; column++ {
				v := ItemRawBlock(version, rh.ColTypes[column], pHeaderList[column], pStartList[column], row, precision, func(ts int64, precision int) driver.Value {
					return common.TimestampConvertToTime(ts, precision)
				})
				rowV[column] = v
			}
			data = append(data, rowV)
		}
	}
	wrapper.TaosFreeResult(res)
	assert.Equal(t, 2, len(data))
	row1 := data[0]
	assert.Equal(t, now.UnixNano()/1e6, row1[0].(time.Time).UnixNano()/1e6)
	assert.Equal(t, true, row1[1].(bool))
	assert.Equal(t, int8(1), row1[2].(int8))
	assert.Equal(t, int16(1), row1[3].(int16))
	assert.Equal(t, int32(1), row1[4].(int32))
	assert.Equal(t, int64(1), row1[5].(int64))
	assert.Equal(t, uint8(1), row1[6].(uint8))
	assert.Equal(t, uint16(1), row1[7].(uint16))
	assert.Equal(t, uint32(1), row1[8].(uint32))
	assert.Equal(t, uint64(1), row1[9].(uint64))
	assert.Equal(t, float32(1), row1[10].(float32))
	assert.Equal(t, float64(1), row1[11].(float64))
	assert.Equal(t, "test_binary", row1[12].(string))
	assert.Equal(t, "test_nchar", row1[13].(string))
	row2 := data[1]
	assert.Equal(t, after1s.UnixNano()/1e6, row2[0].(time.Time).UnixNano()/1e6)
	for i := 1; i < 14; i++ {
		assert.Nil(t, row2[i])
	}
}

// @author: xftan
// @date: 2023/10/13 11:13
// @description: test block tag
func TestBlockTag(t *testing.T) {
	conn, err := wrapper.TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}

	defer wrapper.TaosClose(conn)
	defer func() {
		res := wrapper.TaosQuery(conn, "drop database if exists test_block_abc1")
		code := wrapper.TaosError(res)
		if code != 0 {
			errStr := wrapper.TaosErrorStr(res)
			wrapper.TaosFreeResult(res)
			t.Error(errors.NewError(code, errStr))
			return
		}
		wrapper.TaosFreeResult(res)
	}()
	res := wrapper.TaosQuery(conn, "create database if not exists test_block_abc1")
	code := wrapper.TaosError(res)
	if code != 0 {
		errStr := wrapper.TaosErrorStr(res)
		wrapper.TaosFreeResult(res)
		t.Error(errors.NewError(code, errStr))
		return
	}
	wrapper.TaosFreeResult(res)

	res = wrapper.TaosQuery(conn, "use test_block_abc1")
	code = wrapper.TaosError(res)
	if code != 0 {
		errStr := wrapper.TaosErrorStr(res)
		wrapper.TaosFreeResult(res)
		t.Error(errors.NewError(code, errStr))
		return
	}
	wrapper.TaosFreeResult(res)

	res = wrapper.TaosQuery(conn, "create table if not exists meters(ts timestamp, v int) tags(location varchar(16))")
	code = wrapper.TaosError(res)
	if code != 0 {
		errStr := wrapper.TaosErrorStr(res)
		wrapper.TaosFreeResult(res)
		t.Error(errors.NewError(code, errStr))
		return
	}
	wrapper.TaosFreeResult(res)

	res = wrapper.TaosQuery(conn, "create table if not exists tb1 using meters tags('abcd')")
	code = wrapper.TaosError(res)
	if code != 0 {
		errStr := wrapper.TaosErrorStr(res)
		wrapper.TaosFreeResult(res)
		t.Error(errors.NewError(code, errStr))
		return
	}
	wrapper.TaosFreeResult(res)

	sql := "select distinct tbname,location from meters;"
	res = wrapper.TaosQuery(conn, sql)
	code = wrapper.TaosError(res)
	if code != 0 {
		errStr := wrapper.TaosErrorStr(res)
		wrapper.TaosFreeResult(res)
		t.Error(errors.NewError(code, errStr))
		return
	}
	fileCount := wrapper.TaosNumFields(res)
	rh, err := wrapper.ReadColumn(res, fileCount)
	if err != nil {
		t.Error(err)
		return
	}
	precision := wrapper.TaosResultPrecision(res)
	pHeaderList := make([]unsafe.Pointer, fileCount)
	pStartList := make([]unsafe.Pointer, fileCount)
	var data [][]driver.Value
	for {
		blockSize, errCode, block := wrapper.TaosFetchRawBlock(res)
		if errCode != int(errors.SUCCESS) {
			errStr := wrapper.TaosErrorStr(res)
			err := errors.NewError(code, errStr)
			t.Error(err)
			wrapper.TaosFreeResult(res)
			return
		}
		if blockSize == 0 {
			break
		}
		nullBitMapOffset := uintptr(BitmapLen(blockSize))
		lengthOffset := RawBlockGetColumnLengthOffset(fileCount)
		tmpPHeader := pointer.AddUintptr(block, RawBlockGetColDataOffset(fileCount)) // length i32, group u64
		version := RawBlockGetVersion(block)
		var tmpPStart unsafe.Pointer
		for column := 0; column < fileCount; column++ {
			colLength := *((*int32)(pointer.AddUintptr(block, lengthOffset+uintptr(column)*Int32Size)))
			if IsVarDataType(rh.ColTypes[column]) {
				pHeaderList[column] = tmpPHeader
				tmpPStart = pointer.AddUintptr(tmpPHeader, Int32Size*uintptr(blockSize))
				pStartList[column] = tmpPStart
			} else {
				pHeaderList[column] = tmpPHeader
				tmpPStart = pointer.AddUintptr(tmpPHeader, nullBitMapOffset)
				pStartList[column] = tmpPStart
			}
			tmpPHeader = pointer.AddUintptr(tmpPStart, uintptr(colLength))
		}
		for row := 0; row < blockSize; row++ {
			rowV := make([]driver.Value, fileCount)
			for column := 0; column < fileCount; column++ {
				v := ItemRawBlock(version, rh.ColTypes[column], pHeaderList[column], pStartList[column], row, precision, func(ts int64, precision int) driver.Value {
					return common.TimestampConvertToTime(ts, precision)
				})
				rowV[column] = v
			}
			data = append(data, rowV)
		}
	}
	wrapper.TaosFreeResult(res)
	t.Log(data)
	t.Log(len(data[0][1].(string)))
}

// @author: xftan
// @date: 2023/10/13 11:18
// @description: test read row
func TestReadRow(t *testing.T) {
	conn, err := wrapper.TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}

	defer wrapper.TaosClose(conn)
	res := wrapper.TaosQuery(conn, "drop database if exists test_read_row")
	code := wrapper.TaosError(res)
	if code != 0 {
		errStr := wrapper.TaosErrorStr(res)
		wrapper.TaosFreeResult(res)
		t.Error(errors.NewError(code, errStr))
		return
	}
	wrapper.TaosFreeResult(res)
	defer func() {
		res = wrapper.TaosQuery(conn, "drop database if exists test_read_row")
		code = wrapper.TaosError(res)
		if code != 0 {
			errStr := wrapper.TaosErrorStr(res)
			wrapper.TaosFreeResult(res)
			t.Error(errors.NewError(code, errStr))
			return
		}
		wrapper.TaosFreeResult(res)
	}()
	res = wrapper.TaosQuery(conn, "create database test_read_row")
	code = wrapper.TaosError(res)
	if code != 0 {
		errStr := wrapper.TaosErrorStr(res)
		wrapper.TaosFreeResult(res)
		t.Error(errors.NewError(code, errStr))
		return
	}
	wrapper.TaosFreeResult(res)

	res = wrapper.TaosQuery(conn, "create table if not exists test_read_row.all_type (ts timestamp,"+
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
		") tags (info json)")
	code = wrapper.TaosError(res)
	if code != 0 {
		errStr := wrapper.TaosErrorStr(res)
		wrapper.TaosFreeResult(res)
		t.Error(errors.NewError(code, errStr))
		return
	}
	wrapper.TaosFreeResult(res)
	now := time.Now()
	after1s := now.Add(time.Second)
	sql := fmt.Sprintf("insert into test_read_row.t0 using test_read_row.all_type tags('{\"a\":1}') values('%s',1,1,1,1,1,1,1,1,1,1,1,'test_binary','test_nchar')('%s',null,null,null,null,null,null,null,null,null,null,null,null,null)", now.Format(time.RFC3339Nano), after1s.Format(time.RFC3339Nano))
	res = wrapper.TaosQuery(conn, sql)
	code = wrapper.TaosError(res)
	if code != 0 {
		errStr := wrapper.TaosErrorStr(res)
		wrapper.TaosFreeResult(res)
		t.Error(errors.NewError(code, errStr))
		return
	}
	wrapper.TaosFreeResult(res)

	sql = "select * from test_read_row.all_type"
	res = wrapper.TaosQuery(conn, sql)
	code = wrapper.TaosError(res)
	if code != 0 {
		errStr := wrapper.TaosErrorStr(res)
		wrapper.TaosFreeResult(res)
		t.Error(errors.NewError(code, errStr))
		return
	}
	fileCount := wrapper.TaosNumFields(res)
	rh, err := wrapper.ReadColumn(res, fileCount)
	if err != nil {
		t.Error(err)
		return
	}
	precision := wrapper.TaosResultPrecision(res)
	var data [][]driver.Value
	for {
		blockSize, errCode, block := wrapper.TaosFetchRawBlock(res)
		if errCode != int(errors.SUCCESS) {
			errStr := wrapper.TaosErrorStr(res)
			err := errors.NewError(code, errStr)
			t.Error(err)
			wrapper.TaosFreeResult(res)
			return
		}
		if blockSize == 0 {
			break
		}
		for i := 0; i < blockSize; i++ {
			tmp := make([]driver.Value, fileCount)
			ReadRow(tmp, block, blockSize, i, rh.ColTypes, precision)
			data = append(data, tmp)
		}
	}
	wrapper.TaosFreeResult(res)
	assert.Equal(t, 2, len(data))
	row1 := data[0]
	assert.Equal(t, now.UnixNano()/1e6, row1[0].(time.Time).UnixNano()/1e6)
	assert.Equal(t, true, row1[1].(bool))
	assert.Equal(t, int8(1), row1[2].(int8))
	assert.Equal(t, int16(1), row1[3].(int16))
	assert.Equal(t, int32(1), row1[4].(int32))
	assert.Equal(t, int64(1), row1[5].(int64))
	assert.Equal(t, uint8(1), row1[6].(uint8))
	assert.Equal(t, uint16(1), row1[7].(uint16))
	assert.Equal(t, uint32(1), row1[8].(uint32))
	assert.Equal(t, uint64(1), row1[9].(uint64))
	assert.Equal(t, float32(1), row1[10].(float32))
	assert.Equal(t, float64(1), row1[11].(float64))
	assert.Equal(t, "test_binary", row1[12].(string))
	assert.Equal(t, "test_nchar", row1[13].(string))
	assert.Equal(t, []byte(`{"a":1}`), row1[14].([]byte))
	row2 := data[1]
	assert.Equal(t, after1s.UnixNano()/1e6, row2[0].(time.Time).UnixNano()/1e6)
	for i := 1; i < 14; i++ {
		assert.Nil(t, row2[i])
	}
	assert.Equal(t, []byte(`{"a":1}`), row2[14].([]byte))
}

// @author: xftan
// @date: 2023/10/13 11:18
// @description: test read block with time format
func TestReadBlockWithTimeFormat(t *testing.T) {
	conn, err := wrapper.TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}

	defer wrapper.TaosClose(conn)
	res := wrapper.TaosQuery(conn, "drop database if exists test_read_block_tf")
	code := wrapper.TaosError(res)
	if code != 0 {
		errStr := wrapper.TaosErrorStr(res)
		wrapper.TaosFreeResult(res)
		t.Error(errors.NewError(code, errStr))
		return
	}
	wrapper.TaosFreeResult(res)
	defer func() {
		res = wrapper.TaosQuery(conn, "drop database if exists test_read_block_tf")
		code = wrapper.TaosError(res)
		if code != 0 {
			errStr := wrapper.TaosErrorStr(res)
			wrapper.TaosFreeResult(res)
			t.Error(errors.NewError(code, errStr))
			return
		}
		wrapper.TaosFreeResult(res)
	}()
	res = wrapper.TaosQuery(conn, "create database test_read_block_tf")
	code = wrapper.TaosError(res)
	if code != 0 {
		errStr := wrapper.TaosErrorStr(res)
		wrapper.TaosFreeResult(res)
		t.Error(errors.NewError(code, errStr))
		return
	}
	wrapper.TaosFreeResult(res)

	res = wrapper.TaosQuery(conn, "create table if not exists test_read_block_tf.all_type (ts timestamp,"+
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
		") tags (info json)")
	code = wrapper.TaosError(res)
	if code != 0 {
		errStr := wrapper.TaosErrorStr(res)
		wrapper.TaosFreeResult(res)
		t.Error(errors.NewError(code, errStr))
		return
	}
	wrapper.TaosFreeResult(res)
	now := time.Now()
	after1s := now.Add(time.Second)
	sql := fmt.Sprintf("insert into test_read_block_tf.t0 using test_read_block_tf.all_type tags('{\"a\":1}') values('%s',false,1,1,1,1,1,1,1,1,1,1,'test_binary','test_nchar')('%s',null,null,null,null,null,null,null,null,null,null,null,null,null)", now.Format(time.RFC3339Nano), after1s.Format(time.RFC3339Nano))
	res = wrapper.TaosQuery(conn, sql)
	code = wrapper.TaosError(res)
	if code != 0 {
		errStr := wrapper.TaosErrorStr(res)
		wrapper.TaosFreeResult(res)
		t.Error(errors.NewError(code, errStr))
		return
	}
	wrapper.TaosFreeResult(res)

	sql = "select * from test_read_block_tf.all_type"
	res = wrapper.TaosQuery(conn, sql)
	code = wrapper.TaosError(res)
	if code != 0 {
		errStr := wrapper.TaosErrorStr(res)
		wrapper.TaosFreeResult(res)
		t.Error(errors.NewError(code, errStr))
		return
	}
	fileCount := wrapper.TaosNumFields(res)
	rh, err := wrapper.ReadColumn(res, fileCount)
	if err != nil {
		t.Error(err)
		return
	}
	precision := wrapper.TaosResultPrecision(res)
	var data [][]driver.Value
	for {
		blockSize, errCode, block := wrapper.TaosFetchRawBlock(res)
		if errCode != int(errors.SUCCESS) {
			errStr := wrapper.TaosErrorStr(res)
			err := errors.NewError(code, errStr)
			t.Error(err)
			wrapper.TaosFreeResult(res)
			return
		}
		if blockSize == 0 {
			break
		}
		data = ReadBlockWithTimeFormat(block, blockSize, rh.ColTypes, precision, func(ts int64, precision int) driver.Value {
			return common.TimestampConvertToTime(ts, precision)
		})
	}
	wrapper.TaosFreeResult(res)
	assert.Equal(t, 2, len(data))
	row1 := data[0]
	assert.Equal(t, now.UnixNano()/1e6, row1[0].(time.Time).UnixNano()/1e6)
	assert.Equal(t, false, row1[1].(bool))
	assert.Equal(t, int8(1), row1[2].(int8))
	assert.Equal(t, int16(1), row1[3].(int16))
	assert.Equal(t, int32(1), row1[4].(int32))
	assert.Equal(t, int64(1), row1[5].(int64))
	assert.Equal(t, uint8(1), row1[6].(uint8))
	assert.Equal(t, uint16(1), row1[7].(uint16))
	assert.Equal(t, uint32(1), row1[8].(uint32))
	assert.Equal(t, uint64(1), row1[9].(uint64))
	assert.Equal(t, float32(1), row1[10].(float32))
	assert.Equal(t, float64(1), row1[11].(float64))
	assert.Equal(t, "test_binary", row1[12].(string))
	assert.Equal(t, "test_nchar", row1[13].(string))
	assert.Equal(t, []byte(`{"a":1}`), row1[14].([]byte))
	row2 := data[1]
	assert.Equal(t, after1s.UnixNano()/1e6, row2[0].(time.Time).UnixNano()/1e6)
	for i := 1; i < 14; i++ {
		assert.Nil(t, row2[i])
	}
	assert.Equal(t, []byte(`{"a":1}`), row2[14].([]byte))
}

// @author: xftan
// @date: 2023/10/13 11:18
// @description: test parse block
func TestParseBlock(t *testing.T) {
	conn, err := wrapper.TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}

	defer wrapper.TaosClose(conn)
	res := wrapper.TaosQuery(conn, "drop database if exists parse_block")
	code := wrapper.TaosError(res)
	if code != 0 {
		errStr := wrapper.TaosErrorStr(res)
		wrapper.TaosFreeResult(res)
		t.Error(errors.NewError(code, errStr))
		return
	}
	wrapper.TaosFreeResult(res)
	defer func() {
		res = wrapper.TaosQuery(conn, "drop database if exists parse_block")
		code = wrapper.TaosError(res)
		if code != 0 {
			errStr := wrapper.TaosErrorStr(res)
			wrapper.TaosFreeResult(res)
			t.Error(errors.NewError(code, errStr))
			return
		}
		wrapper.TaosFreeResult(res)
	}()
	res = wrapper.TaosQuery(conn, "create database parse_block vgroups 1")
	code = wrapper.TaosError(res)
	if code != 0 {
		errStr := wrapper.TaosErrorStr(res)
		wrapper.TaosFreeResult(res)
		t.Error(errors.NewError(code, errStr))
		return
	}
	wrapper.TaosFreeResult(res)

	res = wrapper.TaosQuery(conn, "create table if not exists parse_block.all_type (ts timestamp,"+
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
		"c13 nchar(20),"+
		"c14 varbinary(20),"+
		"c15 geometry(100)"+
		") tags (info json)")
	code = wrapper.TaosError(res)
	if code != 0 {
		errStr := wrapper.TaosErrorStr(res)
		wrapper.TaosFreeResult(res)
		t.Error(errors.NewError(code, errStr))
		return
	}
	wrapper.TaosFreeResult(res)
	now := time.Now()
	after1s := now.Add(time.Second)
	sql := fmt.Sprintf("insert into parse_block.t0 using parse_block.all_type tags('{\"a\":1}') "+
		"values('%s',1,1,1,1,1,1,1,1,1,1,1,'test_binary','test_nchar','test_varbinary','POINT(100 100)')"+
		"('%s',null,null,null,null,null,null,null,null,null,null,null,null,null,null,null)", now.Format(time.RFC3339Nano), after1s.Format(time.RFC3339Nano))
	res = wrapper.TaosQuery(conn, sql)
	code = wrapper.TaosError(res)
	if code != 0 {
		errStr := wrapper.TaosErrorStr(res)
		wrapper.TaosFreeResult(res)
		t.Error(errors.NewError(code, errStr))
		return
	}
	wrapper.TaosFreeResult(res)

	sql = "select * from parse_block.all_type"
	res = wrapper.TaosQuery(conn, sql)
	code = wrapper.TaosError(res)
	if code != 0 {
		errStr := wrapper.TaosErrorStr(res)
		wrapper.TaosFreeResult(res)
		t.Error(errors.NewError(code, errStr))
		return
	}
	precision := wrapper.TaosResultPrecision(res)
	var data [][]driver.Value
	for {
		blockSize, errCode, block := wrapper.TaosFetchRawBlock(res)
		if errCode != int(errors.SUCCESS) {
			errStr := wrapper.TaosErrorStr(res)
			err := errors.NewError(code, errStr)
			t.Error(err)
			wrapper.TaosFreeResult(res)
			return
		}
		if blockSize == 0 {
			break
		}
		version := RawBlockGetVersion(block)
		t.Log(version)
		length := RawBlockGetLength(block)
		if version >= 3 {
			assert.Equal(t, int32(459), length)
		} else {
			assert.Equal(t, int32(447), length)
		}
		rows := RawBlockGetNumOfRows(block)
		assert.Equal(t, int32(2), rows)
		columns := RawBlockGetNumOfCols(block)
		assert.Equal(t, int32(17), columns)
		hasColumnSegment := RawBlockGetHasColumnSegment(block)
		assert.Equal(t, int32(-2147483648), hasColumnSegment)
		groupId := RawBlockGetGroupID(block)
		assert.Equal(t, uint64(0), groupId)
		infos := make([]RawBlockColInfo, columns)
		RawBlockGetColInfo(block, infos)
		if version >= 3 {
			assert.Equal(
				t,
				[]RawBlockColInfo{
					{
						ColType: 9,
						Bytes:   8,
					},
					{
						ColType: 1,
						Bytes:   1,
					},
					{
						ColType: 2,
						Bytes:   1,
					},
					{
						ColType: 3,
						Bytes:   2,
					},
					{
						ColType: 4,
						Bytes:   4,
					},
					{
						ColType: 5,
						Bytes:   8,
					},
					{
						ColType: 11,
						Bytes:   1,
					},
					{
						ColType: 12,
						Bytes:   2,
					},
					{
						ColType: 13,
						Bytes:   4,
					},
					{
						ColType: 14,
						Bytes:   8,
					},
					{
						ColType: 6,
						Bytes:   4,
					},
					{
						ColType: 7,
						Bytes:   8,
					},
					{
						ColType: 8,
						Bytes:   24,
					},
					{
						ColType: 10,
						Bytes:   84,
					},
					{
						ColType: 16,
						Bytes:   24,
					},
					{
						ColType: 20,
						Bytes:   104,
					},
					{
						ColType: 15,
						Bytes:   16384,
					},
				},
				infos,
			)
		} else {
			assert.Equal(
				t,
				[]RawBlockColInfo{
					{
						ColType: 9,
						Bytes:   8,
					},
					{
						ColType: 1,
						Bytes:   1,
					},
					{
						ColType: 2,
						Bytes:   1,
					},
					{
						ColType: 3,
						Bytes:   2,
					},
					{
						ColType: 4,
						Bytes:   4,
					},
					{
						ColType: 5,
						Bytes:   8,
					},
					{
						ColType: 11,
						Bytes:   1,
					},
					{
						ColType: 12,
						Bytes:   2,
					},
					{
						ColType: 13,
						Bytes:   4,
					},
					{
						ColType: 14,
						Bytes:   8,
					},
					{
						ColType: 6,
						Bytes:   4,
					},
					{
						ColType: 7,
						Bytes:   8,
					},
					{
						ColType: 8,
						Bytes:   22,
					},
					{
						ColType: 10,
						Bytes:   82,
					},
					{
						ColType: 16,
						Bytes:   22,
					},
					{
						ColType: 20,
						Bytes:   102,
					},
					{
						ColType: 15,
						Bytes:   16384,
					},
				},
				infos,
			)
		}
		d := ReadBlockSimple(block, precision)
		data = append(data, d...)
	}
	wrapper.TaosFreeResult(res)
	assert.Equal(t, 2, len(data))
	row1 := data[0]
	assert.Equal(t, now.UnixNano()/1e6, row1[0].(time.Time).UnixNano()/1e6)
	assert.Equal(t, true, row1[1].(bool))
	assert.Equal(t, int8(1), row1[2].(int8))
	assert.Equal(t, int16(1), row1[3].(int16))
	assert.Equal(t, int32(1), row1[4].(int32))
	assert.Equal(t, int64(1), row1[5].(int64))
	assert.Equal(t, uint8(1), row1[6].(uint8))
	assert.Equal(t, uint16(1), row1[7].(uint16))
	assert.Equal(t, uint32(1), row1[8].(uint32))
	assert.Equal(t, uint64(1), row1[9].(uint64))
	assert.Equal(t, float32(1), row1[10].(float32))
	assert.Equal(t, float64(1), row1[11].(float64))
	assert.Equal(t, "test_binary", row1[12].(string))
	assert.Equal(t, "test_nchar", row1[13].(string))
	assert.Equal(t, []byte("test_varbinary"), row1[14].([]byte))
	assert.Equal(t, []byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40}, row1[15].([]byte))
	assert.Equal(t, []byte(`{"a":1}`), row1[16].([]byte))
	row2 := data[1]
	assert.Equal(t, after1s.UnixNano()/1e6, row2[0].(time.Time).UnixNano()/1e6)
	for i := 1; i < 16; i++ {
		assert.Nil(t, row2[i])
	}
	assert.Equal(t, []byte(`{"a":1}`), row2[16].([]byte))
}

func TestParseDifferentVersion(t *testing.T) {
	v1Data := []byte{
		0x01, 0x00, 0x00, 0x00, //version
		0xf8, 0x00, 0x00, 0x00, //length
		0x01, 0x00, 0x00, 0x00, //rows
		0x0e, 0x00, 0x00, 0x00, //columns
		0x00, 0x00, 0x00, 0x00, //flagSegment
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, //groupID
		//types
		0x09, 0x08, 0x00, 0x00, 0x00, //1
		0x01, 0x01, 0x00, 0x00, 0x00, //2
		0x02, 0x01, 0x00, 0x00, 0x00, //3
		0x03, 0x02, 0x00, 0x00, 0x00, //4
		0x04, 0x04, 0x00, 0x00, 0x00, //5
		0x05, 0x08, 0x00, 0x00, 0x00, //6
		0x0b, 0x01, 0x00, 0x00, 0x00, //7
		0x0c, 0x02, 0x00, 0x00, 0x00, //8
		0x0d, 0x04, 0x00, 0x00, 0x00, //9
		0x0e, 0x08, 0x00, 0x00, 0x00, //10
		0x06, 0x04, 0x00, 0x00, 0x00, //11
		0x07, 0x08, 0x00, 0x00, 0x00, //12
		0x08, 0x00, 0x00, 0x00, 0x00, //13
		0x0a, 0x00, 0x00, 0x00, 0x00, //14
		//lengths
		0x08, 0x00, 0x00, 0x00,
		0x01, 0x00, 0x00, 0x00,
		0x01, 0x00, 0x00, 0x00,
		0x02, 0x00, 0x00, 0x00,
		0x04, 0x00, 0x00, 0x00,
		0x08, 0x00, 0x00, 0x00,
		0x01, 0x00, 0x00, 0x00,
		0x02, 0x00, 0x00, 0x00,
		0x04, 0x00, 0x00, 0x00,
		0x08, 0x00, 0x00, 0x00,
		0x04, 0x00, 0x00, 0x00,
		0x08, 0x00, 0x00, 0x00,
		0x05, 0x00, 0x00, 0x00,
		0x12, 0x00, 0x00, 0x00,
		0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, //ts
		0x00,
		0x01, //bool
		0x00,
		0x7f, //i8
		0x00,
		0xff, 0x7f, //i16
		0x00,
		0xff, 0xff, 0xff, 0x7f, //i32
		0x00,
		0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f, //i64
		0x00,
		0xff, //u8
		0x00,
		0xff, 0xff, //u16
		0x00,
		0xff, 0xff, 0xff, 0xff, //u32
		0x00,
		0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, //u64
		0x00,
		0xff, 0xff, 0x7f, 0x7f, //f32
		0x00,
		0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xef, 0x7f, //f64
		0x00, 0x00, 0x00, 0x00,
		0x03, 0x00, //binary
		0x41, 0x42, 0x43,
		0x00, 0x00, 0x00, 0x00,
		0x10, 0x00, //nchar
		0x9b, 0x6d, 0x00, 0x00, 0x1d, 0x60, 0x00, 0x00, 0x70, 0x65, 0x00, 0x00, 0x6e, 0x63, 0x00, 0x00,
	}
	v3Data := []byte{
		0x03, 0x00, 0x00, 0x00, //version
		0xfc, 0x00, 0x00, 0x00, //length
		0x01, 0x00, 0x00, 0x00, //rows
		0x0e, 0x00, 0x00, 0x00, //columns
		0x00, 0x00, 0x00, 0x00, //flagSegment
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, //groupID
		//types
		0x09, 0x08, 0x00, 0x00, 0x00, //1
		0x01, 0x01, 0x00, 0x00, 0x00, //2
		0x02, 0x01, 0x00, 0x00, 0x00, //3
		0x03, 0x02, 0x00, 0x00, 0x00, //4
		0x04, 0x04, 0x00, 0x00, 0x00, //5
		0x05, 0x08, 0x00, 0x00, 0x00, //6
		0x0b, 0x01, 0x00, 0x00, 0x00, //7
		0x0c, 0x02, 0x00, 0x00, 0x00, //8
		0x0d, 0x04, 0x00, 0x00, 0x00, //9
		0x0e, 0x08, 0x00, 0x00, 0x00, //10
		0x06, 0x04, 0x00, 0x00, 0x00, //11
		0x07, 0x08, 0x00, 0x00, 0x00, //12
		0x08, 0x00, 0x00, 0x00, 0x00, //13
		0x0a, 0x00, 0x00, 0x00, 0x00, //14
		//lengths
		0x08, 0x00, 0x00, 0x00,
		0x01, 0x00, 0x00, 0x00,
		0x01, 0x00, 0x00, 0x00,
		0x02, 0x00, 0x00, 0x00,
		0x04, 0x00, 0x00, 0x00,
		0x08, 0x00, 0x00, 0x00,
		0x01, 0x00, 0x00, 0x00,
		0x02, 0x00, 0x00, 0x00,
		0x04, 0x00, 0x00, 0x00,
		0x08, 0x00, 0x00, 0x00,
		0x04, 0x00, 0x00, 0x00,
		0x08, 0x00, 0x00, 0x00,
		0x07, 0x00, 0x00, 0x00,
		0x14, 0x00, 0x00, 0x00,
		0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, //ts
		0x00,
		0x01, //bool
		0x00,
		0x7f, //i8
		0x00,
		0xff, 0x7f, //i16
		0x00,
		0xff, 0xff, 0xff, 0x7f, //i32
		0x00,
		0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f, //i64
		0x00,
		0xff, //u8
		0x00,
		0xff, 0xff, //u16
		0x00,
		0xff, 0xff, 0xff, 0xff, //u32
		0x00,
		0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, //u64
		0x00,
		0xff, 0xff, 0x7f, 0x7f, //f32
		0x00,
		0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xef, 0x7f, //f64
		0x00, 0x00, 0x00, 0x00,
		0x03, 0x00, 0x00, 0x00, //binary
		0x41, 0x42, 0x43,
		0x00, 0x00, 0x00, 0x00,
		0x10, 0x00, 0x00, 0x00, //nchar
		0x9b, 0x6d, 0x00, 0x00, 0x1d, 0x60, 0x00, 0x00, 0x70, 0x65, 0x00, 0x00, 0x6e, 0x63, 0x00, 0x00,
	}
	expect := [][]driver.Value{
		{
			time.Unix(0, 0),
			true,
			int8(127),
			int16(32767),
			int32(2147483647),
			int64(9223372036854775807),
			uint8(255),
			uint16(65535),
			uint32(4294967295),
			uint64(18446744073709551615),
			float32(math.MaxFloat32),
			float64(math.MaxFloat64),
			"ABC",
			"涛思数据",
		},
	}
	tests := []struct {
		name string
		args []byte
		want [][]driver.Value
	}{
		{
			name: "version 1",
			args: v1Data,
			want: expect,
		},
		{
			name: "version 3",
			args: v3Data,
			want: expect,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := ReadBlockSimple(unsafe.Pointer(&tt.args[0]), 0)
			assert.Equal(t, tt.want, data)
		})
	}
}
