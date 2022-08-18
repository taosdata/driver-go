package wrapper

import (
	"database/sql/driver"
	"fmt"
	"math"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/errors"
)

// @author: xftan
// @date: 2022/4/16 15:12
// @description: test for read raw block
func TestReadBlock(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}

	defer TaosClose(conn)
	res := TaosQuery(conn, "drop database if exists test_block_raw")
	code := TaosError(res)
	if code != 0 {
		errStr := TaosErrorStr(res)
		TaosFreeResult(res)
		t.Error(errors.NewError(code, errStr))
		return
	}
	TaosFreeResult(res)
	defer func() {
		res = TaosQuery(conn, "drop database if exists test_block_raw")
		code = TaosError(res)
		if code != 0 {
			errStr := TaosErrorStr(res)
			TaosFreeResult(res)
			t.Error(errors.NewError(code, errStr))
			return
		}
		TaosFreeResult(res)
	}()
	res = TaosQuery(conn, "create database test_block_raw")
	code = TaosError(res)
	if code != 0 {
		errStr := TaosErrorStr(res)
		TaosFreeResult(res)
		t.Error(errors.NewError(code, errStr))
		return
	}
	TaosFreeResult(res)

	res = TaosQuery(conn, "create table if not exists test_block_raw.all_type (ts timestamp,"+
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
	code = TaosError(res)
	if code != 0 {
		errStr := TaosErrorStr(res)
		TaosFreeResult(res)
		t.Error(errors.NewError(code, errStr))
		return
	}
	TaosFreeResult(res)
	now := time.Now()
	after1s := now.Add(time.Second)
	after2s := now.Add(2 * time.Second)
	sql := fmt.Sprintf("insert into test_block_raw.t0 using test_block_raw.all_type tags('{\"a\":1}') values"+
		"('%s',1,1,1,1,1,1,1,1,1,1,1,'test_binary','test_nchar')"+
		"('%s',null,null,null,null,null,null,null,null,null,null,null,null,null)"+
		"('%s',true,%d,%d,%d,%d,%d,%d,%d,%v,%f,%f,'b','n')",
		now.Format(time.RFC3339Nano),
		after1s.Format(time.RFC3339Nano),
		after2s.Format(time.RFC3339Nano),
		math.MaxInt8,
		math.MaxInt16,
		math.MaxInt32,
		math.MaxInt64,
		math.MaxUint8,
		math.MaxUint16,
		math.MaxUint32,
		uint64(math.MaxUint64),
		math.MaxFloat32,
		math.MaxFloat64,
	)
	res = TaosQuery(conn, sql)
	code = TaosError(res)
	if code != 0 {
		errStr := TaosErrorStr(res)
		TaosFreeResult(res)
		t.Error(errors.NewError(code, errStr))
		return
	}
	TaosFreeResult(res)

	sql = "select * from test_block_raw.all_type"
	res = TaosQuery(conn, sql)
	code = TaosError(res)
	if code != 0 {
		errStr := TaosErrorStr(res)
		TaosFreeResult(res)
		t.Error(errors.NewError(code, errStr))
		return
	}
	fileCount := TaosNumFields(res)
	rh, err := ReadColumn(res, fileCount)
	if err != nil {
		t.Error(err)
		return
	}
	precision := TaosResultPrecision(res)
	var data [][]driver.Value
	for {
		blockSize, errCode, block := TaosFetchRawBlock(res)
		if errCode != int(errors.SUCCESS) {
			errStr := TaosErrorStr(res)
			err := errors.NewError(code, errStr)
			t.Error(err)
			TaosFreeResult(res)
			return
		}
		if blockSize == 0 {
			break
		}
		d := ReadBlock(block, blockSize, rh.ColTypes, precision)
		data = append(data, d...)
	}
	TaosFreeResult(res)
	assert.Equal(t, 3, len(data))
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
	row3 := data[2]
	assert.Equal(t, after2s.UnixNano()/1e6, row3[0].(time.Time).UnixNano()/1e6)
	assert.Equal(t, true, row3[1].(bool))
	assert.Equal(t, int8(math.MaxInt8), row3[2].(int8))
	assert.Equal(t, int16(math.MaxInt16), row3[3].(int16))
	assert.Equal(t, int32(math.MaxInt32), row3[4].(int32))
	assert.Equal(t, int64(math.MaxInt64), row3[5].(int64))
	assert.Equal(t, uint8(math.MaxUint8), row3[6].(uint8))
	assert.Equal(t, uint16(math.MaxUint16), row3[7].(uint16))
	assert.Equal(t, uint32(math.MaxUint32), row3[8].(uint32))
	assert.Equal(t, uint64(math.MaxUint64), row3[9].(uint64))
	assert.Equal(t, float32(math.MaxFloat32), row3[10].(float32))
	assert.Equal(t, float64(math.MaxFloat64), row3[11].(float64))
	assert.Equal(t, "b", row3[12].(string))
	assert.Equal(t, "n", row3[13].(string))
	assert.Equal(t, []byte(`{"a":1}`), row3[14].([]byte))
}

func TestReadBlock2(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}

	defer TaosClose(conn)
	defer func() {
		res := TaosQuery(conn, "drop database if exists test_block_raw")
		code := TaosError(res)
		if code != 0 {
			errStr := TaosErrorStr(res)
			TaosFreeResult(res)
			t.Error(errors.NewError(code, errStr))
			return
		}
		TaosFreeResult(res)
	}()
	res := TaosQuery(conn, "create database if not exists test_block_raw")
	code := TaosError(res)
	if code != 0 {
		errStr := TaosErrorStr(res)
		TaosFreeResult(res)
		t.Error(errors.NewError(code, errStr))
		return
	}
	TaosFreeResult(res)
	res = TaosQuery(conn, "drop table if exists test_block_raw.all_type2")
	code = TaosError(res)
	if code != 0 {
		errStr := TaosErrorStr(res)
		TaosFreeResult(res)
		t.Error(errors.NewError(code, errStr))
		return
	}
	TaosFreeResult(res)
	res = TaosQuery(conn, "create table if not exists test_block_raw.all_type2 (ts timestamp,"+
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
	code = TaosError(res)
	if code != 0 {
		errStr := TaosErrorStr(res)
		TaosFreeResult(res)
		t.Error(errors.NewError(code, errStr))
		return
	}
	TaosFreeResult(res)
	now := time.Now()
	after1s := now.Add(time.Second)
	sql := fmt.Sprintf("insert into test_block_raw.all_type2 values('%s',1,1,1,1,1,1,1,1,1,1,1,'test_binary','test_nchar')('%s',null,null,null,null,null,null,null,null,null,null,null,null,null)", now.Format(time.RFC3339Nano), after1s.Format(time.RFC3339Nano))
	res = TaosQuery(conn, sql)
	code = TaosError(res)
	if code != 0 {
		errStr := TaosErrorStr(res)
		TaosFreeResult(res)
		t.Error(errors.NewError(code, errStr))
		return
	}
	TaosFreeResult(res)

	sql = "select * from test_block_raw.all_type2"
	res = TaosQuery(conn, sql)
	code = TaosError(res)
	if code != 0 {
		errStr := TaosErrorStr(res)
		TaosFreeResult(res)
		t.Error(errors.NewError(code, errStr))
		return
	}
	fileCount := TaosNumFields(res)
	rh, err := ReadColumn(res, fileCount)
	if err != nil {
		t.Error(err)
		return
	}
	precision := TaosResultPrecision(res)
	pHeaderList := make([]uintptr, fileCount)
	pStartList := make([]uintptr, fileCount)
	var data [][]driver.Value
	for {
		blockSize, errCode, block := TaosFetchRawBlock(res)
		if errCode != int(errors.SUCCESS) {
			errStr := TaosErrorStr(res)
			err := errors.NewError(code, errStr)
			t.Error(err)
			TaosFreeResult(res)
			return
		}
		if blockSize == 0 {
			break
		}
		nullBitMapOffset := uintptr(BitmapLen(blockSize))
		lengthOffset := RawBlockGetColumnLengthOffset(fileCount)
		tmpPHeader := uintptr(block) + RawBlockGetColDataOffset(fileCount)
		tmpPStart := tmpPHeader
		for column := 0; column < fileCount; column++ {
			colLength := *((*int32)(unsafe.Pointer(uintptr(block) + lengthOffset + uintptr(column)*Int32Size)))
			if IsVarDataType(rh.ColTypes[column]) {
				pHeaderList[column] = tmpPHeader
				tmpPStart = tmpPHeader + Int32Size*uintptr(blockSize)
				pStartList[column] = tmpPStart
			} else {
				pHeaderList[column] = tmpPHeader
				tmpPStart = tmpPHeader + nullBitMapOffset
				pStartList[column] = tmpPStart
			}
			tmpPHeader = tmpPStart + uintptr(colLength)
		}
		for row := 0; row < blockSize; row++ {
			rowV := make([]driver.Value, fileCount)
			for column := 0; column < fileCount; column++ {
				v := ItemRawBlock(rh.ColTypes[column], pHeaderList[column], pStartList[column], row, precision, func(ts int64, precision int) driver.Value {
					return common.TimestampConvertToTime(ts, precision)
				})
				rowV[column] = v
			}
			data = append(data, rowV)
		}
	}
	TaosFreeResult(res)
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

func TestBlockTag(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}

	defer TaosClose(conn)
	defer func() {
		res := TaosQuery(conn, "drop database if exists test_block_abc1")
		code := TaosError(res)
		if code != 0 {
			errStr := TaosErrorStr(res)
			TaosFreeResult(res)
			t.Error(errors.NewError(code, errStr))
			return
		}
		TaosFreeResult(res)
	}()
	res := TaosQuery(conn, "create database if not exists test_block_abc1")
	code := TaosError(res)
	if code != 0 {
		errStr := TaosErrorStr(res)
		TaosFreeResult(res)
		t.Error(errors.NewError(code, errStr))
		return
	}
	TaosFreeResult(res)

	res = TaosQuery(conn, "use test_block_abc1")
	code = TaosError(res)
	if code != 0 {
		errStr := TaosErrorStr(res)
		TaosFreeResult(res)
		t.Error(errors.NewError(code, errStr))
		return
	}
	TaosFreeResult(res)

	res = TaosQuery(conn, "create table if not exists meters(ts timestamp, v int) tags(location varchar(16))")
	code = TaosError(res)
	if code != 0 {
		errStr := TaosErrorStr(res)
		TaosFreeResult(res)
		t.Error(errors.NewError(code, errStr))
		return
	}
	TaosFreeResult(res)

	res = TaosQuery(conn, "create table if not exists tb1 using meters tags('abcd')")
	code = TaosError(res)
	if code != 0 {
		errStr := TaosErrorStr(res)
		TaosFreeResult(res)
		t.Error(errors.NewError(code, errStr))
		return
	}
	TaosFreeResult(res)

	sql := "select distinct tbname,location from meters;"
	res = TaosQuery(conn, sql)
	code = TaosError(res)
	if code != 0 {
		errStr := TaosErrorStr(res)
		TaosFreeResult(res)
		t.Error(errors.NewError(code, errStr))
		return
	}
	fileCount := TaosNumFields(res)
	rh, err := ReadColumn(res, fileCount)
	if err != nil {
		t.Error(err)
		return
	}
	precision := TaosResultPrecision(res)
	pHeaderList := make([]uintptr, fileCount)
	pStartList := make([]uintptr, fileCount)
	var data [][]driver.Value
	for {
		blockSize, errCode, block := TaosFetchRawBlock(res)
		if errCode != int(errors.SUCCESS) {
			errStr := TaosErrorStr(res)
			err := errors.NewError(code, errStr)
			t.Error(err)
			TaosFreeResult(res)
			return
		}
		if blockSize == 0 {
			break
		}
		nullBitMapOffset := uintptr(BitmapLen(blockSize))
		lengthOffset := RawBlockGetColumnLengthOffset(fileCount)
		tmpPHeader := uintptr(block) + RawBlockGetColDataOffset(fileCount) // length i32, group u64
		tmpPStart := tmpPHeader
		for column := 0; column < fileCount; column++ {
			colLength := *((*int32)(unsafe.Pointer(uintptr(block) + lengthOffset + uintptr(column)*Int32Size)))
			if IsVarDataType(rh.ColTypes[column]) {
				pHeaderList[column] = tmpPHeader
				tmpPStart = tmpPHeader + Int32Size*uintptr(blockSize)
				pStartList[column] = tmpPStart
			} else {
				pHeaderList[column] = tmpPHeader
				tmpPStart = tmpPHeader + nullBitMapOffset
				pStartList[column] = tmpPStart
			}
			tmpPHeader = tmpPStart + uintptr(colLength)
		}
		for row := 0; row < blockSize; row++ {
			rowV := make([]driver.Value, fileCount)
			for column := 0; column < fileCount; column++ {
				v := ItemRawBlock(rh.ColTypes[column], pHeaderList[column], pStartList[column], row, precision, func(ts int64, precision int) driver.Value {
					return common.TimestampConvertToTime(ts, precision)
				})
				rowV[column] = v
			}
			data = append(data, rowV)
		}
	}
	TaosFreeResult(res)
	t.Log(data)
	t.Log(len(data[0][1].(string)))
}

func TestReadRow(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}

	defer TaosClose(conn)
	res := TaosQuery(conn, "drop database if exists test_read_row")
	code := TaosError(res)
	if code != 0 {
		errStr := TaosErrorStr(res)
		TaosFreeResult(res)
		t.Error(errors.NewError(code, errStr))
		return
	}
	TaosFreeResult(res)
	defer func() {
		res = TaosQuery(conn, "drop database if exists test_read_row")
		code = TaosError(res)
		if code != 0 {
			errStr := TaosErrorStr(res)
			TaosFreeResult(res)
			t.Error(errors.NewError(code, errStr))
			return
		}
		TaosFreeResult(res)
	}()
	res = TaosQuery(conn, "create database test_read_row")
	code = TaosError(res)
	if code != 0 {
		errStr := TaosErrorStr(res)
		TaosFreeResult(res)
		t.Error(errors.NewError(code, errStr))
		return
	}
	TaosFreeResult(res)

	res = TaosQuery(conn, "create table if not exists test_read_row.all_type (ts timestamp,"+
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
	code = TaosError(res)
	if code != 0 {
		errStr := TaosErrorStr(res)
		TaosFreeResult(res)
		t.Error(errors.NewError(code, errStr))
		return
	}
	TaosFreeResult(res)
	now := time.Now()
	after1s := now.Add(time.Second)
	sql := fmt.Sprintf("insert into test_read_row.t0 using test_read_row.all_type tags('{\"a\":1}') values('%s',1,1,1,1,1,1,1,1,1,1,1,'test_binary','test_nchar')('%s',null,null,null,null,null,null,null,null,null,null,null,null,null)", now.Format(time.RFC3339Nano), after1s.Format(time.RFC3339Nano))
	res = TaosQuery(conn, sql)
	code = TaosError(res)
	if code != 0 {
		errStr := TaosErrorStr(res)
		TaosFreeResult(res)
		t.Error(errors.NewError(code, errStr))
		return
	}
	TaosFreeResult(res)

	sql = "select * from test_read_row.all_type"
	res = TaosQuery(conn, sql)
	code = TaosError(res)
	if code != 0 {
		errStr := TaosErrorStr(res)
		TaosFreeResult(res)
		t.Error(errors.NewError(code, errStr))
		return
	}
	fileCount := TaosNumFields(res)
	rh, err := ReadColumn(res, fileCount)
	if err != nil {
		t.Error(err)
		return
	}
	precision := TaosResultPrecision(res)
	var data [][]driver.Value
	for {
		blockSize, errCode, block := TaosFetchRawBlock(res)
		if errCode != int(errors.SUCCESS) {
			errStr := TaosErrorStr(res)
			err := errors.NewError(code, errStr)
			t.Error(err)
			TaosFreeResult(res)
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
	TaosFreeResult(res)
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

func TestReadBlockWithTimeFormat(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}

	defer TaosClose(conn)
	res := TaosQuery(conn, "drop database if exists test_read_block_tf")
	code := TaosError(res)
	if code != 0 {
		errStr := TaosErrorStr(res)
		TaosFreeResult(res)
		t.Error(errors.NewError(code, errStr))
		return
	}
	TaosFreeResult(res)
	defer func() {
		res = TaosQuery(conn, "drop database if exists test_read_block_tf")
		code = TaosError(res)
		if code != 0 {
			errStr := TaosErrorStr(res)
			TaosFreeResult(res)
			t.Error(errors.NewError(code, errStr))
			return
		}
		TaosFreeResult(res)
	}()
	res = TaosQuery(conn, "create database test_read_block_tf")
	code = TaosError(res)
	if code != 0 {
		errStr := TaosErrorStr(res)
		TaosFreeResult(res)
		t.Error(errors.NewError(code, errStr))
		return
	}
	TaosFreeResult(res)

	res = TaosQuery(conn, "create table if not exists test_read_block_tf.all_type (ts timestamp,"+
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
	code = TaosError(res)
	if code != 0 {
		errStr := TaosErrorStr(res)
		TaosFreeResult(res)
		t.Error(errors.NewError(code, errStr))
		return
	}
	TaosFreeResult(res)
	now := time.Now()
	after1s := now.Add(time.Second)
	sql := fmt.Sprintf("insert into test_read_block_tf.t0 using test_read_block_tf.all_type tags('{\"a\":1}') values('%s',false,1,1,1,1,1,1,1,1,1,1,'test_binary','test_nchar')('%s',null,null,null,null,null,null,null,null,null,null,null,null,null)", now.Format(time.RFC3339Nano), after1s.Format(time.RFC3339Nano))
	res = TaosQuery(conn, sql)
	code = TaosError(res)
	if code != 0 {
		errStr := TaosErrorStr(res)
		TaosFreeResult(res)
		t.Error(errors.NewError(code, errStr))
		return
	}
	TaosFreeResult(res)

	sql = "select * from test_read_block_tf.all_type"
	res = TaosQuery(conn, sql)
	code = TaosError(res)
	if code != 0 {
		errStr := TaosErrorStr(res)
		TaosFreeResult(res)
		t.Error(errors.NewError(code, errStr))
		return
	}
	fileCount := TaosNumFields(res)
	rh, err := ReadColumn(res, fileCount)
	if err != nil {
		t.Error(err)
		return
	}
	precision := TaosResultPrecision(res)
	var data [][]driver.Value
	for {
		blockSize, errCode, block := TaosFetchRawBlock(res)
		if errCode != int(errors.SUCCESS) {
			errStr := TaosErrorStr(res)
			err := errors.NewError(code, errStr)
			t.Error(err)
			TaosFreeResult(res)
			return
		}
		if blockSize == 0 {
			break
		}
		data = ReadBlockWithTimeFormat(block, blockSize, rh.ColTypes, precision, func(ts int64, precision int) driver.Value {
			return common.TimestampConvertToTime(ts, precision)
		})
	}
	TaosFreeResult(res)
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

func TestTaosWriteRawBlock(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}

	defer TaosClose(conn)
	res := TaosQuery(conn, "drop database if exists test_write_block_raw")
	code := TaosError(res)
	if code != 0 {
		errStr := TaosErrorStr(res)
		TaosFreeResult(res)
		t.Error(errors.NewError(code, errStr))
		return
	}
	TaosFreeResult(res)
	defer func() {
		res = TaosQuery(conn, "drop database if exists test_write_block_raw")
		code = TaosError(res)
		if code != 0 {
			errStr := TaosErrorStr(res)
			TaosFreeResult(res)
			t.Error(errors.NewError(code, errStr))
			return
		}
		TaosFreeResult(res)
	}()
	res = TaosQuery(conn, "create database test_write_block_raw")
	code = TaosError(res)
	if code != 0 {
		errStr := TaosErrorStr(res)
		TaosFreeResult(res)
		t.Error(errors.NewError(code, errStr))
		return
	}
	TaosFreeResult(res)

	res = TaosQuery(conn, "create table if not exists test_write_block_raw.all_type (ts timestamp,"+
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
	code = TaosError(res)
	if code != 0 {
		errStr := TaosErrorStr(res)
		TaosFreeResult(res)
		t.Error(errors.NewError(code, errStr))
		return
	}
	TaosFreeResult(res)
	now := time.Now()
	after1s := now.Add(time.Second)
	sql := fmt.Sprintf("insert into test_write_block_raw.t0 using test_write_block_raw.all_type tags('{\"a\":1}') values('%s',1,1,1,1,1,1,1,1,1,1,1,'test_binary','test_nchar')('%s',null,null,null,null,null,null,null,null,null,null,null,null,null)", now.Format(time.RFC3339Nano), after1s.Format(time.RFC3339Nano))
	res = TaosQuery(conn, sql)
	code = TaosError(res)
	if code != 0 {
		errStr := TaosErrorStr(res)
		TaosFreeResult(res)
		t.Error(errors.NewError(code, errStr))
		return
	}
	TaosFreeResult(res)

	sql = fmt.Sprintf("create table test_write_block_raw.t1 using test_write_block_raw.all_type tags('{\"a\":2}')")
	res = TaosQuery(conn, sql)
	code = TaosError(res)
	if code != 0 {
		errStr := TaosErrorStr(res)
		TaosFreeResult(res)
		t.Error(errors.NewError(code, errStr))
		return
	}
	TaosFreeResult(res)

	sql = "use test_write_block_raw"
	res = TaosQuery(conn, sql)
	code = TaosError(res)
	if code != 0 {
		errStr := TaosErrorStr(res)
		TaosFreeResult(res)
		t.Error(errors.NewError(code, errStr))
		return
	}
	TaosFreeResult(res)

	sql = "select * from test_write_block_raw.t0"
	res = TaosQuery(conn, sql)
	code = TaosError(res)
	if code != 0 {
		errStr := TaosErrorStr(res)
		TaosFreeResult(res)
		t.Error(errors.NewError(code, errStr))
		return
	}
	for {
		blockSize, errCode, block := TaosFetchRawBlock(res)
		if errCode != int(errors.SUCCESS) {
			errStr := TaosErrorStr(res)
			err := errors.NewError(errCode, errStr)
			t.Error(err)
			TaosFreeResult(res)
			return
		}
		if blockSize == 0 {
			break
		}

		errCode = TaosWriteRawBlock(conn, blockSize, block, "t1")
		if errCode != int(errors.SUCCESS) {
			errStr := TaosErrorStr(nil)
			err := errors.NewError(errCode, errStr)
			t.Error(err)
			TaosFreeResult(res)
			return
		}
	}
	TaosFreeResult(res)

	sql = "select * from test_write_block_raw.t1"
	res = TaosQuery(conn, sql)
	code = TaosError(res)
	if code != 0 {
		errStr := TaosErrorStr(res)
		TaosFreeResult(res)
		t.Error(errors.NewError(code, errStr))
		return
	}
	fileCount := TaosNumFields(res)
	rh, err := ReadColumn(res, fileCount)
	if err != nil {
		t.Error(err)
		return
	}
	precision := TaosResultPrecision(res)
	var data [][]driver.Value
	for {
		blockSize, errCode, block := TaosFetchRawBlock(res)
		if errCode != int(errors.SUCCESS) {
			errStr := TaosErrorStr(res)
			err := errors.NewError(code, errStr)
			t.Error(err)
			TaosFreeResult(res)
			return
		}
		if blockSize == 0 {
			break
		}
		d := ReadBlock(block, blockSize, rh.ColTypes, precision)
		data = append(data, d...)
	}
	TaosFreeResult(res)

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

func TestParseBlock(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}

	defer TaosClose(conn)
	res := TaosQuery(conn, "drop database if exists parse_block")
	code := TaosError(res)
	if code != 0 {
		errStr := TaosErrorStr(res)
		TaosFreeResult(res)
		t.Error(errors.NewError(code, errStr))
		return
	}
	TaosFreeResult(res)
	defer func() {
		res = TaosQuery(conn, "drop database if exists parse_block")
		code = TaosError(res)
		if code != 0 {
			errStr := TaosErrorStr(res)
			TaosFreeResult(res)
			t.Error(errors.NewError(code, errStr))
			return
		}
		TaosFreeResult(res)
	}()
	res = TaosQuery(conn, "create database parse_block vgroups 1")
	code = TaosError(res)
	if code != 0 {
		errStr := TaosErrorStr(res)
		TaosFreeResult(res)
		t.Error(errors.NewError(code, errStr))
		return
	}
	TaosFreeResult(res)

	res = TaosQuery(conn, "create table if not exists parse_block.all_type (ts timestamp,"+
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
	code = TaosError(res)
	if code != 0 {
		errStr := TaosErrorStr(res)
		TaosFreeResult(res)
		t.Error(errors.NewError(code, errStr))
		return
	}
	TaosFreeResult(res)
	now := time.Now()
	after1s := now.Add(time.Second)
	sql := fmt.Sprintf("insert into parse_block.t0 using parse_block.all_type tags('{\"a\":1}') values('%s',1,1,1,1,1,1,1,1,1,1,1,'test_binary','test_nchar')('%s',null,null,null,null,null,null,null,null,null,null,null,null,null)", now.Format(time.RFC3339Nano), after1s.Format(time.RFC3339Nano))
	res = TaosQuery(conn, sql)
	code = TaosError(res)
	if code != 0 {
		errStr := TaosErrorStr(res)
		TaosFreeResult(res)
		t.Error(errors.NewError(code, errStr))
		return
	}
	TaosFreeResult(res)

	sql = "select * from parse_block.all_type"
	res = TaosQuery(conn, sql)
	code = TaosError(res)
	if code != 0 {
		errStr := TaosErrorStr(res)
		TaosFreeResult(res)
		t.Error(errors.NewError(code, errStr))
		return
	}
	fileCount := TaosNumFields(res)
	rh, err := ReadColumn(res, fileCount)
	if err != nil {
		t.Error(err)
		return
	}
	precision := TaosResultPrecision(res)
	var data [][]driver.Value
	for {
		blockSize, errCode, block := TaosFetchRawBlock(res)
		if errCode != int(errors.SUCCESS) {
			errStr := TaosErrorStr(res)
			err := errors.NewError(code, errStr)
			t.Error(err)
			TaosFreeResult(res)
			return
		}
		if blockSize == 0 {
			break
		}
		version := RawBlockGetVersion(block)
		assert.Equal(t, int32(1), version)
		length := RawBlockGetLength(block)
		assert.Equal(t, int32(374), length)
		rows := RawBlockGetNumOfRows(block)
		assert.Equal(t, int32(2), rows)
		columns := RawBlockGetNumOfCols(block)
		assert.Equal(t, int32(15), columns)
		hasColumnSegment := RawBlockGetHasColumnSegment(block)
		assert.Equal(t, int32(-2147483648), hasColumnSegment)
		groupId := RawBlockGetGroupID(block)
		assert.Equal(t, uint64(0), groupId)
		infos := make([]RawBlockColInfo, columns)
		RawBlockGetColInfo(block, infos)
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
					ColType: 15,
					Bytes:   16384,
				},
			},
			infos,
		)
		d := ReadBlock(block, blockSize, rh.ColTypes, precision)
		data = append(data, d...)
	}
	TaosFreeResult(res)
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
