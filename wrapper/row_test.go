package wrapper

import (
	"database/sql/driver"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/taosdata/driver-go/v2/errors"
)

func TestFetchRowJSON(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer TaosClose(conn)
	res := TaosQuery(conn, "create database if not exists test_json")
	code := TaosError(res)
	if code != 0 {
		errStr := TaosErrorStr(res)
		TaosFreeResult(res)
		t.Error(&errors.TaosError{
			Code:   int32(code) & 0xffff,
			ErrStr: errStr,
		})
		return
	}
	TaosFreeResult(res)
	res = TaosQuery(conn, "drop table if exists test_json.tjsonr")
	code = TaosError(res)
	if code != 0 {
		errStr := TaosErrorStr(res)
		TaosFreeResult(res)
		t.Error(&errors.TaosError{
			Code:   int32(code) & 0xffff,
			ErrStr: errStr,
		})
		return
	}
	TaosFreeResult(res)
	res = TaosQuery(conn, "create stable if not exists test_json.tjsonr(ts timestamp,value int )tags(t json(14))")
	code = TaosError(res)
	if code != 0 {
		errStr := TaosErrorStr(res)
		TaosFreeResult(res)
		t.Error(&errors.TaosError{
			Code:   int32(code) & 0xffff,
			ErrStr: errStr,
		})
		return
	}
	TaosFreeResult(res)
	res = TaosQuery(conn, `insert into test_json.tjr_1 using test_json.tjsonr tags('{"a":1,"b":"b"}')values (now,1)`)
	code = TaosError(res)
	if code != 0 {
		errStr := TaosErrorStr(res)
		TaosFreeResult(res)
		t.Error(&errors.TaosError{
			Code:   int32(code) & 0xffff,
			ErrStr: errStr,
		})
		return
	}
	TaosFreeResult(res)
	res = TaosQuery(conn, `insert into test_json.tjr_2 using test_json.tjsonr tags('{"a":1,"c":"c"}')values (now,1)`)
	code = TaosError(res)
	if code != 0 {
		errStr := TaosErrorStr(res)
		TaosFreeResult(res)
		t.Error(&errors.TaosError{
			Code:   int32(code) & 0xffff,
			ErrStr: errStr,
		})
		return
	}
	TaosFreeResult(res)
	res = TaosQuery(conn, `insert into test_json.tjr_3 using test_json.tjsonr tags('null')values (now,1)`)
	code = TaosError(res)
	if code != 0 {
		errStr := TaosErrorStr(res)
		TaosFreeResult(res)
		t.Error(&errors.TaosError{
			Code:   int32(code) & 0xffff,
			ErrStr: errStr,
		})
		return
	}
	TaosFreeResult(res)

	res = TaosQuery(conn, `select * from test_json.tjsonr`)
	code = TaosError(res)
	if code != 0 {
		errStr := TaosErrorStr(res)
		TaosFreeResult(res)
		t.Error(&errors.TaosError{
			Code:   int32(code) & 0xffff,
			ErrStr: errStr,
		})
		return
	}
	numFields := TaosFieldCount(res)
	precision := TaosResultPrecision(res)
	assert.Equal(t, 3, numFields)
	headers, err := ReadColumn(res, numFields)
	assert.NoError(t, err)
	var data [][]driver.Value
	for i := 0; i < 3; i++ {
		var d []driver.Value
		row := TaosFetchRow(res)
		for j := range headers.ColTypes {
			d = append(d, FetchRow(row, j, headers.ColTypes[j], precision))
		}
		data = append(data, d)
	}
	TaosFreeResult(res)
	assert.Equal(t, `{"a":1,"b":"b"}`, string(data[0][2].([]byte)))
	assert.Equal(t, `{"a":1,"c":"c"}`, string(data[1][2].([]byte)))
	assert.Nil(t, data[2][2])
}
