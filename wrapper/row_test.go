package wrapper

import (
	"database/sql/driver"
	"fmt"
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
	res = TaosQuery(conn, "create stable if not exists test_json.tjsonr(ts timestamp,value int )tags(t json)")
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
		lengths := FetchLengths(res, numFields)
		for j := range headers.ColTypes {
			d = append(d, FetchRow(row, j, headers.ColTypes[j], lengths[j], precision))
		}
		data = append(data, d)
	}
	TaosFreeResult(res)
	assert.Equal(t, `{"a":1,"b":"b"}`, string(data[0][2].([]byte)))
	assert.Equal(t, `{"a":1,"c":"c"}`, string(data[1][2].([]byte)))
	assert.Nil(t, data[2][2])
}

func TestFetchRow(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	db := "test_ts_781"
	//create stable stb1 (ts timestamp, name binary(10)) tags(n int);
	//insert into tb1 using stb1 tags(1) values(now, 'log');
	//insert into tb2 using stb1 tags(2) values(now, 'test');
	//insert into tb3 using stb1 tags(3) values(now, 'db02');
	//insert into tb4 using stb1 tags(4) values(now, 'db3');
	res := TaosQuery(conn, "create database if not exists "+db)
	code := TaosError(res)
	if code != int(errors.SUCCESS) {
		errStr := TaosErrorStr(res)
		err := errors.NewError(code, errStr)
		t.Error(err)
		TaosFreeResult(res)
		return
	}
	TaosFreeResult(res)
	res = TaosQuery(conn, fmt.Sprintf("create stable if not exists %s.stb1 (ts timestamp, name binary(10)) tags(n int);", db))
	code = TaosError(res)
	if code != int(errors.SUCCESS) {
		errStr := TaosErrorStr(res)
		err := errors.NewError(code, errStr)
		t.Error(err)
		TaosFreeResult(res)
		return
	}
	TaosFreeResult(res)
	res = TaosQuery(conn, fmt.Sprintf("insert into %s.tb1 using %s.stb1 tags(1) values(now, 'log');", db, db))
	code = TaosError(res)
	if code != int(errors.SUCCESS) {
		errStr := TaosErrorStr(res)
		err := errors.NewError(code, errStr)
		t.Error(err)
		TaosFreeResult(res)
		return
	}
	TaosFreeResult(res)
	res = TaosQuery(conn, fmt.Sprintf("insert into %s.tb2 using %s.stb1 tags(2) values(now, 'test');", db, db))
	code = TaosError(res)
	if code != int(errors.SUCCESS) {
		errStr := TaosErrorStr(res)
		err := errors.NewError(code, errStr)
		t.Error(err)
		TaosFreeResult(res)
		return
	}
	TaosFreeResult(res)
	res = TaosQuery(conn, fmt.Sprintf("insert into %s.tb3 using %s.stb1 tags(3) values(now, 'db02')", db, db))
	code = TaosError(res)
	if code != int(errors.SUCCESS) {
		errStr := TaosErrorStr(res)
		err := errors.NewError(code, errStr)
		t.Error(err)
		TaosFreeResult(res)
		return
	}
	TaosFreeResult(res)
	res = TaosQuery(conn, fmt.Sprintf("insert into %s.tb4 using %s.stb1 tags(4) values(now, 'db3');", db, db))
	code = TaosError(res)
	if code != int(errors.SUCCESS) {
		errStr := TaosErrorStr(res)
		err := errors.NewError(code, errStr)
		t.Error(err)
		TaosFreeResult(res)
		return
	}
	TaosFreeResult(res)
	res = TaosQuery(conn, fmt.Sprintf("select distinct(name) from %s.stb1;", db))
	code = TaosError(res)
	if code != int(errors.SUCCESS) {
		errStr := TaosErrorStr(res)
		err := errors.NewError(code, errStr)
		t.Error(err)
		TaosFreeResult(res)
		return
	}
	numFields := TaosFieldCount(res)
	header, err := ReadColumn(res, numFields)
	if err != nil {
		TaosFreeResult(res)
		t.Error(err)
		return
	}
	names := map[string]struct{}{
		"log":  {},
		"test": {},
		"db02": {},
		"db3":  {},
	}
	for {
		rr := TaosFetchRow(res)
		lengths := FetchLengths(res, numFields)
		if rr == nil {
			break
		}
		d := FetchRow(rr, 0, header.ColTypes[0], lengths[0])
		delete(names, d.(string))
	}
	TaosFreeResult(res)
	TaosClose(conn)
	assert.Empty(t, names)
}

func TestFetchRowNchar(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	db := "test_ts_781_nchar"
	//create stable stb1 (ts timestamp, name nchar(10)) tags(n int);
	//insert into tb1 using stb1 tags(1) values(now, 'log');
	//insert into tb2 using stb1 tags(2) values(now, 'test');
	//insert into tb3 using stb1 tags(3) values(now, 'db02');
	//insert into tb4 using stb1 tags(4) values(now, 'db3');
	res := TaosQuery(conn, "create database if not exists "+db)
	code := TaosError(res)
	if code != int(errors.SUCCESS) {
		errStr := TaosErrorStr(res)
		err := errors.NewError(code, errStr)
		t.Error(err)
		TaosFreeResult(res)
		return
	}
	TaosFreeResult(res)
	res = TaosQuery(conn, fmt.Sprintf("create stable if not exists %s.stb1 (ts timestamp, name nchar(10)) tags(n int);", db))
	code = TaosError(res)
	if code != int(errors.SUCCESS) {
		errStr := TaosErrorStr(res)
		err := errors.NewError(code, errStr)
		t.Error(err)
		TaosFreeResult(res)
		return
	}
	TaosFreeResult(res)
	res = TaosQuery(conn, fmt.Sprintf("insert into %s.tb1 using %s.stb1 tags(1) values(now, 'log');", db, db))
	code = TaosError(res)
	if code != int(errors.SUCCESS) {
		errStr := TaosErrorStr(res)
		err := errors.NewError(code, errStr)
		t.Error(err)
		TaosFreeResult(res)
		return
	}
	TaosFreeResult(res)
	res = TaosQuery(conn, fmt.Sprintf("insert into %s.tb2 using %s.stb1 tags(2) values(now, 'test');", db, db))
	code = TaosError(res)
	if code != int(errors.SUCCESS) {
		errStr := TaosErrorStr(res)
		err := errors.NewError(code, errStr)
		t.Error(err)
		TaosFreeResult(res)
		return
	}
	TaosFreeResult(res)
	res = TaosQuery(conn, fmt.Sprintf("insert into %s.tb3 using %s.stb1 tags(3) values(now, 'db02')", db, db))
	code = TaosError(res)
	if code != int(errors.SUCCESS) {
		errStr := TaosErrorStr(res)
		err := errors.NewError(code, errStr)
		t.Error(err)
		TaosFreeResult(res)
		return
	}
	TaosFreeResult(res)
	res = TaosQuery(conn, fmt.Sprintf("insert into %s.tb4 using %s.stb1 tags(4) values(now, 'db3');", db, db))
	code = TaosError(res)
	if code != int(errors.SUCCESS) {
		errStr := TaosErrorStr(res)
		err := errors.NewError(code, errStr)
		t.Error(err)
		TaosFreeResult(res)
		return
	}
	TaosFreeResult(res)
	res = TaosQuery(conn, fmt.Sprintf("select distinct(name) from %s.stb1;", db))
	code = TaosError(res)
	if code != int(errors.SUCCESS) {
		errStr := TaosErrorStr(res)
		err := errors.NewError(code, errStr)
		t.Error(err)
		TaosFreeResult(res)
		return
	}
	numFields := TaosFieldCount(res)
	header, err := ReadColumn(res, numFields)
	if err != nil {
		TaosFreeResult(res)
		t.Error(err)
		return
	}
	names := map[string]struct{}{
		"log":  {},
		"test": {},
		"db02": {},
		"db3":  {},
	}
	for {
		rr := TaosFetchRow(res)
		lengths := FetchLengths(res, numFields)
		if rr == nil {
			break
		}
		d := FetchRow(rr, 0, header.ColTypes[0], lengths[0])
		delete(names, d.(string))
	}
	TaosFreeResult(res)
	TaosClose(conn)
	assert.Empty(t, names)
}
