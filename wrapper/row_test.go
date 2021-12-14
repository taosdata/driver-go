package wrapper

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/taosdata/driver-go/v2/errors"
)

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
