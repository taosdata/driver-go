package wrapper

import (
	"database/sql/driver"
	"fmt"
	"testing"
	"time"
	"unsafe"

	"github.com/taosdata/driver-go/v2/errors"
)

// @author: xftan
// @date: 2022/1/27 17:28
// @description: test subscribe
func TestSubscribe(t *testing.T) {
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
	err = exec(conn, "drop table if exists test_subscribe_wrapper")
	if err != nil {
		t.Error(err)
		return
	}
	err = exec(conn, "create table if not exists test_subscribe_wrapper(ts timestamp, value bool, degress int)")
	if err != nil {
		t.Error(err)
		return
	}
	sql := "select ts, value, degress from test_subscribe_wrapper"
	subscriber := TaosSubscribe(conn, "test_subscribe_wrapper", sql, true, time.Second*1)
	defer TaosUnsubscribe(subscriber, true)
	consume := func() int {
		fmt.Println(uintptr(subscriber))
		result := TaosConsume(subscriber)
		code := TaosError(result)
		if code != 0 {
			errStr := TaosErrorStr(result)
			t.Error(errors.NewError(code, errStr))
			return 0
		}
		count := TaosNumFields(result)
		rh, err := ReadColumn(result, count)
		if err != nil {
			t.Error(err)
			return 0
		}
		precision := TaosResultPrecision(result)
		rowsCount := 0
		for {
			values := make([]driver.Value, count)
			row := TaosFetchRow(result)
			if row == nil {
				break
			}
			lengths := FetchLengths(result, count)
			for i := range rh.ColTypes {
				values[i] = FetchRow(row, i, rh.ColTypes[i], lengths[i], precision)
			}
			rowsCount += 1
		}
		return rowsCount
	}
	err = exec(conn, "insert into test_subscribe_wrapper values(now, false, 10)")
	if err != nil {
		t.Error(err)
		return
	}
	count := consume()
	if count != 1 {
		t.Errorf("want %d got %d", 1, count)
		return
	}
	err = exec(conn, "insert into test_subscribe_wrapper values(now + 10s, true, 11)")
	if err != nil {
		t.Error(err)
		return
	}
	err = exec(conn, "insert into test_subscribe_wrapper values(now + 15s, true, 12)")
	if err != nil {
		t.Error(err)
		return
	}
	count = consume()
	if count != 2 {
		t.Errorf("want %d got %d", 2, count)
		return
	}
}

func exec(conn unsafe.Pointer, sql string) error {
	res := TaosQuery(conn, sql)
	defer TaosFreeResult(res)
	code := TaosError(res)
	if code != 0 {
		errStr := TaosErrorStr(res)
		return errors.NewError(code, errStr)
	}
	return nil
}
