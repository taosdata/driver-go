package taosSql

import (
	"database/sql/driver"
	"fmt"
	"io"
	"testing"
	"time"
)

func testDatabase(t *testing.T) DB {
	db, err := Open("")
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec("drop database if exists test")
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec("create database if not exists test precision 'us' update 1")
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec("use test")
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func TestOpen(t *testing.T) {
	db := testDatabase(t)
	defer db.Close()
	// select database
	_, err := db.Exec("use log")
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		sql  string
		want string
	}{
		{"select 1", "server_status()"},
		{"select server_status()", "server_status()"},
		{"select client_version()", "client_version()"},
		{"select server_version()", "server_version()"},
		{"select database()", "database()"},
		{"select current_user()", "current_user()"},
	} {
		t.Run(c.sql, func(t *testing.T) {
			rows, err := db.Query(c.sql)
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			cols := rows.Columns()
			if len(cols) != 1 {
				t.Fatal(cols)
			}
			col := cols[0]
			if col != c.want {
				t.Log(cols)
			}
		})
	}
}

func TestSubscribe(t *testing.T) {
	db := testDatabase(t)
	defer db.Close()
	_, err := db.Exec("drop table if exists super")
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec("create table if not exists super(ts timestamp, value bool, degress int)")
	if err != nil {
		t.Fatal(err)
	}
	sql := "select ts, value, degress from super"
	topic, err := db.Subscribe(true, "supertopic", sql, time.Second*1)
	if err != nil {
		t.Fatal(err, db)
	}
	consume := func() int {
		rows, err := topic.Consume()
		if err != nil {
			t.Fatal(err)
		}
		if rows == nil {
			return 0
		}
		defer rows.Close()
		count := 0
		for err == nil {
			row := make([]driver.Value, 3)
			err = rows.Next(row)
			if err == io.EOF {
				break
			}
			ts, _ := row[0].(time.Time)
			value, _ := row[1].(bool)
			degress, _ := row[2].(int32)
			t.Log(err, ts, value, degress)
			if err != nil {
				t.Fatal(err)
			}
			count++
		}
		return count
	}
	defer topic.Unsubscribe(true)
	_, err = db.Exec("insert into super values(now, false, 10)")
	if err != nil {
		t.Fatal(err)
	}
	count := consume()
	if count != 1 {
		t.Fatal(count)
	}
	_, err = db.Exec("insert into super values(now, true, 11)")
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec("insert into super values(now, true, 12)")
	if err != nil {
		t.Fatal(err)
	}
	count = consume()
	if count != 2 {
		t.Fatal(count)
	}
}

func TestQuery(t *testing.T) {
	db := testDatabase(t)
	defer db.Close()
	_, err := db.Exec("drop table if exists testtypes")
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec("create table if not exists testtypes(ts timestamp, fint int, fbigint bigint, ffloat float, fdouble double, fbinary binary(16), fsmallint smallint, ftinyint tinyint, fbool bool, fnchar nchar(16))")
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec("insert into testtypes values(now, 1, 2, 3000000.3, 400000000.4, '5binary', 6, 7, true, '9nchar')")
	if err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query("select ts, fint, fbigint, ffloat, fdouble, fbinary, fsmallint, ftinyint, fbool, fnchar from testtypes limit 1")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	values := make([]driver.Value, 10)
	if err = rows.Next(values); err != nil {
		t.Fatal(err)
	}
	ts := values[0].(time.Time)
	if ts.IsZero() {
		t.Fatal(ts)
	}
	fint := values[1].(int32)
	if fint != 1 {
		t.Fatal(fint)
	}
	fbigint := values[2].(int64)
	if fbigint != 2 {
		t.Fatal(fbigint)
	}
	ffloat := values[3].(float32)
	if ffloat != 3000000.3 {
		t.Fatal(ffloat)
	}
	fdouble := values[4].(float64)
	if fdouble != 400000000.4 {
		t.Fatal(fdouble)
	}
	fbinary := values[5].(string)
	if fbinary != "5binary" {
		t.Fatal(fbinary)
	}
	fsmallint := values[6].(int16)
	if fsmallint != 6 {
		t.Fatal(fsmallint)
	}
	ftinyint := values[7].(int8)
	if ftinyint != 7 {
		t.Fatal(ftinyint)
	}
	fbool := values[8].(bool)
	if !fbool {
		t.Fatal(fbool)
	}
	fnchar := values[9].(string)
	if fnchar != "9nchar" {
		t.Fatal(fnchar)
	}
}

func TestStmtExec(t *testing.T) {
	db := testDatabase(t)
	defer db.Close()
	values := func(v ...driver.Value) []driver.Value { return v }
	for i, tc := range []struct {
		tbtype string
		pos    string
		params []driver.Value
		skip   bool
	}{
		{"ts timestamp, value int", "now, ?", values(1), false},
		{"ts timestamp, value bool", "now, ?", values(true), false},
		{"ts timestamp, value int", "now, ?", values(int32(1)), false},
		{"ts timestamp, value int", "now, 1", nil, false},
		{"ts timestamp, value int", "now, 1", values(), false},
		{"ts timestamp, value tinyint", "now, ?", values(int8(1)), false},
		{"ts timestamp, value smallint", "now, ?", values(int16(1)), false},
		{"ts timestamp, value int", "now, ?", values(int32(1)), false},
		{"ts timestamp, value bigint", "now, ?", values(int64(1)), false},
		{"ts timestamp, value tinyint unsigned", "now, ?", values(uint8(1)), true},
		{"ts timestamp, value smallint unsigned", "now, ?", values(uint16(1)), true},
		{"ts timestamp, value int unsigned", "now, ?", values(uint32(1)), true},
		{"ts timestamp, value bigint unsigned", "now, ?", values(uint64(1)), true},
		{"ts timestamp, value tinyint unsigned", "now, ?", values(int8(1)), true},
		{"ts timestamp, value smallint unsigned", "now, ?", values(int16(1)), true},
		{"ts timestamp, value int unsigned", "now, ?", values(int32(1)), true},
		{"ts timestamp, value bigint unsigned", "now, ?", values(int64(1)), true},
		{"ts timestamp, value float", "now, ?", values(float32(1.2)), false},
		{"ts timestamp, value float", "now, ?", values(float32(1.2)), false},
		{"ts timestamp, value double", "now, ?", values(1.2), false},
		{"ts timestamp, value double", "now, ?", values(float64(1.2)), false},
		{"ts timestamp, value binary(8)", "now, ?", values([]byte("yes")), false},
		{"ts timestamp, value nchar(8)", "now, ?", values("yes"), false},
		{"ts timestamp, value nchar(8)", "?, ?", values(time.Now(), "yes"), false},
	} {
		tbname := fmt.Sprintf("teststmt%02d", i)
		tbtype := tc.tbtype
		create := fmt.Sprintf("create table %s(%s)", tbname, tbtype)
		params := tc.params
		name := fmt.Sprintf("%02d-%s", i, tbtype)
		pos := tc.pos
		sql := fmt.Sprintf("insert into %s values(%s)", tbname, pos)
		var err error
		t.Run(name, func(t *testing.T) {
			if tc.skip {
				t.Skip("Skip, not support yet")
			}
			if _, err = db.Exec(create); err != nil {
				t.Fatal(err)
			}

			if _, err = db.Exec(sql, params...); err != nil {
				t.Fatal(err)
			}
			if len(params) == 0 {
				return
			}
			value := params[0]
			var rows driver.Rows
			if rows, err = db.Query(fmt.Sprintf("select value from %s", tbname)); err != nil {
				t.Fatal(rows, value)
			}
			defer rows.Close()
			v := make([]driver.Value, 1)
			if err = rows.Next(v); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestStmtQuery(t *testing.T) {
	db := testDatabase(t)
	defer db.Close()
	vs := func(v ...driver.Value) []driver.Value { return v }
	for i, tc := range []struct {
		tbtype string
		data   string
		clause string
		params []driver.Value
		skip   bool
	}{
		{"ts timestamp, value int", "now, 1", "value = ?", vs(1), false},
		{"ts timestamp, value bool", "now, true", "value = ?", vs(true), false},
		{"ts timestamp, value tinyint", "now, 3", "value = ?", vs(int8(3)), false},
		{"ts timestamp, value tinyint", "now, 4", "value = ?", vs(4), false},
		{"ts timestamp, value smallint", "now, 5", "value = ?", vs(int16(5)), false},
		{"ts timestamp, value int", "now, 6", "value = ?", vs(int32(6)), false},
		{"ts timestamp, value bigint", "now, 7", "value = ?", vs(int32(7)), false},
		{"ts timestamp, value tinyint unsigned", "now, 1", "value = ?", vs(uint8(1)), true},
		{"ts timestamp, value smallint unsigned", "now, 2", "value = ?", vs(uint16(2)), true},
		{"ts timestamp, value int unsigned", "now, 3", "value = ?", vs(uint32(3)), true},
		{"ts timestamp, value bigint unsiged", "now, 4", "value = ?", vs(uint64(4)), true},
		{"ts timestamp, value tinyint unsigned", "now, 1", "value = ?", vs(int8(1)), false},
		{"ts timestamp, value smallint unsigned", "now, 2", "value = ?", vs(int16(2)), true},
		{"ts timestamp, value int unsigned", "now, 3", "value = ?", vs(int32(3)), true},
		{"ts timestamp, value bigint unsiged", "now, 4", "value = ?", vs(int64(4)), true},
		{"ts timestamp, value float", "now, 1.2", "value = ?", vs(float32(1.2)), false},
		{"ts timestamp, value double", "now, 1.3", "value = ?", vs(1.3), false},
		{"ts timestamp, value double", "now, 1.4", "value = ?", vs(float64(1.4)), false},
		{"ts timestamp, value binary(8)", "now, 'yes'", "value = ?", vs([]byte("yes")), false},
		{"ts timestamp, value nchar(8)", "now, 'OK'", "value = ?", vs("OK"), false},
		{"ts timestamp, value nchar(8)", "1622282105000000, 'NOW'", "ts = ? and value = ?", vs(time.Unix(1622282105, 0), "NOW"), true},
		{"ts timestamp, value nchar(8)", "1622282105000000, 'NOW'", "ts = ? and value = ?", vs(int64(1622282105000000), "NOW"), false},
	} {
		tbname := fmt.Sprintf("teststmt%02d", i)
		tbtype := tc.tbtype
		create := fmt.Sprintf("create table %s(%s)", tbname, tbtype)
		insert := fmt.Sprintf("insert into %s values(%s)", tbname, tc.data)
		params := tc.params
		sql := fmt.Sprintf("select * from %s where %s", tbname, tc.clause)
		name := fmt.Sprintf("%02d-%s", i, tbtype)
		var err error
		t.Run(name, func(t *testing.T) {
			if tc.skip {
				t.Skip("Skip, not support yet")
			}
			if _, err = db.Exec(create); err != nil {
				t.Fatal(err)
			}
			if _, err = db.Exec(insert); err != nil {
				t.Fatal(err)
			}
			var rows driver.Rows
			if rows, err = db.Query(sql, params...); err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			names := rows.Columns()
			if len(names) == 0 {
				t.Fatal(names)
			}
			values := make([]driver.Value, len(names))
			if err = rows.Next(values); err != nil {
				t.Fatal(err)
			}
		})
	}
}
