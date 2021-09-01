package af

import (
	"database/sql/driver"
	"fmt"
	"io"
	"testing"
	"time"
)

func testDatabase(t *testing.T) *Connector {
	db, err := Open("", "", "", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	//_, err = db.Exec("drop database if exists test_af")
	//if err != nil {
	//	t.Fatal(err)
	//}
	_, err = db.Exec("create database if not exists test_af precision 'us' update 1 keep 36500")
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec("use test_af")
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func TestOpen(t *testing.T) {
	db := testDatabase(t)
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
	_, err := db.Exec("drop table if exists test_subscribe")
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec("create table if not exists test_subscribe(ts timestamp, value bool, degress int)")
	if err != nil {
		t.Fatal(err)
	}
	sql := "select ts, value, degress from test_subscribe"
	subscriber, err := db.Subscribe(true, "test_subscribe", sql, time.Second*1)
	if err != nil {
		t.Fatal(err, db)
	}
	consume := func() int {
		rows, err := subscriber.Consume()
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
			//ts, _ := row[0].(time.Time)
			//value, _ := row[1].(bool)
			//degress, _ := row[2].(int32)
			//t.Log(err, ts, value, degress)
			if err != nil {
				t.Fatal(err)
			}
			count++
		}
		return count
	}
	defer subscriber.Unsubscribe(true)
	_, err = db.Exec("insert into test_subscribe values(now, false, 10)")
	if err != nil {
		t.Fatal(err)
	}
	count := consume()
	if count != 1 {
		t.Fatal(count)
	}
	_, err = db.Exec("insert into test_subscribe values(now, true, 11)")
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec("insert into test_subscribe values(now, true, 12)")
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second)
	count = consume()
	if count != 2 {
		t.Fatal(count)
	}
}

func TestQuery(t *testing.T) {
	db := testDatabase(t)
	_, err := db.Exec("drop table if exists test_types")
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec("create table if not exists test_types(ts timestamp, f_int int, f_bigint bigint, f_float float, f_double double, f_binary binary(16), f_smallint smallint, f_tinyint tinyint, f_bool bool, f_nchar nchar(16))")
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec("insert into test_types values(now, 1, 2, 3000000.3, 400000000.4, '5binary', 6, 7, true, '9nchar')")
	if err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query("select ts, f_int, f_bigint, f_float, f_double, f_binary, f_smallint, f_tinyint, f_bool, f_nchar from test_types limit 1")
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
	fInt := values[1].(int32)
	if fInt != 1 {
		t.Fatal(fInt)
	}
	fBigint := values[2].(int64)
	if fBigint != 2 {
		t.Fatal(fBigint)
	}
	fFloat := values[3].(float32)
	if fFloat != 3000000.3 {
		t.Fatal(fFloat)
	}
	fDouble := values[4].(float64)
	if fDouble != 400000000.4 {
		t.Fatal(fDouble)
	}
	fBinary := values[5].(string)
	if fBinary != "5binary" {
		t.Fatal(fBinary)
	}
	fSmallint := values[6].(int16)
	if fSmallint != 6 {
		t.Fatal(fSmallint)
	}
	fTinyint := values[7].(int8)
	if fTinyint != 7 {
		t.Fatal(fTinyint)
	}
	fBool := values[8].(bool)
	if !fBool {
		t.Fatal(fBool)
	}
	fNchar := values[9].(string)
	if fNchar != "9nchar" {
		t.Fatal(fNchar)
	}
}

func TestStmtExec(t *testing.T) {
	db := testDatabase(t)
	now := time.Now()
	for i, tc := range []struct {
		tbType string
		pos    string
		params *Param
	}{
		{"ts timestamp, value int", "?, ?", NewParam(2).AddTimestamp(now, 0).AddInt(1)},
		{"ts timestamp, value bool", "?, ?", NewParam(2).AddTimestamp(now, 0).AddBool(true)},
		{"ts timestamp, value int", "?, ?", NewParam(2).AddTimestamp(now, 0).AddInt(1)},
		{"ts timestamp, value tinyint", "?, ?", NewParam(2).AddTimestamp(now, 0).AddTinyint(1)},
		{"ts timestamp, value smallint", "?, ?", NewParam(2).AddTimestamp(now, 0).AddSmallint(1)},
		{"ts timestamp, value int", "?, ?", NewParam(2).AddTimestamp(now, 0).AddInt(1)},
		{"ts timestamp, value bigint", "?, ?", NewParam(2).AddTimestamp(now, 0).AddBigint(1)},
		{"ts timestamp, value tinyint unsigned", "?, ?", NewParam(2).AddTimestamp(now, 0).AddUTinyint(1)},
		{"ts timestamp, value smallint unsigned", "?, ?", NewParam(2).AddTimestamp(now, 0).AddUSmallint(1)},
		{"ts timestamp, value int unsigned", "?, ?", NewParam(2).AddTimestamp(now, 0).AddUInt(1)},
		{"ts timestamp, value bigint unsigned", "?, ?", NewParam(2).AddTimestamp(now, 0).AddUBigint(1)},
		{"ts timestamp, value tinyint unsigned", "?, ?", NewParam(2).AddTimestamp(now, 0).AddUTinyint(1)},
		{"ts timestamp, value smallint unsigned", "?, ?", NewParam(2).AddTimestamp(now, 0).AddUSmallint(1)},
		{"ts timestamp, value int unsigned", "?, ?", NewParam(2).AddTimestamp(now, 0).AddUInt(1)},
		{"ts timestamp, value float", "?, ?", NewParam(2).AddTimestamp(now, 0).AddFloat(1.2)},
		{"ts timestamp, value double", "?, ?", NewParam(2).AddTimestamp(now, 0).AddDouble(1.2)},
		{"ts timestamp, value binary(8)", "?, ?", NewParam(2).AddTimestamp(now, 0).AddBinary([]byte("yes"))},
		{"ts timestamp, value nchar(8)", "?, ?", NewParam(2).AddTimestamp(now, 0).AddNchar("yes")},
		{"ts timestamp, value nchar(8)", "?, ?", NewParam(2).AddTimestamp(time.Now(), 0)},
	} {
		tbName := fmt.Sprintf("test_stmt_insert%02d", i)
		tbType := tc.tbType
		create := fmt.Sprintf("create table if not exists %s(%s)", tbName, tbType)
		params := tc.params
		name := fmt.Sprintf("%02d-%s", i, tbType)
		pos := tc.pos
		sql := fmt.Sprintf("insert into %s values(%s)", tbName, pos)
		var err error
		t.Run(name, func(t *testing.T) {
			if _, err = db.Exec(create); err != nil {
				t.Fatal(err)
			}
			_, err = db.StmtExecute(sql, params)
			if err != nil {
				t.Fatal(err)
			}
			var rows driver.Rows
			if rows, err = db.Query(fmt.Sprintf("select value from %s", tbName)); err != nil {
				t.Fatal(rows, tbName)
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
	for i, tc := range []struct {
		tbType string
		data   string
		clause string
		params *Param
		skip   bool
	}{
		{"ts timestamp, value int", "0, 1", "value = ?", NewParam(1).AddInt(1), false},
		{"ts timestamp, value bool", "now, true", "value = ?", NewParam(1).AddBool(true), false},
		{"ts timestamp, value tinyint", "now, 3", "value = ?", NewParam(1).AddTinyint(3), false},
		{"ts timestamp, value smallint", "now, 5", "value = ?", NewParam(1).AddSmallint(5), false},
		{"ts timestamp, value int", "now, 6", "value = ?", NewParam(1).AddInt(6), false},
		{"ts timestamp, value bigint", "now, 7", "value = ?", NewParam(1).AddBigint(7), false},
		{"ts timestamp, value tinyint unsigned", "now, 1", "value = ?", NewParam(1).AddUTinyint(1), true},
		{"ts timestamp, value smallint unsigned", "now, 2", "value = ?", NewParam(1).AddUSmallint(2), true},
		{"ts timestamp, value int unsigned", "now, 3", "value = ?", NewParam(1).AddUInt(3), true},
		{"ts timestamp, value bigint unsigned", "now, 4", "value = ?", NewParam(1).AddUBigint(4), true},
		{"ts timestamp, value tinyint unsigned", "now, 1", "value = ?", NewParam(1).AddUTinyint(1), true},
		{"ts timestamp, value smallint unsigned", "now, 2", "value = ?", NewParam(1).AddUSmallint(2), true},
		{"ts timestamp, value int unsigned", "now, 3", "value = ?", NewParam(1).AddUInt(3), true},
		{"ts timestamp, value bigint unsigned", "now, 4", "value = ?", NewParam(1).AddUBigint(4), true},
		{"ts timestamp, value float", "now, 1.2", "value = ?", NewParam(1).AddFloat(1.2), false},
		{"ts timestamp, value double", "now, 1.3", "value = ?", NewParam(1).AddDouble(1.3), false},
		{"ts timestamp, value double", "now, 1.4", "value = ?", NewParam(1).AddDouble(1.4), false},
		{"ts timestamp, value binary(8)", "now, 'yes'", "value = ?", NewParam(1).AddBinary([]byte("yes")), false},
		{"ts timestamp, value nchar(8)", "now, 'OK'", "value = ?", NewParam(1).AddNchar("OK"), false},
		{"ts timestamp, value nchar(8)", "1622282105000000, 'NOW'", "ts = ? and value = ?", NewParam(2).AddTimestamp(time.Unix(1622282105, 0), 2).AddBinary([]byte("NOW")), true},
		{"ts timestamp, value nchar(8)", "1622282105000000, 'NOW'", "ts = ? and value = ?", NewParam(2).AddBigint(1622282105000000).AddBinary([]byte("NOW")), false},
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
			if _, err = db.Exec(create); err != nil {
				t.Fatal(err)
			}
			if _, err = db.Exec(insert); err != nil {
				t.Fatal(err)
			}
			var rows driver.Rows

			if rows, err = db.StmtQuery(sql, params); err != nil {
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
