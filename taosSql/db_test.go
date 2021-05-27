package taosSql

import (
	"database/sql/driver"
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
		// defer rows.Close()
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
	db.Exec("insert into super values(now, false, 10)")
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
