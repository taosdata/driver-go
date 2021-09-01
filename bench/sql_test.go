package bench

import (
	"database/sql"
	"fmt"
	_ "github.com/taosdata/driver-go/v2/taosSql"
	"testing"
	"time"
)

var (
	driverName     = "taosSql"
	user           = "root"
	password       = "taosdata"
	host           = ""
	port           = 6030
	dataSourceName = fmt.Sprintf("%s:%s@/tcp(%s:%d)/%s?interpolateParams=true", user, password, host, port, "log")
)

func TestMain(m *testing.M) {
	b := testing.B{}
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		b.Fatalf("error on:  sql.open %s", err.Error())
	}
	defer db.Close()
	_, err = db.Exec("create database if not exists bench_test")
	if err != nil {
		b.Fatalf("create database banch_test error %s", err.Error())
	}
	_, err = db.Exec("drop table IF EXISTS bench_test.test_insert")
	if err != nil {
		b.Fatalf("drop table bench_test.test_insert error %s", err.Error())
	}
	_, err = db.Exec("create table  bench_test.test_insert (ts timestamp ,value double)")
	if err != nil {
		b.Fatalf("create table bench_test.test_insert error %s", err.Error())
	}
	_, err = db.Exec("drop table IF EXISTS bench_test.test_select")
	if err != nil {
		b.Fatalf("drop table bench_test.test_select error %s", err.Error())
	}
	_, err = db.Exec("create table bench_test.test_select (ts timestamp ,value double)")
	if err != nil {
		b.Fatalf("create table bench_test.test_select error %s", err.Error())
	}
	for i := 0; i < 1; i++ {
		_, err = db.Exec(fmt.Sprintf("insert into bench_test.test_select values ( now + %ds ,123.456)", i))
		if err != nil {
			b.Fatalf("insert data error %s", err)
		}
	}
	m.Run()
}

func BenchmarkInsert(b *testing.B) {
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		b.Fatalf("error on:  sql.open %s", err.Error())
		return
	}
	for i := 0; i < b.N; i++ {
		_, err = db.Exec("insert into bench_test.test_insert values (now,123.456)")
		if err != nil {
			b.Fatalf("insert data error %s", err)
		}
	}
}

func BenchmarkSelect(b *testing.B) {
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		b.Fatalf("error on:  sql.open %s", err.Error())
		return
	}
	for i := 0; i < b.N; i++ {
		rows, err := db.Query("select * from bench_test.test_select")
		if err != nil {
			b.Fatalf("select data error %s", err.Error())
		}
		var t time.Time
		var s float64
		for rows.Next() {
			err := rows.Scan(&t, &s)
			if err != nil {
				b.Fatalf("scan error %s", err.Error())
			}
			if s != 123.456 {
				b.Fatalf("result error expect 123.456 got %f", s)
			}
		}
	}
}
