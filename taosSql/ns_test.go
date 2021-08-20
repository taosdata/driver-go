package taosSql

import (
	"database/sql"
	"fmt"
	"testing"
	"time"
)

func TestNanosecond(t *testing.T) {
	db, err := sql.Open("taosSql", dataSourceName)
	if err != nil {
		t.Fatal(err)
	}
	err = db.Ping()
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec("create database if not exists test_go_ns_ precision 'ns'")
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec("create table if not exists test_go_ns_.tb1 (ts timestamp, n int)")
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec("insert into test_go_ns_.tb1 values(1629363529469478001, 1)(1629363529469478002,2)")
	if err != nil {
		t.Fatal(err)
	}
	rows, err := db.Query("select ts from test_go_ns_.tb1")
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var ts time.Time
		err := rows.Scan(&ts)
		if err != nil {
			t.Fatal(err)
		}
		if ts.Nanosecond()%1000 == 0 {
			fmt.Println(ts.UnixNano())
			t.Fatal("nanosecond is not correct")
		}
	}
}
