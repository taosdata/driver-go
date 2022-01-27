package taosSql

import (
	"database/sql"
	"testing"
	"time"
)

// @author: xftan
// @date: 2022/1/27 16:15
// @description: test sql query
func TestOpenQuery(t *testing.T) {
	db, err := sql.Open("taosSql", dataSourceName)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	err = db.Ping()
	if err != nil {
		t.Fatal(err)
	}
	rows, err := db.Query("select ts, dnodeid from dn")
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var dnodeID int
		var ts time.Time
		err := rows.Scan(&ts, &dnodeID)
		if err != nil {
			t.Fatal(err)
		}
		if ts.IsZero() {
			t.Fatal(ts)
		}
	}
}

// @author: xftan
// @date: 2022/1/27 16:16
// @description: test set config
func TestSetConfig(t *testing.T) {
	db, err := sql.Open("taosSql", "root:taosdata/tcp(localhost:6030)/log?debugFlag=135&asyncLog=0")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	err = db.Ping()
	if err != nil {
		t.Fatal(err)
	}
	rows, err := db.Query("select ts, dnodeid from dn")
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var dnodeID int
		var ts time.Time
		err := rows.Scan(&ts, &dnodeID)
		if err != nil {
			t.Fatal(err)
		}
		if ts.IsZero() {
			t.Fatal(ts)
		}
	}
}
