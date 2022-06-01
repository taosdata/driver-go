package taosSql

import (
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// @author: xftan
// @date: 2022/5/31 20:24
// @description: test stmt query
func TestStmtQuery(t *testing.T) {
	db, err := sql.Open("taosSql", dataSourceName)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	err = db.Ping()
	if err != nil {
		t.Fatal(err)
	}
	stmt, err := db.Prepare("select ts, dnodeid from dn")
	if err != nil {
		t.Fatal(err)
	}
	rows, err := stmt.Query()
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
// @date: 2022/5/31 20:23
// @description: test stmt exec
func TestStmtExec(t *testing.T) {
	db, err := sql.Open("taosSql", dataSourceName)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	err = db.Ping()
	if err != nil {
		t.Fatal(err)
	}
	stmt, err := db.Prepare("create database if not exists test")
	if err != nil {
		t.Fatal(err)
	}
	result, err := stmt.Exec()
	if err != nil {
		t.Fatal(err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		t.Fatal(err)
	}
	assert.GreaterOrEqual(t, affected, int64(0))
}
