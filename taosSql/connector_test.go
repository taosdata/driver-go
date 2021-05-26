package taosSql

import (
	"database/sql"
	"testing"
	"time"
)

func TestOpenQuery(t *testing.T) {
	db, err := sql.Open("taosSql", dataSourceName)
	if err != nil {
		t.Fatal(err)
	}
	err = db.Ping()
	if err != nil {
		t.Fatal(err)
	}
	rows, err := db.Query("select ts, dnodeid from dn")
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var dnodeid int
		var ts time.Time
		err := rows.Scan(&ts, &dnodeid)
		if err != nil {
			t.Fatal(err)
		}
		if ts.IsZero() {
			t.Fatal(ts)
		}
	}
}
