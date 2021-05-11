package taosSql

import (
	"database/sql"
	"testing"
	"time"
)

func TestConnect(t *testing.T) {
	db, err := sql.Open("taosSql", "/wo")
	if err != nil {
		t.Fatal(err)
	}
	err = db.Ping()
	if err != nil {
		t.Fatal(err)
	}
	rows, err := db.Query("select ts, tbid from testit")
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var tbid int
		var ts time.Time
		err := rows.Scan(&ts, &tbid)
		if err != nil {
			t.Fatal(err)
		}
		if ts.IsZero() {
			t.Fatal(ts)
		}
	}
}
