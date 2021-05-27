package taosSql

import (
	"database/sql/driver"
	"io"
	"reflect"
	"testing"
)

func TestColumns(t *testing.T) {
	db, err := Open("log")
	if err != nil {
		t.Fatal(err)
	}
	rows, err := db.Query("show databases")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	columns := rows.Columns()
	if !reflect.DeepEqual(columns, []string{"name", "created_time", "ntables",
		"vgroups", "replica", "quorum", "days", "keep0,keep1,keep(D)",
		"cache(MB)", "blocks", "minrows", "maxrows", "wallevel", "fsync",
		"comp", "cachelast", "precision", "update", "status"}) {
		t.Fatal(columns)
	}
}

func TestNext(t *testing.T) {
	db := openTestDB("")
	if db == nil {
		t.Fatal()
	}
	res := db.query("show databases")
	columns := res.Columns()
	if len(columns) == 0 {
		t.Fatal(columns)
	}
	values := make([]driver.Value, len(columns))
	err := res.Next(values)
	t.Log(values)
	if err != nil {
		t.Fatal(err, values)
	}
	for {
		err = res.Next(values)
		if err == io.EOF {
			break
		}
		t.Log(values)
	}
}
