package taosSql

import (
	"database/sql/driver"
	"fmt"
	"reflect"
	"testing"
)

func openTestDB(dbname string) *taosDB {
	return taosConnect("", "", "", dbname, 0)
}

func TestPrecision(t *testing.T) {
	db := openTestDB("log")
	if db == nil {
		t.Fatal()
	}
	tcs := []struct {
		sql  string
		want int32
	}{
		{"show databases", 0},
		{"select * from log limit 1", 1}}
	for i := range tcs {
		tc := tcs[i]
		name := fmt.Sprintf("%02d-%s", i, tc.sql)
		t.Run(name, func(t *testing.T) {
			res := db.query(tc.sql)
			if res == nil {
				t.Fatal()
			}
			row := res.fetchRow()
			if row == nil {
				t.Fatal(row)
			}
			want := res.resultPrecision()
			errno := res.errno()
			if errno != 0 {
				t.Fatal(res.errstr())
			}
			if want != tc.want { // show database output timestamp in ms.
				t.Fatal(want, tc.want)
			}
			defer res.freeResult()
		})
	}
	defer db.close()
}

func TestFetchRow(t *testing.T) {
	db := openTestDB("log")
	if db == nil {
		t.Fatal()
	}
	cs := []struct {
		sql  string
		want int
	}{
		{"show databases", 0},
		{"select ts, level, content from log", 1}}

	for i, c := range cs {
		name := fmt.Sprintf("%02d:%s", i, c.sql)
		t.Run(name, func(t *testing.T) {
			r := db.query(c.sql)
			rows := r.fetchRow()
			if rows == nil {
				t.Fatal()
			}
			defer r.freeResult()
		})
	}
}

func TestNumFields(t *testing.T) {
	db := openTestDB("log")
	if db == nil {
		t.Fatal()
	}
	cs := []struct {
		sql  string
		want int
	}{
		{"show databases", 19},
		{"select ts, level, content from log limit 1", 3},
		{"select 1", 1},
		{"select server_version()", 1},
		{"select client_version()", 1},
	}

	for i, c := range cs {
		name := fmt.Sprintf("%02d:%s", i, c.sql)
		t.Run(name, func(t *testing.T) {
			r := db.query(c.sql)
			numFields := r.numFields()
			if numFields != c.want {
				t.Fatal(numFields, c)
			}
			defer r.freeResult()
		})
	}
}

func TestFetchLengths(t *testing.T) {
	db := openTestDB("log")
	if db == nil {
		t.Fatal()
	}
	cs := []struct {
		sql  string
		num  int
		want []int32
	}{
		{"show stables", 5, []int32{4, 8, 2, 2, 4}},
		{"select 1", 1, []int32{4}},
		{"select server_version()", 1, []int32{7}},
		{"select client_version()", 1, []int32{7}},
	}

	for i, c := range cs {
		name := fmt.Sprintf("%02d:%s", i, c.sql)
		t.Run(name, func(t *testing.T) {
			res := db.query(c.sql)
			if res == nil {
				t.Fatal()
			}
			row := res.fetchRow()
			if row == nil {
				t.Fatal(row)
			}
			lengths := res.fetchLengths(c.num)
			if lengths == nil {
				t.Fatal(lengths)
			}
			if !reflect.DeepEqual(lengths, c.want) {
				t.Fatal(lengths, c)
			}
			defer res.freeResult()
		})
	}
}

func TestFieldCount(t *testing.T) {
	db := openTestDB("log")
	if db == nil {
		t.Fatal()
	}
	cs := []struct {
		sql  string
		want int
	}{
		{"show databases", 19},
		{"select ts, level, content from log limit 1", 3},
		{"select 1", 1},
		{"select server_version()", 1},
		{"select client_version()", 1},
		{"describe log", 4},
		{"show tables", 7},
	}

	for i, c := range cs {
		name := fmt.Sprintf("%02d:%s", i, c.sql)
		t.Run(name, func(t *testing.T) {
			r := db.query(c.sql)
			fieldCount := r.fieldCount()
			if fieldCount != c.want {
				t.Fatal(fieldCount, c)
			}
			defer r.freeResult()
		})
	}
}

func TestAffectedRows(t *testing.T) {
	db := openTestDB("log")
	if db == nil {
		t.Fatal()
	}
	db.query("drop database if exists test")
	db.query("create database test precision 'ms'")
	if code := db.selectDB("test"); code != 0 {
		return
	}
	db.query("create table test01(ts timestamp, node int)")
	cs := []struct {
		sql  string
		want int64
	}{
		{"insert into test01 values(now, 1)", 1},
	}

	for i, c := range cs {
		name := fmt.Sprintf("%02d:%s", i, c.sql)
		t.Run(name, func(t *testing.T) {
			r := db.query(c.sql)
			affectedRows := r.affectedRows()
			if affectedRows != c.want {
				t.Fatal(affectedRows, c)
			}
			defer r.freeResult()
		})
	}
}

func TestFetchFields(t *testing.T) {
	db := openTestDB("log")
	if db == nil {
		t.Fatal()
	}
	cs := []struct {
		sql    string
		num    int
		fields []taosField
	}{
		{"show databases", 19, []taosField{{"name", 8, 32}, {"created_time", 9, 8}, {"ntables", 4, 4}, {"vgroups", 4, 4}, {"replica", 3, 2}, {"quorum", 3, 2}, {"days", 3, 2}, {"keep0,keep1,keep(D)", 8, 24}, {"cache(MB)", 4, 4}, {"blocks", 4, 4}, {"minrows", 4, 4}, {"maxrows", 4, 4}, {"wallevel", 2, 1}, {"fsync", 4, 4}, {"comp", 2, 1}, {"cachelast", 2, 1}, {"precision", 8, 3}, {"update", 2, 1}, {"status", 8, 10}}},
		{"select ts, level, content from log limit 1", 3, []taosField{{"ts", 9, 8}, {"level", 2, 1}, {"content", 8, 100}}},
		{"select 1", 1, []taosField{{"server_status()", 4, 4}}},
		{"select server_version()", 1, []taosField{{"server_version()", 8, 7}}},
		{"select client_version()", 1, []taosField{{"client_version()", 8, 7}}},
	}

	for i, c := range cs {
		name := fmt.Sprintf("%02d:%s", i, c.sql)
		t.Run(name, func(t *testing.T) {
			r := db.query(c.sql)
			fields := r.fetchFields()
			if len(fields) != c.num {
				t.Fatal(fields)
			}
			if !reflect.DeepEqual(fields, c.fields) {
				t.Fatal("\n", fields, "\n", c.fields)
			}
		})
	}
}

func TestStmtBindParam(t *testing.T) {
	db := openTestDB("log")
	if db == nil {
		t.Fatal()
	}
	stmt := db.stmtInit()
	if stmt == nil {
		t.Fatal()
	}
	defer stmt.close()
	stmt.prepare("select * from log where level = ?")
	params := []driver.Value{int8(0)}
	rc := stmt.bindParam(params)
	if rc != 0 {
		t.Fatal(rc)
	}
	rc = stmt.execute()
	if rc != 0 {
		t.Fatal(rc)
	}
	res := stmt.useResult()
	if res == nil {
		t.Fatal()
	}
	defer res.freeResult()
	cols := res.Columns()
	if len(cols) != 4 {
		t.Fatal()
	}
	t.Log(cols)
}
