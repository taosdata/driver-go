package taosSql

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// Ensure that all the driver interfaces are implemented

var (
	driverName     = "taosSql"
	user           = "root"
	password       = "taosdata"
	host           = ""
	port           = 6030
	dbName         = "test_taos_sql"
	dataSourceName = fmt.Sprintf("%s:%s@/tcp(%s:%d)/%s?interpolateParams=true", user, password, host, port, "log")
)

type DBTest struct {
	*testing.T
	*sql.DB
}

func NewDBTest(t *testing.T) (dbt *DBTest) {
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		t.Fatalf("error on:  sql.open %s", err.Error())
		return
	}
	dbt = &DBTest{t, db}
	return
}

func (dbt *DBTest) CreateTables(numOfSubTab int) {
	dbt.mustExec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbName))
	dbt.mustExec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", dbName))
	dbt.mustExec(fmt.Sprintf("drop table if exists %s.super", dbName))
	dbt.mustExec(fmt.Sprintf("CREATE TABLE %s.super (ts timestamp, value BOOL) tags (degress int)", dbName))
	for i := 0; i < numOfSubTab; i++ {
		dbt.mustExec(fmt.Sprintf("create table %s.t%d using %s.super tags(%d)", dbName, i%10, dbName, i))
	}
}
func (dbt *DBTest) InsertInto(numOfSubTab, numOfItems int) {
	now := time.Now()
	t := now.Add(-100 * time.Hour)
	for i := 0; i < numOfItems; i++ {
		dbt.mustExec(fmt.Sprintf("insert into %s.t%d values(%d, %t)", dbName, i%numOfSubTab, t.UnixNano()/int64(time.Millisecond)+int64(i), i%2 == 0))
	}
}

type TestResult struct {
	ts      string
	value   bool
	degress int
}

func runTests(t *testing.T, tests ...func(dbt *DBTest)) {
	dbt := NewDBTest(t)
	// prepare data
	dbt.Exec("DROP TABLE IF EXISTS test")
	var numOfSubTables = 10
	var numOfItems = 200
	dbt.CreateTables(numOfSubTables)
	dbt.InsertInto(numOfSubTables, numOfItems)
	for _, test := range tests {
		test(dbt)
		dbt.Exec("DROP TABLE IF EXISTS test")
	}
}
func (dbt *DBTest) fail(method, query string, err error) {
	if len(query) > 300 {
		query = "[query too large to print]"
	}
	dbt.Fatalf("error on %s %s: %s", method, query, err.Error())
}

func (dbt *DBTest) mustExec(query string, args ...interface{}) (res sql.Result, err error) {
	res, err = dbt.Exec(query, args...)
	return
}

func (dbt *DBTest) mustQuery(query string, args ...interface{}) (rows *sql.Rows, err error) {
	rows, err = dbt.Query(query, args...)
	return
}
func TestEmptyQuery(t *testing.T) {
	runTests(t, func(dbt *DBTest) {
		// just a comment, no query
		_, err := dbt.mustExec("")
		if err == nil {
			dbt.Fatalf("error is expected")
		}

	})
}
func TestErrorQuery(t *testing.T) {
	runTests(t, func(dbt *DBTest) {
		// just a comment, no query
		_, err := dbt.mustExec("xxxxxxx inot")
		if err == nil {
			dbt.Fatalf("error is expected")
		}
	})
}

type (
	execFunc func(dbt *DBTest, query string, exec bool, err error, expected int64) int64
)

type Obj struct {
	query  string
	err    error
	exec   bool
	fp     execFunc
	expect int64
}

var (
	userErr = errors.New("user error")
	fp      = func(dbt *DBTest, query string, exec bool, eErr error, expected int64) int64 {
		var ret int64 = 0
		if exec == false {
			rows, err := dbt.mustQuery(query)
			if eErr == userErr && err != nil {
				return ret
			}
			if err != nil {
				dbt.Errorf("%s is not expected, err: %s", query, err.Error())
				return ret
			} else {
				var count int64 = 0
				for rows.Next() {
					var row TestResult
					if err := rows.Scan(&(row.ts), &(row.value)); err != nil {
						dbt.Error(err.Error())
						return ret
					}
					count = count + 1
				}
				rows.Close()
				ret = count
				if expected != -1 && count != expected {
					dbt.Errorf("%s is not expected, err: %s", query, errors.New("result is not expected"))
				}
			}
		} else {
			res, err := dbt.mustExec(query)
			if err != eErr {
				dbt.Fatalf("%s is not expected, err: %s", query, err.Error())
			} else {
				count, err := res.RowsAffected()
				if err != nil {
					dbt.Fatalf("%s is not expected, err: %s", query, err.Error())
				}
				if expected != -1 && count != expected {
					dbt.Fatalf("%s is not expected , err: %s", query, errors.New("result is not expected"))
				}
			}
		}
		return ret
	}
)

func TestAny(t *testing.T) {
	runTests(t, func(dbt *DBTest) {
		now := time.Now()
		tests := make([]*Obj, 0, 100)
		tests = append(tests,
			&Obj{fmt.Sprintf("insert into %s.t%d values(%d, %t)", dbName, 0, now.UnixNano()/int64(time.Millisecond)-1, false), nil, true, fp, int64(1)})
		tests = append(tests,
			&Obj{fmt.Sprintf("insert into %s.t%d values(%d, %t)", dbName, 0, now.UnixNano()/int64(time.Millisecond)-1, false), nil, true, fp, int64(1)})
		tests = append(tests,
			&Obj{fmt.Sprintf("select last_row(*) from %s.t%d", dbName, 0), nil, false, fp, int64(1)})
		tests = append(tests,
			&Obj{fmt.Sprintf("select first(*) from %s.t%d", dbName, 0), nil, false, fp, int64(1)})
		tests = append(tests,
			&Obj{fmt.Sprintf("select error"), userErr, false, fp, int64(1)})
		tests = append(tests,
			&Obj{fmt.Sprintf("select * from %s.t%d", dbName, 0), nil, false, fp, int64(-1)})
		tests = append(tests,
			&Obj{fmt.Sprintf("select * from %s.t%d", dbName, 0), nil, false, fp, int64(-1)})

		for _, obj := range tests {
			fp = obj.fp
			fp(dbt, obj.query, obj.exec, obj.err, obj.expect)
		}
	})
}
func TestCRUD(t *testing.T) {
	runTests(t, func(dbt *DBTest) {
		// Create Data
		now := time.Now()
		res, err := dbt.mustExec(fmt.Sprintf("insert into %s.t%d values(%d, %t)", dbName, 0, now.UnixNano()/int64(time.Millisecond)-1, false))
		if err != nil {
			dbt.Fatalf("insert failed %s", err.Error())
		}
		count, err := res.RowsAffected()
		if err != nil {
			dbt.Fatalf("res.RowsAffected() returned error: %s", err.Error())
		}
		if count != 1 {
			dbt.Fatalf("expected 1 affected row, got %d", count)
		}

		id, err := res.LastInsertId()
		if err == nil {
			dbt.Fatalf("res.LastInsertId() expect error")
		}
		if id != 0 {
			dbt.Fatalf("expected InsertId 0, got %d", id)
		}

		// Read
		rows, err := dbt.mustQuery(fmt.Sprintf("select * from %s.super", dbName))
		if err != nil {
			dbt.Fatalf("select failed")
		}
		for rows.Next() {
			var row TestResult
			err := rows.Scan(&(row.ts), &(row.value), &(row.degress))
			if err != nil {
				dbt.Error(err.Error())
			}
		}
		rows.Close()

		rows, err = dbt.mustQuery(fmt.Sprintf("select last_row(*) from %s.super", dbName))
		if err != nil {
			dbt.Fatalf("select last_row failed")
		} else {
			for rows.Next() {
				var value TestResult
				err := rows.Scan(&(value.ts), &(value.value))
				if err != nil {
					dbt.Error(err.Error())
				}
			}
			rows.Close()
		}

		query2 := "drop table if exists super"
		dbt.mustExec(query2)
		if err != nil {
			dbt.Fatalf(query2)
		}
	})
}

func TestStmt(t *testing.T) {
	runTests(t, func(dbt *DBTest) {
		stmt, err := dbt.Prepare(fmt.Sprintf("insert into %s.t0 values(?, ?)", dbName))
		if err != nil {
			dbt.fail("prepare", "prepare", err)
		}
		now := time.Now()
		stmt.Exec(now.UnixNano()/int64(time.Millisecond), false)
		stmt.Exec(now.UnixNano()/int64(time.Millisecond)+int64(1), false)
		stmt.Close()
	})
}

func TestJson(t *testing.T) {
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		t.Fatalf("error on:  sql.open %s", err.Error())
		return
	}
	defer db.Close()
	_, err = db.Exec("create database if not exists test_json")
	if err != nil {
		t.Error(err)
		return
	}
	_, err = db.Exec("drop table if exists test_json.tjson")
	if err != nil {
		t.Error(err)
		return
	}
	_, err = db.Exec("create stable if not exists test_json.tjson(ts timestamp,value int )tags(t json)")
	if err != nil {
		t.Error(err)
		return
	}
	_, err = db.Exec(`insert into test_json.tj_1 using test_json.tjson tags('{"a":1,"b":"b"}')values (now,1)`)
	if err != nil {
		t.Error(err)
		return
	}
	_, err = db.Exec(`insert into test_json.tj_2 using test_json.tjson tags('{"a":1,"c":"c"}')values (now,1)`)
	if err != nil {
		t.Error(err)
		return
	}
	_, err = db.Exec(`insert into test_json.tj_3 using test_json.tjson tags('null')values (now,1)`)
	if err != nil {
		t.Error(err)
		return
	}
	rows, err := db.Query("select * from test_json.tjson")
	if err != nil {
		t.Error(err)
		return
	}
	counter := 0
	for rows.Next() {
		var ts time.Time
		var value int32
		var info []byte
		err := rows.Scan(&ts, &value, &info)
		if err != nil {
			t.Error(err)
			return
		}
		if info != nil && !json.Valid(info) {
			t.Error("invalid json ", string(info))
			return
		}
		if info == nil {
			t.Logf("null")
		} else {
			t.Logf("%s", string(info))
		}
		counter += 1
	}
	assert.Equal(t, 3, counter)
}

func TestJsonSearch(t *testing.T) {
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		t.Fatalf("error on:  sql.open %s", err.Error())
		return
	}
	defer db.Close()
	_, err = db.Exec("create database if not exists test_json")
	if err != nil {
		t.Error(err)
		return
	}
	_, err = db.Exec("drop table if exists test_json.tjson_search")
	if err != nil {
		t.Error(err)
		return
	}
	_, err = db.Exec("create stable if not exists test_json.tjson_search(ts timestamp,value int )tags(t json)")
	if err != nil {
		t.Error(err)
		return
	}
	_, err = db.Exec(`insert into test_json.tjs_1 using test_json.tjson_search tags('{"a":1,"b":"b"}')values (now,1)`)
	if err != nil {
		t.Error(err)
		return
	}
	_, err = db.Exec(`insert into test_json.tjs_2 using test_json.tjson_search tags('{"a":1,"c":"c"}')values (now,2)`)
	if err != nil {
		t.Error(err)
		return
	}
	rows, err := db.Query("select * from test_json.tjson_search where t contains 'a' and t->'b'='b' and value = 1")
	if err != nil {
		t.Error(err)
		return
	}
	counter := 0
	for rows.Next() {
		counter += 1
		row := make([]driver.Value, 3)
		err := rows.Scan(&row[0], &row[1], &row[2])
		if err != nil {
			t.Error(err)
			return
		}
		s := row[2].([]byte)
		if !json.Valid(s) {
			t.Error("invalid json ", string(s))
		}
		t.Logf("%s", string(row[2].([]byte)))
	}
	assert.Equal(t, 1, counter)
}

func TestJsonMatch(t *testing.T) {
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		t.Fatalf("error on:  sql.open %s", err.Error())
		return
	}
	defer db.Close()
	_, err = db.Exec("create database if not exists test_json")
	if err != nil {
		t.Error(err)
		return
	}
	_, err = db.Exec("drop table if exists test_json.tjson_match")
	if err != nil {
		t.Error(err)
		return
	}
	_, err = db.Exec("create stable if not exists test_json.tjson_match(ts timestamp,value int )tags(t json)")
	if err != nil {
		t.Error(err)
		return
	}
	_, err = db.Exec(`insert into test_json.tjm_1 using test_json.tjson_match tags('{"a":1,"b":"b"}')values (now,1)`)
	if err != nil {
		t.Error(err)
		return
	}
	_, err = db.Exec(`insert into test_json.tjm_2 using test_json.tjson_match tags('{"a":1,"c":"c"}')values (now,2)`)
	if err != nil {
		t.Error(err)
		return
	}
	rows, err := db.Query("select * from test_json.tjson_match where t contains 'a' and t->'b' match '.*b.*|.*e.*' and value = 1")
	if err != nil {
		t.Error(err)
		return
	}
	counter := 0
	for rows.Next() {
		counter += 1
		row := make([]driver.Value, 3)
		err := rows.Scan(&row[0], &row[1], &row[2])
		if err != nil {
			t.Error(err)
			return
		}
		s := row[2].([]byte)
		if !json.Valid(s) {
			t.Error("invalid json ", string(s))
		}
		t.Logf("%s", string(row[2].([]byte)))
	}
	assert.Equal(t, 1, counter)
}
