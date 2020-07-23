package taosSql

import (
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// Ensure that all the driver interfaces are implemented

var (
	DRIVER_NAME    = "taosSql"
	user           = "root"
	password       = "taosdata"
	host           = "127.0.0.1"
	port           = 6030
	dbName         = "test"
	dataSourceName = fmt.Sprintf("%s:%s@/tcp(%s:%d)/%s?interpolateParams=true", user, password, host, port, dbName)
	total          = 0
	lock           sync.Mutex
	nThreads       = 10
	nRequests      = 10
	profile        = "CPU.profile"
)

type DBTest struct {
	*testing.T
	db *sql.DB
}

func CreateTables(dbt *DBTest, numOfSubTab int) {
	dbt.mustExec("drop table if exists super")
	dbt.mustExec("CREATE TABLE super (ts timestamp, value BOOL) tags (degress int)")
	for i := 0; i < numOfSubTab; i++ {
		dbt.mustExec(fmt.Sprintf("create table t%d using super tags(%d)", i%10, i))
	}
}
func InsertInto(dbt *DBTest, numOfSubTab, numOfItems int) {
	now := time.Now()
	t := now.Add(-100 * time.Hour)
	for i := 0; i < numOfItems; i++ {
		dbt.mustExec(fmt.Sprintf("insert into t%d values(%d, %t)", i%numOfSubTab, t.UnixNano()/int64(time.Millisecond)+int64(i), i%2 == 0))
	}
}

type Result struct {
	ts      string
	value   bool
	degress int
}

func runTests(t *testing.T, tests ...func(dbt *DBTest)) {
	db, err := sql.Open(DRIVER_NAME, dataSourceName)
	if err != nil {
		t.Fatalf("error on:  sql.open %s", err.Error())
		return
	}
	defer db.Close()
	dbt := &DBTest{t, db}
	// prepare data
	dbt.db.Exec("DROP TABLE IF EXISTS test")
	var numOfSubTables = 10
	var numOfItems = 10000
	CreateTables(dbt, numOfSubTables)
	InsertInto(dbt, numOfSubTables, numOfItems)
	for _, test := range tests {
		test(dbt)
		dbt.db.Exec("DROP TABLE IF EXISTS test")
	}
}
func (dbt *DBTest) fail(method, query string, err error) {
	if len(query) > 300 {
		query = "[query too large to print]"
	}
	dbt.Fatalf("error on %s %s: %s", method, query, err.Error())
}

func (dbt *DBTest) mustExec(query string, args ...interface{}) (res sql.Result, err error) {
	res, err = dbt.db.Exec(query, args...)
	return
}

func (dbt *DBTest) mustQuery(query string, args ...interface{}) (rows *sql.Rows, err error) {
	rows, err = dbt.db.Query(query, args...)
	return
}
func TestEmpytQuery(t *testing.T) {
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
	fp      = func(dbt *DBTest, query string, exec bool, eerr error, expected int64) int64 {
		var ret int64 = 0
		if exec == false {
			rows, err := dbt.mustQuery(query)
			if eerr == userErr && err != nil {
				return ret
			}
			if err != nil {
				dbt.Errorf("%s is no respected, err: %s", query, err.Error())
				return ret
			} else {
				var count int64 = 0
				for rows.Next() {
					var row Result
					if err := rows.Scan(&(row.ts), &(row.value)); err != nil {
						dbt.Error(err.Error())
						return ret
					}
					count = count + 1
				}
				rows.Close()
				ret = count
				if expected != -1 && count != expected {
					dbt.Errorf("%s is no respected, err: %s", query, errors.New("result is not repected"))
				}
			}
		} else {
			res, err := dbt.mustExec(query)
			if err != eerr {
				dbt.Fatalf("%s is no respected, err: %s", query, err.Error())
			} else {
				count, err := res.RowsAffected()
				if err != nil {
					dbt.Fatalf("%s is no respected, err: %s", query, err.Error())
				}
				if expected != -1 && count != expected {
					dbt.Fatalf("%s is no respected, err: %s", query, errors.New("result is not repected"))
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
			&Obj{fmt.Sprintf("insert into t%d values(%d, %t)", 0, now.UnixNano()/int64(time.Millisecond)-1, false), nil, true, fp, int64(1)})
		tests = append(tests,
			&Obj{fmt.Sprintf("insert into t%d values(%d, %t)", 0, now.UnixNano()/int64(time.Millisecond)-1, false), nil, true, fp, int64(1)})
		tests = append(tests,
			&Obj{fmt.Sprintf("select last_row(*) from t%d", 0), nil, false, fp, int64(1)})
		tests = append(tests,
			&Obj{fmt.Sprintf("select first(*) from t%d", 0), nil, false, fp, int64(1)})
		tests = append(tests,
			&Obj{fmt.Sprintf("select errror"), userErr, false, fp, int64(1)})
		tests = append(tests,
			&Obj{fmt.Sprintf("select * from t%d", 0), nil, false, fp, int64(-1)})
		tests = append(tests,
			&Obj{fmt.Sprintf("select * from t%d", 0), nil, false, fp, int64(-1)})

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
		res, err := dbt.mustExec(fmt.Sprintf("insert into t%d values(%d, %t)", 0, now.UnixNano()/int64(time.Millisecond)-1, false))
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
		if err != nil {
			dbt.Fatalf("res.LastInsertId() returned error: %s", err.Error())
		}
		if id != 0 {
			dbt.Fatalf("expected InsertId 0, got %d", id)
		}

		// Read
		rows, err := dbt.mustQuery("select * from super")
		if err != nil {
			dbt.Fatalf("select failed")
		}
		for rows.Next() {
			var row Result
			err := rows.Scan(&(row.ts), &(row.value), &(row.degress))
			if err != nil {
				dbt.Error(err.Error())
			}
			dbt.Logf("ts: %s\t val: %t \t tag:%d", row.ts, row.value, row.degress)
		}
		rows.Close()

		rows, err = dbt.mustQuery("select last_row(*) from super")
		if err != nil {
			dbt.Fatalf("select last_row failed")
		} else {
			for rows.Next() {
				var value Result
				err := rows.Scan(&(value.ts), &(value.value))
				if err != nil {
					dbt.Error(err.Error())
				}
				dbt.Logf("ts: %s\t val: %t", value.ts, value.value)
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
		stmt, err := dbt.db.Prepare("insert into t0 values(?, ?)")
		if err != nil {
			dbt.fail("prepare", "prepare", err)
		}
		now := time.Now()
		stmt.Exec(now.UnixNano()/int64(time.Millisecond), false)
		stmt.Exec(now.UnixNano()/int64(time.Millisecond)+int64(1), false)
		stmt.Close()
	})
}
