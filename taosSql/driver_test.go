package taosSql

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"sync"
	"testing"
	"time"
)

// Ensure that all the driver interfaces are implemented
var (
	_ driver.Rows = &binaryRows{}
	_ driver.Rows = &textRows{}
)

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

func runTests(t *testing.T, tests ...func(dbt *DBTest)) {
	db, err := sql.Open(DRIVER_NAME, dataSourceName)
	if err != nil {
		t.Fatalf("error on:  sql.open %s", err.Error())
		return
	}
	defer db.Close()
	dbt := &DBTest{t, db}
	dbt.db.Exec("DROP TABLE IF EXISTS test")
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

func (dbt *DBTest) mustExec(query string, args ...interface{}) (res sql.Result) {
	res, err := dbt.db.Exec(query, args...)
	if err != nil {
		dbt.fail("exec:", query, err)
	}
	return res
}

func (dbt *DBTest) mustQuery(query string, args ...interface{}) (rows *sql.Rows) {
	rows, err := dbt.db.Query(query, args...)
	if err != nil {
		dbt.fail("query:", query, err)
	}
	return rows
}
func TestEmpytQuery(t *testing.T) {
	runTests(t, func(dbt *DBTest) {
		// just a comment, no query
		rows := dbt.mustQuery("--")
		defer rows.Close()
		// will hang before #255
		if rows.Next() {
			dbt.Errorf("next on rows must be false")
		}
	})
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
func TestCRUD(t *testing.T) {
	runTests(t, func(dbt *DBTest) {
		var numOfSubTables = 10
		var numOfItems = 10000
		CreateTables(dbt, numOfSubTables)
		InsertInto(dbt, numOfSubTables, numOfItems)

		// Create Data
		now := time.Now()
		res := dbt.mustExec(fmt.Sprintf("insert into t%d values(%d, %t)", 0, now.UnixNano()/int64(time.Millisecond)-1, false))
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
		type Result struct {
			ts      string
			value   bool
			degress int
		}
		var result Result
		rows := dbt.mustQuery("select * from super")
		for rows.Next() {
			err := rows.Scan(&(result.ts), &(result.value), &(result.degress))
			if err != nil {
				dbt.Error(err.Error())
			}
			dbt.Logf("ts: %s\t val: %t \t tag:%d", result.ts, result.value, result.degress)
		}

		rows.Close()

		dbt.Logf("===============================================")
		rows = dbt.mustQuery("select last_row(*) from super")
		for rows.Next() {
			var value Result
			err := rows.Scan(&(value.ts), &(value.value))
			if err != nil {
				dbt.Error(err.Error())
			}
			dbt.Logf("ts: %s\t val: %t", value.ts, value.value)
		}
		rows.Close()
		dbt.mustExec("drop table if exists super")
	})
}
func TestStmt(t *testing.T) {
	runTests(t, func(dbt *DBTest) {
		var numOfSubTables = 10
		var numOfItems = 1000
		CreateTables(dbt, numOfSubTables)
		InsertInto(dbt, numOfSubTables, numOfItems)
		stmt, err := dbt.db.Prepare("insert into t0 values(?, ?)")
		if err != nil {
			dbt.fail("prepare", "prepar", err)
		}
		now := time.Now()
		stmt.Exec(now.UnixNano()/int64(time.Millisecond), false)
		stmt.Exec(now.UnixNano()/int64(time.Millisecond)+int64(1), false)
	})
}
