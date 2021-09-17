package taosSql

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/taosdata/driver-go/v2/errors"
)

func TestErrorCode(t *testing.T) {
	db, err := sql.Open("taosSql", dataSourceName)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		sql    string
		code   int32
		errStr string
	}{
		{"use invalid_db_name", errors.MND_INVALID_DB, "Invalid database name"},
		{"create database log", errors.MND_DB_ALREADY_EXIST, "Database already exists"},
		{"create table log.log (ts timestamp, n int)", errors.MND_TABLE_ALREADY_EXIST, "Table already exists"},
		{"create table log.ac", errors.TSC_SQL_SYNTAX_ERROR, "Incomplete SQL statement"},
		{"create table log.ac (ts timestamp, n iii)", errors.TSC_INVALID_OPERATION, "invalid data type"},
		{"alter table log.dn add tag fqdn binary(128)", errors.TSC_INVALID_OPERATION, "duplicated column names"},
	}
	for i, c := range cases {
		name := fmt.Sprintf("%02d:%s", i, c.sql)
		t.Run(name, func(t *testing.T) {
			_, err = db.Exec(c.sql)
			if err != nil {
				switch e := err.(type) {
				case *errors.TaosError:
					assert.Equal(t, e.Code, c.code)
					assert.Contains(t, e.ErrStr, c.errStr)
				default:
					t.Fatal("expect a TaosError")
				}
			} else {
				t.Fatal("expect a TaosError")
			}
		})
	}
}
