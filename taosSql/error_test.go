package taosSql

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
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
		errstr string
	}{
		{"use invalid_db_name", CODE_MND_INVALID_DB, "Invalid database name"},
		{"create database log", CODE_MND_DB_ALREADY_EXIST, "Database already exists"},
		{"create table log.log (ts timestamp, n int)", CODE_MND_TABLE_ALREADY_EXIST, "Table already exists"},
		{"create table log.ac", CODE_TSC_SQL_SYNTAX_ERROR, "Incomplete SQL statement"},
		{"create table log.ac (ts timestamp, n iii)", CODE_TSC_INVALID_OPERATION, "invalid data type"},
		{"alter table log.dn add tag fqdn binary(128)", CODE_TSC_INVALID_OPERATION, "duplicated column names"},
	}
	for i, c := range cases {
		name := fmt.Sprintf("%02d:%s", i, c.sql)
		t.Run(name, func(t *testing.T) {
			_, err = db.Exec(c.sql)
			if err != nil {
				switch e := err.(type) {
				case *TaosError:
					fmt.Println("TaosError: ", e)
					assert.Equal(t, e.Code, c.code)
					assert.Contains(t, e.ErrStr, c.errstr)
				default:
					t.Fatal("expect a TaosError")
				}
			} else {
				t.Fatal("expect a TaosError")
			}
		})
	}
}
