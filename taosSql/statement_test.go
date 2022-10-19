package taosSql

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestStmtExec(t *testing.T) {
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		t.Error(err)
		return
	}
	defer db.Close()
	defer func() {
		_, err = db.Exec("drop database if exists test_stmt_driver")
		if err != nil {
			t.Error(err)
			return
		}
	}()
	_, err = db.Exec("create database if not exists test_stmt_driver")
	if err != nil {
		t.Error(err)
		return
	}
	_, err = db.Exec("create table if not exists test_stmt_driver.ct(ts timestamp," +
		"c1 bool," +
		"c2 tinyint," +
		"c3 smallint," +
		"c4 int," +
		"c5 bigint," +
		"c6 tinyint unsigned," +
		"c7 smallint unsigned," +
		"c8 int unsigned," +
		"c9 bigint unsigned," +
		"c10 float," +
		"c11 double," +
		"c12 binary(20)," +
		"c13 nchar(20)" +
		")")
	if err != nil {
		t.Error(err)
		return
	}
	stmt, err := db.Prepare("insert into test_stmt_driver.ct values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)")

	if err != nil {
		t.Error(err)
		return
	}
	result, err := stmt.Exec(time.Now(), 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, "binary", "nchar")
	if err != nil {
		t.Error(err)
		return
	}
	affected, err := result.RowsAffected()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), affected)
}

//func TestStmtQuery(t *testing.T) {
//	db, err := sql.Open(driverName, dataSourceName)
//	if err != nil {
//		t.Error(err)
//		return
//	}
//	defer db.Close()
//	defer func() {
//		db.Exec("drop database if exists test_stmt_driver")
//	}()
//	_, err = db.Exec("create database if not exists test_stmt_driver")
//	if err != nil {
//		t.Error(err)
//		return
//	}
//	_, err = db.Exec("create table if not exists test_stmt_driver.ct(ts timestamp," +
//		"c1 bool," +
//		"c2 tinyint," +
//		"c3 smallint," +
//		"c4 int," +
//		"c5 bigint," +
//		"c6 tinyint unsigned," +
//		"c7 smallint unsigned," +
//		"c8 int unsigned," +
//		"c9 bigint unsigned," +
//		"c10 float," +
//		"c11 double," +
//		"c12 binary(20)," +
//		"c13 nchar(20)" +
//		")")
//	if err != nil {
//		t.Error(err)
//		return
//	}
//	stmt, err := db.Prepare("insert into test_stmt_driver.ct values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
//	if err != nil {
//		t.Error(err)
//		return
//	}
//	now := time.Now()
//	result, err := stmt.Exec(now, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, "binary", "nchar")
//	if err != nil {
//		t.Error(err)
//		return
//	}
//	affected, err := result.RowsAffected()
//	if err != nil {
//		t.Error(err)
//		return
//	}
//	assert.Equal(t, int64(1), affected)
//	stmt.Close()
//	stmt, err = db.Prepare("select * from test_stmt_driver.ct where ts = ?")
//	if err != nil {
//		t.Error(err)
//		return
//	}
//	rows, err := stmt.Query(now)
//	if err != nil {
//		t.Error(err)
//		return
//	}
//	columns, err := rows.Columns()
//	if err != nil {
//		t.Error(err)
//		return
//	}
//	assert.Equal(t, []string{"ts", "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11", "c12", "c13"}, columns)
//	count := 0
//	for rows.Next() {
//		count += 1
//		var (
//			ts  time.Time
//			c1  bool
//			c2  int8
//			c3  int16
//			c4  int32
//			c5  int64
//			c6  uint8
//			c7  uint16
//			c8  uint32
//			c9  uint64
//			c10 float32
//			c11 float64
//			c12 string
//			c13 string
//		)
//		err = rows.Scan(&ts,
//			&c1,
//			&c2,
//			&c3,
//			&c4,
//			&c5,
//			&c6,
//			&c7,
//			&c8,
//			&c9,
//			&c10,
//			&c11,
//			&c12,
//			&c13)
//		assert.NoError(t, err)
//		assert.Equal(t, now.UnixNano()/1e6, ts.UnixNano()/1e6)
//		assert.Equal(t, true, c1)
//		assert.Equal(t, int8(2), c2)
//		assert.Equal(t, int16(3), c3)
//		assert.Equal(t, int32(4), c4)
//		assert.Equal(t, int64(5), c5)
//		assert.Equal(t, uint8(6), c6)
//		assert.Equal(t, uint16(7), c7)
//		assert.Equal(t, uint32(8), c8)
//		assert.Equal(t, uint64(9), c9)
//		assert.Equal(t, float32(10), c10)
//		assert.Equal(t, float64(11), c11)
//		assert.Equal(t, "binary", c12)
//		assert.Equal(t, "nchar", c13)
//	}
//	assert.Equal(t, 1, count)
//}

func TestStmtConvertExec(t *testing.T) {
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		t.Error(err)
		return
	}
	defer db.Close()
	_, err = db.Exec("drop database if exists test_stmt_driver_convert")
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		_, err = db.Exec("drop database if exists test_stmt_driver_convert")
		if err != nil {
			t.Error(err)
			return
		}
	}()
	_, err = db.Exec("create database test_stmt_driver_convert")
	if err != nil {
		t.Error(err)
		return
	}
	_, err = db.Exec("use test_stmt_driver_convert")
	if err != nil {
		t.Error(err)
		return
	}
	now := time.Now().Format(time.RFC3339Nano)
	tests := []struct {
		name        string
		tbType      string
		pos         string
		bind        []interface{}
		expectValue interface{}
		expectError bool
	}{
		//bool
		{
			name:        "bool_null",
			tbType:      "ts timestamp,v bool",
			pos:         "?,?",
			bind:        []interface{}{now, nil},
			expectValue: nil,
		},
		{
			name:        "bool_err",
			tbType:      "ts timestamp,v bool",
			pos:         "?,?",
			bind:        []interface{}{now, []int{123}},
			expectValue: nil,
			expectError: true,
		},
		{
			name:        "bool_bool_true",
			tbType:      "ts timestamp,v bool",
			pos:         "?,?",
			bind:        []interface{}{now, true},
			expectValue: true,
		},
		{
			name:        "bool_bool_false",
			tbType:      "ts timestamp,v bool",
			pos:         "?,?",
			bind:        []interface{}{now, false},
			expectValue: false,
		},
		{
			name:        "bool_float_true",
			tbType:      "ts timestamp,v bool",
			pos:         "?,?",
			bind:        []interface{}{now, float32(1)},
			expectValue: true,
		},
		{
			name:        "bool_float_false",
			tbType:      "ts timestamp,v bool",
			pos:         "?,?",
			bind:        []interface{}{now, float32(0)},
			expectValue: false,
		},
		{
			name:        "bool_int_true",
			tbType:      "ts timestamp,v bool",
			pos:         "?,?",
			bind:        []interface{}{now, int32(1)},
			expectValue: true,
		},
		{
			name:        "bool_int_false",
			tbType:      "ts timestamp,v bool",
			pos:         "?,?",
			bind:        []interface{}{now, int32(0)},
			expectValue: false,
		},
		{
			name:        "bool_uint_true",
			tbType:      "ts timestamp,v bool",
			pos:         "?,?",
			bind:        []interface{}{now, uint32(1)},
			expectValue: true,
		},
		{
			name:        "bool_uint_false",
			tbType:      "ts timestamp,v bool",
			pos:         "?,?",
			bind:        []interface{}{now, uint32(0)},
			expectValue: false,
		},
		{
			name:        "bool_string_true",
			tbType:      "ts timestamp,v bool",
			pos:         "?,?",
			bind:        []interface{}{now, "true"},
			expectValue: true,
		},
		{
			name:        "bool_string_false",
			tbType:      "ts timestamp,v bool",
			pos:         "?,?",
			bind:        []interface{}{now, "false"},
			expectValue: false,
		},
		//tiny int
		{
			name:        "tiny_nil",
			tbType:      "ts timestamp,v tinyint",
			pos:         "?,?",
			bind:        []interface{}{now, nil},
			expectValue: nil,
		},
		{
			name:        "tiny_err",
			tbType:      "ts timestamp,v tinyint",
			pos:         "?,?",
			bind:        []interface{}{now, []int{1}},
			expectValue: nil,
			expectError: true,
		},
		{
			name:        "tiny_bool_1",
			tbType:      "ts timestamp,v tinyint",
			pos:         "?,?",
			bind:        []interface{}{now, true},
			expectValue: int8(1),
		},
		{
			name:        "tiny_bool_0",
			tbType:      "ts timestamp,v tinyint",
			pos:         "?,?",
			bind:        []interface{}{now, false},
			expectValue: int8(0),
		},
		{
			name:        "tiny_float_1",
			tbType:      "ts timestamp,v tinyint",
			pos:         "?,?",
			bind:        []interface{}{now, float32(1)},
			expectValue: int8(1),
		},
		{
			name:        "tiny_int_1",
			tbType:      "ts timestamp,v tinyint",
			pos:         "?,?",
			bind:        []interface{}{now, int(1)},
			expectValue: int8(1),
		},
		{
			name:        "tiny_uint_1",
			tbType:      "ts timestamp,v tinyint",
			pos:         "?,?",
			bind:        []interface{}{now, uint(1)},
			expectValue: int8(1),
		},
		{
			name:        "tiny_string_1",
			tbType:      "ts timestamp,v tinyint",
			pos:         "?,?",
			bind:        []interface{}{now, "1"},
			expectValue: int8(1),
		},
		// small int
		{
			name:        "small_nil",
			tbType:      "ts timestamp,v smallint",
			pos:         "?,?",
			bind:        []interface{}{now, nil},
			expectValue: nil,
		},
		{
			name:        "small_err",
			tbType:      "ts timestamp,v smallint",
			pos:         "?,?",
			bind:        []interface{}{now, []int{1}},
			expectValue: nil,
			expectError: true,
		},
		{
			name:        "small_bool_1",
			tbType:      "ts timestamp,v smallint",
			pos:         "?,?",
			bind:        []interface{}{now, true},
			expectValue: int16(1),
		},
		{
			name:        "small_bool_0",
			tbType:      "ts timestamp,v smallint",
			pos:         "?,?",
			bind:        []interface{}{now, false},
			expectValue: int16(0),
		},
		{
			name:        "small_float_1",
			tbType:      "ts timestamp,v smallint",
			pos:         "?,?",
			bind:        []interface{}{now, float32(1)},
			expectValue: int16(1),
		},
		{
			name:        "small_int_1",
			tbType:      "ts timestamp,v smallint",
			pos:         "?,?",
			bind:        []interface{}{now, int(1)},
			expectValue: int16(1),
		},
		{
			name:        "small_uint_1",
			tbType:      "ts timestamp,v smallint",
			pos:         "?,?",
			bind:        []interface{}{now, uint(1)},
			expectValue: int16(1),
		},
		{
			name:        "small_string_1",
			tbType:      "ts timestamp,v smallint",
			pos:         "?,?",
			bind:        []interface{}{now, "1"},
			expectValue: int16(1),
		},
		// int
		{
			name:        "int_nil",
			tbType:      "ts timestamp,v int",
			pos:         "?,?",
			bind:        []interface{}{now, nil},
			expectValue: nil,
		},
		{
			name:        "int_err",
			tbType:      "ts timestamp,v int",
			pos:         "?,?",
			bind:        []interface{}{now, []int{1}},
			expectValue: nil,
			expectError: true,
		},
		{
			name:        "int_bool_1",
			tbType:      "ts timestamp,v int",
			pos:         "?,?",
			bind:        []interface{}{now, true},
			expectValue: int32(1),
		},
		{
			name:        "int_bool_0",
			tbType:      "ts timestamp,v int",
			pos:         "?,?",
			bind:        []interface{}{now, false},
			expectValue: int32(0),
		},
		{
			name:        "int_float_1",
			tbType:      "ts timestamp,v int",
			pos:         "?,?",
			bind:        []interface{}{now, float32(1)},
			expectValue: int32(1),
		},
		{
			name:        "int_int_1",
			tbType:      "ts timestamp,v int",
			pos:         "?,?",
			bind:        []interface{}{now, int(1)},
			expectValue: int32(1),
		},
		{
			name:        "int_uint_1",
			tbType:      "ts timestamp,v int",
			pos:         "?,?",
			bind:        []interface{}{now, uint(1)},
			expectValue: int32(1),
		},
		{
			name:        "int_string_1",
			tbType:      "ts timestamp,v int",
			pos:         "?,?",
			bind:        []interface{}{now, "1"},
			expectValue: int32(1),
		},
		// big int
		{
			name:        "big_nil",
			tbType:      "ts timestamp,v bigint",
			pos:         "?,?",
			bind:        []interface{}{now, nil},
			expectValue: nil,
		},
		{
			name:        "big_err",
			tbType:      "ts timestamp,v bigint",
			pos:         "?,?",
			bind:        []interface{}{now, []int{1}},
			expectValue: nil,
			expectError: true,
		},
		{
			name:        "big_bool_1",
			tbType:      "ts timestamp,v bigint",
			pos:         "?,?",
			bind:        []interface{}{now, true},
			expectValue: int64(1),
		},
		{
			name:        "big_bool_0",
			tbType:      "ts timestamp,v bigint",
			pos:         "?,?",
			bind:        []interface{}{now, false},
			expectValue: int64(0),
		},
		{
			name:        "big_float_1",
			tbType:      "ts timestamp,v bigint",
			pos:         "?,?",
			bind:        []interface{}{now, float32(1)},
			expectValue: int64(1),
		},
		{
			name:        "big_int_1",
			tbType:      "ts timestamp,v bigint",
			pos:         "?,?",
			bind:        []interface{}{now, int(1)},
			expectValue: int64(1),
		},
		{
			name:        "big_uint_1",
			tbType:      "ts timestamp,v bigint",
			pos:         "?,?",
			bind:        []interface{}{now, uint(1)},
			expectValue: int64(1),
		},
		{
			name:        "big_string_1",
			tbType:      "ts timestamp,v bigint",
			pos:         "?,?",
			bind:        []interface{}{now, "1"},
			expectValue: int64(1),
		},
		// float
		{
			name:        "float_nil",
			tbType:      "ts timestamp,v float",
			pos:         "?,?",
			bind:        []interface{}{now, nil},
			expectValue: nil,
		},
		{
			name:        "float_err",
			tbType:      "ts timestamp,v float",
			pos:         "?,?",
			bind:        []interface{}{now, []int{1}},
			expectValue: nil,
			expectError: true,
		},
		{
			name:        "float_bool_1",
			tbType:      "ts timestamp,v float",
			pos:         "?,?",
			bind:        []interface{}{now, true},
			expectValue: float32(1),
		},
		{
			name:        "float_bool_0",
			tbType:      "ts timestamp,v float",
			pos:         "?,?",
			bind:        []interface{}{now, false},
			expectValue: float32(0),
		},
		{
			name:        "float_float_1",
			tbType:      "ts timestamp,v float",
			pos:         "?,?",
			bind:        []interface{}{now, float32(1)},
			expectValue: float32(1),
		},
		{
			name:        "float_int_1",
			tbType:      "ts timestamp,v float",
			pos:         "?,?",
			bind:        []interface{}{now, int(1)},
			expectValue: float32(1),
		},
		{
			name:        "float_uint_1",
			tbType:      "ts timestamp,v float",
			pos:         "?,?",
			bind:        []interface{}{now, uint(1)},
			expectValue: float32(1),
		},
		{
			name:        "float_string_1",
			tbType:      "ts timestamp,v float",
			pos:         "?,?",
			bind:        []interface{}{now, "1"},
			expectValue: float32(1),
		},
		//double
		{
			name:        "double_nil",
			tbType:      "ts timestamp,v double",
			pos:         "?,?",
			bind:        []interface{}{now, nil},
			expectValue: nil,
		},
		{
			name:        "double_err",
			tbType:      "ts timestamp,v double",
			pos:         "?,?",
			bind:        []interface{}{now, []int{1}},
			expectValue: nil,
			expectError: true,
		},
		{
			name:        "double_bool_1",
			tbType:      "ts timestamp,v double",
			pos:         "?,?",
			bind:        []interface{}{now, true},
			expectValue: float64(1),
		},
		{
			name:        "double_bool_0",
			tbType:      "ts timestamp,v double",
			pos:         "?,?",
			bind:        []interface{}{now, false},
			expectValue: float64(0),
		},
		{
			name:        "double_double_1",
			tbType:      "ts timestamp,v double",
			pos:         "?,?",
			bind:        []interface{}{now, float32(1)},
			expectValue: float64(1),
		},
		{
			name:        "double_int_1",
			tbType:      "ts timestamp,v double",
			pos:         "?,?",
			bind:        []interface{}{now, int(1)},
			expectValue: float64(1),
		},
		{
			name:        "double_uint_1",
			tbType:      "ts timestamp,v double",
			pos:         "?,?",
			bind:        []interface{}{now, uint(1)},
			expectValue: float64(1),
		},
		{
			name:        "double_string_1",
			tbType:      "ts timestamp,v double",
			pos:         "?,?",
			bind:        []interface{}{now, "1"},
			expectValue: float64(1),
		},

		//tiny int unsigned
		{
			name:        "utiny_nil",
			tbType:      "ts timestamp,v tinyint unsigned",
			pos:         "?,?",
			bind:        []interface{}{now, nil},
			expectValue: nil,
		},
		{
			name:        "utiny_err",
			tbType:      "ts timestamp,v tinyint unsigned",
			pos:         "?,?",
			bind:        []interface{}{now, []int{1}},
			expectValue: nil,
			expectError: true,
		},
		{
			name:        "utiny_bool_1",
			tbType:      "ts timestamp,v tinyint unsigned",
			pos:         "?,?",
			bind:        []interface{}{now, true},
			expectValue: uint8(1),
		},
		{
			name:        "utiny_bool_0",
			tbType:      "ts timestamp,v tinyint unsigned",
			pos:         "?,?",
			bind:        []interface{}{now, false},
			expectValue: uint8(0),
		},
		{
			name:        "utiny_float_1",
			tbType:      "ts timestamp,v tinyint unsigned",
			pos:         "?,?",
			bind:        []interface{}{now, float32(1)},
			expectValue: uint8(1),
		},
		{
			name:        "utiny_int_1",
			tbType:      "ts timestamp,v tinyint unsigned",
			pos:         "?,?",
			bind:        []interface{}{now, int(1)},
			expectValue: uint8(1),
		},
		{
			name:        "utiny_uint_1",
			tbType:      "ts timestamp,v tinyint unsigned",
			pos:         "?,?",
			bind:        []interface{}{now, uint(1)},
			expectValue: uint8(1),
		},
		{
			name:        "utiny_string_1",
			tbType:      "ts timestamp,v tinyint unsigned",
			pos:         "?,?",
			bind:        []interface{}{now, "1"},
			expectValue: uint8(1),
		},
		// small int unsigned
		{
			name:        "usmall_nil",
			tbType:      "ts timestamp,v smallint unsigned",
			pos:         "?,?",
			bind:        []interface{}{now, nil},
			expectValue: nil,
		},
		{
			name:        "usmall_err",
			tbType:      "ts timestamp,v smallint unsigned",
			pos:         "?,?",
			bind:        []interface{}{now, []int{1}},
			expectValue: nil,
			expectError: true,
		},
		{
			name:        "usmall_bool_1",
			tbType:      "ts timestamp,v smallint unsigned",
			pos:         "?,?",
			bind:        []interface{}{now, true},
			expectValue: uint16(1),
		},
		{
			name:        "usmall_bool_0",
			tbType:      "ts timestamp,v smallint unsigned",
			pos:         "?,?",
			bind:        []interface{}{now, false},
			expectValue: uint16(0),
		},
		{
			name:        "usmall_float_1",
			tbType:      "ts timestamp,v smallint unsigned",
			pos:         "?,?",
			bind:        []interface{}{now, float32(1)},
			expectValue: uint16(1),
		},
		{
			name:        "usmall_int_1",
			tbType:      "ts timestamp,v smallint unsigned",
			pos:         "?,?",
			bind:        []interface{}{now, int(1)},
			expectValue: uint16(1),
		},
		{
			name:        "usmall_uint_1",
			tbType:      "ts timestamp,v smallint unsigned",
			pos:         "?,?",
			bind:        []interface{}{now, uint(1)},
			expectValue: uint16(1),
		},
		{
			name:        "usmall_string_1",
			tbType:      "ts timestamp,v smallint unsigned",
			pos:         "?,?",
			bind:        []interface{}{now, "1"},
			expectValue: uint16(1),
		},
		// int unsigned
		{
			name:        "uint_nil",
			tbType:      "ts timestamp,v int unsigned",
			pos:         "?,?",
			bind:        []interface{}{now, nil},
			expectValue: nil,
		},
		{
			name:        "uint_err",
			tbType:      "ts timestamp,v int unsigned",
			pos:         "?,?",
			bind:        []interface{}{now, []int{1}},
			expectValue: nil,
			expectError: true,
		},
		{
			name:        "uint_bool_1",
			tbType:      "ts timestamp,v int unsigned",
			pos:         "?,?",
			bind:        []interface{}{now, true},
			expectValue: uint32(1),
		},
		{
			name:        "uint_bool_0",
			tbType:      "ts timestamp,v int unsigned",
			pos:         "?,?",
			bind:        []interface{}{now, false},
			expectValue: uint32(0),
		},
		{
			name:        "uint_float_1",
			tbType:      "ts timestamp,v int unsigned",
			pos:         "?,?",
			bind:        []interface{}{now, float32(1)},
			expectValue: uint32(1),
		},
		{
			name:        "uint_int_1",
			tbType:      "ts timestamp,v int unsigned",
			pos:         "?,?",
			bind:        []interface{}{now, int(1)},
			expectValue: uint32(1),
		},
		{
			name:        "uint_uint_1",
			tbType:      "ts timestamp,v int unsigned",
			pos:         "?,?",
			bind:        []interface{}{now, uint(1)},
			expectValue: uint32(1),
		},
		{
			name:        "uint_string_1",
			tbType:      "ts timestamp,v int unsigned",
			pos:         "?,?",
			bind:        []interface{}{now, "1"},
			expectValue: uint32(1),
		},
		// big int unsigned
		{
			name:        "ubig_nil",
			tbType:      "ts timestamp,v bigint unsigned",
			pos:         "?,?",
			bind:        []interface{}{now, nil},
			expectValue: nil,
		},
		{
			name:        "ubig_err",
			tbType:      "ts timestamp,v bigint unsigned",
			pos:         "?,?",
			bind:        []interface{}{now, []int{1}},
			expectValue: nil,
			expectError: true,
		},
		{
			name:        "ubig_bool_1",
			tbType:      "ts timestamp,v bigint unsigned",
			pos:         "?,?",
			bind:        []interface{}{now, true},
			expectValue: uint64(1),
		},
		{
			name:        "ubig_bool_0",
			tbType:      "ts timestamp,v bigint unsigned",
			pos:         "?,?",
			bind:        []interface{}{now, false},
			expectValue: uint64(0),
		},
		{
			name:        "ubig_float_1",
			tbType:      "ts timestamp,v bigint unsigned",
			pos:         "?,?",
			bind:        []interface{}{now, float32(1)},
			expectValue: uint64(1),
		},
		{
			name:        "ubig_int_1",
			tbType:      "ts timestamp,v bigint unsigned",
			pos:         "?,?",
			bind:        []interface{}{now, int(1)},
			expectValue: uint64(1),
		},
		{
			name:        "ubig_uint_1",
			tbType:      "ts timestamp,v bigint unsigned",
			pos:         "?,?",
			bind:        []interface{}{now, uint(1)},
			expectValue: uint64(1),
		},
		{
			name:        "ubig_string_1",
			tbType:      "ts timestamp,v bigint unsigned",
			pos:         "?,?",
			bind:        []interface{}{now, "1"},
			expectValue: uint64(1),
		},
		//binary
		{
			name:        "binary_nil",
			tbType:      "ts timestamp,v binary(24)",
			pos:         "?,?",
			bind:        []interface{}{now, nil},
			expectValue: nil,
		},
		{
			name:        "binary_err",
			tbType:      "ts timestamp,v binary(24)",
			pos:         "?,?",
			bind:        []interface{}{now, []int{1}},
			expectValue: nil,
			expectError: true,
		},
		{
			name:        "binary_string_chinese",
			tbType:      "ts timestamp,v binary(24)",
			pos:         "?,?",
			bind:        []interface{}{now, "中文"},
			expectValue: "中文",
		},
		{
			name:        "binary_bytes_chinese",
			tbType:      "ts timestamp,v binary(24)",
			pos:         "?,?",
			bind:        []interface{}{now, []byte("中文")},
			expectValue: "中文",
		},
		//nchar
		{
			name:        "nchar_nil",
			tbType:      "ts timestamp,v nchar(24)",
			pos:         "?,?",
			bind:        []interface{}{now, nil},
			expectValue: nil,
		},
		{
			name:        "nchar_err",
			tbType:      "ts timestamp,v nchar(24)",
			pos:         "?,?",
			bind:        []interface{}{now, []int{1}},
			expectValue: nil,
			expectError: true,
		},
		{
			name:        "binary_string_chinese",
			tbType:      "ts timestamp,v nchar(24)",
			pos:         "?,?",
			bind:        []interface{}{now, "中文"},
			expectValue: "中文",
		},
		{
			name:        "binary_bytes_chinese",
			tbType:      "ts timestamp,v nchar(24)",
			pos:         "?,?",
			bind:        []interface{}{now, []byte("中文")},
			expectValue: "中文",
		},
		// timestamp
		{
			name:        "ts_nil",
			tbType:      "ts timestamp,v timestamp",
			pos:         "?,?",
			bind:        []interface{}{now, nil},
			expectValue: nil,
		},
		{
			name:        "ts_err",
			tbType:      "ts timestamp,v timestamp",
			pos:         "?,?",
			bind:        []interface{}{now, []int{1}},
			expectValue: nil,
			expectError: true,
		},
		{
			name:        "ts_time_1",
			tbType:      "ts timestamp,v timestamp",
			pos:         "?,?",
			bind:        []interface{}{now, time.Unix(0, 1e6)},
			expectValue: time.Unix(0, 1e6),
		},
		{
			name:        "ts_float_1",
			tbType:      "ts timestamp,v timestamp",
			pos:         "?,?",
			bind:        []interface{}{now, float32(1)},
			expectValue: time.Unix(0, 1e6),
		},
		{
			name:        "ts_int_1",
			tbType:      "ts timestamp,v timestamp",
			pos:         "?,?",
			bind:        []interface{}{now, int(1)},
			expectValue: time.Unix(0, 1e6),
		},
		{
			name:        "ts_uint_1",
			tbType:      "ts timestamp,v timestamp",
			pos:         "?,?",
			bind:        []interface{}{now, uint(1)},
			expectValue: time.Unix(0, 1e6),
		},
		{
			name:        "ts_string_1",
			tbType:      "ts timestamp,v timestamp",
			pos:         "?,?",
			bind:        []interface{}{now, "1970-01-01T00:00:00.001Z"},
			expectValue: time.Unix(0, 1e6),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tbName := fmt.Sprintf("test_%s", tt.name)
			tbType := tt.tbType
			drop := fmt.Sprintf("drop table if exists %s", tbName)
			create := fmt.Sprintf("create table if not exists %s(%s)", tbName, tbType)
			pos := tt.pos
			sql := fmt.Sprintf("insert into %s values(%s)", tbName, pos)
			var err error
			if _, err = db.Exec(drop); err != nil {
				t.Error(err)
				return
			}
			if _, err = db.Exec(create); err != nil {
				t.Error(err)
				return
			}
			stmt, err := db.Prepare(sql)
			if err != nil {
				t.Error(err)
				return
			}
			result, err := stmt.Exec(tt.bind...)
			if tt.expectError {
				assert.NotNil(t, err)
				stmt.Close()
				return
			}
			if err != nil {
				t.Error(err)
				return
			}
			affected, err := result.RowsAffected()
			if err != nil {
				t.Error(err)
				return
			}
			assert.Equal(t, int64(1), affected)
			rows, err := db.Query(fmt.Sprintf("select v from %s", tbName))
			if err != nil {
				t.Error(err)
				return
			}
			var data []driver.Value
			tts, err := rows.ColumnTypes()
			if err != nil {
				t.Error(err)
				return
			}
			typesL := make([]reflect.Type, 1)
			for i, tp := range tts {
				st := tp.ScanType()
				if st == nil {
					t.Errorf("scantype is null for column %q", tp.Name())
					continue
				}
				typesL[i] = st
			}
			for rows.Next() {
				values := make([]interface{}, 1)
				for i := range values {
					values[i] = reflect.New(typesL[i]).Interface()
				}
				err = rows.Scan(values...)
				if err != nil {
					t.Error(err)
					return
				}
				v, err := values[0].(driver.Valuer).Value()
				if err != nil {
					t.Error(err)
				}
				data = append(data, v)
			}
			if len(data) != 1 {
				t.Errorf("expect %d got %d", 1, len(data))
				return
			}
			if data[0] != tt.expectValue {
				t.Errorf("expect %v got %v", tt.expectValue, data[0])
				return
			}
		})
	}
}

//func TestStmtConvertQuery(t *testing.T) {
//	db, err := sql.Open(driverName, dataSourceName)
//	if err != nil {
//		t.Error(err)
//		return
//	}
//	defer db.Close()
//	_, err = db.Exec("drop database if exists test_stmt_driver_convert_q")
//	if err != nil {
//		t.Error(err)
//		return
//	}
//	defer func() {
//		_, err = db.Exec("drop database if exists test_stmt_driver_convert_q")
//		if err != nil {
//			t.Error(err)
//			return
//		}
//	}()
//	_, err = db.Exec("create database test_stmt_driver_convert_q")
//	if err != nil {
//		t.Error(err)
//		return
//	}
//	_, err = db.Exec("use test_stmt_driver_convert_q")
//	if err != nil {
//		t.Error(err)
//		return
//	}
//	_, err = db.Exec("create table t0 (ts timestamp," +
//		"c1 bool," +
//		"c2 tinyint," +
//		"c3 smallint," +
//		"c4 int," +
//		"c5 bigint," +
//		"c6 tinyint unsigned," +
//		"c7 smallint unsigned," +
//		"c8 int unsigned," +
//		"c9 bigint unsigned," +
//		"c10 float," +
//		"c11 double," +
//		"c12 binary(20)," +
//		"c13 nchar(20)" +
//		")")
//	if err != nil {
//		t.Error(err)
//		return
//	}
//	now := time.Now()
//	after1s := now.Add(time.Second)
//	_, err = db.Exec(fmt.Sprintf("insert into t0 values('%s',true,2,3,4,5,6,7,8,9,10,11,'binary','nchar')", now.Format(time.RFC3339Nano)))
//	if err != nil {
//		t.Error(err)
//		return
//	}
//	_, err = db.Exec(fmt.Sprintf("insert into t0 values('%s',null,null,null,null,null,null,null,null,null,null,null,null,null)", after1s.Format(time.RFC3339Nano)))
//	if err != nil {
//		t.Error(err)
//		return
//	}
//	tests := []struct {
//		name          string
//		field         string
//		where         string
//		bind          interface{}
//		expectNoValue bool
//		expectValue   driver.Value
//		expectError   bool
//	}{
//		{
//			name:        "bool_true",
//			field:       "c1",
//			where:       "c1 = ?",
//			bind:        true,
//			expectValue: true,
//		},
//		{
//			name:          "bool_false",
//			field:         "c1",
//			where:         "c1 = ?",
//			bind:          false,
//			expectNoValue: true,
//		},
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			sql := fmt.Sprintf("select %s from t0 where %s", tt.field, tt.where)
//
//			stmt, err := db.Prepare(sql)
//			if err != nil {
//				t.Error(err)
//				return
//			}
//			rows, err := stmt.Query(tt.bind)
//			if tt.expectError {
//				assert.NotNil(t, err)
//				stmt.Close()
//				return
//			}
//			if err != nil {
//				t.Error(err)
//				return
//			}
//			tts, err := rows.ColumnTypes()
//			typesL := make([]reflect.Type, 1)
//			for i, tp := range tts {
//				st := tp.ScanType()
//				if st == nil {
//					t.Errorf("scantype is null for column %q", tp.Name())
//					continue
//				}
//				typesL[i] = st
//			}
//			var data []driver.Value
//			for rows.Next() {
//				values := make([]interface{}, 1)
//				for i := range values {
//					values[i] = reflect.New(typesL[i]).Interface()
//				}
//				err = rows.Scan(values...)
//				if err != nil {
//					t.Error(err)
//					return
//				}
//				v, err := values[0].(driver.Valuer).Value()
//				if err != nil {
//					t.Error(err)
//				}
//				data = append(data, v)
//			}
//			if tt.expectNoValue {
//				if len(data) > 0 {
//					t.Errorf("expect no value got %#v", data)
//					return
//				}
//				return
//			}
//			if len(data) != 1 {
//				t.Errorf("expect %d got %d", 1, len(data))
//				return
//			}
//			if data[0] != tt.expectValue {
//				t.Errorf("expect %v got %v", tt.expectValue, data[0])
//				return
//			}
//		})
//	}
//}
