package af

import (
	"database/sql/driver"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/taosdata/driver-go/v3/common"
	param2 "github.com/taosdata/driver-go/v3/common/param"
)

func TestMain(m *testing.M) {
	m.Run()
	db, err := Open("", "", "", "", 0)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	_, err = db.Exec("drop database if exists test_af")
	if err != nil {
		panic(err)
	}
}
func testDatabase(t *testing.T) *Connector {
	db, err := Open("", "", "", "", 0)
	if err != nil {
		t.Error(err)
		return nil
	}
	_, err = db.Exec("create database if not exists test_af precision 'us'  keep 36500")
	if err != nil {
		t.Error(err)
		return nil
	}
	_, err = db.Exec("use test_af")
	if err != nil {
		t.Error(err)
		return nil
	}
	return db
}

// @author: xftan
// @date: 2022/1/27 16:06
// @description: test af open connect
func TestOpen(t *testing.T) {
	db := testDatabase(t)
	defer db.Close()
	// select database
	_, err := db.Exec("create database if not exists test_af")
	if err != nil {
		t.Error(err)
		return
	}
}

// @author: xftan
// @date: 2022/1/27 16:07
// @description: test query
func TestQuery(t *testing.T) {
	db := testDatabase(t)
	defer db.Close()
	_, err := db.Exec("drop table if exists test_types")
	if err != nil {
		t.Error(err)
		return
	}
	_, err = db.Exec("create table if not exists test_types(ts timestamp, f_int int, f_bigint bigint, f_float float, f_double double, f_binary binary(16), f_smallint smallint, f_tinyint tinyint, f_bool bool, f_nchar nchar(16))")
	if err != nil {
		t.Error(err)
		return
	}
	_, err = db.Exec("insert into test_types values(now, 1, 2, 3000000.3, 400000000.4, '5binary', 6, 7, true, '9nchar')")
	if err != nil {
		t.Error(err)
		return
	}

	rows, err := db.Query("select ts, f_int, f_bigint, f_float, f_double, f_binary, f_smallint, f_tinyint, f_bool, f_nchar from test_types limit 1")
	if err != nil {
		t.Error(err)
		return
	}
	defer rows.Close()
	values := make([]driver.Value, 10)
	if err = rows.Next(values); err != nil {
		t.Error(err)
		return
	}
	ts := values[0].(time.Time)
	if ts.IsZero() {
		t.Fatal(ts)
	}
	fInt := values[1].(int32)
	if fInt != 1 {
		t.Fatal(fInt)
	}
	fBigint := values[2].(int64)
	if fBigint != 2 {
		t.Fatal(fBigint)
	}
	fFloat := values[3].(float32)
	if fFloat != 3000000.3 {
		t.Fatal(fFloat)
	}
	fDouble := values[4].(float64)
	if fDouble != 400000000.4 {
		t.Fatal(fDouble)
	}
	fBinary := values[5].(string)
	if fBinary != "5binary" {
		t.Fatal(fBinary)
	}
	fSmallint := values[6].(int16)
	if fSmallint != 6 {
		t.Fatal(fSmallint)
	}
	fTinyint := values[7].(int8)
	if fTinyint != 7 {
		t.Fatal(fTinyint)
	}
	fBool := values[8].(bool)
	if !fBool {
		t.Fatal(fBool)
	}
	fNchar := values[9].(string)
	if fNchar != "9nchar" {
		t.Fatal(fNchar)
	}
	rows.Columns()

}

// @author: xftan
// @date: 2022/1/27 16:07
// @description: test stmt exec
func TestStmtExec(t *testing.T) {
	db := testDatabase(t)
	defer db.Close()
	now := time.Now()
	for i, tc := range []struct {
		tbType string
		pos    string
		params *param2.Param
	}{
		{"ts timestamp, `value` int", "?, ?", param2.NewParam(2).AddTimestamp(now, 0).AddInt(1)},
		{"ts timestamp, `value` bool", "?, ?", param2.NewParam(2).AddTimestamp(now, 0).AddBool(true)},
		{"ts timestamp, `value` int", "?, ?", param2.NewParam(2).AddTimestamp(now, 0).AddInt(1)},
		{"ts timestamp, `value` tinyint", "?, ?", param2.NewParam(2).AddTimestamp(now, 0).AddTinyint(1)},
		{"ts timestamp, `value` smallint", "?, ?", param2.NewParam(2).AddTimestamp(now, 0).AddSmallint(1)},
		{"ts timestamp, `value` int", "?, ?", param2.NewParam(2).AddTimestamp(now, 0).AddInt(1)},
		{"ts timestamp, `value` bigint", "?, ?", param2.NewParam(2).AddTimestamp(now, 0).AddBigint(1)},
		{"ts timestamp, `value` tinyint unsigned", "?, ?", param2.NewParam(2).AddTimestamp(now, 0).AddUTinyint(1)},
		{"ts timestamp, `value` smallint unsigned", "?, ?", param2.NewParam(2).AddTimestamp(now, 0).AddUSmallint(1)},
		{"ts timestamp, `value` int unsigned", "?, ?", param2.NewParam(2).AddTimestamp(now, 0).AddUInt(1)},
		{"ts timestamp, `value` bigint unsigned", "?, ?", param2.NewParam(2).AddTimestamp(now, 0).AddUBigint(1)},
		{"ts timestamp, `value` tinyint unsigned", "?, ?", param2.NewParam(2).AddTimestamp(now, 0).AddUTinyint(1)},
		{"ts timestamp, `value` smallint unsigned", "?, ?", param2.NewParam(2).AddTimestamp(now, 0).AddUSmallint(1)},
		{"ts timestamp, `value` int unsigned", "?, ?", param2.NewParam(2).AddTimestamp(now, 0).AddUInt(1)},
		{"ts timestamp, `value` float", "?, ?", param2.NewParam(2).AddTimestamp(now, 0).AddFloat(1.2)},
		{"ts timestamp, `value` double", "?, ?", param2.NewParam(2).AddTimestamp(now, 0).AddDouble(1.2)},
		{"ts timestamp, `value` binary(8)", "?, ?", param2.NewParam(2).AddTimestamp(now, 0).AddBinary([]byte("yes"))},
		{"ts timestamp, `value` nchar(8)", "?, ?", param2.NewParam(2).AddTimestamp(now, 0).AddNchar("yes")},
		{"ts timestamp, `value` nchar(8)", "?, ?", param2.NewParam(2).AddTimestamp(time.Now(), 0)},
	} {
		tbName := fmt.Sprintf("test_stmt_insert%02d", i)
		tbType := tc.tbType
		create := fmt.Sprintf("create table if not exists %s(%s)", tbName, tbType)
		params := tc.params
		name := fmt.Sprintf("%02d-%s", i, tbType)
		pos := tc.pos
		sql := fmt.Sprintf("insert into %s values(%s)", tbName, pos)
		var err error
		t.Run(name, func(t *testing.T) {
			if _, err = db.Exec(create); err != nil {
				t.Error(err)
				return
			}
			result, err := db.StmtExecute(sql, params)
			if err != nil {
				t.Error(err)
				return
			}
			affectRows, err := result.RowsAffected()
			if err != nil {
				t.Error(err)
				return
			}
			if affectRows != 1 {
				t.Errorf("expect 1 got %d", affectRows)
				return
			}
			var rows driver.Rows
			if rows, err = db.Query(fmt.Sprintf("select `value` from %s", tbName)); err != nil {
				t.Fatal(rows, tbName)
			}
			defer rows.Close()
			v := make([]driver.Value, 1)
			if err = rows.Next(v); err != nil {
				t.Error(err)
				return
			}
		})
	}
}

// @author: xftan
// @date: 2022/1/27 16:07
// @description: test stmt query
//func TestStmtQuery(t *testing.T) {
//	db := testDatabase(t)
//	defer db.Close()
//	for i, tc := range []struct {
//		tbType string
//		data   string
//		clause string
//		params *param2.Param
//		skip   bool
//	}{
//		{"ts timestamp, v int", "0, 1", "v = ?", param2.NewParam(1).AddInt(1), false},
//		{"ts timestamp, v bool", "now, true", "v = ?", param2.NewParam(1).AddBool(true), false},
//		{"ts timestamp, v tinyint", "now, 3", "v = ?", param2.NewParam(1).AddTinyint(3), false},
//		{"ts timestamp, v smallint", "now, 5", "v = ?", param2.NewParam(1).AddSmallint(5), false},
//		{"ts timestamp, v int", "now, 6", "v = ?", param2.NewParam(1).AddInt(6), false},
//		{"ts timestamp, v bigint", "now, 7", "v = ?", param2.NewParam(1).AddBigint(7), false},
//		{"ts timestamp, v tinyint unsigned", "now, 1", "v = ?", param2.NewParam(1).AddUTinyint(1), false},
//		{"ts timestamp, v smallint unsigned", "now, 2", "v = ?", param2.NewParam(1).AddUSmallint(2), false},
//		{"ts timestamp, v int unsigned", "now, 3", "v = ?", param2.NewParam(1).AddUInt(3), false},
//		{"ts timestamp, v bigint unsigned", "now, 4", "v = ?", param2.NewParam(1).AddUBigint(4), false},
//		{"ts timestamp, v tinyint unsigned", "now, 1", "v = ?", param2.NewParam(1).AddUTinyint(1), false},
//		{"ts timestamp, v smallint unsigned", "now, 2", "v = ?", param2.NewParam(1).AddUSmallint(2), false},
//		{"ts timestamp, v int unsigned", "now, 3", "v = ?", param2.NewParam(1).AddUInt(3), false},
//		{"ts timestamp, v bigint unsigned", "now, 4", "v = ?", param2.NewParam(1).AddUBigint(4), false},
//		{"ts timestamp, v float", "now, 1.2", "v = ?", param2.NewParam(1).AddFloat(1.2), false},
//		{"ts timestamp, v double", "now, 1.3", "v = ?", param2.NewParam(1).AddDouble(1.3), false},
//		{"ts timestamp, v double", "now, 1.4", "v = ?", param2.NewParam(1).AddDouble(1.4), false},
//		{"ts timestamp, v binary(8)", "now, 'yes'", "v = ?", param2.NewParam(1).AddBinary([]byte("yes")), false},
//		{"ts timestamp, v nchar(8)", "now, 'OK'", "v = ?", param2.NewParam(1).AddNchar("OK"), false},
//		{"ts timestamp, v nchar(8)", "1622282105000000, 'NOW'", "ts = ? and v = ?", param2.NewParam(2).AddTimestamp(time.Unix(1622282105, 0), common.PrecisionMicroSecond).AddBinary([]byte("NOW")), false},
//		{"ts timestamp, v nchar(8)", "1622282105000000, 'NOW'", "ts = ? and v = ?", param2.NewParam(2).AddBigint(1622282105000000).AddBinary([]byte("NOW")), false},
//	} {
//		tbName := fmt.Sprintf("test_stmt_query%02d", i)
//		tbType := tc.tbType
//		create := fmt.Sprintf("create table if not exists %s(%s)", tbName, tbType)
//		insert := fmt.Sprintf("insert into %s values(%s)", tbName, tc.data)
//		params := tc.params
//		sql := fmt.Sprintf("select * from %s where %s", tbName, tc.clause)
//		name := fmt.Sprintf("%02d-%s", i, tbType)
//		var err error
//		t.Run(name, func(t *testing.T) {
//			if tc.skip {
//				t.Skip("Skip, not support yet")
//			}
//			if _, err = db.Exec(create); err != nil {
//				t.Error(err)
//				return
//			}
//			if _, err = db.Exec(insert); err != nil {
//				t.Error(err)
//				return
//			}
//			var rows driver.Rows
//
//			if rows, err = db.StmtQuery(sql, params); err != nil {
//				t.Error(err)
//				return
//			}
//			defer rows.Close()
//			names := rows.Columns()
//			if len(names) == 0 {
//				t.Fatal(names)
//			}
//			values := make([]driver.Value, len(names))
//			if err = rows.Next(values); err != nil {
//				t.Error(err)
//				return
//			}
//		})
//	}
//}

// @author: xftan
// @date: 2022/1/27 16:07
// @description: test stmt insert
func TestFastInsert(t *testing.T) {
	db := testDatabase(t)
	defer db.Close()
	now := time.Now()
	for i, tc := range []struct {
		tbType   string
		pos      string
		params   []*param2.Param
		bindType *param2.ColumnType
	}{
		{"ts timestamp, `value` int", "?, ?", []*param2.Param{param2.NewParam(1).AddTimestamp(now, common.PrecisionMicroSecond), param2.NewParam(1).AddInt(1)}, param2.NewColumnType(2).AddTimestamp().AddInt()},
		{"ts timestamp, `value` bool", "?, ?", []*param2.Param{param2.NewParam(1).AddTimestamp(now, common.PrecisionMicroSecond), param2.NewParam(1).AddBool(true)}, param2.NewColumnType(2).AddTimestamp().AddBool()},
		{"ts timestamp, `value` tinyint", "?, ?", []*param2.Param{param2.NewParam(1).AddTimestamp(now, common.PrecisionMicroSecond), param2.NewParam(1).AddTinyint(1)}, param2.NewColumnType(2).AddTimestamp().AddTinyint()},
		{"ts timestamp, `value` smallint", "?, ?", []*param2.Param{param2.NewParam(1).AddTimestamp(now, common.PrecisionMicroSecond), param2.NewParam(1).AddSmallint(1)}, param2.NewColumnType(2).AddTimestamp().AddSmallint()},
		{"ts timestamp, `value` bigint", "?, ?", []*param2.Param{param2.NewParam(1).AddTimestamp(now, common.PrecisionMicroSecond), param2.NewParam(1).AddBigint(1)}, param2.NewColumnType(2).AddTimestamp().AddBigint()},
		{"ts timestamp, `value` tinyint unsigned", "?, ?", []*param2.Param{param2.NewParam(1).AddTimestamp(now, common.PrecisionMicroSecond), param2.NewParam(1).AddUTinyint(1)}, param2.NewColumnType(2).AddTimestamp().AddUTinyint()},
		{"ts timestamp, `value` smallint unsigned", "?, ?", []*param2.Param{param2.NewParam(1).AddTimestamp(now, common.PrecisionMicroSecond), param2.NewParam(1).AddUSmallint(1)}, param2.NewColumnType(2).AddTimestamp().AddUSmallint()},
		{"ts timestamp, `value` int unsigned", "?, ?", []*param2.Param{param2.NewParam(1).AddTimestamp(now, common.PrecisionMicroSecond), param2.NewParam(1).AddUInt(1)}, param2.NewColumnType(2).AddTimestamp().AddUInt()},
		{"ts timestamp, `value` bigint unsigned", "?, ?", []*param2.Param{param2.NewParam(1).AddTimestamp(now, common.PrecisionMicroSecond), param2.NewParam(1).AddUBigint(1)}, param2.NewColumnType(2).AddTimestamp().AddUBigint()},
		{"ts timestamp, `value` float", "?, ?", []*param2.Param{param2.NewParam(1).AddTimestamp(now, common.PrecisionMicroSecond), param2.NewParam(1).AddFloat(1.2)}, param2.NewColumnType(2).AddTimestamp().AddFloat()},
		{"ts timestamp, `value` double", "?, ?", []*param2.Param{param2.NewParam(1).AddTimestamp(now, common.PrecisionMicroSecond), param2.NewParam(1).AddDouble(1.2)}, param2.NewColumnType(2).AddTimestamp().AddDouble()},
		{"ts timestamp, `value` binary(8)", "?, ?", []*param2.Param{param2.NewParam(1).AddTimestamp(now, common.PrecisionMicroSecond), param2.NewParam(1).AddBinary([]byte("yes"))}, param2.NewColumnType(2).AddTimestamp().AddBinary(3)},
		{"ts timestamp, `value` nchar(8)", "?, ?", []*param2.Param{param2.NewParam(1).AddTimestamp(now, common.PrecisionMicroSecond), param2.NewParam(1).AddNchar("yes")}, param2.NewColumnType(2).AddTimestamp().AddNchar(3)},
		{"ts timestamp, `value` nchar(8)", "?, ?", []*param2.Param{param2.NewParam(1).AddTimestamp(time.Now(), common.PrecisionMicroSecond), param2.NewParam(1).AddNull()}, param2.NewColumnType(2).AddTimestamp().AddNchar(1)},
	} {
		tbName := fmt.Sprintf("test_fast_insert2_%02d", i)
		tbType := tc.tbType
		create := fmt.Sprintf("create table if not exists %s(%s)", tbName, tbType)
		params := tc.params
		bindType := tc.bindType
		name := fmt.Sprintf("%02d-%s", i, tbType)
		pos := tc.pos
		sql := fmt.Sprintf("insert into %s values(%s)", tbName, pos)
		var err error
		t.Run(name, func(t *testing.T) {
			if _, err = db.Exec(create); err != nil {
				t.Error(err)
				return
			}
			insertStmt := db.InsertStmt()
			err := insertStmt.Prepare(sql)
			if err != nil {
				t.Error(err)
				return
			}
			err = insertStmt.BindParam(params, bindType)
			if err != nil {
				t.Error(err)
				return
			}
			err = insertStmt.AddBatch()
			if err != nil {
				t.Error(err)
				return
			}
			err = insertStmt.Execute()
			if err != nil {
				t.Error(err)
				return
			}
			err = insertStmt.Close()
			if err != nil {
				t.Error(err)
				return
			}
			var rows driver.Rows
			if rows, err = db.Query(fmt.Sprintf("select `value` from %s", tbName)); err != nil {
				t.Fatal(rows, tbName)
			}
			defer rows.Close()
			v := make([]driver.Value, 1)
			if err = rows.Next(v); err != nil {
				t.Error(err)
				return
			}
		})
	}
}

// @author: xftan
// @date: 2022/1/27 16:08
// @description: test stmt insert with set table name
func TestFastInsertWithSetTableName(t *testing.T) {
	db := testDatabase(t)
	defer db.Close()
	now := time.Now()
	for i, tc := range []struct {
		tbType   string
		pos      string
		params   []*param2.Param
		bindType *param2.ColumnType
	}{
		{"ts timestamp, `value` int", "?, ?", []*param2.Param{param2.NewParam(1).AddTimestamp(now, common.PrecisionMicroSecond), param2.NewParam(1).AddInt(1)}, param2.NewColumnType(2).AddTimestamp().AddInt()},
		{"ts timestamp, `value` bool", "?, ?", []*param2.Param{param2.NewParam(1).AddTimestamp(now, common.PrecisionMicroSecond), param2.NewParam(1).AddBool(true)}, param2.NewColumnType(2).AddTimestamp().AddBool()},
		{"ts timestamp, `value` tinyint", "?, ?", []*param2.Param{param2.NewParam(1).AddTimestamp(now, common.PrecisionMicroSecond), param2.NewParam(1).AddTinyint(1)}, param2.NewColumnType(2).AddTimestamp().AddTinyint()},
		{"ts timestamp, `value` smallint", "?, ?", []*param2.Param{param2.NewParam(1).AddTimestamp(now, common.PrecisionMicroSecond), param2.NewParam(1).AddSmallint(1)}, param2.NewColumnType(2).AddTimestamp().AddSmallint()},
		{"ts timestamp, `value` bigint", "?, ?", []*param2.Param{param2.NewParam(1).AddTimestamp(now, common.PrecisionMicroSecond), param2.NewParam(1).AddBigint(1)}, param2.NewColumnType(2).AddTimestamp().AddBigint()},
		{"ts timestamp, `value` tinyint unsigned", "?, ?", []*param2.Param{param2.NewParam(1).AddTimestamp(now, common.PrecisionMicroSecond), param2.NewParam(1).AddUTinyint(1)}, param2.NewColumnType(2).AddTimestamp().AddUTinyint()},
		{"ts timestamp, `value` smallint unsigned", "?, ?", []*param2.Param{param2.NewParam(1).AddTimestamp(now, common.PrecisionMicroSecond), param2.NewParam(1).AddUSmallint(1)}, param2.NewColumnType(2).AddTimestamp().AddUSmallint()},
		{"ts timestamp, `value` int unsigned", "?, ?", []*param2.Param{param2.NewParam(1).AddTimestamp(now, common.PrecisionMicroSecond), param2.NewParam(1).AddUInt(1)}, param2.NewColumnType(2).AddTimestamp().AddUInt()},
		{"ts timestamp, `value` bigint unsigned", "?, ?", []*param2.Param{param2.NewParam(1).AddTimestamp(now, common.PrecisionMicroSecond), param2.NewParam(1).AddUBigint(1)}, param2.NewColumnType(2).AddTimestamp().AddUBigint()},
		{"ts timestamp, `value` float", "?, ?", []*param2.Param{param2.NewParam(1).AddTimestamp(now, common.PrecisionMicroSecond), param2.NewParam(1).AddFloat(1.2)}, param2.NewColumnType(2).AddTimestamp().AddFloat()},
		{"ts timestamp, `value` double", "?, ?", []*param2.Param{param2.NewParam(1).AddTimestamp(now, common.PrecisionMicroSecond), param2.NewParam(1).AddDouble(1.2)}, param2.NewColumnType(2).AddTimestamp().AddDouble()},
		{"ts timestamp, `value` binary(8)", "?, ?", []*param2.Param{param2.NewParam(1).AddTimestamp(now, common.PrecisionMicroSecond), param2.NewParam(1).AddBinary([]byte("yes"))}, param2.NewColumnType(2).AddTimestamp().AddBinary(3)},
		{"ts timestamp, `value` nchar(8)", "?, ?", []*param2.Param{param2.NewParam(1).AddTimestamp(now, common.PrecisionMicroSecond), param2.NewParam(1).AddNchar("yes")}, param2.NewColumnType(2).AddTimestamp().AddNchar(3)},

		{"ts timestamp, `value` nchar(8)", "?, ?", []*param2.Param{param2.NewParam(1).AddTimestamp(time.Now(), common.PrecisionMicroSecond), param2.NewParam(1).AddNull()}, param2.NewColumnType(2).AddTimestamp().AddNchar(1)},
		{"ts timestamp, `value` int", "?, ?", []*param2.Param{param2.NewParam(1).AddTimestamp(now, common.PrecisionMicroSecond), param2.NewParam(1).AddNull()}, param2.NewColumnType(2).AddTimestamp().AddInt()},
		{"ts timestamp, `value` bool", "?, ?", []*param2.Param{param2.NewParam(1).AddTimestamp(now, common.PrecisionMicroSecond), param2.NewParam(1).AddNull()}, param2.NewColumnType(2).AddTimestamp().AddBool()},
		{"ts timestamp, `value` tinyint", "?, ?", []*param2.Param{param2.NewParam(1).AddTimestamp(now, common.PrecisionMicroSecond), param2.NewParam(1).AddNull()}, param2.NewColumnType(2).AddTimestamp().AddTinyint()},
		{"ts timestamp, `value` smallint", "?, ?", []*param2.Param{param2.NewParam(1).AddTimestamp(now, common.PrecisionMicroSecond), param2.NewParam(1).AddNull()}, param2.NewColumnType(2).AddTimestamp().AddSmallint()},
		{"ts timestamp, `value` bigint", "?, ?", []*param2.Param{param2.NewParam(1).AddTimestamp(now, common.PrecisionMicroSecond), param2.NewParam(1).AddNull()}, param2.NewColumnType(2).AddTimestamp().AddBigint()},
		{"ts timestamp, `value` tinyint unsigned", "?, ?", []*param2.Param{param2.NewParam(1).AddTimestamp(now, common.PrecisionMicroSecond), param2.NewParam(1).AddNull()}, param2.NewColumnType(2).AddTimestamp().AddUTinyint()},
		{"ts timestamp, `value` smallint unsigned", "?, ?", []*param2.Param{param2.NewParam(1).AddTimestamp(now, common.PrecisionMicroSecond), param2.NewParam(1).AddNull()}, param2.NewColumnType(2).AddTimestamp().AddUSmallint()},
		{"ts timestamp, `value` int unsigned", "?, ?", []*param2.Param{param2.NewParam(1).AddTimestamp(now, common.PrecisionMicroSecond), param2.NewParam(1).AddNull()}, param2.NewColumnType(2).AddTimestamp().AddUInt()},
		{"ts timestamp, `value` bigint unsigned", "?, ?", []*param2.Param{param2.NewParam(1).AddTimestamp(now, common.PrecisionMicroSecond), param2.NewParam(1).AddNull()}, param2.NewColumnType(2).AddTimestamp().AddUBigint()},
		{"ts timestamp, `value` float", "?, ?", []*param2.Param{param2.NewParam(1).AddTimestamp(now, common.PrecisionMicroSecond), param2.NewParam(1).AddNull()}, param2.NewColumnType(2).AddTimestamp().AddFloat()},
		{"ts timestamp, `value` double", "?, ?", []*param2.Param{param2.NewParam(1).AddTimestamp(now, common.PrecisionMicroSecond), param2.NewParam(1).AddNull()}, param2.NewColumnType(2).AddTimestamp().AddDouble()},
		{"ts timestamp, `value` binary(8)", "?, ?", []*param2.Param{param2.NewParam(1).AddTimestamp(now, common.PrecisionMicroSecond), param2.NewParam(1).AddNull()}, param2.NewColumnType(2).AddTimestamp().AddBinary(3)},
	} {
		tbName := fmt.Sprintf("test_fast_insert_with_table_name_%02d", i)
		tbType := tc.tbType
		create := fmt.Sprintf("create table if not exists %s(%s)", tbName, tbType)
		params := tc.params
		bindType := tc.bindType
		name := fmt.Sprintf("%02d-%s", i, tbType)
		pos := tc.pos
		sql := fmt.Sprintf("insert into ? values(%s)", pos)
		var err error
		t.Run(name, func(t *testing.T) {
			if _, err = db.Exec(create); err != nil {
				t.Error(err)
				return
			}
			insertStmt := db.InsertStmt()
			err := insertStmt.Prepare(sql)
			if err != nil {
				t.Error(err)
				return
			}
			err = insertStmt.SetTableName(tbName)
			if err != nil {
				t.Error(err)
				return
			}
			err = insertStmt.BindParam(params, bindType)
			if err != nil {
				t.Error(err)
				return
			}
			err = insertStmt.AddBatch()
			if err != nil {
				t.Error(err)
				return
			}
			err = insertStmt.Execute()
			if err != nil {
				t.Error(err)
				return
			}
			err = insertStmt.Close()
			if err != nil {
				t.Error(err)
				return
			}
			var rows driver.Rows
			if rows, err = db.Query(fmt.Sprintf("select `value` from %s", tbName)); err != nil {
				t.Fatal(rows, tbName)
			}
			defer rows.Close()
			v := make([]driver.Value, 1)
			if err = rows.Next(v); err != nil {
				t.Error(err)
				return
			}
		})
	}
}

// @author: xftan
// @date: 2022/1/27 16:08
// @description: test stmt insert with set table name and tag
func TestFastInsertWithSetTableNameTag(t *testing.T) {
	db := testDatabase(t)
	defer db.Close()
	now := time.Now()
	_, err := db.Exec("create stable if not exists set_table_name_tag_int (ts timestamp,`value` int) tags(i smallint,v binary(8))")
	if err != nil {
		t.Error(err)
		return
	}
	_, err = db.Exec("create stable if not exists set_table_name_tag_nchar (ts timestamp,`value` nchar(8)) tags(i smallint,v binary(8))")
	if err != nil {
		t.Error(err)
		return
	}
	for i, tc := range []struct {
		sTableName string
		tags       *param2.Param
		tbType     string
		pos        string
		params     []*param2.Param
		bindType   *param2.ColumnType
	}{
		{"set_table_name_tag_int", param2.NewParam(2).AddSmallint(1).AddBinary([]byte("int")), "ts timestamp, `value` int", "?, ?", []*param2.Param{param2.NewParam(1).AddTimestamp(now, common.PrecisionMicroSecond), param2.NewParam(1).AddInt(1)}, param2.NewColumnType(2).AddTimestamp().AddInt()},
		{"set_table_name_tag_nchar", param2.NewParam(2).AddSmallint(2).AddBinary([]byte("nchar")), "ts timestamp, `value` nchar(8)", "?, ?", []*param2.Param{param2.NewParam(1).AddTimestamp(time.Now(), common.PrecisionMicroSecond), param2.NewParam(1).AddNull()}, param2.NewColumnType(2).AddTimestamp().AddNchar(1)},
	} {
		tbName := fmt.Sprintf("test_fast_insert_with_table_name_tag_%02d", i)
		tbType := tc.tbType
		params := tc.params
		bindType := tc.bindType
		name := fmt.Sprintf("%02d-%s", i, tbType)
		pos := tc.pos
		sql := fmt.Sprintf("insert into ? using %s tags(?,?) values(%s)", tc.sTableName, pos)
		t.Run(name, func(t *testing.T) {
			insertStmt := db.InsertStmt()
			err := insertStmt.Prepare(sql)
			if err != nil {
				t.Error(err)
				return
			}
			err = insertStmt.SetTableNameWithTags(tbName, tc.tags)
			if err != nil {
				t.Error(err)
				return
			}
			err = insertStmt.BindParam(params, bindType)
			if err != nil {
				t.Error(err)
				return
			}
			err = insertStmt.AddBatch()
			if err != nil {
				t.Error(err)
				return
			}
			err = insertStmt.Execute()
			if err != nil {
				t.Error(err)
				return
			}
			err = insertStmt.Close()
			if err != nil {
				t.Error(err)
				return
			}
			var rows driver.Rows
			if rows, err = db.Query(fmt.Sprintf("select `value` from %s", tbName)); err != nil {
				t.Fatal(rows, tbName)
			}
			defer rows.Close()
			v := make([]driver.Value, 1)
			if err = rows.Next(v); err != nil {
				t.Error(err)
				return
			}
		})
	}

}

// @author: xftan
// @date: 2022/1/27 16:08
// @description: test stmt insert with set sub table name
func TestFastInsertWithSetSubTableName(t *testing.T) {
	db := testDatabase(t)
	defer db.Close()
	now := time.Now()
	_, err := db.Exec("create stable if not exists set_table_name_sub_int (ts timestamp,`value` int) tags(i smallint,v binary(8))")
	if err != nil {
		t.Error(err)
		return
	}
	_, err = db.Exec("create stable if not exists set_table_name_sub_nchar (ts timestamp,`value` nchar(8)) tags(i smallint,v binary(8))")
	if err != nil {
		t.Error(err)
		return
	}
	//err = db.LoadTableInfo([]string{"test_fast_insert_with_sub_table_name_00", "test_fast_insert_with_sub_table_name_01"})
	//if err != nil {
	//	t.Error(err)
	//	return
	//}
	for i, tc := range []struct {
		sTableName string
		tags       string
		tbType     string
		pos        string
		params     []*param2.Param
		bindType   *param2.ColumnType
	}{
		{"set_table_name_sub_int", "1,'int'", "ts timestamp, `value` int", "?, ?", []*param2.Param{param2.NewParam(1).AddTimestamp(now, common.PrecisionMicroSecond), param2.NewParam(1).AddInt(1)}, param2.NewColumnType(2).AddTimestamp().AddInt()},
		{"set_table_name_sub_nchar", "2,'nchar'", "ts timestamp, `value` nchar(8)", "?, ?", []*param2.Param{param2.NewParam(1).AddTimestamp(time.Now(), common.PrecisionMicroSecond), param2.NewParam(1).AddNchar("ttt")}, param2.NewColumnType(2).AddTimestamp().AddNchar(1)},
	} {
		tbName := fmt.Sprintf("test_fast_insert_with_sub_table_name_%02d", i)
		tbType := tc.tbType
		params := tc.params
		bindType := tc.bindType
		create := fmt.Sprintf("create table if not exists %s using %s tags(%s)", tbName, tc.sTableName, tc.tags)
		name := fmt.Sprintf("%02d-%s", i, tbType)
		pos := tc.pos
		sql := fmt.Sprintf("insert into ? values(%s)", pos)
		t.Run(name, func(t *testing.T) {
			_, err := db.Exec(create)
			if err != nil {
				t.Error(err)
				return
			}
			insertStmt := db.InsertStmt()
			err = insertStmt.Prepare(sql)
			if err != nil {
				t.Error(err)
				return
			}
			err = insertStmt.SetSubTableName(tbName)
			if err != nil {
				t.Error(err)
				return
			}
			err = insertStmt.BindParam(params, bindType)
			if err != nil {
				t.Error(err)
				return
			}
			err = insertStmt.AddBatch()
			if err != nil {
				t.Error(err)
				return
			}
			err = insertStmt.Execute()
			if err != nil {
				t.Error(err)
				return
			}
			err = insertStmt.Close()
			if err != nil {
				t.Error(err)
				return
			}
			var rows driver.Rows
			if rows, err = db.Query(fmt.Sprintf("select `value` from %s", tbName)); err != nil {
				t.Fatal(rows, tbName)
			}
			defer rows.Close()
			v := make([]driver.Value, 1)
			if err = rows.Next(v); err != nil {
				t.Error(err)
				return
			}
		})
	}
}

const raw = `http_response,host=host161,method=GET,result=success,server=http://localhost,status_code=404 response_time=0.003226372,http_response_code=404i,content_length=19i,result_type="success",result_code=0i 1648090640000000000
request_histogram_latency_seconds_max,aaa=bb,api_range=all,host=host161,url=http://192.168.17.148:8080/actuator/prometheus gauge=0 1648090640000000000
process_files_max_files,host=host161,url=http://192.168.17.148:8080/actuator/prometheus gauge=10240 1648090640000000000
request_timer_seconds,host=host161,quantile=0.5,url=http://192.168.17.148:8080/actuator/prometheus count=0,sum=0 1648090640000000000
request_timer_seconds,host=host161,quantile=0.9,url=http://192.168.17.148:8080/actuator/prometheus count=0,sum=0 1648090640000000000
request_timer_seconds,host=host161,quantile=0.95,url=http://192.168.17.148:8080/actuator/prometheus count=0,sum=0 1648090640000000000
request_timer_seconds,host=host161,quantile=0.99,url=http://192.168.17.148:8080/actuator/prometheus count=0,sum=0 1648090640000000000
request_timer_seconds,host=host161,url=http://192.168.17.148:8080/actuator/prometheus 0.223696211=0,0.016777216=0,0.178956969=0,0.156587348=0,0.2=0,0.626349396=0,0.015379112=0,5=0,0.089478485=0,0.357913941=0,5.726623061=0,0.008388607=0,0.894784851=0,0.006990506=0,3.937053352=0,0.001=0,0.061516456=0,0.134217727=0,1.431655765=0,0.005592405=0,0.984263336=0,0.001398101=0,3.22122547=0,0.033554431=0,0.805306366=0,0.002446676=0,0.003844776=0,0.20132659=0,1.073741824=0,0.022369621=0,1=0,0.002796201=0,1.789569706=0,0.001048576=0,0.246065832=0,0.050331646=0,4.294967296=0,8.589934591=0,0.536870911=0,0.447392426=0,2.505397588=0,10=0,0.013981011=0,0.003495251=0,0.044739241=0,2.863311529=0,0.039146836=0,0.268435456=0,sum=0,3.579139411=0,7.158278826=0,0.011184809=0,0.01258291=0,0.1=0,0.003145726=0,0.055924051=0,0.067108864=0,0.004194304=0,0.001747626=0,0.002097151=0,2.147483647=0,count=0,0.715827881=0,0.009786708=0,0.111848106=0,0.027962026=0,+Inf=0 1648090640000000000
executor_completed_tasks_total,host=host161,name=applicationTaskExecutor,url=http://192.168.17.148:8080/actuator/prometheus counter=0 1648090640000000000
jvm_memory_committed_bytes,area=heap,host=host161,id=PS\ Survivor\ Space,url=http://192.168.17.148:8080/actuator/prometheus gauge=4718592 1648090640000000000
jvm_memory_committed_bytes,area=heap,host=host161,id=PS\ Old\ Gen,url=http://192.168.17.148:8080/actuator/prometheus gauge=100139008 1648090640000000000
jvm_memory_committed_bytes,area=heap,host=host161,id=PS\ Eden\ Space,url=http://192.168.17.148:8080/actuator/prometheus gauge=123207680 1648090640000000000
jvm_memory_committed_bytes,area=nonheap,host=host161,id=Metaspace,url=http://192.168.17.148:8080/actuator/prometheus gauge=44998656 1648090640000000000
jvm_memory_committed_bytes,area=nonheap,host=host161,id=Code\ Cache,url=http://192.168.17.148:8080/actuator/prometheus gauge=8847360 1648090640000000000
jvm_memory_committed_bytes,area=nonheap,host=host161,id=Compressed\ Class\ Space,url=http://192.168.17.148:8080/actuator/prometheus gauge=6463488 1648090640000000000
executor_active_threads,host=host161,name=applicationTaskExecutor,url=http://192.168.17.148:8080/actuator/prometheus gauge=0 1648090640000000000
tomcat_sessions_active_max_sessions,host=host161,url=http://192.168.17.148:8080/actuator/prometheus gauge=0 1648090640000000000
system_cpu_count,host=host161,url=http://192.168.17.148:8080/actuator/prometheus gauge=8 1648090640000000000
logback_events_total,host=host161,level=warn,url=http://192.168.17.148:8080/actuator/prometheus counter=0 1648090640000000000
logback_events_total,host=host161,level=debug,url=http://192.168.17.148:8080/actuator/prometheus counter=0 1648090640000000000
logback_events_total,host=host161,level=error,url=http://192.168.17.148:8080/actuator/prometheus counter=0 1648090640000000000
logback_events_total,host=host161,level=trace,url=http://192.168.17.148:8080/actuator/prometheus counter=0 1648090640000000000
logback_events_total,host=host161,level=info,url=http://192.168.17.148:8080/actuator/prometheus counter=7 1648090640000000000
application_ready_time_seconds,host=host161,main_application_class=cn.iospider.actuatormicrometer.ActuatorMicrometerApplication,url=http://192.168.17.148:8080/actuator/prometheus gauge=28.542 1648090640000000000
jvm_buffer_total_capacity_bytes,host=host161,id=direct,url=http://192.168.17.148:8080/actuator/prometheus gauge=57345 1648090640000000000
jvm_buffer_total_capacity_bytes,host=host161,id=mapped,url=http://192.168.17.148:8080/actuator/prometheus gauge=0 1648090640000000000
jvm_threads_live_threads,host=host161,url=http://192.168.17.148:8080/actuator/prometheus gauge=41 1648090640000000000
jvm_gc_max_data_size_bytes,host=host161,url=http://192.168.17.148:8080/actuator/prometheus gauge=2863661056 1648090640000000000
executor_pool_max_threads,host=host161,name=applicationTaskExecutor,url=http://192.168.17.148:8080/actuator/prometheus gauge=2147483647 1648090640000000000
jvm_gc_overhead_percent,host=host161,url=http://192.168.17.148:8080/actuator/prometheus gauge=0.00010333333333333333 1648090640000000000
http_server_requests_seconds_max,exception=None,host=host161,method=GET,outcome=SUCCESS,status=200,uri=/actuator/prometheus,url=http://192.168.17.148:8080/actuator/prometheus gauge=0.008994315 1648090640000000000
http_server_requests_seconds_max,exception=None,host=host161,method=GET,outcome=CLIENT_ERROR,status=404,uri=/**,url=http://192.168.17.148:8080/actuator/prometheus gauge=0 1648090640000000000
tomcat_sessions_rejected_sessions_total,host=host161,url=http://192.168.17.148:8080/actuator/prometheus counter=0 1648090640000000000
request_histogram_latency_seconds,aaa=bb,api_range=all,host=host161,url=http://192.168.17.148:8080/actuator/prometheus count=0,sum=0 1648090640000000000
disk_free_bytes,host=host161,path=/Users/jtlian/Downloads/actuator-micrometer/.,url=http://192.168.17.148:8080/actuator/prometheus gauge=77683585024 1648090640000000000
process_cpu_usage,host=host161,url=http://192.168.17.148:8080/actuator/prometheus gauge=0.0005609754336738071 1648090640000000000
jvm_threads_peak_threads,host=host161,url=http://192.168.17.148:8080/actuator/prometheus gauge=42 1648090640000000000
jvm_gc_memory_allocated_bytes_total,host=host161,url=http://192.168.17.148:8080/actuator/prometheus counter=271541440 1648090640000000000
jvm_gc_live_data_size_bytes,host=host161,url=http://192.168.17.148:8080/actuator/prometheus gauge=14251648 1648090640000000000
jvm_memory_used_bytes,area=heap,host=host161,id=PS\ Survivor\ Space,url=http://192.168.17.148:8080/actuator/prometheus gauge=4565576 1648090640000000000
jvm_memory_used_bytes,area=heap,host=host161,id=PS\ Old\ Gen,url=http://192.168.17.148:8080/actuator/prometheus gauge=14268032 1648090640000000000
jvm_memory_used_bytes,area=heap,host=host161,id=PS\ Eden\ Space,url=http://192.168.17.148:8080/actuator/prometheus gauge=16630104 1648090640000000000
jvm_memory_used_bytes,area=nonheap,host=host161,id=Metaspace,url=http://192.168.17.148:8080/actuator/prometheus gauge=41165008 1648090640000000000
jvm_memory_used_bytes,area=nonheap,host=host161,id=Code\ Cache,url=http://192.168.17.148:8080/actuator/prometheus gauge=8792832 1648090640000000000
jvm_memory_used_bytes,area=nonheap,host=host161,id=Compressed\ Class\ Space,url=http://192.168.17.148:8080/actuator/prometheus gauge=5735248 1648090640000000000
jvm_buffer_count_buffers,host=host161,id=direct,url=http://192.168.17.148:8080/actuator/prometheus gauge=9 1648090640000000000
jvm_buffer_count_buffers,host=host161,id=mapped,url=http://192.168.17.148:8080/actuator/prometheus gauge=0 1648090640000000000
application_started_time_seconds,host=host161,main_application_class=cn.iospider.actuatormicrometer.ActuatorMicrometerApplication,url=http://192.168.17.148:8080/actuator/prometheus gauge=28.535 1648090640000000000
process_start_time_seconds,host=host161,url=http://192.168.17.148:8080/actuator/prometheus gauge=1648087193.449 1648090640000000000
jvm_memory_usage_after_gc_percent,area=heap,host=host161,pool=long-lived,url=http://192.168.17.148:8080/actuator/prometheus gauge=0.004982444402805749 1648090640000000000
system_cpu_usage,host=host161,url=http://192.168.17.148:8080/actuator/prometheus gauge=0.11106101593026751 1648090640000000000
tomcat_sessions_active_current_sessions,host=host161,url=http://192.168.17.148:8080/actuator/prometheus gauge=0 1648090640000000000
executor_queue_remaining_tasks,host=host161,name=applicationTaskExecutor,url=http://192.168.17.148:8080/actuator/prometheus gauge=2147483647 1648090640000000000
jvm_threads_daemon_threads,host=host161,url=http://192.168.17.148:8080/actuator/prometheus gauge=37 1648090640000000000
process_uptime_seconds,host=host161,url=http://192.168.17.148:8080/actuator/prometheus gauge=3446.817 1648090640000000000
tomcat_sessions_alive_max_seconds,host=host161,url=http://192.168.17.148:8080/actuator/prometheus gauge=0 1648090640000000000
executor_queued_tasks,host=host161,name=applicationTaskExecutor,url=http://192.168.17.148:8080/actuator/prometheus gauge=0 1648090640000000000
request_timer_seconds_max,host=host161,url=http://192.168.17.148:8080/actuator/prometheus gauge=0 1648090640000000000
tomcat_sessions_created_sessions_total,host=host161,url=http://192.168.17.148:8080/actuator/prometheus counter=0 1648090640000000000
jvm_threads_states_threads,host=host161,state=runnable,url=http://192.168.17.148:8080/actuator/prometheus gauge=17 1648090640000000000
jvm_threads_states_threads,host=host161,state=blocked,url=http://192.168.17.148:8080/actuator/prometheus gauge=0 1648090640000000000
jvm_threads_states_threads,host=host161,state=waiting,url=http://192.168.17.148:8080/actuator/prometheus gauge=19 1648090640000000000
jvm_threads_states_threads,host=host161,state=timed-waiting,url=http://192.168.17.148:8080/actuator/prometheus gauge=5 1648090640000000000
jvm_threads_states_threads,host=host161,state=new,url=http://192.168.17.148:8080/actuator/prometheus gauge=0 1648090640000000000
jvm_threads_states_threads,host=host161,state=terminated,url=http://192.168.17.148:8080/actuator/prometheus gauge=0 1648090640000000000
process_files_open_files,host=host161,url=http://192.168.17.148:8080/actuator/prometheus gauge=119 1648090640000000000
jvm_memory_max_bytes,area=heap,host=host161,id=PS\ Survivor\ Space,url=http://192.168.17.148:8080/actuator/prometheus gauge=4718592 1648090640000000000
jvm_memory_max_bytes,area=heap,host=host161,id=PS\ Old\ Gen,url=http://192.168.17.148:8080/actuator/prometheus gauge=2863661056 1648090640000000000
jvm_memory_max_bytes,area=heap,host=host161,id=PS\ Eden\ Space,url=http://192.168.17.148:8080/actuator/prometheus gauge=1411907584 1648090640000000000
jvm_memory_max_bytes,area=nonheap,host=host161,id=Metaspace,url=http://192.168.17.148:8080/actuator/prometheus gauge=-1 1648090640000000000
jvm_memory_max_bytes,area=nonheap,host=host161,id=Code\ Cache,url=http://192.168.17.148:8080/actuator/prometheus gauge=251658240 1648090640000000000
jvm_memory_max_bytes,area=nonheap,host=host161,id=Compressed\ Class\ Space,url=http://192.168.17.148:8080/actuator/prometheus gauge=1073741824 1648090640000000000
executor_pool_size_threads,host=host161,name=applicationTaskExecutor,url=http://192.168.17.148:8080/actuator/prometheus gauge=0 1648090640000000000
disk_total_bytes,host=host161,path=/Users/jtlian/Downloads/actuator-micrometer/.,url=http://192.168.17.148:8080/actuator/prometheus gauge=328000839680 1648090640000000000
http_server_requests_seconds,exception=None,host=host161,method=GET,outcome=SUCCESS,status=200,uri=/actuator/prometheus,url=http://192.168.17.148:8080/actuator/prometheus count=7,sum=0.120204066 1648090640000000000
http_server_requests_seconds,exception=None,host=host161,method=GET,outcome=CLIENT_ERROR,status=404,uri=/**,url=http://192.168.17.148:8080/actuator/prometheus count=4,sum=0.019408184 1648090640000000000
jvm_buffer_memory_used_bytes,host=host161,id=direct,url=http://192.168.17.148:8080/actuator/prometheus gauge=57346 1648090640000000000
jvm_buffer_memory_used_bytes,host=host161,id=mapped,url=http://192.168.17.148:8080/actuator/prometheus gauge=0 1648090640000000000
jvm_gc_memory_promoted_bytes_total,host=host161,url=http://192.168.17.148:8080/actuator/prometheus counter=3055728 1648090640000000000
jvm_classes_loaded_classes,host=host161,url=http://192.168.17.148:8080/actuator/prometheus gauge=8526 1648090640000000000
system_load_average_1m,host=host161,url=http://192.168.17.148:8080/actuator/prometheus gauge=3.10107421875 1648090640000000000
tomcat_sessions_expired_sessions_total,host=host161,url=http://192.168.17.148:8080/actuator/prometheus counter=0 1648090640000000000
executor_pool_core_threads,host=host161,name=applicationTaskExecutor,url=http://192.168.17.148:8080/actuator/prometheus gauge=8 1648090640000000000
jvm_classes_unloaded_classes_total,host=host161,url=http://192.168.17.148:8080/actuator/prometheus counter=0 1648090640000000000
jvm_gc_pause_seconds,action=end\ of\ major\ GC,cause=Metadata\ GC\ Threshold,host=host161,url=http://192.168.17.148:8080/actuator/prometheus count=1,sum=0.037 1648090640000000000
jvm_gc_pause_seconds,action=end\ of\ minor\ GC,cause=Metadata\ GC\ Threshold,host=host161,url=http://192.168.17.148:8080/actuator/prometheus count=1,sum=0.005 1648090640000000000
jvm_gc_pause_seconds,action=end\ of\ minor\ GC,cause=Allocation\ Failure,host=host161,url=http://192.168.17.148:8080/actuator/prometheus count=2,sum=0.041 1648090640000000000
jvm_gc_pause_seconds_max,action=end\ of\ major\ GC,cause=Metadata\ GC\ Threshold,host=host161,url=http://192.168.17.148:8080/actuator/prometheus gauge=0 1648090640000000000
jvm_gc_pause_seconds_max,action=end\ of\ minor\ GC,cause=Metadata\ GC\ Threshold,host=host161,url=http://192.168.17.148:8080/actuator/prometheus gauge=0 1648090640000000000
jvm_gc_pause_seconds_max,action=end\ of\ minor\ GC,cause=Allocation\ Failure,host=host161,url=http://192.168.17.148:8080/actuator/prometheus gauge=0 1648090640000000000`

// @author: xftan
// @date: 2022/1/27 16:08
// @description: test influxDB insert with line protocol
func TestInfluxDBInsertLines(t *testing.T) {
	db := testDatabase(t)
	defer db.Close()
	data := strings.Split(raw, "\n")
	err := db.InfluxDBInsertLines(data, "ns")
	if err != nil {
		t.Error(err)
		return
	}
}

// @author: xftan
// @date: 2022/1/27 16:09
// @description: test telnet insert with line protocol
func TestOpenTSDBInsertTelnetLines(t *testing.T) {
	db := testDatabase(t)
	defer db.Close()
	err := db.OpenTSDBInsertTelnetLines([]string{
		"sys_if_bytes_out 1479496100 1.3E3 host=web01 interface=eth0",
		"sys_procs_running 1479496100 42 host=web01",
	})
	if err != nil {
		t.Error(err)
		return
	}
}

// @author: xftan
// @date: 2022/1/27 16:09
// @description: test telnet insert with
func TestOpenTSDBInsertJsonPayload(t *testing.T) {
	db := testDatabase(t)
	defer db.Close()
	err := db.OpenTSDBInsertJsonPayload(`{
    "metric": "sys",
    "timestamp": 1346846400,
    "value": 18,
    "tags": {
       "host": "web01",
       "dc": "lga"
    }
}`)
	if err != nil {
		t.Error(err)
		return
	}
}

func TestOpenTSDBInsertJsonPayloadWrong(t *testing.T) {
	db := testDatabase(t)
	defer db.Close()
	err := db.OpenTSDBInsertJsonPayload(`{
    "metric": "sys",
    "timestamp": 
    "value": 18,
    "tags": {
       "host": "web01",
       "dc": "lga"
    }
}`)
	if err == nil {
		t.Error("expect error")
		return
	}
}
