package af

import (
	"database/sql/driver"
	"fmt"
	"github.com/taosdata/driver-go/v2/af/param"
	"github.com/taosdata/driver-go/v2/common"
	"io"
	"testing"
	"time"
)

func testDatabase(t *testing.T) *Connector {
	db, err := Open("", "", "", "", 0)
	if err != nil {
		t.Error(err)
		return nil
	}
	//_, err = db.Exec("drop database if exists test_af")
	//if err != nil {
	//	t.Error(err)
	//}
	_, err = db.Exec("create database if not exists test_af precision 'us' update 1 keep 36500")
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

func TestOpen(t *testing.T) {
	db := testDatabase(t)
	// select database
	_, err := db.Exec("use log")
	if err != nil {
		t.Error(err)
		return
	}
	for _, c := range []struct {
		sql  string
		want string
	}{
		{"select 1", "server_status()"},
		{"select server_status()", "server_status()"},
		{"select client_version()", "client_version()"},
		{"select server_version()", "server_version()"},
		{"select database()", "database()"},
		{"select current_user()", "current_user()"},
	} {
		t.Run(c.sql, func(t *testing.T) {
			rows, err := db.Query(c.sql)
			if err != nil {
				t.Error(err)
				return
			}
			defer rows.Close()
			cols := rows.Columns()
			if len(cols) != 1 {
				t.Fatal(cols)
			}
			col := cols[0]
			if col != c.want {
				t.Log(cols)
			}
		})
	}
}

func TestSubscribe(t *testing.T) {
	db := testDatabase(t)
	_, err := db.Exec("drop table if exists test_subscribe")
	if err != nil {
		t.Error(err)
		return
	}
	_, err = db.Exec("create table if not exists test_subscribe(ts timestamp, value bool, degress int)")
	if err != nil {
		t.Error(err)
		return
	}
	sql := "select ts, value, degress from test_subscribe"
	subscriber, err := db.Subscribe(true, "test_subscribe", sql, time.Second*1)
	if err != nil {
		t.Error(err)
	}
	consume := func() int {
		rows, err := subscriber.Consume()
		if err != nil {
			t.Error(err)
			return 0
		}
		if rows == nil {
			return 0
		}
		defer rows.Close()
		count := 0
		for err == nil {
			row := make([]driver.Value, 3)
			err = rows.Next(row)
			if err == io.EOF {
				break
			}
			//ts, _ := row[0].(time.Time)
			//value, _ := row[1].(bool)
			//degress, _ := row[2].(int32)
			//t.Log(err, ts, value, degress)
			if err != nil {
				t.Error(err)
				return 0
			}
			count++
		}
		return count
	}
	defer subscriber.Unsubscribe(true)
	_, err = db.Exec("insert into test_subscribe values(now, false, 10)")
	if err != nil {
		t.Error(err)
		return
	}
	count := consume()
	if count != 1 {
		t.Fatal(count)
	}
	_, err = db.Exec("insert into test_subscribe values(now + 10s, true, 11)")
	if err != nil {
		t.Error(err)
		return
	}
	_, err = db.Exec("insert into test_subscribe values(now + 15s, true, 12)")
	if err != nil {
		t.Error(err)
		return
	}
	count = consume()
	if count != 2 {
		t.Fatal(count)
	}
}

func TestQuery(t *testing.T) {
	db := testDatabase(t)
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
}

func TestStmtExec(t *testing.T) {
	db := testDatabase(t)
	now := time.Now()
	for i, tc := range []struct {
		tbType string
		pos    string
		params *param.Param
	}{
		{"ts timestamp, value int", "?, ?", param.NewParam(2).AddTimestamp(now, 0).AddInt(1)},
		{"ts timestamp, value bool", "?, ?", param.NewParam(2).AddTimestamp(now, 0).AddBool(true)},
		{"ts timestamp, value int", "?, ?", param.NewParam(2).AddTimestamp(now, 0).AddInt(1)},
		{"ts timestamp, value tinyint", "?, ?", param.NewParam(2).AddTimestamp(now, 0).AddTinyint(1)},
		{"ts timestamp, value smallint", "?, ?", param.NewParam(2).AddTimestamp(now, 0).AddSmallint(1)},
		{"ts timestamp, value int", "?, ?", param.NewParam(2).AddTimestamp(now, 0).AddInt(1)},
		{"ts timestamp, value bigint", "?, ?", param.NewParam(2).AddTimestamp(now, 0).AddBigint(1)},
		{"ts timestamp, value tinyint unsigned", "?, ?", param.NewParam(2).AddTimestamp(now, 0).AddUTinyint(1)},
		{"ts timestamp, value smallint unsigned", "?, ?", param.NewParam(2).AddTimestamp(now, 0).AddUSmallint(1)},
		{"ts timestamp, value int unsigned", "?, ?", param.NewParam(2).AddTimestamp(now, 0).AddUInt(1)},
		{"ts timestamp, value bigint unsigned", "?, ?", param.NewParam(2).AddTimestamp(now, 0).AddUBigint(1)},
		{"ts timestamp, value tinyint unsigned", "?, ?", param.NewParam(2).AddTimestamp(now, 0).AddUTinyint(1)},
		{"ts timestamp, value smallint unsigned", "?, ?", param.NewParam(2).AddTimestamp(now, 0).AddUSmallint(1)},
		{"ts timestamp, value int unsigned", "?, ?", param.NewParam(2).AddTimestamp(now, 0).AddUInt(1)},
		{"ts timestamp, value float", "?, ?", param.NewParam(2).AddTimestamp(now, 0).AddFloat(1.2)},
		{"ts timestamp, value double", "?, ?", param.NewParam(2).AddTimestamp(now, 0).AddDouble(1.2)},
		{"ts timestamp, value binary(8)", "?, ?", param.NewParam(2).AddTimestamp(now, 0).AddBinary([]byte("yes"))},
		{"ts timestamp, value nchar(8)", "?, ?", param.NewParam(2).AddTimestamp(now, 0).AddNchar("yes")},
		{"ts timestamp, value nchar(8)", "?, ?", param.NewParam(2).AddTimestamp(time.Now(), 0)},
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
			_, err = db.StmtExecute(sql, params)
			if err != nil {
				t.Error(err)
				return
			}
			var rows driver.Rows
			if rows, err = db.Query(fmt.Sprintf("select value from %s", tbName)); err != nil {
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

func TestStmtQuery(t *testing.T) {
	db := testDatabase(t)
	for i, tc := range []struct {
		tbType string
		data   string
		clause string
		params *param.Param
		skip   bool
	}{
		{"ts timestamp, value int", "0, 1", "value = ?", param.NewParam(1).AddInt(1), false},
		{"ts timestamp, value bool", "now, true", "value = ?", param.NewParam(1).AddBool(true), false},
		{"ts timestamp, value tinyint", "now, 3", "value = ?", param.NewParam(1).AddTinyint(3), false},
		{"ts timestamp, value smallint", "now, 5", "value = ?", param.NewParam(1).AddSmallint(5), false},
		{"ts timestamp, value int", "now, 6", "value = ?", param.NewParam(1).AddInt(6), false},
		{"ts timestamp, value bigint", "now, 7", "value = ?", param.NewParam(1).AddBigint(7), false},
		{"ts timestamp, value tinyint unsigned", "now, 1", "value = ?", param.NewParam(1).AddUTinyint(1), false},
		{"ts timestamp, value smallint unsigned", "now, 2", "value = ?", param.NewParam(1).AddUSmallint(2), false},
		{"ts timestamp, value int unsigned", "now, 3", "value = ?", param.NewParam(1).AddUInt(3), false},
		{"ts timestamp, value bigint unsigned", "now, 4", "value = ?", param.NewParam(1).AddUBigint(4), false},
		{"ts timestamp, value tinyint unsigned", "now, 1", "value = ?", param.NewParam(1).AddUTinyint(1), false},
		{"ts timestamp, value smallint unsigned", "now, 2", "value = ?", param.NewParam(1).AddUSmallint(2), false},
		{"ts timestamp, value int unsigned", "now, 3", "value = ?", param.NewParam(1).AddUInt(3), false},
		{"ts timestamp, value bigint unsigned", "now, 4", "value = ?", param.NewParam(1).AddUBigint(4), false},
		{"ts timestamp, value float", "now, 1.2", "value = ?", param.NewParam(1).AddFloat(1.2), false},
		{"ts timestamp, value double", "now, 1.3", "value = ?", param.NewParam(1).AddDouble(1.3), false},
		{"ts timestamp, value double", "now, 1.4", "value = ?", param.NewParam(1).AddDouble(1.4), false},
		{"ts timestamp, value binary(8)", "now, 'yes'", "value = ?", param.NewParam(1).AddBinary([]byte("yes")), false},
		{"ts timestamp, value nchar(8)", "now, 'OK'", "value = ?", param.NewParam(1).AddNchar("OK"), false},
		{"ts timestamp, value nchar(8)", "1622282105000000, 'NOW'", "ts = ? and value = ?", param.NewParam(2).AddTimestamp(time.Unix(1622282105, 0), common.PrecisionMicroSecond).AddBinary([]byte("NOW")), false},
		{"ts timestamp, value nchar(8)", "1622282105000000, 'NOW'", "ts = ? and value = ?", param.NewParam(2).AddBigint(1622282105000000).AddBinary([]byte("NOW")), false},
	} {
		tbName := fmt.Sprintf("test_stmt_query%02d", i)
		tbType := tc.tbType
		create := fmt.Sprintf("create table if not exists %s(%s)", tbName, tbType)
		insert := fmt.Sprintf("insert into %s values(%s)", tbName, tc.data)
		params := tc.params
		sql := fmt.Sprintf("select * from %s where %s", tbName, tc.clause)
		name := fmt.Sprintf("%02d-%s", i, tbType)
		var err error
		t.Run(name, func(t *testing.T) {
			if tc.skip {
				t.Skip("Skip, not support yet")
			}
			if _, err = db.Exec(create); err != nil {
				t.Error(err)
				return
			}
			if _, err = db.Exec(insert); err != nil {
				t.Error(err)
				return
			}
			var rows driver.Rows

			if rows, err = db.StmtQuery(sql, params); err != nil {
				t.Error(err)
				return
			}
			defer rows.Close()
			names := rows.Columns()
			if len(names) == 0 {
				t.Fatal(names)
			}
			values := make([]driver.Value, len(names))
			if err = rows.Next(values); err != nil {
				t.Error(err)
				return
			}
		})
	}
}

func TestFastInsert(t *testing.T) {
	db := testDatabase(t)
	now := time.Now()
	for i, tc := range []struct {
		tbType   string
		pos      string
		params   []*param.Param
		bindType *param.ColumnType
	}{
		{"ts timestamp, value int", "?, ?", []*param.Param{param.NewParam(1).AddTimestamp(now, common.PrecisionMicroSecond), param.NewParam(1).AddInt(1)}, param.NewColumnType(2).AddTimestamp().AddInt()},
		{"ts timestamp, value bool", "?, ?", []*param.Param{param.NewParam(1).AddTimestamp(now, common.PrecisionMicroSecond), param.NewParam(1).AddBool(true)}, param.NewColumnType(2).AddTimestamp().AddBool()},
		{"ts timestamp, value tinyint", "?, ?", []*param.Param{param.NewParam(1).AddTimestamp(now, common.PrecisionMicroSecond), param.NewParam(1).AddTinyint(1)}, param.NewColumnType(2).AddTimestamp().AddTinyint()},
		{"ts timestamp, value smallint", "?, ?", []*param.Param{param.NewParam(1).AddTimestamp(now, common.PrecisionMicroSecond), param.NewParam(1).AddSmallint(1)}, param.NewColumnType(2).AddTimestamp().AddSmallint()},
		{"ts timestamp, value bigint", "?, ?", []*param.Param{param.NewParam(1).AddTimestamp(now, common.PrecisionMicroSecond), param.NewParam(1).AddBigint(1)}, param.NewColumnType(2).AddTimestamp().AddBigint()},
		{"ts timestamp, value tinyint unsigned", "?, ?", []*param.Param{param.NewParam(1).AddTimestamp(now, common.PrecisionMicroSecond), param.NewParam(1).AddUTinyint(1)}, param.NewColumnType(2).AddTimestamp().AddUTinyint()},
		{"ts timestamp, value smallint unsigned", "?, ?", []*param.Param{param.NewParam(1).AddTimestamp(now, common.PrecisionMicroSecond), param.NewParam(1).AddUSmallint(1)}, param.NewColumnType(2).AddTimestamp().AddUSmallint()},
		{"ts timestamp, value int unsigned", "?, ?", []*param.Param{param.NewParam(1).AddTimestamp(now, common.PrecisionMicroSecond), param.NewParam(1).AddUInt(1)}, param.NewColumnType(2).AddTimestamp().AddUInt()},
		{"ts timestamp, value bigint unsigned", "?, ?", []*param.Param{param.NewParam(1).AddTimestamp(now, common.PrecisionMicroSecond), param.NewParam(1).AddUBigint(1)}, param.NewColumnType(2).AddTimestamp().AddUBigint()},
		{"ts timestamp, value float", "?, ?", []*param.Param{param.NewParam(1).AddTimestamp(now, common.PrecisionMicroSecond), param.NewParam(1).AddFloat(1.2)}, param.NewColumnType(2).AddTimestamp().AddFloat()},
		{"ts timestamp, value double", "?, ?", []*param.Param{param.NewParam(1).AddTimestamp(now, common.PrecisionMicroSecond), param.NewParam(1).AddDouble(1.2)}, param.NewColumnType(2).AddTimestamp().AddDouble()},
		{"ts timestamp, value binary(8)", "?, ?", []*param.Param{param.NewParam(1).AddTimestamp(now, common.PrecisionMicroSecond), param.NewParam(1).AddBinary([]byte("yes"))}, param.NewColumnType(2).AddTimestamp().AddBinary(3)},
		{"ts timestamp, value nchar(8)", "?, ?", []*param.Param{param.NewParam(1).AddTimestamp(now, common.PrecisionMicroSecond), param.NewParam(1).AddNchar("yes")}, param.NewColumnType(2).AddTimestamp().AddNchar(3)},
		{"ts timestamp, value nchar(8)", "?, ?", []*param.Param{param.NewParam(1).AddTimestamp(time.Now(), common.PrecisionMicroSecond), param.NewParam(1).AddNull()}, param.NewColumnType(2).AddTimestamp().AddNchar(1)},
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
			if rows, err = db.Query(fmt.Sprintf("select value from %s", tbName)); err != nil {
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

func TestFastInsertWithSetTableName(t *testing.T) {
	db := testDatabase(t)
	now := time.Now()
	for i, tc := range []struct {
		tbType   string
		pos      string
		params   []*param.Param
		bindType *param.ColumnType
	}{
		{"ts timestamp, value int", "?, ?", []*param.Param{param.NewParam(1).AddTimestamp(now, common.PrecisionMicroSecond), param.NewParam(1).AddInt(1)}, param.NewColumnType(2).AddTimestamp().AddInt()},
		{"ts timestamp, value bool", "?, ?", []*param.Param{param.NewParam(1).AddTimestamp(now, common.PrecisionMicroSecond), param.NewParam(1).AddBool(true)}, param.NewColumnType(2).AddTimestamp().AddBool()},
		{"ts timestamp, value tinyint", "?, ?", []*param.Param{param.NewParam(1).AddTimestamp(now, common.PrecisionMicroSecond), param.NewParam(1).AddTinyint(1)}, param.NewColumnType(2).AddTimestamp().AddTinyint()},
		{"ts timestamp, value smallint", "?, ?", []*param.Param{param.NewParam(1).AddTimestamp(now, common.PrecisionMicroSecond), param.NewParam(1).AddSmallint(1)}, param.NewColumnType(2).AddTimestamp().AddSmallint()},
		{"ts timestamp, value bigint", "?, ?", []*param.Param{param.NewParam(1).AddTimestamp(now, common.PrecisionMicroSecond), param.NewParam(1).AddBigint(1)}, param.NewColumnType(2).AddTimestamp().AddBigint()},
		{"ts timestamp, value tinyint unsigned", "?, ?", []*param.Param{param.NewParam(1).AddTimestamp(now, common.PrecisionMicroSecond), param.NewParam(1).AddUTinyint(1)}, param.NewColumnType(2).AddTimestamp().AddUTinyint()},
		{"ts timestamp, value smallint unsigned", "?, ?", []*param.Param{param.NewParam(1).AddTimestamp(now, common.PrecisionMicroSecond), param.NewParam(1).AddUSmallint(1)}, param.NewColumnType(2).AddTimestamp().AddUSmallint()},
		{"ts timestamp, value int unsigned", "?, ?", []*param.Param{param.NewParam(1).AddTimestamp(now, common.PrecisionMicroSecond), param.NewParam(1).AddUInt(1)}, param.NewColumnType(2).AddTimestamp().AddUInt()},
		{"ts timestamp, value bigint unsigned", "?, ?", []*param.Param{param.NewParam(1).AddTimestamp(now, common.PrecisionMicroSecond), param.NewParam(1).AddUBigint(1)}, param.NewColumnType(2).AddTimestamp().AddUBigint()},
		{"ts timestamp, value float", "?, ?", []*param.Param{param.NewParam(1).AddTimestamp(now, common.PrecisionMicroSecond), param.NewParam(1).AddFloat(1.2)}, param.NewColumnType(2).AddTimestamp().AddFloat()},
		{"ts timestamp, value double", "?, ?", []*param.Param{param.NewParam(1).AddTimestamp(now, common.PrecisionMicroSecond), param.NewParam(1).AddDouble(1.2)}, param.NewColumnType(2).AddTimestamp().AddDouble()},
		{"ts timestamp, value binary(8)", "?, ?", []*param.Param{param.NewParam(1).AddTimestamp(now, common.PrecisionMicroSecond), param.NewParam(1).AddBinary([]byte("yes"))}, param.NewColumnType(2).AddTimestamp().AddBinary(3)},
		{"ts timestamp, value nchar(8)", "?, ?", []*param.Param{param.NewParam(1).AddTimestamp(now, common.PrecisionMicroSecond), param.NewParam(1).AddNchar("yes")}, param.NewColumnType(2).AddTimestamp().AddNchar(3)},

		{"ts timestamp, value nchar(8)", "?, ?", []*param.Param{param.NewParam(1).AddTimestamp(time.Now(), common.PrecisionMicroSecond), param.NewParam(1).AddNull()}, param.NewColumnType(2).AddTimestamp().AddNchar(1)},
		{"ts timestamp, value int", "?, ?", []*param.Param{param.NewParam(1).AddTimestamp(now, common.PrecisionMicroSecond), param.NewParam(1).AddNull()}, param.NewColumnType(2).AddTimestamp().AddInt()},
		{"ts timestamp, value bool", "?, ?", []*param.Param{param.NewParam(1).AddTimestamp(now, common.PrecisionMicroSecond), param.NewParam(1).AddNull()}, param.NewColumnType(2).AddTimestamp().AddBool()},
		{"ts timestamp, value tinyint", "?, ?", []*param.Param{param.NewParam(1).AddTimestamp(now, common.PrecisionMicroSecond), param.NewParam(1).AddNull()}, param.NewColumnType(2).AddTimestamp().AddTinyint()},
		{"ts timestamp, value smallint", "?, ?", []*param.Param{param.NewParam(1).AddTimestamp(now, common.PrecisionMicroSecond), param.NewParam(1).AddNull()}, param.NewColumnType(2).AddTimestamp().AddSmallint()},
		{"ts timestamp, value bigint", "?, ?", []*param.Param{param.NewParam(1).AddTimestamp(now, common.PrecisionMicroSecond), param.NewParam(1).AddNull()}, param.NewColumnType(2).AddTimestamp().AddBigint()},
		{"ts timestamp, value tinyint unsigned", "?, ?", []*param.Param{param.NewParam(1).AddTimestamp(now, common.PrecisionMicroSecond), param.NewParam(1).AddNull()}, param.NewColumnType(2).AddTimestamp().AddUTinyint()},
		{"ts timestamp, value smallint unsigned", "?, ?", []*param.Param{param.NewParam(1).AddTimestamp(now, common.PrecisionMicroSecond), param.NewParam(1).AddNull()}, param.NewColumnType(2).AddTimestamp().AddUSmallint()},
		{"ts timestamp, value int unsigned", "?, ?", []*param.Param{param.NewParam(1).AddTimestamp(now, common.PrecisionMicroSecond), param.NewParam(1).AddNull()}, param.NewColumnType(2).AddTimestamp().AddUInt()},
		{"ts timestamp, value bigint unsigned", "?, ?", []*param.Param{param.NewParam(1).AddTimestamp(now, common.PrecisionMicroSecond), param.NewParam(1).AddNull()}, param.NewColumnType(2).AddTimestamp().AddUBigint()},
		{"ts timestamp, value float", "?, ?", []*param.Param{param.NewParam(1).AddTimestamp(now, common.PrecisionMicroSecond), param.NewParam(1).AddNull()}, param.NewColumnType(2).AddTimestamp().AddFloat()},
		{"ts timestamp, value double", "?, ?", []*param.Param{param.NewParam(1).AddTimestamp(now, common.PrecisionMicroSecond), param.NewParam(1).AddNull()}, param.NewColumnType(2).AddTimestamp().AddDouble()},
		{"ts timestamp, value binary(8)", "?, ?", []*param.Param{param.NewParam(1).AddTimestamp(now, common.PrecisionMicroSecond), param.NewParam(1).AddNull()}, param.NewColumnType(2).AddTimestamp().AddBinary(3)},
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
			if rows, err = db.Query(fmt.Sprintf("select value from %s", tbName)); err != nil {
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

func TestFastInsertWithSetTableNameTag(t *testing.T) {
	db := testDatabase(t)
	now := time.Now()
	_, err := db.Exec("create stable if not exists set_table_name_tag_int (ts timestamp,value int) tags(i smallint,v binary(8))")
	if err != nil {
		t.Error(err)
		return
	}
	_, err = db.Exec("create stable if not exists set_table_name_tag_nchar (ts timestamp,value nchar(8)) tags(i smallint,v binary(8))")
	if err != nil {
		t.Error(err)
		return
	}
	for i, tc := range []struct {
		sTableName string
		tags       *param.Param
		tbType     string
		pos        string
		params     []*param.Param
		bindType   *param.ColumnType
	}{
		{"set_table_name_tag_int", param.NewParam(2).AddSmallint(1).AddBinary([]byte("int")), "ts timestamp, value int", "?, ?", []*param.Param{param.NewParam(1).AddTimestamp(now, common.PrecisionMicroSecond), param.NewParam(1).AddInt(1)}, param.NewColumnType(2).AddTimestamp().AddInt()},
		{"set_table_name_tag_nchar", param.NewParam(2).AddSmallint(2).AddBinary([]byte("nchar")), "ts timestamp, value nchar(8)", "?, ?", []*param.Param{param.NewParam(1).AddTimestamp(time.Now(), common.PrecisionMicroSecond), param.NewParam(1).AddNull()}, param.NewColumnType(2).AddTimestamp().AddNchar(1)},
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
			if rows, err = db.Query(fmt.Sprintf("select value from %s", tbName)); err != nil {
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

func TestFastInsertWithSetSubTableName(t *testing.T) {
	db := testDatabase(t)
	now := time.Now()
	_, err := db.Exec("create stable if not exists set_table_name_sub_int (ts timestamp,value int) tags(i smallint,v binary(8))")
	if err != nil {
		t.Error(err)
		return
	}
	_, err = db.Exec("create stable if not exists set_table_name_sub_nchar (ts timestamp,value nchar(8)) tags(i smallint,v binary(8))")
	if err != nil {
		t.Error(err)
		return
	}
	err = db.LoadTableInfo([]string{"test_fast_insert_with_sub_table_name_00", "test_fast_insert_with_sub_table_name_01"})
	if err != nil {
		t.Error(err)
		return
	}
	for i, tc := range []struct {
		sTableName string
		tags       string
		tbType     string
		pos        string
		params     []*param.Param
		bindType   *param.ColumnType
	}{
		{"set_table_name_sub_int", "1,'int'", "ts timestamp, value int", "?, ?", []*param.Param{param.NewParam(1).AddTimestamp(now, common.PrecisionMicroSecond), param.NewParam(1).AddInt(1)}, param.NewColumnType(2).AddTimestamp().AddInt()},
		{"set_table_name_sub_nchar", "2,'nchar'", "ts timestamp, value nchar(8)", "?, ?", []*param.Param{param.NewParam(1).AddTimestamp(time.Now(), common.PrecisionMicroSecond), param.NewParam(1).AddNchar("ttt")}, param.NewColumnType(2).AddTimestamp().AddNchar(1)},
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
			if rows, err = db.Query(fmt.Sprintf("select value from %s", tbName)); err != nil {
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

func TestInfluxDBInsertLines(t *testing.T) {
	db := testDatabase(t)
	err := db.InfluxDBInsertLines([]string{
		"measurement,host=host1 field1=2i,field2=2.0 1577836800000000000",
	}, "ns")
	if err != nil {
		t.Error(err)
		return
	}
}

func TestOpenTSDBInsertTelnetLines(t *testing.T) {
	db := testDatabase(t)
	err := db.OpenTSDBInsertTelnetLines([]string{
		"sys_if_bytes_out 1479496100 1.3E3 host=web01 interface=eth0",
		"sys_procs_running 1479496100 42 host=web01",
	})
	if err != nil {
		t.Error(err)
		return
	}
}

func TestOpenTSDBInsertJsonPayload(t *testing.T) {
	db := testDatabase(t)
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
