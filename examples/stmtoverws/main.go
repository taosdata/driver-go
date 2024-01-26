package main

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/common/param"
	_ "github.com/taosdata/driver-go/v3/taosRestful"
	"github.com/taosdata/driver-go/v3/ws/stmt"
)

func main() {
	db, err := sql.Open("taosRestful", "root:taosdata@http(localhost:6041)/")
	if err != nil {
		panic(err)
	}
	defer db.Close()
	prepareEnv(db)

	config := stmt.NewConfig("ws://127.0.0.1:6041", 0)
	config.SetConnectUser("root")
	config.SetConnectPass("taosdata")
	config.SetConnectDB("example_ws_stmt")
	config.SetMessageTimeout(common.DefaultMessageTimeout)
	config.SetWriteWait(common.DefaultWriteWait)
	config.SetErrorHandler(func(connector *stmt.Connector, err error) {
		panic(err)
	})
	config.SetCloseHandler(func() {
		fmt.Println("stmt connector closed")
	})

	connector, err := stmt.NewConnector(config)
	if err != nil {
		panic(err)
	}
	now := time.Now()
	{
		stmt, err := connector.Init()
		if err != nil {
			panic(err)
		}
		err = stmt.Prepare("insert into ? using all_json tags(?) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
		if err != nil {
			panic(err)
		}
		err = stmt.SetTableName("tb1")
		if err != nil {
			panic(err)
		}
		err = stmt.SetTags(param.NewParam(1).AddJson([]byte(`{"tb":1}`)), param.NewColumnType(1).AddJson(0))
		if err != nil {
			panic(err)
		}
		params := []*param.Param{
			param.NewParam(3).AddTimestamp(now, 0).AddTimestamp(now.Add(time.Second), 0).AddTimestamp(now.Add(time.Second*2), 0),
			param.NewParam(3).AddBool(true).AddNull().AddBool(true),
			param.NewParam(3).AddTinyint(1).AddNull().AddTinyint(1),
			param.NewParam(3).AddSmallint(1).AddNull().AddSmallint(1),
			param.NewParam(3).AddInt(1).AddNull().AddInt(1),
			param.NewParam(3).AddBigint(1).AddNull().AddBigint(1),
			param.NewParam(3).AddUTinyint(1).AddNull().AddUTinyint(1),
			param.NewParam(3).AddUSmallint(1).AddNull().AddUSmallint(1),
			param.NewParam(3).AddUInt(1).AddNull().AddUInt(1),
			param.NewParam(3).AddUBigint(1).AddNull().AddUBigint(1),
			param.NewParam(3).AddFloat(1).AddNull().AddFloat(1),
			param.NewParam(3).AddDouble(1).AddNull().AddDouble(1),
			param.NewParam(3).AddBinary([]byte("test_binary")).AddNull().AddBinary([]byte("test_binary")),
			param.NewParam(3).AddNchar("test_nchar").AddNull().AddNchar("test_nchar"),
		}
		paramTypes := param.NewColumnType(14).
			AddTimestamp().
			AddBool().
			AddTinyint().
			AddSmallint().
			AddInt().
			AddBigint().
			AddUTinyint().
			AddUSmallint().
			AddUInt().
			AddUBigint().
			AddFloat().
			AddDouble().
			AddBinary(0).
			AddNchar(0)
		err = stmt.BindParam(params, paramTypes)
		if err != nil {
			panic(err)
		}
		err = stmt.AddBatch()
		if err != nil {
			panic(err)
		}
		err = stmt.Exec()
		if err != nil {
			panic(err)
		}
		affected := stmt.GetAffectedRows()
		fmt.Println("all_json affected rows:", affected)
		err = stmt.Close()
		if err != nil {
			panic(err)
		}
	}
	{
		stmt, err := connector.Init()
		if err != nil {
			panic(err)
		}
		err = stmt.Prepare("insert into ? using all_all tags(?,?,?,?,?,?,?,?,?,?,?,?,?,?) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
		err = stmt.SetTableName("tb1")
		if err != nil {
			panic(err)
		}

		err = stmt.SetTableName("tb2")
		if err != nil {
			panic(err)
		}
		err = stmt.SetTags(
			param.NewParam(14).
				AddTimestamp(now, 0).
				AddBool(true).
				AddTinyint(2).
				AddSmallint(2).
				AddInt(2).
				AddBigint(2).
				AddUTinyint(2).
				AddUSmallint(2).
				AddUInt(2).
				AddUBigint(2).
				AddFloat(2).
				AddDouble(2).
				AddBinary([]byte("tb2")).
				AddNchar("tb2"),
			param.NewColumnType(14).
				AddTimestamp().
				AddBool().
				AddTinyint().
				AddSmallint().
				AddInt().
				AddBigint().
				AddUTinyint().
				AddUSmallint().
				AddUInt().
				AddUBigint().
				AddFloat().
				AddDouble().
				AddBinary(0).
				AddNchar(0),
		)
		if err != nil {
			panic(err)
		}
		params := []*param.Param{
			param.NewParam(3).AddTimestamp(now, 0).AddTimestamp(now.Add(time.Second), 0).AddTimestamp(now.Add(time.Second*2), 0),
			param.NewParam(3).AddBool(true).AddNull().AddBool(true),
			param.NewParam(3).AddTinyint(1).AddNull().AddTinyint(1),
			param.NewParam(3).AddSmallint(1).AddNull().AddSmallint(1),
			param.NewParam(3).AddInt(1).AddNull().AddInt(1),
			param.NewParam(3).AddBigint(1).AddNull().AddBigint(1),
			param.NewParam(3).AddUTinyint(1).AddNull().AddUTinyint(1),
			param.NewParam(3).AddUSmallint(1).AddNull().AddUSmallint(1),
			param.NewParam(3).AddUInt(1).AddNull().AddUInt(1),
			param.NewParam(3).AddUBigint(1).AddNull().AddUBigint(1),
			param.NewParam(3).AddFloat(1).AddNull().AddFloat(1),
			param.NewParam(3).AddDouble(1).AddNull().AddDouble(1),
			param.NewParam(3).AddBinary([]byte("test_binary")).AddNull().AddBinary([]byte("test_binary")),
			param.NewParam(3).AddNchar("test_nchar").AddNull().AddNchar("test_nchar"),
		}
		paramTypes := param.NewColumnType(14).
			AddTimestamp().
			AddBool().
			AddTinyint().
			AddSmallint().
			AddInt().
			AddBigint().
			AddUTinyint().
			AddUSmallint().
			AddUInt().
			AddUBigint().
			AddFloat().
			AddDouble().
			AddBinary(0).
			AddNchar(0)
		err = stmt.BindParam(params, paramTypes)
		if err != nil {
			panic(err)
		}
		err = stmt.AddBatch()
		if err != nil {
			panic(err)
		}
		err = stmt.Exec()
		if err != nil {
			panic(err)
		}
		affected := stmt.GetAffectedRows()
		fmt.Println("all_all affected rows:", affected)
		err = stmt.Close()
		if err != nil {
			panic(err)
		}

	}
}

func prepareEnv(db *sql.DB) {
	steps := []string{
		"create database example_ws_stmt",
		"create table example_ws_stmt.all_json(ts timestamp," +
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
			")" +
			"tags(t json)",
		"create table example_ws_stmt.all_all(" +
			"ts timestamp," +
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
			")" +
			"tags(" +
			"tts timestamp," +
			"tc1 bool," +
			"tc2 tinyint," +
			"tc3 smallint," +
			"tc4 int," +
			"tc5 bigint," +
			"tc6 tinyint unsigned," +
			"tc7 smallint unsigned," +
			"tc8 int unsigned," +
			"tc9 bigint unsigned," +
			"tc10 float," +
			"tc11 double," +
			"tc12 binary(20)," +
			"tc13 nchar(20))",
	}
	for _, step := range steps {
		_, err := db.Exec(step)
		if err != nil {
			panic(err)
		}
	}
}
