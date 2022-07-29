package main

import (
	"time"

	"github.com/taosdata/driver-go/v3/af"
	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/common/param"
)

func main() {
	db, err := af.Open("", "root", "taosdata", "", 0)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	_, err = db.Exec("create database if not exists example_stmt")
	if err != nil {
		panic(err)
	}
	_, err = db.Exec("create table if not exists example_stmt.tb1(ts timestamp," +
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
		panic(err)
	}
	stmt := db.InsertStmt()
	err = stmt.Prepare("insert into example_stmt.tb1 values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
	if err != nil {
		panic(err)
	}
	now := time.Now()
	params := make([]*param.Param, 14)
	params[0] = param.NewParam(2).
		AddTimestamp(now, common.PrecisionMilliSecond).
		AddTimestamp(now.Add(time.Second), common.PrecisionMilliSecond)
	params[1] = param.NewParam(2).AddBool(true).AddNull()
	params[2] = param.NewParam(2).AddTinyint(2).AddNull()
	params[3] = param.NewParam(2).AddSmallint(3).AddNull()
	params[4] = param.NewParam(2).AddInt(4).AddNull()
	params[5] = param.NewParam(2).AddBigint(5).AddNull()
	params[6] = param.NewParam(2).AddUTinyint(6).AddNull()
	params[7] = param.NewParam(2).AddUSmallint(7).AddNull()
	params[8] = param.NewParam(2).AddUInt(8).AddNull()
	params[9] = param.NewParam(2).AddUBigint(9).AddNull()
	params[10] = param.NewParam(2).AddFloat(10).AddNull()
	params[11] = param.NewParam(2).AddDouble(11).AddNull()
	params[12] = param.NewParam(2).AddBinary([]byte("binary")).AddNull()
	params[13] = param.NewParam(2).AddNchar("nchar").AddNull()

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
		AddBinary(6).
		AddNchar(5)
	err = stmt.BindParam(params, paramTypes)
	if err != nil {
		panic(err)
	}
	err = stmt.AddBatch()
	if err != nil {
		panic(err)
	}
	err = stmt.Execute()
	if err != nil {
		panic(err)
	}
	err = stmt.Close()
	if err != nil {
		panic(err)
	}
	// select * from example_stmt.tb1
}
