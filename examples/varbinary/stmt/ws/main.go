package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/common/param"
	_ "github.com/taosdata/driver-go/v3/taosRestful"
	"github.com/taosdata/driver-go/v3/ws/stmt"
)

func main() {
	host := "127.0.0.1"

	taosDSN := fmt.Sprintf("root:taosdata@http(%s:6041)/", host)
	db, err := sql.Open("taosRestful", taosDSN)
	if err != nil {
		log.Fatalln("Failed to connect to " + taosDSN + "; ErrMessage: " + err.Error())
	}
	defer db.Close()
	// prepare database and table
	_, err = db.Exec("CREATE DATABASE IF NOT EXISTS example_stmt_varbinary_ws")
	if err != nil {
		log.Fatalln("Failed to create database example_stmt_varbinary_ws, ErrMessage: " + err.Error())
	}
	_, err = db.Exec("CREATE TABLE IF NOT EXISTS example_stmt_varbinary_ws.ntb (ts TIMESTAMP, val VARBINARY(100))")
	if err != nil {
		log.Fatalln("Failed to create table example_stmt_varbinary_ws.ntb, ErrMessage: " + err.Error())
	}

	config := stmt.NewConfig(fmt.Sprintf("ws://%s:6041", host), 0)
	config.SetConnectUser("root")
	config.SetConnectPass("taosdata")
	config.SetConnectDB("example_stmt_varbinary_ws")
	config.SetMessageTimeout(common.DefaultMessageTimeout)
	config.SetWriteWait(common.DefaultWriteWait)

	connector, err := stmt.NewConnector(config)
	if err != nil {
		log.Fatalln("Failed to create stmt connector,url: " + taosDSN + "; ErrMessage: " + err.Error())
	}
	// prepare statement
	sql := "INSERT INTO ntb VALUES (?,?)"
	stmt, err := connector.Init()
	if err != nil {
		log.Fatalln("Failed to init stmt, sql: " + sql + ", ErrMessage: " + err.Error())
	}
	err = stmt.Prepare(sql)
	if err != nil {
		log.Fatal("Failed to prepare sql, sql: " + sql + ", ErrMessage: " + err.Error())
	}

	columnType := param.NewColumnType(2).AddTimestamp().AddVarBinary(100)

	// bind column data
	current := time.Now()
	// "\x98f46e"
	varbinaryData := []byte{0x98, 0xf4, 0x6e}

	columnData := make([]*param.Param, 2)
	columnData[0] = param.NewParam(1).AddTimestamp(current, common.PrecisionMilliSecond)
	columnData[1] = param.NewParam(1).AddVarBinary(varbinaryData)
	err = stmt.BindParam(columnData, columnType)
	if err != nil {
		log.Fatal("Failed to bind params, ErrMessage: " + err.Error())
	}

	// add batch
	err = stmt.AddBatch()
	if err != nil {
		log.Fatal("Failed to add batch, ErrMessage: " + err.Error())
	}
	// execute batch
	err = stmt.Exec()
	if err != nil {
		log.Fatal("Failed to exec, ErrMessage: " + err.Error())
	}
	// get affected rows
	affected := stmt.GetAffectedRows()
	// you can check exeResult here
	fmt.Printf("Successfully inserted %d rows to example_stmt_varbinary_ws.ntb.\n", affected)

	err = stmt.Close()
	if err != nil {
		log.Fatal("Failed to close stmt, ErrMessage: " + err.Error())
	}
}
