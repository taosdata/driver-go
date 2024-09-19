package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/common/param"
	_ "github.com/taosdata/driver-go/v3/taosWS"
	"github.com/taosdata/driver-go/v3/ws/stmt"
)

func main() {
	var taosDSN = "root:taosdata@ws(localhost:6041)/"
	db, err := sql.Open("taosWS", taosDSN)
	if err != nil {
		log.Fatalln("Failed to connect to " + taosDSN + ", ErrMessage: " + err.Error())
	}
	defer db.Close()
	// create database
	res, err := db.Exec("CREATE DATABASE IF NOT EXISTS example_all_type_stmt")
	if err != nil {
		log.Fatalln("Failed to create database example_all_type_stmt, ErrMessage: " + err.Error())
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		log.Fatalln("Failed to get create database rowsAffected, ErrMessage: " + err.Error())
	}
	// you can check rowsAffected here
	fmt.Println("Create database example_all_type_stmt successfully, rowsAffected: ", rowsAffected)
	// create table with json
	res, err = db.Exec("CREATE STABLE IF NOT EXISTS example_all_type_stmt.stb_json (" +
		"ts TIMESTAMP, " +
		"int_col INT) " +
		"tags (json_tag json)")
	if err != nil {
		log.Fatalln("Failed to create table example_all_type_stmt.stb_json, ErrMessage: " + err.Error())
	}
	rowsAffected, err = res.RowsAffected()
	if err != nil {
		log.Fatalln("Failed to get create table rowsAffected, ErrMessage: " + err.Error())
	}
	// you can check rowsAffected here
	fmt.Println("Create table example_all_type_stmt.stb_json successfully, rowsAffected:", rowsAffected)
	// create table without json
	res, err = db.Exec("CREATE STABLE IF NOT EXISTS example_all_type_stmt.stb (" +
		"ts TIMESTAMP, " +
		"int_col INT, " +
		"double_col DOUBLE, " +
		"bool_col BOOL, " +
		"binary_col BINARY(100), " +
		"nchar_col NCHAR(100), " +
		"varbinary_col VARBINARY(100), " +
		"geometry_col GEOMETRY(100)) " +
		"tags (" +
		"int_tag INT, " +
		"double_tag DOUBLE, " +
		"bool_tag BOOL, " +
		"binary_tag BINARY(100), " +
		"nchar_tag NCHAR(100), " +
		"varbinary_tag VARBINARY(100), " +
		"geometry_tag GEOMETRY(100))")
	if err != nil {
		log.Fatalln("Failed to create table example_all_type_stmt.stb, ErrMessage: " + err.Error())
	}
	rowsAffected, err = res.RowsAffected()
	if err != nil {
		log.Fatalln("Failed to get create table rowsAffected, ErrMessage: " + err.Error())
	}
	// you can check rowsAffected here
	fmt.Println("Create table example_all_type_stmt.stb successfully, rowsAffected:", rowsAffected)

	// stmt bind with json tag
	stmtWithJson()
	// stmt bind without json tag
	stmtWithoutJson()
}

func stmtWithJson() {
	config := stmt.NewConfig(fmt.Sprintf("ws://127.0.0.1:6041"), 0)
	config.SetConnectUser("root")
	config.SetConnectPass("taosdata")
	config.SetConnectDB("example_all_type_stmt")
	config.SetMessageTimeout(common.DefaultMessageTimeout)
	config.SetWriteWait(common.DefaultWriteWait)

	connector, err := stmt.NewConnector(config)
	if err != nil {
		log.Fatalln("Failed to create stmt connector, ErrMessage: " + err.Error())
	}
	stmt, err := connector.Init()
	if err != nil {
		log.Fatalln("Failed to init stmt, ErrMessage: " + err.Error())
	}
	defer stmt.Close()
	// prepare statement with json
	sql := "INSERT INTO ? using stb_json tags(?) VALUES (?,?)"
	err = stmt.Prepare(sql)
	if err != nil {
		log.Fatal("Failed to prepare sql, sql: " + sql + ", ErrMessage: " + err.Error())
	}
	// set table name
	err = stmt.SetTableName("ntb_json")
	if err != nil {
		log.Fatal("Failed to set table name, ErrMessage: " + err.Error())
	}
	// set tags
	tagType := param.NewColumnType(1).AddJson(100)
	tagData := param.NewParam(1).AddJson([]byte("{\"device\":\"device_1\"}"))
	err = stmt.SetTags(tagData, tagType)
	if err != nil {
		log.Fatal("Failed to set table name, ErrMessage: " + err.Error())
	}

	// bind column data
	columnType := param.NewColumnType(2).AddTimestamp().AddInt()
	current := time.Now()
	columnData := make([]*param.Param, 2)
	columnData[0] = param.NewParam(1).AddTimestamp(current, common.PrecisionMilliSecond)
	columnData[1] = param.NewParam(1).AddInt(1)
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
	fmt.Printf("Successfully inserted %d rows to example_all_type_stmt.ntb_json.\n", affected)
}

func stmtWithoutJson() {
	config := stmt.NewConfig(fmt.Sprintf("ws://127.0.0.1:6041"), 0)
	config.SetConnectUser("root")
	config.SetConnectPass("taosdata")
	config.SetConnectDB("example_all_type_stmt")
	config.SetMessageTimeout(common.DefaultMessageTimeout)
	config.SetWriteWait(common.DefaultWriteWait)

	connector, err := stmt.NewConnector(config)
	if err != nil {
		log.Fatalln("Failed to create stmt connector, ErrMessage: " + err.Error())
	}
	stmt, err := connector.Init()
	if err != nil {
		log.Fatalln("Failed to init stmt, ErrMessage: " + err.Error())
	}
	defer stmt.Close()
	sql := "INSERT INTO ? using stb tags(?,?,?,?,?,?,?) VALUES (?,?,?,?,?,?,?,?)"
	err = stmt.Prepare(sql)
	if err != nil {
		log.Fatal("Failed to prepare sql, sql: " + sql + ", ErrMessage: " + err.Error())
	}
	// set table name
	err = stmt.SetTableName("ntb")
	if err != nil {
		log.Fatal("Failed to set table name, ErrMessage: " + err.Error())
	}
	// set tags
	tagType := param.NewColumnType(7).
		AddInt().
		AddDouble().
		AddBool().
		AddBinary(100).
		AddNchar(100).
		AddVarBinary(100).
		AddGeometry(100)
	tagData := param.NewParam(7).
		AddInt(1).
		AddDouble(1.1).
		AddBool(true).
		AddBinary([]byte("binary_value")).
		AddNchar("nchar_value").
		AddVarBinary([]byte{0x98, 0xf4, 0x6e}).
		AddGeometry([]byte{
			0x01, 0x01, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x59,
			0x40, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x59, 0x40,
		})
	err = stmt.SetTags(tagData, tagType)
	if err != nil {
		log.Fatal("Failed to set table name, ErrMessage: " + err.Error())
	}

	// bind column data
	columnType := param.NewColumnType(8).AddTimestamp().
		AddInt().
		AddDouble().
		AddBool().
		AddBinary(100).
		AddNchar(100).
		AddVarBinary(100).
		AddGeometry(100)
	current := time.Now()
	columnData := make([]*param.Param, 8)
	columnData[0] = param.NewParam(1).AddTimestamp(current, common.PrecisionMilliSecond)
	columnData[1] = param.NewParam(1).AddInt(1)
	columnData[2] = param.NewParam(1).AddDouble(1.1)
	columnData[3] = param.NewParam(1).AddBool(true)
	columnData[4] = param.NewParam(1).AddBinary([]byte("binary_value"))
	columnData[5] = param.NewParam(1).AddNchar("nchar_value")
	columnData[6] = param.NewParam(1).AddVarBinary([]byte{0x98, 0xf4, 0x6e})
	columnData[7] = param.NewParam(1).AddGeometry([]byte{
		0x01, 0x01, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x59,
		0x40, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x59, 0x40,
	})
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
	fmt.Printf("Successfully inserted %d rows to example_all_type_stmt.ntb.\n", affected)
}
