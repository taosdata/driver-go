package main

import (
	"fmt"
	"log"
	"time"

	"github.com/taosdata/driver-go/v3/af"
	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/common/param"
)

func main() {
	db, err := af.Open("127.0.0.1", "root", "taosdata", "", 0)
	if err != nil {
		log.Fatalln("Failed to connect to 127.0.0.1, ErrMessage: " + err.Error())
	}
	defer db.Close()
	// prepare database and table
	_, err = db.Exec("CREATE DATABASE IF NOT EXISTS example_all_type_stmt")
	if err != nil {
		log.Fatalln("Failed to create database example_all_type_stmt, ErrMessage: " + err.Error())
	}
	_, err = db.Exec("USE example_all_type_stmt")
	if err != nil {
		log.Fatalln("Failed to use database example_all_type_stmt, ErrMessage: " + err.Error())
	}
	_, err = db.Exec("CREATE STABLE IF NOT EXISTS example_all_type_stmt.stb_json (" +
		"ts TIMESTAMP, " +
		"int_col INT) " +
		"tags (json_tag json)")
	if err != nil {
		log.Fatalln("Failed to create table stb_json, ErrMessage: " + err.Error())
	}
	_, err = db.Exec("CREATE STABLE IF NOT EXISTS example_all_type_stmt.stb (" +
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
		log.Fatalln("Failed to create table stb, ErrMessage: " + err.Error())
	}
	// stmt bind with json tag
	stmtWithJson()
	// stmt bind without json tag
	stmtWithoutJson()
}

func stmtWithJson() {
	db, err := af.Open("127.0.0.1", "root", "taosdata", "example_all_type_stmt", 0)
	if err != nil {
		log.Fatalln("Failed to connect to 127.0.0.1, ErrMessage: " + err.Error())
	}
	defer db.Close()
	stmt := db.Stmt()
	defer stmt.Close()
	// prepare statement with json
	sql := "INSERT INTO ? using stb_json tags(?) VALUES (?,?)"
	err = stmt.Prepare(sql)
	if err != nil {
		log.Fatalln("Failed to prepare sql, sql: " + sql + ", ErrMessage: " + err.Error())
	}
	// set table name with tags
	err = stmt.SetTableNameWithTags("ntb_json", param.NewParam(2).AddJson([]byte("{\"device\":\"device_1\"}")))
	if err != nil {
		log.Fatalln("Failed to set table name with tags, ErrMessage: " + err.Error())
	}
	// bind column data
	current := time.Now()
	row := param.NewParam(2).
		AddTimestamp(current, common.PrecisionMilliSecond).
		AddInt(1)
	err = stmt.BindRow(row)
	if err != nil {
		log.Fatalln("Failed to bind params, ErrMessage: " + err.Error())
	}

	// add batch
	err = stmt.AddBatch()
	if err != nil {
		log.Fatalln("Failed to add batch, ErrMessage: " + err.Error())
	}
	// execute batch
	err = stmt.Execute()
	if err != nil {
		log.Fatalln("Failed to exec, ErrMessage: " + err.Error())
	}
	// get affected rows
	affected := stmt.GetAffectedRows()
	// you can check exeResult here
	fmt.Printf("Successfully inserted %d rows.\n", affected)
}

func stmtWithoutJson() {
	db, err := af.Open("127.0.0.1", "root", "taosdata", "example_all_type_stmt", 0)
	if err != nil {
		log.Fatalln("Failed to connect to 127.0.0.1, ErrMessage: " + err.Error())
	}
	defer db.Close()
	stmt := db.Stmt()
	defer stmt.Close()
	// prepare statement without json
	sql := "INSERT INTO ? using stb tags(?,?,?,?,?,?,?) VALUES (?,?,?,?,?,?,?,?)"
	err = stmt.Prepare(sql)
	if err != nil {
		log.Fatalln("Failed to prepare sql, sql: " + sql + ", ErrMessage: " + err.Error())
	}
	// set table name with tags
	err = stmt.SetTableNameWithTags("ntb", param.NewParam(7).
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
		}))
	if err != nil {
		log.Fatalln("Failed to set table name with tags, ErrMessage: " + err.Error())
	}
	// bind column data
	current := time.Now()
	row := param.NewParam(8).
		AddTimestamp(current, common.PrecisionMilliSecond).
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
	err = stmt.BindRow(row)
	if err != nil {
		log.Fatalln("Failed to bind params, ErrMessage: " + err.Error())
	}

	// add batch
	err = stmt.AddBatch()
	if err != nil {
		log.Fatalln("Failed to add batch, ErrMessage: " + err.Error())
	}
	// execute batch
	err = stmt.Execute()
	if err != nil {
		log.Fatalln("Failed to exec, ErrMessage: " + err.Error())
	}
	// get affected rows
	affected := stmt.GetAffectedRows()
	// you can check exeResult here
	fmt.Printf("Successfully inserted %d rows.\n", affected)
}
