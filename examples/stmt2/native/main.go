package main

import (
	"database/sql/driver"
	"fmt"
	"log"
	"time"

	"github.com/taosdata/driver-go/v3/af"
	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/common/stmt"
)

func main() {
	db, err := af.Open("127.0.0.1", "root", "taosdata", "", 0)
	if err != nil {
		log.Fatalln("Failed to connect to 127.0.0.1, ErrMessage: " + err.Error())
	}
	defer db.Close()
	// prepare database and table
	_, err = db.Exec("CREATE DATABASE IF NOT EXISTS example_stmt2")
	if err != nil {
		log.Fatalln("Failed to create database example_stmt2, ErrMessage: " + err.Error())
	}
	_, err = db.Exec("USE example_stmt2")
	if err != nil {
		log.Fatalln("Failed to use database example_stmt2, ErrMessage: " + err.Error())
	}
	_, err = db.Exec("CREATE STABLE IF NOT EXISTS example_stmt2.stb_json (" +
		"ts TIMESTAMP, " +
		"int_col INT) " +
		"tags (json_tag json)")
	if err != nil {
		log.Fatalln("Failed to create table stb_json, ErrMessage: " + err.Error())
	}
	_, err = db.Exec("CREATE STABLE IF NOT EXISTS example_stmt2.stb (" +
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
	db, err := af.Open("127.0.0.1", "root", "taosdata", "example_stmt2", 0)
	if err != nil {
		log.Fatalln("Failed to connect to 127.0.0.1, ErrMessage: " + err.Error())
	}
	defer db.Close()
	reqID := common.GetReqID()
	fmt.Printf("reqID: 0x%x\n", reqID)
	stmt2 := db.Stmt2(reqID, false)
	defer stmt2.Close()
	// prepare statement with json
	sql := "INSERT INTO ntb_json using stb_json tags(?) VALUES (?,?)"
	err = stmt2.Prepare(sql)
	if err != nil {
		log.Fatalln("Failed to prepare sql, sql: " + sql + ", ErrMessage: " + err.Error())
	}
	// bind data
	current := time.Now()
	bindData := &stmt.TaosStmt2BindData{
		Cols: [][]driver.Value{
			{
				current,
			},
			{
				int32(1),
			},
		},
		Tags: []driver.Value{
			[]byte("{\"device\":\"device_1\"}"),
		},
	}
	params := []*stmt.TaosStmt2BindData{bindData}
	err = stmt2.Bind(params)
	if err != nil {
		log.Fatalln("Failed to bind params, ErrMessage: " + err.Error())
	}
	// execute batch
	err = stmt2.Execute()
	if err != nil {
		log.Fatalln("Failed to exec, ErrMessage: " + err.Error())
	}
	// get affected rows
	affected := stmt2.GetAffectedRows()
	// you can check exeResult here
	fmt.Printf("Successfully inserted %d rows.\n", affected)
}

func stmtWithoutJson() {
	db, err := af.Open("127.0.0.1", "root", "taosdata", "example_stmt2", 0)
	if err != nil {
		log.Fatalln("Failed to connect to 127.0.0.1, ErrMessage: " + err.Error())
	}
	defer db.Close()
	reqID := common.GetReqID()
	fmt.Printf("reqID: 0x%x\n", reqID)
	stmt2 := db.Stmt2(reqID, false)
	defer stmt2.Close()
	// prepare statement without json
	sql := "INSERT INTO ntb using stb tags(?,?,?,?,?,?,?) VALUES (?,?,?,?,?,?,?,?)"
	err = stmt2.Prepare(sql)
	if err != nil {
		log.Fatalln("Failed to prepare sql, sql: " + sql + ", ErrMessage: " + err.Error())
	}
	current := time.Now()
	bindData := &stmt.TaosStmt2BindData{
		Cols: [][]driver.Value{
			{
				current,
			},
			{
				int32(1),
			},
			{
				float64(1.1),
			},
			{
				true,
			},
			{
				[]byte("binary_value"),
			},
			{
				"nchar_value",
			},
			{
				[]byte{0x98, 0xf4, 0x6e},
			},
			{
				[]byte{
					0x01, 0x01, 0x00, 0x00,
					0x00, 0x00, 0x00, 0x00,
					0x00, 0x00, 0x00, 0x59,
					0x40, 0x00, 0x00, 0x00,
					0x00, 0x00, 0x00, 0x59, 0x40,
				},
			},
		},
		Tags: []driver.Value{
			int32(1),
			float64(1.1),
			true,
			[]byte("binary_value"),
			"nchar_value",
			[]byte{0x98, 0xf4, 0x6e},
			[]byte{
				0x01, 0x01, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x59,
				0x40, 0x00, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x59, 0x40,
			},
		},
	}
	params := []*stmt.TaosStmt2BindData{bindData}
	err = stmt2.Bind(params)
	if err != nil {
		log.Fatalln("Failed to bind params, ErrMessage: " + err.Error())
	}

	// execute batch
	err = stmt2.Execute()
	if err != nil {
		log.Fatalln("Failed to exec, ErrMessage: " + err.Error())
	}
	// get affected rows
	affected := stmt2.GetAffectedRows()
	// you can check exeResult here
	fmt.Printf("Successfully inserted %d rows.\n", affected)
}
