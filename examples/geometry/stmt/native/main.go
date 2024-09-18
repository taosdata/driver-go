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
	host := "127.0.0.1"
	db, err := af.Open(host, "root", "taosdata", "", 0)
	if err != nil {
		log.Fatalln("Failed to connect to " + host + "; ErrMessage: " + err.Error())
	}
	defer db.Close()
	// prepare database and table
	_, err = db.Exec("CREATE DATABASE IF NOT EXISTS example_stmt_geometry_native")
	if err != nil {
		log.Fatalln("Failed to create database example_stmt_geometry_native, ErrMessage: " + err.Error())
	}
	_, err = db.Exec("USE example_stmt_geometry_native")
	if err != nil {
		log.Fatalln("Failed to use database example_stmt_geometry_native, ErrMessage: " + err.Error())
	}
	_, err = db.Exec("CREATE TABLE IF NOT EXISTS ntb (ts TIMESTAMP, val GEOMETRY(100))")
	if err != nil {
		log.Fatalln("Failed to create table ntb, ErrMessage: " + err.Error())
	}
	// prepare statement
	sql := "INSERT INTO ntb VALUES (?,?)"
	stmt := db.Stmt()
	err = stmt.Prepare(sql)
	if err != nil {
		log.Fatalln("Failed to prepare sql, sql: " + sql + ", ErrMessage: " + err.Error())
	}

	// bind column data
	current := time.Now()
	// point(100 100)
	geometryData := []byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40}

	row := param.NewParam(2).
		AddTimestamp(current, common.PrecisionMilliSecond).
		AddGeometry(geometryData)
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
	// close statement
	err = stmt.Close()
	if err != nil {
		log.Fatal("failed to close statement, err:", err)
	}
	// select * from example_stmt_geometry_native.ntb
}
