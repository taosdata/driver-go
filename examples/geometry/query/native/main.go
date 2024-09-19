package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/taosdata/driver-go/v3/taosSql"
)

func main() {
	var taosDSN = "root:taosdata@tcp(localhost:6030)/"
	db, err := sql.Open("taosSql", taosDSN)
	if err != nil {
		log.Fatalln("Failed to connect to " + taosDSN + ", ErrMessage: " + err.Error())
	}
	defer db.Close()
	// create database
	res, err := db.Exec("CREATE DATABASE IF NOT EXISTS example_query_geometry_native")
	if err != nil {
		log.Fatalln("Failed to create database example_query_geometry_native, ErrMessage: " + err.Error())
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		log.Fatalln("Failed to get create database rowsAffected, ErrMessage: " + err.Error())
	}
	// you can check rowsAffected here
	fmt.Println("Create database example_query_geometry_native successfully, rowsAffected: ", rowsAffected)
	// create table
	res, err = db.Exec("CREATE TABLE IF NOT EXISTS example_query_geometry_native.ntb (ts TIMESTAMP, val GEOMETRY(100))")
	if err != nil {
		log.Fatalln("Failed to create table example_query_geometry_native, ErrMessage: " + err.Error())
	}
	rowsAffected, err = res.RowsAffected()
	if err != nil {
		log.Fatalln("Failed to get create table rowsAffected, ErrMessage: " + err.Error())
	}
	// you can check rowsAffected here
	fmt.Println("Create table example_query_geometry_native.ntb successfully, rowsAffected:", rowsAffected)
	// insert data, please make sure the database and table are created before
	insertQuery := "INSERT INTO example_query_geometry_native.ntb VALUES (now, 'POINT(100 100)')"
	res, err = db.Exec(insertQuery)
	if err != nil {
		log.Fatalf("Failed to insert data to example_query_geometry_native.ntb, sql: %s, ErrMessage: %s\n", insertQuery, err.Error())
	}
	rowsAffected, err = res.RowsAffected()
	if err != nil {
		log.Fatalf("Failed to get insert rowsAffected, sql: %s, ErrMessage: %s\n", insertQuery, err.Error())
	}
	// you can check affectedRows here
	fmt.Printf("Successfully inserted %d rows to example_query_geometry_native.ntb.\n", rowsAffected)
	// query data
	sql := "SELECT ts, val FROM example_query_geometry_native.ntb"
	rows, err := db.Query(sql)
	if err != nil {
		log.Fatalf("Failed to query data from example_query_geometry_native.ntb, sql: %s, ErrMessage: %s\n", sql, err.Error())
	}
	for rows.Next() {
		// Add your data processing logic here
		var (
			ts  time.Time
			val []byte
		)
		err = rows.Scan(&ts, &val)
		if err != nil {
			log.Fatalf("Failed to scan data, sql: %s, ErrMessage: %s\n", sql, err)
		}
		fmt.Printf("ts: %s, val: %v\n", ts, val)
	}
}
