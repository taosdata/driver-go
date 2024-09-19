package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/taosdata/driver-go/v3/taosWS"
)

func main() {
	var taosDSN = "root:taosdata@ws(localhost:6041)/"
	db, err := sql.Open("taosWS", taosDSN)
	if err != nil {
		log.Fatalln("Failed to connect to " + taosDSN + ", ErrMessage: " + err.Error())
	}
	defer db.Close()
	// create database
	res, err := db.Exec("CREATE DATABASE IF NOT EXISTS example_all_type_query")
	if err != nil {
		log.Fatalln("Failed to create database example_all_type_query, ErrMessage: " + err.Error())
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		log.Fatalln("Failed to get create database rowsAffected, ErrMessage: " + err.Error())
	}
	// you can check rowsAffected here
	fmt.Println("Create database example_all_type_query successfully, rowsAffected: ", rowsAffected)
	// create table with json
	res, err = db.Exec("CREATE STABLE IF NOT EXISTS example_all_type_query.stb_json (" +
		"ts TIMESTAMP, " +
		"int_col INT) " +
		"tags (json_tag json)")
	if err != nil {
		log.Fatalln("Failed to create table example_all_type_query.stb_json, ErrMessage: " + err.Error())
	}
	rowsAffected, err = res.RowsAffected()
	if err != nil {
		log.Fatalln("Failed to get create table rowsAffected, ErrMessage: " + err.Error())
	}
	fmt.Println("Create table example_query_varbinary_native.stb_json successfully, rowsAffected:", rowsAffected)
	// insert data
	var insertQuery = "INSERT INTO example_all_type_query.ntb_json using example_all_type_query.stb_json tags('{\"device\":\"device_1\"}') " +
		"values(now, 1)"
	res, err = db.Exec(insertQuery)
	if err != nil {
		log.Fatalf("Failed to insert data to example_all_type_query.ntb_json, sql: %s, ErrMessage: %s\n", insertQuery, err.Error())
	}
	rowsAffected, err = res.RowsAffected()
	if err != nil {
		log.Fatalf("Failed to get insert rowsAffected, sql: %s, ErrMessage: %s\n", insertQuery, err.Error())
	}
	// you can check affectedRows here
	fmt.Printf("Successfully inserted %d rows to example_all_type_query.ntb_json.\n", rowsAffected)
	// query data
	sql := "SELECT * FROM example_all_type_query.stb_json"
	rows, err := db.Query(sql)
	if err != nil {
		log.Fatalf("Failed to query data from example_all_type_query.stb_json, sql: %s, ErrMessage: %s\n", sql, err.Error())
	}
	for rows.Next() {
		// Add your data processing logic here
		var (
			ts      time.Time
			intVal  int32
			jsonVal []byte
		)
		err = rows.Scan(&ts, &intVal, &jsonVal)
		if err != nil {
			log.Fatalf("Failed to scan data, sql: %s, ErrMessage: %s\n", sql, err)
		}
		fmt.Printf(
			"ts: %s, "+
				"int_col: %d, "+
				"json_tag: %s\n",
			ts,
			intVal,
			jsonVal,
		)
	}

	// create table without json
	res, err = db.Exec("CREATE STABLE IF NOT EXISTS example_all_type_query.stb (" +
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
		"geometry_tag GEOMETRY(100)) ")
	if err != nil {
		log.Fatalln("Failed to create table example_all_type_query.stb, ErrMessage: " + err.Error())
	}
	rowsAffected, err = res.RowsAffected()
	if err != nil {
		log.Fatalln("Failed to get create table rowsAffected, ErrMessage: " + err.Error())
	}
	fmt.Println("Create table example_query_varbinary_native.stb successfully, rowsAffected:", rowsAffected)
	// insert data
	insertQuery = "INSERT INTO example_all_type_query.ntb using example_all_type_query.stb tags(1, 1.1, true, 'binary_value', 'nchar_value', '\\x98f46e', 'POINT(100 100)') " +
		"values(now, 1, 1.1, true, 'binary_value', 'nchar_value', '\\x98f46e', 'POINT(100 100)')"
	res, err = db.Exec(insertQuery)
	if err != nil {
		log.Fatalf("Failed to insert data to example_all_type_query.ntb_json, sql: %s, ErrMessage: %s\n", insertQuery, err.Error())
	}
	rowsAffected, err = res.RowsAffected()
	if err != nil {
		log.Fatalf("Failed to get insert rowsAffected, sql: %s, ErrMessage: %s\n", insertQuery, err.Error())
	}
	// you can check affectedRows here
	fmt.Printf("Successfully inserted %d rows to example_all_type_query.ntb_json.\n", rowsAffected)
	// query data
	sql = "SELECT * FROM example_all_type_query.stb"
	rows, err = db.Query(sql)
	if err != nil {
		log.Fatalf("Failed to query data from example_all_type_query.stb, sql: %s, ErrMessage: %s\n", sql, err.Error())
	}
	for rows.Next() {
		// Add your data processing logic here
		var (
			ts           time.Time
			intVal       int32
			doubleVal    float64
			boolVal      bool
			binaryVal    []byte
			ncharVal     string
			varbinaryVal []byte
			geometryVal  []byte
			intTag       int32
			doubleTag    float64
			boolTag      bool
			binaryTag    []byte
			ncharTag     string
			varbinaryTag []byte
			geometryTag  []byte
		)
		err = rows.Scan(&ts, &intVal, &doubleVal, &boolVal, &binaryVal, &ncharVal, &varbinaryVal, &geometryVal, &intTag, &doubleTag, &boolTag, &binaryTag, &ncharTag, &varbinaryTag, &geometryTag)
		if err != nil {
			log.Fatalf("Failed to scan data, sql: %s, ErrMessage: %s\n", sql, err)
		}
		fmt.Printf(
			"ts: %s, "+
				"int_col: %d, "+
				"double_col: %f, "+
				"bool_col: %t, "+
				"binary_col: %s, "+
				"nchar_col: %s, "+
				"varbinary_col: %v, "+
				"geometry_col: %v, "+
				"int_tag: %d, "+
				"double_tag: %f, "+
				"bool_tag: %t, "+
				"binary_tag: %s, "+
				"nchar_tag: %s, "+
				"varbinary_tag: %v, "+
				"geometry_tag: %v\n",
			ts,
			intVal,
			doubleVal,
			boolVal,
			binaryVal,
			ncharVal,
			varbinaryVal,
			geometryVal,
			intTag,
			doubleTag,
			boolTag,
			binaryTag,
			ncharTag,
			varbinaryTag,
			geometryTag,
		)
	}
}
