package main

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/taosdata/driver-go/v3/taosSql"
)

var (
	driverName     = "taosSql"
	user           = "root"
	password       = "taosdata"
	host           = ""
	port           = 6030
	dataSourceName = fmt.Sprintf("%s:%s@/tcp(%s:%d)/%s?interpolateParams=true", user, password, host, port, "")
)

func main() {
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	defer func() {
		db.Exec("drop database if exists test_stmt_driver")
	}()
	_, err = db.Exec("create database if not exists test_stmt_driver")
	if err != nil {
		panic(err)
	}
	_, err = db.Exec("create table if not exists test_stmt_driver.ct(ts timestamp," +
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
	stmt, err := db.Prepare("insert into test_stmt_driver.ct values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
	if err != nil {
		panic(err)
	}
	now := time.Now()
	result, err := stmt.Exec(now, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, "binary", "nchar")
	if err != nil {
		panic(err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		panic(err)
	}
	fmt.Println("affected", affected)
	stmt.Close()
	cr := 0
	err = db.QueryRow("select count(*) from test_stmt_driver.ct where ts = ?", now).Scan(&cr)
	if err != nil {
		panic(err)
	}
	fmt.Println("count", cr)
	stmt, err = db.Prepare("select * from test_stmt_driver.ct where ts = ?")
	if err != nil {
		panic(err)
	}
	rows, err := stmt.Query(now)
	if err != nil {
		panic(err)
	}
	columns, err := rows.Columns()
	if err != nil {
		panic(err)
	}
	fmt.Println(columns)
	count := 0
	for rows.Next() {
		count += 1
		var (
			ts  time.Time
			c1  bool
			c2  int8
			c3  int16
			c4  int32
			c5  int64
			c6  uint8
			c7  uint16
			c8  uint32
			c9  uint64
			c10 float32
			c11 float64
			c12 string
			c13 string
		)
		err = rows.Scan(&ts,
			&c1,
			&c2,
			&c3,
			&c4,
			&c5,
			&c6,
			&c7,
			&c8,
			&c9,
			&c10,
			&c11,
			&c12,
			&c13)
		fmt.Println(ts, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13)
	}
	fmt.Println("rows", count)
}
