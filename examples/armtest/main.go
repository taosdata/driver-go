package main

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/taosdata/driver-go/v2/taosSql"
)

func main() {
	db, err := sql.Open("taosSql", "root:taosdata@tcp(localhost:6030)/")
	if err != nil {
		panic(err)
	}
	defer db.Close()
	_, err = db.Exec("create database if not exists example_taos_sql8")
	if err != nil {
		panic(err)
	}
	_, err = db.Exec("use example_taos_sql8")
	if err != nil {
		panic(err)
	}
	_, err = db.Exec("create table if not exists t1(ts timestamp,c1 bool,c2 tinyint)")
	if err != nil {
		panic(err)
	}
	now := time.Now()
	_, err = db.Exec(fmt.Sprintf("insert into t1 values ('%s',true,1)", now.Format(time.RFC3339Nano)))
	if err != nil {
		panic(err)
	}
	_, err = db.Exec("create table if not exists t2(ts timestamp,c1 bool,c2 smallint)")
	if err != nil {
		panic(err)
	}
	_, err = db.Exec(fmt.Sprintf("insert into t2 values ('%s',true,2)", now.Format(time.RFC3339Nano)))
	if err != nil {
		panic(err)
	}
	_, err = db.Exec("create table if not exists t3(ts timestamp,c1 bool,c2 int)")
	if err != nil {
		panic(err)
	}
	_, err = db.Exec(fmt.Sprintf("insert into t3 values ('%s',true,3)", now.Format(time.RFC3339Nano)))
	if err != nil {
		panic(err)
	}
	_, err = db.Exec("create table if not exists t4(ts timestamp,c1 bool,c2 bigint)")
	if err != nil {
		panic(err)
	}
	_, err = db.Exec(fmt.Sprintf("insert into t4 values ('%s',true,4)", now.Format(time.RFC3339Nano)))
	if err != nil {
		panic(err)
	}
	_, err = db.Exec("create table if not exists t5(ts timestamp,c1 bool,c2 tinyint unsigned)")
	if err != nil {
		panic(err)
	}
	_, err = db.Exec(fmt.Sprintf("insert into t5 values ('%s',true,5)", now.Format(time.RFC3339Nano)))
	if err != nil {
		panic(err)
	}
	_, err = db.Exec("create table if not exists t6(ts timestamp,c1 bool,c2 smallint unsigned)")
	if err != nil {
		panic(err)
	}
	_, err = db.Exec(fmt.Sprintf("insert into t6 values ('%s',true,6)", now.Format(time.RFC3339Nano)))
	if err != nil {
		panic(err)
	}
	_, err = db.Exec("create table if not exists t7(ts timestamp,c1 bool,c2 int unsigned)")
	if err != nil {
		panic(err)
	}
	_, err = db.Exec(fmt.Sprintf("insert into t7 values ('%s',true,7)", now.Format(time.RFC3339Nano)))
	if err != nil {
		panic(err)
	}
	_, err = db.Exec("create table if not exists t8(ts timestamp,c1 bool,c2 bigint unsigned)")
	if err != nil {
		panic(err)
	}
	_, err = db.Exec(fmt.Sprintf("insert into t8 values ('%s',true,8)", now.Format(time.RFC3339Nano)))
	if err != nil {
		panic(err)
	}
	_, err = db.Exec("create table if not exists t9(ts timestamp,c1 bool,c2 float)")
	if err != nil {
		panic(err)
	}
	_, err = db.Exec(fmt.Sprintf("insert into t9 values ('%s',true,9.123)", now.Format(time.RFC3339Nano)))
	if err != nil {
		panic(err)
	}
	_, err = db.Exec("create table if not exists t10(ts timestamp,c1 bool,c2 double)")
	if err != nil {
		panic(err)
	}
	_, err = db.Exec(fmt.Sprintf("insert into t10 values ('%s',true,10.123)", now.Format(time.RFC3339Nano)))
	if err != nil {
		panic(err)
	}
	_, err = db.Exec("create table if not exists t11(ts timestamp,c1 bool,c2 binary(10))")
	if err != nil {
		panic(err)
	}
	_, err = db.Exec(fmt.Sprintf("insert into t11 values ('%s',true,'b')", now.Format(time.RFC3339Nano)))
	if err != nil {
		panic(err)
	}
	_, err = db.Exec("create table if not exists t12(ts timestamp,c1 bool,c2 nchar(10))")
	if err != nil {
		panic(err)
	}
	_, err = db.Exec(fmt.Sprintf("insert into t12 values ('%s',true,'n')", now.Format(time.RFC3339Nano)))
	if err != nil {
		panic(err)
	}
	{

		rows, err := db.Query(fmt.Sprintf("select * from example_taos_sql8.t1 where ts = '%s'", now.Format(time.RFC3339Nano)))
		if err != nil {
			panic(err)
		}
		for rows.Next() {
			var (
				ts time.Time
				c1 bool
				c2 int8
			)
			err = rows.Scan(
				&ts,
				&c1,
				&c2,
			)
			if err != nil {
				panic(err)
			}
			fmt.Println("ts:", ts.Local())
			fmt.Println("c1:", c1)
			fmt.Println("c2:", c2)
		}
	}
	{
		rows, err := db.Query(fmt.Sprintf("select * from example_taos_sql8.t2 where ts = '%s'", now.Format(time.RFC3339Nano)))
		if err != nil {
			panic(err)
		}
		for rows.Next() {
			var (
				ts time.Time
				c1 bool
				c2 int16
			)
			err = rows.Scan(
				&ts,
				&c1,
				&c2,
			)
			if err != nil {
				panic(err)
			}
			fmt.Println("ts:", ts.Local())
			fmt.Println("c1:", c1)
			fmt.Println("c2:", c2)
		}
	}
	{
		rows, err := db.Query(fmt.Sprintf("select * from example_taos_sql8.t3 where ts = '%s'", now.Format(time.RFC3339Nano)))
		if err != nil {
			panic(err)
		}
		for rows.Next() {
			var (
				ts time.Time
				c1 bool
				c2 int32
			)
			err = rows.Scan(
				&ts,
				&c1,
				&c2,
			)
			if err != nil {
				panic(err)
			}
			fmt.Println("ts:", ts.Local())
			fmt.Println("c1:", c1)
			fmt.Println("c2:", c2)
		}
	}
	{
		rows, err := db.Query(fmt.Sprintf("select * from example_taos_sql8.t4 where ts = '%s'", now.Format(time.RFC3339Nano)))
		if err != nil {
			panic(err)
		}
		for rows.Next() {
			var (
				ts time.Time
				c1 bool
				c2 int64
			)
			err = rows.Scan(
				&ts,
				&c1,
				&c2,
			)
			if err != nil {
				panic(err)
			}
			fmt.Println("ts:", ts.Local())
			fmt.Println("c1:", c1)
			fmt.Println("c2:", c2)
		}
	}
	{
		rows, err := db.Query(fmt.Sprintf("select * from example_taos_sql8.t5 where ts = '%s'", now.Format(time.RFC3339Nano)))
		if err != nil {
			panic(err)
		}
		for rows.Next() {
			var (
				ts time.Time
				c1 bool
				c2 uint8
			)
			err = rows.Scan(
				&ts,
				&c1,
				&c2,
			)
			if err != nil {
				panic(err)
			}
			fmt.Println("ts:", ts.Local())
			fmt.Println("c1:", c1)
			fmt.Println("c2:", c2)
		}
	}
	{
		rows, err := db.Query(fmt.Sprintf("select * from example_taos_sql8.t6 where ts = '%s'", now.Format(time.RFC3339Nano)))
		if err != nil {
			panic(err)
		}
		for rows.Next() {
			var (
				ts time.Time
				c1 bool
				c2 uint16
			)
			err = rows.Scan(
				&ts,
				&c1,
				&c2,
			)
			if err != nil {
				panic(err)
			}
			fmt.Println("ts:", ts.Local())
			fmt.Println("c1:", c1)
			fmt.Println("c2:", c2)
		}
	}
	{
		rows, err := db.Query(fmt.Sprintf("select * from example_taos_sql8.t7 where ts = '%s'", now.Format(time.RFC3339Nano)))
		if err != nil {
			panic(err)
		}
		for rows.Next() {
			var (
				ts time.Time
				c1 bool
				c2 uint32
			)
			err = rows.Scan(
				&ts,
				&c1,
				&c2,
			)
			if err != nil {
				panic(err)
			}
			fmt.Println("ts:", ts.Local())
			fmt.Println("c1:", c1)
			fmt.Println("c2:", c2)
		}
	}
	{
		rows, err := db.Query(fmt.Sprintf("select * from example_taos_sql8.t8 where ts = '%s'", now.Format(time.RFC3339Nano)))
		if err != nil {
			panic(err)
		}
		for rows.Next() {
			var (
				ts time.Time
				c1 bool
				c2 uint64
			)
			err = rows.Scan(
				&ts,
				&c1,
				&c2,
			)
			if err != nil {
				panic(err)
			}
			fmt.Println("ts:", ts.Local())
			fmt.Println("c1:", c1)
			fmt.Println("c2:", c2)
		}
	}
	{
		rows, err := db.Query(fmt.Sprintf("select * from example_taos_sql8.t9 where ts = '%s'", now.Format(time.RFC3339Nano)))
		if err != nil {
			panic(err)
		}
		for rows.Next() {
			var (
				ts time.Time
				c1 bool
				c2 float32
			)
			err = rows.Scan(
				&ts,
				&c1,
				&c2,
			)
			if err != nil {
				panic(err)
			}
			fmt.Println("ts:", ts.Local())
			fmt.Println("c1:", c1)
			fmt.Println("c2:", c2)
		}
	}
	{
		rows, err := db.Query(fmt.Sprintf("select * from example_taos_sql8.t10 where ts = '%s'", now.Format(time.RFC3339Nano)))
		if err != nil {
			panic(err)
		}
		for rows.Next() {
			var (
				ts time.Time
				c1 bool
				c2 float64
			)
			err = rows.Scan(
				&ts,
				&c1,
				&c2,
			)
			if err != nil {
				panic(err)
			}
			fmt.Println("ts:", ts.Local())
			fmt.Println("c1:", c1)
			fmt.Println("c2:", c2)
		}
	}
	{
		rows, err := db.Query(fmt.Sprintf("select * from example_taos_sql8.t11 where ts = '%s'", now.Format(time.RFC3339Nano)))
		if err != nil {
			panic(err)
		}
		for rows.Next() {
			var (
				ts time.Time
				c1 bool
				c2 string
			)
			err = rows.Scan(
				&ts,
				&c1,
				&c2,
			)
			if err != nil {
				panic(err)
			}
			fmt.Println("ts:", ts.Local())
			fmt.Println("c1:", c1)
			fmt.Println("c2:", c2)
		}
	}
	{
		rows, err := db.Query(fmt.Sprintf("select * from example_taos_sql8.t12 where ts = '%s'", now.Format(time.RFC3339Nano)))
		if err != nil {
			panic(err)
		}
		for rows.Next() {
			var (
				ts time.Time
				c1 bool
				c2 string
			)
			err = rows.Scan(
				&ts,
				&c1,
				&c2,
			)
			if err != nil {
				panic(err)
			}
			fmt.Println("ts:", ts.Local())
			fmt.Println("c1:", c1)
			fmt.Println("c2:", c2)
		}
	}
}
