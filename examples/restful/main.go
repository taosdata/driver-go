package main

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/taosdata/driver-go/v3/taosRestful"
)

func main() {
	db, err := sql.Open("taosRestful", "root:taosdata@http(localhost:6041)/")
	if err != nil {
		panic(err)
	}
	defer db.Close()
	//_, err = db.Exec("create database if not exists example_taos_rest")
	//if err != nil {
	//	panic(err)
	//}
	//_, err = db.Exec("create table if not exists example_taos_rest.stb(ts timestamp," +
	//	"c1 bool," +
	//	"c2 tinyint," +
	//	"c3 smallint," +
	//	"c4 int," +
	//	"c5 bigint," +
	//	"c6 tinyint unsigned," +
	//	"c7 smallint unsigned," +
	//	"c8 int unsigned," +
	//	"c9 bigint unsigned," +
	//	"c10 float," +
	//	"c11 double," +
	//	"c12 binary(20)," +
	//	"c13 nchar(20)" +
	//	") tags (info json)")
	//if err != nil {
	//	panic(err)
	//}
	//_, err = db.Exec("create table if not exists example_taos_rest.tb1 using example_taos_rest.stb tags ('{\"name\":\"tb1\"}')")
	//if err != nil {
	//	panic(err)
	//}
	//now := time.Now()
	//_, err = db.Exec(fmt.Sprintf("insert into example_taos_rest.tb1 values ('%s',true,2,3,4,5,6,7,8,9,10,11,'binary','nchar')", now.Format(time.RFC3339Nano)))
	//if err != nil {
	//	panic(err)
	//}
	rows, err := db.Query("select * from test.t")
	if err != nil {
		panic(err)
	}
	for rows.Next() {
		var (
			ts time.Time
			v  string
		)
		err = rows.Scan(
			&ts,
			&v,
		)
		if err != nil {
			panic(err)
		}
		fmt.Println("ts:", ts.Local())
		fmt.Println("c1:", v)

	}
}
