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
	_, err = db.Exec("create database if not exists example_taos_rest")
	if err != nil {
		panic(err)
	}
	_, err = db.Exec("create table if not exists example_taos_rest.stb(ts timestamp," +
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
		") tags (info json)")
	if err != nil {
		panic(err)
	}
	_, err = db.Exec("create table if not exists example_taos_rest.tb1 using example_taos_rest.stb tags ('{\"name\":\"tb1\"}')")
	if err != nil {
		panic(err)
	}
	now := time.Now()
	_, err = db.Exec(fmt.Sprintf("insert into example_taos_rest.tb1 values ('%s',true,2,3,4,5,6,7,8,9,10,11,'binary','nchar')", now.Format(time.RFC3339Nano)))
	if err != nil {
		panic(err)
	}
	rows, err := db.Query(fmt.Sprintf("select * from example_taos_rest.tb1 where ts = '%s'", now.Format(time.RFC3339Nano)))
	if err != nil {
		panic(err)
	}
	for rows.Next() {
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
		err = rows.Scan(
			&ts,
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
			&c13,
		)
		if err != nil {
			panic(err)
		}
		fmt.Println("ts:", ts.Local())
		fmt.Println("c1:", c1)
		fmt.Println("c2:", c2)
		fmt.Println("c3:", c3)
		fmt.Println("c4:", c4)
		fmt.Println("c5:", c5)
		fmt.Println("c6:", c6)
		fmt.Println("c7:", c7)
		fmt.Println("c8:", c8)
		fmt.Println("c9:", c9)
		fmt.Println("c10:", c10)
		fmt.Println("c11:", c11)
		fmt.Println("c12:", c12)
		fmt.Println("c13:", c13)
	}
}
