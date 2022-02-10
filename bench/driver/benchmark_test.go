package main_test

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/taosdata/driver-go/v2/taosRestful"
	_ "github.com/taosdata/driver-go/v2/taosSql"
)

var cdb *sql.DB
var restfulDB *sql.DB
var data []string

func TestMain(m *testing.M) {
	var err error
	cdb, err = sql.Open("taosSql", "root:taosdata@tcp(localhost:6030)/")
	if err != nil {
		panic(err)
	}
	restfulDB, err = sql.Open("taosRestful", "root:taosdata@http(127.0.0.1:6041)/?readBufferSize=52428800")
	if err != nil {
		panic(err)
	}
	_, err = cdb.Exec("drop database if exists benchmark_driver")
	if err != nil {
		panic(err)
	}
	_, err = cdb.Exec("create database if not exists benchmark_driver")
	if err != nil {
		panic(err)
	}
	_, err = cdb.Exec("create table benchmark_driver.alltype_insert(ts timestamp," +
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
	_, err = cdb.Exec("create table benchmark_driver.alltype_query(ts timestamp," +
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
	now := time.Now()
	for i := 0; i < 3000; i++ {
		_, err := cdb.Exec(fmt.Sprintf(`insert into benchmark_driver.alltype_query values('%s',1,1,1,1,1,1,1,1,1,1,1,'test_binary','test_nchar')`, now.Add(time.Second*time.Duration(i)).Format(time.RFC3339Nano)))
		if err != nil {
			panic(err)
		}
	}
	ts := time.Now().UnixNano() / 1e6
	for i := 0; i < 10000; i++ {
		data = append(data, fmt.Sprintf("insert into benchmark_driver.alltype_insert values(%d,1,1,1,1,1,1,1,1,1,1,1,'test_binary','test_nchar')", int64(i)+ts))
	}
	m.Run()
}

func BenchmarkCGOInsert(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, err := cdb.Exec(data[i])
		if err != nil {
			b.Error(err)
			return
		}
	}
}

func BenchmarkRestfulInsert(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, err := restfulDB.Exec(data[i])
		if err != nil {
			b.Error(err)
			return
		}
	}
}

func BenchmarkCGOQuery(b *testing.B) {
	for i := 0; i < b.N; i++ {
		result, err := cdb.Query(`select * from benchmark_driver.alltype_query limit 3000`)
		if err != nil {
			b.Error(err)
			return
		}
		for result.Next() {
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
			err := result.Scan(
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
				b.Error(err)
				return
			}
		}
	}
}

func BenchmarkRestfulQuery(b *testing.B) {
	for i := 0; i < b.N; i++ {
		result, err := restfulDB.Query(`select * from benchmark_driver.alltype_query limit 3000`)
		if err != nil {
			b.Error(err)
			return
		}
		for result.Next() {
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

			err := result.Scan(
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
				b.Error(err)
				return
			}
		}
	}
}
