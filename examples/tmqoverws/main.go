package main

import (
	"database/sql"
	"fmt"

	"github.com/taosdata/driver-go/v3/common"
	tmqcommon "github.com/taosdata/driver-go/v3/common/tmq"
	_ "github.com/taosdata/driver-go/v3/taosRestful"
	"github.com/taosdata/driver-go/v3/ws/tmq"
)

func main() {
	db, err := sql.Open("taosRestful", "root:taosdata@http(localhost:6041)/")
	if err != nil {
		panic(err)
	}
	defer db.Close()
	prepareEnv(db)
	consumer, err := tmq.NewConsumer(&tmqcommon.ConfigMap{
		"ws.url":                "ws://127.0.0.1:6041/rest/tmq",
		"ws.message.channelLen": uint(0),
		"ws.message.timeout":    common.DefaultMessageTimeout,
		"ws.message.writeWait":  common.DefaultWriteWait,
		"td.connect.user":       "root",
		"td.connect.pass":       "taosdata",
		"group.id":              "example",
		"client.id":             "example_consumer",
		"auto.offset.reset":     "earliest",
	})
	if err != nil {
		panic(err)
	}
	err = consumer.Subscribe("example_ws_tmq_topic", nil)
	if err != nil {
		panic(err)
	}
	go func() {
		_, err := db.Exec("create table example_ws_tmq.t_all(ts timestamp," +
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
		_, err = db.Exec("insert into example_ws_tmq.t_all values(now,true,2,3,4,5,6,7,8,9,10.123,11.123,'binary','nchar')")
		if err != nil {
			panic(err)
		}
	}()
	for i := 0; i < 5; i++ {
		ev := consumer.Poll(0)
		if ev != nil {
			switch e := ev.(type) {
			case *tmqcommon.DataMessage:
				fmt.Printf("get message:%v", e)
			case tmqcommon.Error:
				fmt.Printf("%% Error: %v: %v\n", e.Code(), e)
				panic(e)
			}
		}
	}
	err = consumer.Close()
	if err != nil {
		panic(err)
	}
}

func prepareEnv(db *sql.DB) {
	_, err := db.Exec("create database example_ws_tmq")
	if err != nil {
		panic(err)
	}
	_, err = db.Exec("create topic example_ws_tmq_topic as database example_ws_tmq")
	if err != nil {
		panic(err)
	}
}