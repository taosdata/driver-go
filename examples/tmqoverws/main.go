package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/taosdata/driver-go/v3/common"
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

	config := tmq.NewConfig("ws://localhost:6041/rest/tmq", 0)
	config.SetConnectUser("root")
	config.SetConnectPass("taosdata")
	config.SetGroupID("example")
	config.SetClientID("example_consumer")
	config.SetAutoOffsetReset("earliest")
	config.SetMessageTimeout(common.DefaultMessageTimeout)
	config.SetWriteWait(common.DefaultWriteWait)
	config.SetErrorHandler(func(consumer *tmq.Consumer, err error) {
		panic(err)
	})
	config.SetCloseHandler(func() {
		fmt.Println("consumer closed")
	})

	consumer, err := tmq.NewConsumer(config)
	if err != nil {
		panic(err)
	}
	err = consumer.Subscribe([]string{"example_ws_tmq_topic"})
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
		result, err := consumer.Poll(0)
		if err != nil {
			panic(err)
		}
		if result != nil {
			b, err := json.Marshal(result)
			if err != nil {
				panic(err)
			}
			fmt.Println("poll message:", string(b))
		}
	}
	consumer.Close()
	time.Sleep(time.Second)
}

func prepareEnv(db *sql.DB) {
	_, err := db.Exec("create database example_ws_tmq")
	if err != nil {
		panic(err)
	}
	_, err = db.Exec("create topic example_ws_tmq_topic with meta as database example_ws_tmq")
	if err != nil {
		panic(err)
	}
}
