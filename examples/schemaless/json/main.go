package main

import "github.com/taosdata/driver-go/v3/af"

func main() {
	conn, err := af.Open("", "root", "taosdata", "", 0)
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	_, err = conn.Exec("create database if not exists example_json")
	if err != nil {
		panic(err)
	}
	_, err = conn.Exec("use example_json")
	err = conn.OpenTSDBInsertJsonPayload(`{
    "metric": "sys",
    "timestamp": 1346846400,
    "value": 18,
    "tags": {
       "host": "web01",
       "dc": "lga"
    }
}`)
	if err != nil {
		panic(err)
	}
}
