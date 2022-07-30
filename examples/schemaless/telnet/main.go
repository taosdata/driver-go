package main

import (
	"github.com/taosdata/driver-go/v3/af"
)

func main() {
	conn, err := af.Open("", "root", "taosdata", "", 0)
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	_, err = conn.Exec("create database if not exists example_telnet")
	if err != nil {
		panic(err)
	}
	_, err = conn.Exec("use example_telnet")
	err = conn.OpenTSDBInsertTelnetLines([]string{
		"sys_if_bytes_out 1479496100 1.3E3 host=web01 interface=eth0",
		"sys_procs_running 1479496100 42 host=web01",
	})
	if err != nil {
		panic(err)
	}
}
