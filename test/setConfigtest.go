package main

import (
    "database/sql"
    _ "github.com/taosdata/driver-go/v2/taosSql"
)
import "fmt"

func main(){
	//var taosurl = "root:taosdata/tcp(BCC-2:6030)/log?debugFlag=135&asyncLog=0"
	//var taosurl = "root:taosdata/tcp(BCC-2:6030)/log?debugFlag=143&asyncLog=0&rpcTimer=400" //normal
	var taosurl = "root:taosdata/tcp(BCC-2:6030)/log?debugFlag=0&rpcDebugFlag=143&tmrDebugFlag=135&asyncLog=0&cDebugFlag=143&jniDebugFlag=135&odbcDebugFlag=143&uDebugFlag=135&qDebugFlag=143&rpcTimer=400"//normal
	//var taosurl = "root:taosdata/tcp(BCC-2:6030)/log?debugFlag=0&asyncLog=0&rpcDebugFlag=143&tmrDebugFlag=141&rpcTimer=400" //normal
	//var taosurl = "root:taosdata/tcp(BCC-2:6030)/log?debugFlag=abc&asyncLog=0&rpcTimer=400" //wrong type
	//var taosurl = "root:taosdata/tcp(BCC-2:6030)/log?debugFlag=135&asyncLog=0&numOfThreadsPerCore=abc&rpcTimer=400" //wrong type
	//var taosurl = "root:taosdata/tcp(BCC-2:6030)/log?debugFlag=143&asyncLog=0&rpcTimer=10000" //Greater than upper boundary
	//var taosurl = "root:taosdata/tcp(BCC-2:6030)/log?debugFlag=143&asyncLog=0&rpcTimer=0" //Less than the lower boundary
	taos,err :=sql.Open("taosSql",taosurl)
	if err != nil {
		fmt.Println("failed to connect TDengine, err:", err)
		return
	}
	defer taos.Close()
	taos.Exec("create database if not exists test1")
	taos.Exec("use test")
	taos.Exec("create table if not exists tb1 (ts timestamp, a int)")
	_, err = taos.Exec("insert into tb1 values(now, 0)(now+1s,1)(now+2s,2)(now+3s,3)")
	if err != nil {
		fmt.Println("failed to insert, err:", err)
		return
	}
	rows, err := taos.Query("select * from tb1")
	if err != nil {
		fmt.Println("failed to select from table, err:", err)
		return
	}

	defer rows.Close()

}