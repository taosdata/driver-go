/*
 * In this test program, we'll create a database and import 1000 records
 * with unsigned integers
 *
 * Authored by <Huo Linhe> linhe.huo@gmail.com
 */
package main

import (
	"database/sql"
	"flag"
	"fmt"
	"github.com/taosdata/driver-go/v2/types"
	"os"
	"time"

	_ "github.com/taosdata/driver-go/v2/taosSql"
)

type config struct {
	hostName   string
	serverPort string
	user       string
	password   string
	dbName     string
}

var configPara config
var taosDriverName = "taosSql"
var url string

func init() {
	flag.StringVar(&configPara.hostName, "h", "", "The host to connect to TDengine server.")
	flag.StringVar(&configPara.serverPort, "p", "", "The TCP/IP port number to use for the connection to TDengine server.")
	flag.StringVar(&configPara.user, "u", "root", "The TDengine user name to use when connecting to the server.")
	flag.StringVar(&configPara.password, "P", "taosdata", "The password to use when connecting to the server.")
	flag.StringVar(&configPara.dbName, "d", "taosnstest", "Destination database.")
	flag.Parse()
}

func printAllArgs() {
	fmt.Printf("============= args parse result: =============\n")
	fmt.Printf("hostName:             %v\n", configPara.hostName)
	fmt.Printf("serverPort:           %v\n", configPara.serverPort)
	fmt.Printf("usr:                  %v\n", configPara.user)
	fmt.Printf("password:             %v\n", configPara.password)
	fmt.Printf("dbName:               %v\n", configPara.dbName)
	fmt.Printf("================================================\n")
}

func main() {
	printAllArgs()

	url = "root:taosdata@/tcp(" + configPara.hostName + ":" + configPara.serverPort + ")/"

	test(configPara.dbName)
}

func test(dbName string) {
	db, err := sql.Open(taosDriverName, url)
	if err != nil {
		fmt.Printf("Open database error: %s\n", err)
		os.Exit(1)
	}
	defer db.Close()

	sqlStr := "create database if not exists " + dbName + " precision 'ns'"
	fmt.Printf("- %s\n", sqlStr)
	_, err = db.Exec(sqlStr)
	checkErr(err, sqlStr)

	sqlStr = "create table if not exists " + dbName + ".tb1 (ts timestamp, n int)"
	fmt.Printf("- %s\n", sqlStr)
	_, err = db.Exec(sqlStr)
	checkErr(err, sqlStr)

	sqlStr = "insert into " + dbName + ".tb1 values(now, 0)"
	fmt.Printf("- %s\n", sqlStr)
	_, err = db.Exec(sqlStr)
	checkErr(err, sqlStr)

	sqlStr = "select * from " + dbName + ".tb1"
	fmt.Printf("- %s\n", sqlStr)
	res, err := db.Query(sqlStr)
	checkErr(err, sqlStr)
	defer res.Close()
	for res.Next() {
		var (
			ts time.Time
			n  types.NullInt32
		)
		err = res.Scan(&ts, &n)
		checkErr(err, sqlStr)
		if ts.Nanosecond()%1000 > 0 {
			fmt.Println("nanosecond is correct: ", ts)
		}
	}

	sqlStr = "drop database " + dbName
	fmt.Printf("- %s\n", sqlStr)
	_, err = db.Exec(sqlStr)
	checkErr(err, sqlStr)
}

func checkErr(err error, prompt string) {
	if err != nil {
		fmt.Errorf("ERROR: %s\n", prompt)
		panic(err)
	}
}
