package main

import (
	"fmt"
	"os"
	"time"

	taos "github.com/taosdata/driver-go/v2/af"
)

func main() {
	db, err := taos.Open("", "", "", "log", 0)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", "err")
		os.Exit(1)
	}
	defer db.Close()
	timestamp := func() int64 {
		t := time.Now()
		return t.Unix()*1e6 + int64(t.Nanosecond()/1000)
	}
	var bytes int64
	now := timestamp()
	for i := 0; ; i++ {
		ts := timestamp()
		if ts == now {
			ts += 1
		}
		now = ts
		content := fmt.Sprintf("message %d", i)
		ipaddr := fmt.Sprintf("taoslogpump %d", os.Getpid())
		sql := fmt.Sprintf("insert into log(ts, level, ipaddr, content) values(%d, %d, '%s', '%s')", ts, i%8, ipaddr, content)
		_, err := db.Exec(sql)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%v\n", err)
			os.Exit(2)
		}
		bytes += int64(len(ipaddr) + len(content) + 1 + 8)
		if (i+1)%1e5 == 0 {
			fmt.Printf("%d MB pumped\n", bytes/1e6)
		}
	}
}
