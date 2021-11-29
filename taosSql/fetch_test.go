package taosSql

import (
	"database/sql"
	"testing"
	"time"

	"github.com/taosdata/driver-go/v2/types"
)

func TestFetchBlock(t *testing.T) {
	db, err := sql.Open("taosSql", dataSourceName)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows, err := db.Query("select ts,cpu_taosd,disk_used,req_insert from log.dn")
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	for rows.Next() {
		var r struct {
			ts        time.Time
			cpu       float32
			diskUsed  types.NullFloat32
			reqInsert types.NullInt64
		}
		err := rows.Scan(&r.ts, &r.cpu, &r.diskUsed, &r.reqInsert)
		if err != nil {
			t.Fatal(err)
		}
		// fmt.Println(r.ts, r.cpu_taosd, r.disk_used, r.req_insert)
	}
	end := time.Now()
	t.Logf("time cost %v", end.Sub(start))
}

func TestFetchDatabases(t *testing.T) {
	db, err := sql.Open("taosSql", dataSourceName)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows, err := db.Query("show databases")
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	for rows.Next() {
		var (
			name        string
			createdTime string
			nTables     int
			vGroups     int
			replica     int16
			quorum      int16
			days        string
			keep        string
			cache       int
			blocks      int
			minRows     int
			maxRows     int
			walLevel    int8
			fsync       int
			comp        int
			cachelast   int
			precision   string
			update      int
			status      string
		)
		err := rows.Scan(&name, &createdTime, &nTables, &vGroups, &replica, &quorum, &days, &keep, &cache, &blocks, &minRows, &maxRows, &walLevel, &fsync, &comp, &cachelast, &precision, &update, &status)
		if err != nil {
			t.Fatal(err)
		}
	}
	end := time.Now()
	t.Logf("time cost %v", end.Sub(start))
}
