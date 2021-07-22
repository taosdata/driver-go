package taosSql

import (
	"database/sql"
	"fmt"
	"testing"
	"time"
)

func TestFetchBlock(t *testing.T) {
	db, err := sql.Open("taosSql", dataSourceName)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := db.Query("select ts,cpu_taosd,disk_used,req_insert from log.dn")
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	for rows.Next() {
		var r struct {
			ts         time.Time
			cpu_taosd  float32
			disk_used  NullFloat32
			req_insert NullInt32
		}
		err := rows.Scan(&r.ts, &r.cpu_taosd, &r.disk_used, &r.req_insert)
		if err != nil {
			t.Fatal(err)
		}
		// fmt.Println(r.ts, r.cpu_taosd, r.disk_used, r.req_insert)
	}
	end := time.Now()
	fmt.Printf("time cost %v", end.Sub(start))
}
func TestFetchDatabases(t *testing.T) {
	db, err := sql.Open("taosSql", dataSourceName)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := db.Query("show databases")
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	for rows.Next() {
		var (
			name         string
			created_time string
			ntables      int
			vgroups      int
			replica      int16
			quorum       int16
			days         string
			keep         string
			cache        int
			blocks       int
			minrows      int
			maxrows      int
			wallevel     int8
			fsync        int
			comp         int
			cachelast    int
			precision    string
			update       int
			status       string
		)
		err := rows.Scan(&name, &created_time, &ntables, &vgroups, &replica, &quorum, &days, &keep, &cache, &blocks, &minrows, &maxrows, &wallevel, &fsync, &comp, &cachelast, &precision, &update, &status)
		if err != nil {
			t.Fatal(err)
		}
		fmt.Println(name, created_time, ntables, vgroups, replica, quorum, days)
	}
	end := time.Now()
	fmt.Printf("time cost %v", end.Sub(start))
}
