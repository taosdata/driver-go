package taosSql

import (
	"context"
	"database/sql"
	"testing"

	"github.com/taosdata/driver-go/v3/common"
)

// @author: xftan
// @date: 2023/10/13 11:21
// @description: test taos connection exec context
func TestTaosConn_ExecContext(t *testing.T) {
	ctx := context.WithValue(context.Background(), common.ReqIDKey, common.GetReqID())
	db, err := sql.Open("taosSql", dataSourceName)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	defer func() {
		_, err = db.ExecContext(ctx, "drop database if exists test_connection")
	}()
	_, err = db.ExecContext(ctx, "create database if not exists test_connection")
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.ExecContext(ctx, "use test_connection")
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.ExecContext(ctx, "create stable if not exists meters (ts timestamp, current float, voltage int, phase float) tags (location binary(64), groupId int)")
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.ExecContext(ctx, "INSERT INTO d21001 USING meters TAGS ('California.SanFrancisco', 2) VALUES ('?', ?, ?, ?)", "2021-07-13 14:06:32.272", 10.2, 219, 0.32)
	if err != nil {
		t.Fatal(err)
	}
	rs, err := db.QueryContext(ctx, "select count(*) from meters")
	if err != nil {
		t.Fatal(err)
	}
	defer rs.Close()
	rs.Next()
	var count int64
	if err = rs.Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatal("result miss")
	}
}
