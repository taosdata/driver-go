package taosSql

import (
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestStmtExec(t *testing.T) {
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		t.Error(err)
		return
	}
	defer db.Close()
	_, err = db.Exec("create database if not exists test_stmt_driver")
	if err != nil {
		t.Error(err)
		return
	}
	_, err = db.Exec("create table if not exists test_stmt_driver.ct(ts timestamp," +
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
		t.Error(err)
		return
	}
	stmt, err := db.Prepare("insert into test_stmt_driver.ct values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)")

	if err != nil {
		t.Error(err)
		return
	}
	result, err := stmt.Exec(time.Now(), 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, "binary", "nchar")
	if err != nil {
		t.Error(err)
		return
	}
	affected, err := result.RowsAffected()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), affected)
}

func TestStmtQuery(t *testing.T) {
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		t.Error(err)
		return
	}
	defer db.Close()
	_, err = db.Exec("create database if not exists test_stmt_driver")
	if err != nil {
		t.Error(err)
		return
	}
	_, err = db.Exec("create table if not exists test_stmt_driver.ct(ts timestamp," +
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
		t.Error(err)
		return
	}
	stmt, err := db.Prepare("insert into test_stmt_driver.ct values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
	if err != nil {
		t.Error(err)
		return
	}
	now := time.Now()
	result, err := stmt.Exec(now, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, "binary", "nchar")
	if err != nil {
		t.Error(err)
		return
	}
	affected, err := result.RowsAffected()
	if err != nil {
		t.Error(err)
		return
	}
	assert.Equal(t, int64(1), affected)
	stmt.Close()
	stmt, err = db.Prepare("select * from test_stmt_driver.ct where ts = ?")
	if err != nil {
		t.Error(err)
		return
	}
	rows, err := stmt.Query(now)
	if err != nil {
		t.Error(err)
		return
	}
	columns, err := rows.Columns()
	if err != nil {
		t.Error(err)
		return
	}
	assert.Equal(t, []string{"ts", "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11", "c12", "c13"}, columns)
	count := 0
	for rows.Next() {
		count += 1
		var (
			ts  time.Time
			c1  bool
			c2  int8
			c3  int16
			c4  int32
			c5  int64
			c6  uint8
			c7  uint16
			c8  uint32
			c9  uint64
			c10 float32
			c11 float64
			c12 string
			c13 string
		)
		err = rows.Scan(&ts,
			&c1,
			&c2,
			&c3,
			&c4,
			&c5,
			&c6,
			&c7,
			&c8,
			&c9,
			&c10,
			&c11,
			&c12,
			&c13)
		assert.NoError(t, err)
		assert.Equal(t, now.UnixNano()/1e6, ts.UnixNano()/1e6)
		assert.Equal(t, true, c1)
		assert.Equal(t, int8(2), c2)
		assert.Equal(t, int16(3), c3)
		assert.Equal(t, int32(4), c4)
		assert.Equal(t, int64(5), c5)
		assert.Equal(t, uint8(6), c6)
		assert.Equal(t, uint16(7), c7)
		assert.Equal(t, uint32(8), c8)
		assert.Equal(t, uint64(9), c9)
		assert.Equal(t, float32(10), c10)
		assert.Equal(t, float64(11), c11)
		assert.Equal(t, "binary", c12)
		assert.Equal(t, "nchar", c13)
	}
	assert.Equal(t, 1, count)
}
