package taosRestful

import (
	"database/sql"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/taosdata/driver-go/v2/types"
)

// @author: xftan
// @date: 2021/12/21 10:58
// @description: test common use of restful
func TestOpenQuery(t *testing.T) {
	db, err := sql.Open("taosRestful", "root:taosdata@http(127.0.0.1:6041)/?token=123")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	err = db.Ping()
	if err != nil {
		t.Fatal(err)
	}
	rows, err := db.Query("select ts, dnodeid from log.dn")
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var dnodeID int
		var ts time.Time
		err := rows.Scan(&ts, &dnodeID)
		if err != nil {
			t.Fatal(err)
		}
		if ts.IsZero() {
			t.Fatal(ts)
		}
	}
}

// @author: xftan
// @date: 2021/12/21 10:59
// @description: test restful query of all type
func TestAllTypeQuery(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	db, err := sql.Open("taosRestful", dataSourceName)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	err = db.Ping()
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec("create database if not exists restful_test")
	if err != nil {
		t.Fatal(err)
	}
	var (
		v1  = true
		v2  = int8(rand.Int())
		v3  = int16(rand.Int())
		v4  = rand.Int31()
		v5  = int64(rand.Int31())
		v6  = uint8(rand.Uint32())
		v7  = uint16(rand.Uint32())
		v8  = rand.Uint32()
		v9  = uint64(rand.Uint32())
		v10 = rand.Float32()
		v11 = rand.Float64()
		v12 = "test_binary"
		v13 = "test_nchar"
	)

	_, err = db.Exec("create table if not exists restful_test.alltype(ts timestamp," +
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
		")" +
		"tags(t json)",
	)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().Round(time.Millisecond)
	_, err = db.Exec(fmt.Sprintf(`insert into restful_test.t1 using restful_test.alltype tags('{"a":"b"}') values('%s',%v,%v,%v,%v,%v,%v,%v,%v,%v,%v,%v,'test_binary','test_nchar')`, now.Format(time.RFC3339Nano), v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11))
	if err != nil {
		t.Fatal(err)
	}
	rows, err := db.Query(fmt.Sprintf("select * from restful_test.alltype where ts = '%s'", now.Format(time.RFC3339Nano)))
	assert.NoError(t, err)
	columns, err := rows.Columns()
	assert.NoError(t, err)
	t.Log(columns)
	cTypes, err := rows.ColumnTypes()
	assert.NoError(t, err)
	t.Log(cTypes)
	for rows.Next() {
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
			tt  types.RawMessage
		)
		err := rows.Scan(
			&ts,
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
			&c13,
			&tt,
		)
		assert.Equal(t, now.UTC(), ts.UTC())
		assert.Equal(t, v1, c1)
		assert.Equal(t, v2, c2)
		assert.Equal(t, v3, c3)
		assert.Equal(t, v4, c4)
		assert.Equal(t, v5, c5)
		assert.Equal(t, v6, c6)
		assert.Equal(t, v7, c7)
		assert.Equal(t, v8, c8)
		assert.Equal(t, v9, c9)
		assert.Equal(t, v10, c10)
		assert.Equal(t, v11, c11)
		assert.Equal(t, v12, c12)
		assert.Equal(t, v13, c13)
		assert.Equal(t, types.RawMessage(`{"a":"b"}`), tt)
		if err != nil {
			t.Fatal(err)
		}
		if ts.IsZero() {
			t.Fatal(ts)
		}

	}
}

// @author: xftan
// @date: 2022/2/8 12:51
// @description: test query all null value
func TestAllTypeQueryNull(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	db, err := sql.Open("taosRestful", dataSourceName)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	err = db.Ping()
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec("create database if not exists restful_test_null")
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec("create table if not exists restful_test_null.alltype(ts timestamp," +
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
		")" +
		"tags(t json)",
	)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().Round(time.Millisecond)
	_, err = db.Exec(fmt.Sprintf(`insert into restful_test_null.t1 using restful_test_null.alltype tags('null') values('%s',null,null,null,null,null,null,null,null,null,null,null,null,null)`, now.Format(time.RFC3339Nano)))
	if err != nil {
		t.Fatal(err)
	}
	rows, err := db.Query(fmt.Sprintf("select * from restful_test_null.alltype where ts = '%s'", now.Format(time.RFC3339Nano)))
	assert.NoError(t, err)
	columns, err := rows.Columns()
	assert.NoError(t, err)
	t.Log(columns)
	cTypes, err := rows.ColumnTypes()
	assert.NoError(t, err)
	t.Log(cTypes)
	for rows.Next() {
		var (
			ts  time.Time
			c1  *bool
			c2  *int8
			c3  *int16
			c4  *int32
			c5  *int64
			c6  *uint8
			c7  *uint16
			c8  *uint32
			c9  *uint64
			c10 *float32
			c11 *float64
			c12 *string
			c13 *string
			tt  *types.RawMessage
		)
		err := rows.Scan(
			&ts,
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
			&c13,
			&tt,
		)
		assert.Equal(t, now.UTC(), ts.UTC())
		assert.Nil(t, c1)
		assert.Nil(t, c2)
		assert.Nil(t, c3)
		assert.Nil(t, c4)
		assert.Nil(t, c5)
		assert.Nil(t, c6)
		assert.Nil(t, c7)
		assert.Nil(t, c8)
		assert.Nil(t, c9)
		assert.Nil(t, c10)
		assert.Nil(t, c11)
		assert.Nil(t, c12)
		assert.Nil(t, c13)
		assert.Equal(t, types.RawMessage("null"), *tt)
		if err != nil {

			t.Fatal(err)
		}
		if ts.IsZero() {
			t.Fatal(ts)
		}

	}
}

// @author: xftan
// @date: 2022/2/10 14:32
// @description: test restful query of all type with compression
func TestAllTypeQueryCompression(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	db, err := sql.Open("taosRestful", dataSourceNameWithCompression)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	err = db.Ping()
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec("create database if not exists restful_test")
	if err != nil {
		t.Fatal(err)
	}
	var (
		v1  = true
		v2  = int8(rand.Int())
		v3  = int16(rand.Int())
		v4  = rand.Int31()
		v5  = int64(rand.Int31())
		v6  = uint8(rand.Uint32())
		v7  = uint16(rand.Uint32())
		v8  = rand.Uint32()
		v9  = uint64(rand.Uint32())
		v10 = rand.Float32()
		v11 = rand.Float64()
		v12 = "test_binary"
		v13 = "test_nchar"
	)

	_, err = db.Exec("create table if not exists restful_test.alltype(ts timestamp," +
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
		")" +
		"tags(t json)",
	)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().Round(time.Millisecond)
	_, err = db.Exec(fmt.Sprintf(`insert into restful_test.t1 using restful_test.alltype tags('{"a":"b"}') values('%s',%v,%v,%v,%v,%v,%v,%v,%v,%v,%v,%v,'test_binary','test_nchar')`, now.Format(time.RFC3339Nano), v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11))
	if err != nil {
		t.Fatal(err)
	}
	rows, err := db.Query(fmt.Sprintf("select * from restful_test.alltype where ts = '%s'", now.Format(time.RFC3339Nano)))
	assert.NoError(t, err)
	columns, err := rows.Columns()
	assert.NoError(t, err)
	t.Log(columns)
	cTypes, err := rows.ColumnTypes()
	assert.NoError(t, err)
	t.Log(cTypes)
	for rows.Next() {
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
			tt  types.RawMessage
		)
		err := rows.Scan(
			&ts,
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
			&c13,
			&tt,
		)
		assert.Equal(t, now.UTC(), ts.UTC())
		assert.Equal(t, v1, c1)
		assert.Equal(t, v2, c2)
		assert.Equal(t, v3, c3)
		assert.Equal(t, v4, c4)
		assert.Equal(t, v5, c5)
		assert.Equal(t, v6, c6)
		assert.Equal(t, v7, c7)
		assert.Equal(t, v8, c8)
		assert.Equal(t, v9, c9)
		assert.Equal(t, v10, c10)
		assert.Equal(t, v11, c11)
		assert.Equal(t, v12, c12)
		assert.Equal(t, v13, c13)
		assert.Equal(t, types.RawMessage(`{"a":"b"}`), tt)
		if err != nil {
			t.Fatal(err)
		}
		if ts.IsZero() {
			t.Fatal(ts)
		}
	}
}
