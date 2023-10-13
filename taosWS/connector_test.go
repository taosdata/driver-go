package taosWS

import (
	"database/sql"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/taosdata/driver-go/v3/types"
)

// @author: xftan
// @date: 2023/10/13 11:22
// @description: test all type query
func TestAllTypeQuery(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	db, err := sql.Open("taosWS", dataSourceName)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	err = db.Ping()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, err = db.Exec("drop database if exists ws_test")
		if err != nil {
			t.Fatal(err)
		}
	}()
	_, err = db.Exec("create database if not exists ws_test")
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

	_, err = db.Exec("create table if not exists ws_test.alltype(ts timestamp," +
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
	_, err = db.Exec(fmt.Sprintf(`insert into ws_test.t1 using ws_test.alltype tags('{"a":"b"}') values('%s',%v,%v,%v,%v,%v,%v,%v,%v,%v,%v,%v,'test_binary','test_nchar')`, now.Format(time.RFC3339Nano), v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11))
	if err != nil {
		t.Fatal(err)
	}
	rows, err := db.Query(fmt.Sprintf("select * from ws_test.alltype where ts = '%s'", now.Format(time.RFC3339Nano)))
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
// @date: 2023/10/13 11:22
// @description: test null value
func TestAllTypeQueryNull(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	db, err := sql.Open("taosWS", dataSourceName)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	err = db.Ping()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, err = db.Exec("drop database if exists ws_test_null")
		if err != nil {
			t.Fatal(err)
		}
	}()
	_, err = db.Exec("create database if not exists ws_test_null")
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec("create table if not exists ws_test_null.alltype(ts timestamp," +
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
	_, err = db.Exec(fmt.Sprintf(`insert into ws_test_null.t1 using ws_test_null.alltype tags('null') values('%s',null,null,null,null,null,null,null,null,null,null,null,null,null)`, now.Format(time.RFC3339Nano)))
	if err != nil {
		t.Fatal(err)
	}
	rows, err := db.Query(fmt.Sprintf("select * from ws_test_null.alltype where ts = '%s'", now.Format(time.RFC3339Nano)))
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
			tt  *string
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
		assert.Nil(t, tt)
		if err != nil {

			t.Fatal(err)
		}
		if ts.IsZero() {
			t.Fatal(ts)
		}

	}
}

// @author: xftan
// @date: 2023/10/13 11:24
// @description: test compression
func TestAllTypeQueryCompression(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	db, err := sql.Open("taosWS", dataSourceNameWithCompression)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	err = db.Ping()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, err = db.Exec("drop database if exists ws_test")
		if err != nil {
			t.Fatal(err)
		}
	}()
	_, err = db.Exec("create database if not exists ws_test")
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

	_, err = db.Exec("create table if not exists ws_test.alltype(ts timestamp," +
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
	_, err = db.Exec(fmt.Sprintf(`insert into ws_test.t1 using ws_test.alltype tags('{"a":"b"}') values('%s',%v,%v,%v,%v,%v,%v,%v,%v,%v,%v,%v,'test_binary','test_nchar')`, now.Format(time.RFC3339Nano), v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11))
	if err != nil {
		t.Fatal(err)
	}
	rows, err := db.Query(fmt.Sprintf("select * from ws_test.alltype where ts = '%s'", now.Format(time.RFC3339Nano)))
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
// @date: 2023/10/13 11:24
// @description: test all type query without json
func TestAllTypeQueryWithoutJson(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	db, err := sql.Open("taosWS", dataSourceName)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	err = db.Ping()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, err = db.Exec("drop database if exists ws_test_without_json")
		if err != nil {
			t.Fatal(err)
		}
	}()
	_, err = db.Exec("create database if not exists ws_test_without_json")
	if err != nil {
		t.Fatal(err)
	}
	var (
		v1  = false
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

	_, err = db.Exec("create table if not exists ws_test_without_json.all_type(ts timestamp," +
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
		")",
	)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().Round(time.Millisecond)
	_, err = db.Exec(fmt.Sprintf(`insert into ws_test_without_json.all_type values('%s',%v,%v,%v,%v,%v,%v,%v,%v,%v,%v,%v,'test_binary','test_nchar')`, now.Format(time.RFC3339Nano), v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11))
	if err != nil {
		t.Fatal(err)
	}
	rows, err := db.Query(fmt.Sprintf("select * from ws_test_without_json.all_type where ts = '%s'", now.Format(time.RFC3339Nano)))
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
		if err != nil {
			t.Fatal(err)
		}
		if ts.IsZero() {
			t.Fatal(ts)
		}

	}
}

// @author: xftan
// @date: 2023/10/13 11:24
// @description: test all type query with null without json
func TestAllTypeQueryNullWithoutJson(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	db, err := sql.Open("taosWS", dataSourceName)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	err = db.Ping()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, err = db.Exec("drop database if exists ws_test_without_json_null")
		if err != nil {
			t.Fatal(err)
		}
	}()
	_, err = db.Exec("create database if not exists ws_test_without_json_null")
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec("create table if not exists ws_test_without_json_null.all_type(ts timestamp," +
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
		")",
	)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().Round(time.Millisecond)
	_, err = db.Exec(fmt.Sprintf(`insert into ws_test_without_json_null.all_type values('%s',null,null,null,null,null,null,null,null,null,null,null,null,null)`, now.Format(time.RFC3339Nano)))
	if err != nil {
		t.Fatal(err)
	}
	rows, err := db.Query(fmt.Sprintf("select * from ws_test_without_json_null.all_type where ts = '%s'", now.Format(time.RFC3339Nano)))
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
		if err != nil {

			t.Fatal(err)
		}
		if ts.IsZero() {
			t.Fatal(ts)
		}

	}
}

// @author: xftan
// @date: 2023/10/13 11:24
// @description: test query
func TestBatch(t *testing.T) {
	now := time.Now()
	tests := []struct {
		name    string
		sql     string
		isQuery bool
	}{
		{
			name: "drop db",
			sql:  "drop database if exists test_batch",
		},
		{
			name: "create db",
			sql:  "create database test_batch",
		},
		{
			name: "use db",
			sql:  "use test_batch",
		},
		{
			name: "create table",
			sql:  "create table test(ts timestamp,v int)",
		},
		{
			name: "insert 1",
			sql:  fmt.Sprintf("insert into test values ('%s',1)", now.Format(time.RFC3339Nano)),
		},
		{
			name: "insert 2",
			sql:  fmt.Sprintf("insert into test values ('%s',2)", now.Add(time.Second).Format(time.RFC3339Nano)),
		},
		{
			name:    "query all",
			sql:     "select * from test order by ts",
			isQuery: true,
		},
		{
			name: "drop database",
			sql:  "drop database if exists test_batch",
		},
	}
	db, err := sql.Open("taosWS", dataSourceName)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	//err = db.Ping()
	//if err != nil {
	//	t.Fatal(err)
	//}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.isQuery {
				result, err := db.Query(tt.sql)
				assert.NoError(t, err)
				var check [][]interface{}
				for result.Next() {
					var ts time.Time
					var v int
					err := result.Scan(&ts, &v)
					assert.NoError(t, err)
					check = append(check, []interface{}{ts, v})
				}
				assert.Equal(t, 2, len(check))
				assert.Equal(t, now.UnixNano()/1e6, check[0][0].(time.Time).UnixNano()/1e6)
				assert.Equal(t, now.Add(time.Second).UnixNano()/1e6, check[1][0].(time.Time).UnixNano()/1e6)
				assert.Equal(t, int(1), check[0][1].(int))
				assert.Equal(t, int(2), check[1][1].(int))
			} else {
				_, err := db.Exec(tt.sql)
				assert.NoError(t, err)
			}
		})
	}
}
