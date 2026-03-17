package tests

import (
	"database/sql/driver"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonstmt "github.com/taosdata/driver-go/v3/common/stmt"
)

func TestUnifiedIntegrationStmt_AllTypesThreeRows(t *testing.T) {
	if testing.Short() {
		t.Skip("skip integration test in short mode")
	}

	client := openUnifiedIntegrationClient(t)
	defer client.Close()

	dbName := fmt.Sprintf("unified_it_stmt_%d", time.Now().UnixNano())
	tableName := "all_types"

	_, err := client.Exec(fmt.Sprintf("create database if not exists %s", dbName), 0)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = client.Exec(fmt.Sprintf("drop database if exists %s", dbName), 0)
	})

	createSQL := fmt.Sprintf(
		"create table if not exists %s.%s("+
			"ts timestamp,"+
			"c_bool bool,"+
			"c_tinyint tinyint,"+
			"c_smallint smallint,"+
			"c_int int,"+
			"c_bigint bigint,"+
			"c_utinyint tinyint unsigned,"+
			"c_usmallint smallint unsigned,"+
			"c_uint int unsigned,"+
			"c_ubigint bigint unsigned,"+
			"c_float float,"+
			"c_double double,"+
			"c_binary binary(32),"+
			"c_nchar nchar(32),"+
			"c_varbinary varbinary(32),"+
			"c_geometry geometry(100),"+
			"c_decimal decimal(20,4),"+
			"c_blob blob)",
		dbName, tableName,
	)
	_, err = client.Exec(createSQL, 0)
	require.NoError(t, err)

	ts1 := time.Unix(1722222222, 456000000).UTC().Round(time.Millisecond)
	ts2 := ts1.Add(time.Second)
	ts3 := ts1.Add(2 * time.Second)
	geo := []byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40}

	insertSQL := fmt.Sprintf(
		"insert into %s.%s values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
		dbName, tableName,
	)
	insertStmt, err := client.InitStmt(0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = insertStmt.Close(0) })
	require.NoError(t, insertStmt.Prepare(insertSQL, 0))
	require.NoError(t, insertStmt.Bind([]*commonstmt.TaosStmt2BindData{
		{
			Cols: [][]driver.Value{
				{ts1, ts2, ts3},
				{true, nil, false},
				{int8(-11), nil, int8(11)},
				{int16(-12), nil, int16(12)},
				{int32(-13), nil, int32(13)},
				{int64(-14), nil, int64(14)},
				{uint8(25), nil, uint8(35)},
				{uint16(26), nil, uint16(36)},
				{uint32(27), nil, uint32(37)},
				{uint64(28), nil, uint64(38)},
				{float32(11.5), nil, float32(13.5)},
				{float64(12.5), nil, float64(14.5)},
				{[]byte("bin_s1"), nil, []byte("bin_s3")},
				{"nchar_s1", nil, "nchar_s3"},
				{[]byte{0x11, 0x12}, nil, []byte{0x13, 0x14}},
				{geo, nil, geo},
				{"21.4300", nil, "87.6500"},
				{[]byte{0x2a, 0x2b, 0x2c}, nil, []byte{0x3a, 0x3b, 0x3c}},
			},
		},
	}))
	affected, err := insertStmt.Exec()
	require.NoError(t, err)
	require.Equal(t, 3, affected)

	querySQL := fmt.Sprintf(
		"select ts,c_bool,c_tinyint,c_smallint,c_int,c_bigint,c_utinyint,c_usmallint,c_uint,c_ubigint,c_float,c_double,c_binary,c_nchar,c_varbinary,c_geometry,c_decimal,c_blob "+
			"from %s.%s where ts >= ? order by ts",
		dbName, tableName,
	)
	queryStmt, err := client.InitStmt(0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = queryStmt.Close(0) })
	require.NoError(t, queryStmt.Prepare(querySQL, 0))
	require.NoError(t, queryStmt.Bind([]*commonstmt.TaosStmt2BindData{
		{
			Cols: [][]driver.Value{
				{ts1},
			},
		},
	}))
	_, err = queryStmt.Exec()
	require.NoError(t, err)

	rows, err := queryStmt.UseResult(0)
	require.NoError(t, err)
	require.NotNil(t, rows)
	t.Cleanup(func() { _ = rows.Close() })

	allRows := readAllResultRows(t, rows, 18)
	require.Len(t, allRows, 3)

	row1 := allRows[0]
	requireTimeEqual(t, row1[0], ts1)
	requireValueEqual(t, row1[1], true)
	requireValueEqual(t, row1[2], int8(-11))
	requireValueEqual(t, row1[3], int16(-12))
	requireValueEqual(t, row1[4], int32(-13))
	requireValueEqual(t, row1[5], int64(-14))
	requireValueEqual(t, row1[6], uint8(25))
	requireValueEqual(t, row1[7], uint16(26))
	requireValueEqual(t, row1[8], uint32(27))
	requireValueEqual(t, row1[9], uint64(28))
	requireValueEqual(t, row1[10], float32(11.5))
	requireValueEqual(t, row1[11], float64(12.5))
	requireValueEqual(t, row1[12], "bin_s1")
	requireValueEqual(t, row1[13], "nchar_s1")
	requireValueEqual(t, row1[14], []byte{0x11, 0x12})
	requireValueEqual(t, row1[15], geo)
	requireValueEqual(t, row1[16], "21.4300")
	requireValueEqual(t, row1[17], []byte{0x2a, 0x2b, 0x2c})

	row2 := allRows[1]
	requireTimeEqual(t, row2[0], ts2)
	for i := 1; i < len(row2); i++ {
		require.Nil(t, row2[i], "row2 col[%d] should be nil", i)
	}

	row3 := allRows[2]
	requireTimeEqual(t, row3[0], ts3)
	requireValueEqual(t, row3[1], false)
	requireValueEqual(t, row3[2], int8(11))
	requireValueEqual(t, row3[3], int16(12))
	requireValueEqual(t, row3[4], int32(13))
	requireValueEqual(t, row3[5], int64(14))
	requireValueEqual(t, row3[6], uint8(35))
	requireValueEqual(t, row3[7], uint16(36))
	requireValueEqual(t, row3[8], uint32(37))
	requireValueEqual(t, row3[9], uint64(38))
	requireValueEqual(t, row3[10], float32(13.5))
	requireValueEqual(t, row3[11], float64(14.5))
	requireValueEqual(t, row3[12], "bin_s3")
	requireValueEqual(t, row3[13], "nchar_s3")
	requireValueEqual(t, row3[14], []byte{0x13, 0x14})
	requireValueEqual(t, row3[15], geo)
	requireValueEqual(t, row3[16], "87.6500")
	requireValueEqual(t, row3[17], []byte{0x3a, 0x3b, 0x3c})
}
