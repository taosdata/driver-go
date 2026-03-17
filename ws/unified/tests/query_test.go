package tests

import (
	"database/sql/driver"
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonstmt "github.com/taosdata/driver-go/v3/common/stmt"
	"github.com/taosdata/driver-go/v3/ws/unified"
)

const unifiedIntegrationDSNEnv = "UNIFIED_IT_DSN"
const unifiedDefaultIntegrationDSN = "root:taosdata@ws(127.0.0.1:6041)/"

func openUnifiedIntegrationClient(t *testing.T) *unified.Client {
	t.Helper()

	dsn := strings.TrimSpace(os.Getenv(unifiedIntegrationDSNEnv))
	if dsn == "" {
		dsn = unifiedDefaultIntegrationDSN
	}

	client, err := unified.Open(dsn)
	if err != nil {
		t.Skipf("skip integration test: unified.Open failed: %v", err)
	}
	if _, err = client.Exec("select server_version()", 0); err != nil {
		client.Close()
		t.Skipf("skip integration test: taosadapter/taosd not available: %v", err)
	}
	return client
}

func readAllResultRows(t *testing.T, rows *unified.ResultSet, colCount int) [][]driver.Value {
	t.Helper()

	out := make([][]driver.Value, 0, 4)
	for {
		values := make([]driver.Value, colCount)
		err := rows.Next(values)
		if err == io.EOF {
			return out
		}
		require.NoError(t, err)
		out = append(out, append([]driver.Value(nil), values...))
	}
}

func requireTimeEqual(t *testing.T, got driver.Value, want time.Time) {
	t.Helper()

	gotTime, ok := got.(time.Time)
	require.True(t, ok, "expect time.Time got %T", got)
	require.Equal(t, want.UnixNano(), gotTime.UnixNano())
}

func requireValueEqual(t *testing.T, got driver.Value, want driver.Value) {
	t.Helper()
	require.True(t, reflect.DeepEqual(got, want), "value mismatch, want=%#v got=%#v", want, got)
}

func TestUnifiedIntegrationQuery_AllTypesThreeRows(t *testing.T) {
	if testing.Short() {
		t.Skip("skip integration test in short mode")
	}

	client := openUnifiedIntegrationClient(t)
	defer client.Close()

	dbName := fmt.Sprintf("unified_it_query_%d", time.Now().UnixNano())
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

	ts1 := time.Unix(1711111111, 123000000).UTC().Round(time.Millisecond)
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
				{int8(-1), nil, int8(1)},
				{int16(-2), nil, int16(2)},
				{int32(-3), nil, int32(3)},
				{int64(-4), nil, int64(4)},
				{uint8(5), nil, uint8(15)},
				{uint16(6), nil, uint16(16)},
				{uint32(7), nil, uint32(17)},
				{uint64(8), nil, uint64(18)},
				{float32(1.5), nil, float32(3.5)},
				{float64(2.5), nil, float64(4.5)},
				{[]byte("bin_r1"), nil, []byte("bin_r3")},
				{"nchar_r1", nil, "nchar_r3"},
				{[]byte{0x01, 0x02}, nil, []byte{0x03, 0x04}},
				{geo, nil, geo},
				{"12.3400", nil, "98.7600"},
				{[]byte{0x0a, 0x0b, 0x0c}, nil, []byte{0x1a, 0x1b, 0x1c}},
			},
		},
	}))
	affected, err := insertStmt.Exec()
	require.NoError(t, err)
	require.Equal(t, 3, affected)

	querySQL := fmt.Sprintf(
		"select ts,c_bool,c_tinyint,c_smallint,c_int,c_bigint,c_utinyint,c_usmallint,c_uint,c_ubigint,c_float,c_double,c_binary,c_nchar,c_varbinary,c_geometry,c_decimal,c_blob "+
			"from %s.%s order by ts",
		dbName, tableName,
	)
	rows, err := client.Query(querySQL, 0)
	require.NoError(t, err)
	require.NotNil(t, rows)
	t.Cleanup(func() { _ = rows.Close() })

	allRows := readAllResultRows(t, rows, 18)
	require.Len(t, allRows, 3)

	row1 := allRows[0]
	requireTimeEqual(t, row1[0], ts1)
	requireValueEqual(t, row1[1], true)
	requireValueEqual(t, row1[2], int8(-1))
	requireValueEqual(t, row1[3], int16(-2))
	requireValueEqual(t, row1[4], int32(-3))
	requireValueEqual(t, row1[5], int64(-4))
	requireValueEqual(t, row1[6], uint8(5))
	requireValueEqual(t, row1[7], uint16(6))
	requireValueEqual(t, row1[8], uint32(7))
	requireValueEqual(t, row1[9], uint64(8))
	requireValueEqual(t, row1[10], float32(1.5))
	requireValueEqual(t, row1[11], float64(2.5))
	requireValueEqual(t, row1[12], "bin_r1")
	requireValueEqual(t, row1[13], "nchar_r1")
	requireValueEqual(t, row1[14], []byte{0x01, 0x02})
	requireValueEqual(t, row1[15], geo)
	requireValueEqual(t, row1[16], "12.3400")
	requireValueEqual(t, row1[17], []byte{0x0a, 0x0b, 0x0c})

	row2 := allRows[1]
	requireTimeEqual(t, row2[0], ts2)
	for i := 1; i < len(row2); i++ {
		require.Nil(t, row2[i], "row2 col[%d] should be nil", i)
	}

	row3 := allRows[2]
	requireTimeEqual(t, row3[0], ts3)
	requireValueEqual(t, row3[1], false)
	requireValueEqual(t, row3[2], int8(1))
	requireValueEqual(t, row3[3], int16(2))
	requireValueEqual(t, row3[4], int32(3))
	requireValueEqual(t, row3[5], int64(4))
	requireValueEqual(t, row3[6], uint8(15))
	requireValueEqual(t, row3[7], uint16(16))
	requireValueEqual(t, row3[8], uint32(17))
	requireValueEqual(t, row3[9], uint64(18))
	requireValueEqual(t, row3[10], float32(3.5))
	requireValueEqual(t, row3[11], float64(4.5))
	requireValueEqual(t, row3[12], "bin_r3")
	requireValueEqual(t, row3[13], "nchar_r3")
	requireValueEqual(t, row3[14], []byte{0x03, 0x04})
	requireValueEqual(t, row3[15], geo)
	requireValueEqual(t, row3[16], "98.7600")
	requireValueEqual(t, row3[17], []byte{0x1a, 0x1b, 0x1c})
}
