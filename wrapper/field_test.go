package wrapper

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/errors"
)

// @author: xftan
// @date: 2022/1/27 17:22
// @description: test taos_fetch_lengths
func TestFetchLengths(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer TaosClose(conn)
	defer func() {
		res := TaosQuery(conn, "drop database if exists test_fetch_lengths")
		code := TaosError(res)
		if code != 0 {
			errStr := TaosErrorStr(res)
			TaosFreeResult(res)
			t.Error(errors.NewError(code, errStr))
			return
		}
		TaosFreeResult(res)
	}()
	res := TaosQuery(conn, "create database if not exists test_fetch_lengths")
	code := TaosError(res)
	if code != 0 {
		errStr := TaosErrorStr(res)
		TaosFreeResult(res)
		t.Error(errors.NewError(code, errStr))
		return
	}
	TaosFreeResult(res)
	defer func() {
		res := TaosQuery(conn, "drop database if exists test_fetch_lengths")
		code := TaosError(res)
		if code != 0 {
			errStr := TaosErrorStr(res)
			TaosFreeResult(res)
			t.Error(errors.NewError(code, errStr))
			return
		}
		TaosFreeResult(res)
	}()
	res = TaosQuery(conn, "drop table if exists test_fetch_lengths.test")
	code = TaosError(res)
	if code != 0 {
		errStr := TaosErrorStr(res)
		TaosFreeResult(res)
		t.Error(errors.NewError(code, errStr))
		return
	}
	TaosFreeResult(res)

	res = TaosQuery(conn, "create table if not exists test_fetch_lengths.test (ts timestamp, c1 int,c2 binary(10),c3 nchar(10))")
	code = TaosError(res)
	if code != 0 {
		errStr := TaosErrorStr(res)
		TaosFreeResult(res)
		t.Error(errors.NewError(code, errStr))
		return
	}
	TaosFreeResult(res)

	res = TaosQuery(conn, "insert into test_fetch_lengths.test values(now,1,'123','456')")
	code = TaosError(res)
	if code != 0 {
		errStr := TaosErrorStr(res)
		TaosFreeResult(res)
		t.Error(errors.NewError(code, errStr))
		return
	}
	TaosFreeResult(res)

	res = TaosQuery(conn, "select * from test_fetch_lengths.test")
	code = TaosError(res)
	if code != 0 {
		errStr := TaosErrorStr(res)
		TaosFreeResult(res)
		t.Error(errors.NewError(code, errStr))
		return
	}
	count := TaosNumFields(res)
	assert.Equal(t, 4, count)
	_, rows := TaosFetchBlock(res)
	_ = rows
	lengthList := FetchLengths(res, count)
	TaosFreeResult(res)
	assert.Equal(t, []int{8, 4, 12, 42}, lengthList)
}

// @author: xftan
// @date: 2022/1/27 17:23
// @description: test result column database name
func TestRowsHeader_TypeDatabaseName(t *testing.T) {
	type fields struct {
		ColNames  []string
		ColTypes  []uint8
		ColLength []int64
	}
	type args struct {
		i int
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   string
	}{
		{
			name: "NULL",
			fields: fields{
				ColTypes: []uint8{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
			},
			args: args{
				i: 0,
			},
			want: "NULL",
		},
		{
			name: "BOOL",
			fields: fields{
				ColTypes: []uint8{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
			},
			args: args{
				i: 1,
			},
			want: "BOOL",
		},
		{
			name: "TINYINT",
			fields: fields{
				ColTypes: []uint8{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
			},
			args: args{
				i: 2,
			},
			want: "TINYINT",
		},
		{
			name: "SMALLINT",
			fields: fields{
				ColTypes: []uint8{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
			},
			args: args{
				i: 3,
			},
			want: "SMALLINT",
		},
		{
			name: "INT",
			fields: fields{
				ColTypes: []uint8{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
			},
			args: args{
				i: 4,
			},
			want: "INT",
		},
		{
			name: "BIGINT",
			fields: fields{
				ColTypes: []uint8{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
			},
			args: args{
				i: 5,
			},
			want: "BIGINT",
		},
		{
			name: "FLOAT",
			fields: fields{
				ColTypes: []uint8{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
			},
			args: args{
				i: 6,
			},
			want: "FLOAT",
		},
		{
			name: "DOUBLE",
			fields: fields{
				ColTypes: []uint8{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
			},
			args: args{
				i: 7,
			},
			want: "DOUBLE",
		},
		{
			name: "BINARY",
			fields: fields{
				ColTypes: []uint8{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
			},
			args: args{
				i: 8,
			},
			want: "VARCHAR",
		},
		{
			name: "TIMESTAMP",
			fields: fields{
				ColTypes: []uint8{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
			},
			args: args{
				i: 9,
			},
			want: "TIMESTAMP",
		},
		{
			name: "NCHAR",
			fields: fields{
				ColTypes: []uint8{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
			},
			args: args{
				i: 10,
			},
			want: "NCHAR",
		},
		{
			name: "TINYINT UNSIGNED",
			fields: fields{
				ColTypes: []uint8{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
			},
			args: args{
				i: 11,
			},
			want: "TINYINT UNSIGNED",
		},
		{
			name: "SMALLINT UNSIGNED",
			fields: fields{
				ColTypes: []uint8{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
			},
			args: args{
				i: 12,
			},
			want: "SMALLINT UNSIGNED",
		},
		{
			name: "INT UNSIGNED",
			fields: fields{
				ColTypes: []uint8{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
			},
			args: args{
				i: 13,
			},
			want: "INT UNSIGNED",
		},
		{
			name: "BIGINT UNSIGNED",
			fields: fields{
				ColTypes: []uint8{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
			},
			args: args{
				i: 14,
			},
			want: "BIGINT UNSIGNED",
		},
		{
			name: "JSON",
			fields: fields{
				ColTypes: []uint8{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
			},
			args: args{
				i: 15,
			},
			want: "JSON",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rh := &RowsHeader{
				ColNames:  tt.fields.ColNames,
				ColTypes:  tt.fields.ColTypes,
				ColLength: tt.fields.ColLength,
			}
			if got := rh.TypeDatabaseName(tt.args.i); got != tt.want {
				t.Errorf("TypeDatabaseName() = %v, want %v", got, tt.want)
			}
		})
	}
}

// @author: xftan
// @date: 2022/1/27 17:23
// @description: test scan result column type
func TestRowsHeader_ScanType(t *testing.T) {
	type fields struct {
		ColNames  []string
		ColTypes  []uint8
		ColLength []int64
	}
	type args struct {
		i int
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   reflect.Type
	}{
		{
			name: "unknown",
			fields: fields{
				ColTypes: []uint8{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
			},
			args: args{
				i: 0,
			},
			want: common.UnknownType,
		},
		{
			name: "BOOL",
			fields: fields{
				ColTypes: []uint8{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
			},
			args: args{
				i: 1,
			},
			want: common.NullBool,
		},
		{
			name: "TINYINT",
			fields: fields{
				ColTypes: []uint8{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
			},
			args: args{
				i: 2,
			},
			want: common.NullInt8,
		},
		{
			name: "SMALLINT",
			fields: fields{
				ColTypes: []uint8{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
			},
			args: args{
				i: 3,
			},
			want: common.NullInt16,
		}, {
			name: "INT",
			fields: fields{
				ColTypes: []uint8{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
			},
			args: args{
				i: 4,
			},
			want: common.NullInt32,
		},
		{
			name: "BIGINT",
			fields: fields{
				ColTypes: []uint8{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
			},
			args: args{
				i: 5,
			},
			want: common.NullInt64,
		},
		{
			name: "FLOAT",
			fields: fields{
				ColTypes: []uint8{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
			},
			args: args{
				i: 6,
			},
			want: common.NullFloat32,
		},
		{
			name: "DOUBLE",
			fields: fields{
				ColTypes: []uint8{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
			},
			args: args{
				i: 7,
			},
			want: common.NullFloat64,
		},
		{
			name: "BINARY",
			fields: fields{
				ColTypes: []uint8{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
			},
			args: args{
				i: 8,
			},
			want: common.NullString,
		},
		{
			name: "TIMESTAMP",
			fields: fields{
				ColTypes: []uint8{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
			},
			args: args{
				i: 9,
			},
			want: common.NullTime,
		},
		{
			name: "NCHAR",
			fields: fields{
				ColTypes: []uint8{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
			},
			args: args{
				i: 10,
			},
			want: common.NullString,
		},
		{
			name: "TINYINT UNSIGNED",
			fields: fields{
				ColTypes: []uint8{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
			},
			args: args{
				i: 11,
			},
			want: common.NullUInt8,
		},
		{
			name: "SMALLINT UNSIGNED",
			fields: fields{
				ColTypes: []uint8{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
			},
			args: args{
				i: 12,
			},
			want: common.NullUInt16,
		},
		{
			name: "INT UNSIGNED",
			fields: fields{
				ColTypes: []uint8{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
			},
			args: args{
				i: 13,
			},
			want: common.NullUInt32,
		},
		{
			name: "BIGINT UNSIGNEDD",
			fields: fields{
				ColTypes: []uint8{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
			},
			args: args{
				i: 14,
			},
			want: common.NullUInt64,
		},
		{
			name: "JSON",
			fields: fields{
				ColTypes: []uint8{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
			},
			args: args{
				i: 15,
			},
			want: common.NullJson,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rh := &RowsHeader{
				ColNames:  tt.fields.ColNames,
				ColTypes:  tt.fields.ColTypes,
				ColLength: tt.fields.ColLength,
			}
			if got := rh.ScanType(tt.args.i); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ScanType() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestReadColumn(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}

	defer TaosClose(conn)
	defer func() {
		err = exec(conn, "drop database if exists test_read_column")
		assert.NoError(t, err)
	}()
	err = exec(conn, "create database if not exists test_read_column")
	assert.NoError(t, err)
	err = exec(conn, "use test_read_column")
	assert.NoError(t, err)
	err = exec(conn, "create table if not exists alltype(ts timestamp,v1 bool,v2 tinyint,v3 smallint,v4 int,v5 bigint,v6 tinyint unsigned,v7 smallint unsigned,v8 int unsigned,v9 bigint unsigned,v10 float,v11 double,v12 binary(20),v13 nchar(20),v14 varbinary(20),v15 geometry(100),v16 decimal(20,4)) tags (info json)")
	assert.NoError(t, err)
	err = exec(conn, "create table if not exists alltype2(ts timestamp,v1 bool,v2 tinyint,v3 smallint,v4 int,v5 bigint,v6 tinyint unsigned,v7 smallint unsigned,v8 int unsigned,v9 bigint unsigned,v10 float,v11 double,v12 binary(20),v13 nchar(20),v14 varbinary(20),v15 geometry(100),v16 decimal(10,4)) tags (info json)")
	assert.NoError(t, err)
	err = exec(conn, `insert into t1 using alltype tags ('{"a":1}') values(now,true,2,3,4,5,6,7,8,9,10.1,11.1,'12345678901','1234567','\xaabbcc','POINT(1 1)',12.1)`)
	assert.NoError(t, err)
	res := TaosQuery(conn, "select * from alltype")
	code := TaosError(res)
	if code != 0 {
		errStr := TaosErrorStr(res)
		TaosFreeResult(res)
		t.Error(errors.NewError(code, errStr))
		return
	}
	defer TaosFreeResult(res)
	count := TaosNumFields(res)
	assert.Equal(t, 18, count)
	ha, err := ReadColumn(res, count)
	assert.NoError(t, err)
	assert.Equal(t, 18, len(ha.ColNames))
	expect := &RowsHeader{
		ColNames: []string{"ts", "v1", "v2", "v3", "v4", "v5", "v6", "v7", "v8", "v9", "v10", "v11", "v12", "v13", "v14", "v15", "v16", "info"},
		ColTypes: []uint8{
			common.TSDB_DATA_TYPE_TIMESTAMP,
			common.TSDB_DATA_TYPE_BOOL,
			common.TSDB_DATA_TYPE_TINYINT,
			common.TSDB_DATA_TYPE_SMALLINT,
			common.TSDB_DATA_TYPE_INT,
			common.TSDB_DATA_TYPE_BIGINT,
			common.TSDB_DATA_TYPE_UTINYINT,
			common.TSDB_DATA_TYPE_USMALLINT,
			common.TSDB_DATA_TYPE_UINT,
			common.TSDB_DATA_TYPE_UBIGINT,
			common.TSDB_DATA_TYPE_FLOAT,
			common.TSDB_DATA_TYPE_DOUBLE,
			common.TSDB_DATA_TYPE_BINARY,
			common.TSDB_DATA_TYPE_NCHAR,
			common.TSDB_DATA_TYPE_VARBINARY,
			common.TSDB_DATA_TYPE_GEOMETRY,
			common.TSDB_DATA_TYPE_DECIMAL,
			common.TSDB_DATA_TYPE_JSON,
		},
		ColLength:  []int64{8, 1, 1, 2, 4, 8, 1, 2, 4, 8, 4, 8, 20, 20, 20, 100, 16, 4095},
		Precisions: []int64{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 20, 0},
		Scales:     []int64{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 0},
	}
	assert.Equal(t, expect, ha)

	res2 := TaosQuery(conn, "select * from alltype2")
	code = TaosError(res2)
	if code != 0 {
		errStr := TaosErrorStr(res2)
		TaosFreeResult(res2)
		t.Error(errors.NewError(code, errStr))
		return
	}
	defer TaosFreeResult(res2)
	count = TaosNumFields(res2)
	assert.Equal(t, 18, count)
	ha, err = ReadColumn(res2, count)
	assert.NoError(t, err)
	assert.Equal(t, 18, len(ha.ColNames))
	expect = &RowsHeader{
		ColNames: []string{"ts", "v1", "v2", "v3", "v4", "v5", "v6", "v7", "v8", "v9", "v10", "v11", "v12", "v13", "v14", "v15", "v16", "info"},
		ColTypes: []uint8{
			common.TSDB_DATA_TYPE_TIMESTAMP,
			common.TSDB_DATA_TYPE_BOOL,
			common.TSDB_DATA_TYPE_TINYINT,
			common.TSDB_DATA_TYPE_SMALLINT,
			common.TSDB_DATA_TYPE_INT,
			common.TSDB_DATA_TYPE_BIGINT,
			common.TSDB_DATA_TYPE_UTINYINT,
			common.TSDB_DATA_TYPE_USMALLINT,
			common.TSDB_DATA_TYPE_UINT,
			common.TSDB_DATA_TYPE_UBIGINT,
			common.TSDB_DATA_TYPE_FLOAT,
			common.TSDB_DATA_TYPE_DOUBLE,
			common.TSDB_DATA_TYPE_BINARY,
			common.TSDB_DATA_TYPE_NCHAR,
			common.TSDB_DATA_TYPE_VARBINARY,
			common.TSDB_DATA_TYPE_GEOMETRY,
			common.TSDB_DATA_TYPE_DECIMAL64,
			common.TSDB_DATA_TYPE_JSON,
		},
		ColLength:  []int64{8, 1, 1, 2, 4, 8, 1, 2, 4, 8, 4, 8, 20, 20, 20, 100, 8, 4095},
		Precisions: []int64{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10, 0},
		Scales:     []int64{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 0},
	}
	assert.Equal(t, expect, ha)
}
