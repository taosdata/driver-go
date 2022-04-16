package wrapper

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/taosdata/driver-go/v2/common"
	"github.com/taosdata/driver-go/v2/errors"
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

	res := TaosQuery(conn, "create database if not exists test_fetch_lengths")
	code := TaosError(res)
	if code != 0 {
		errStr := TaosErrorStr(res)
		TaosFreeResult(res)
		t.Error(errors.NewError(code, errStr))
		return
	}
	TaosFreeResult(res)

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
			want: "BINARY",
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
