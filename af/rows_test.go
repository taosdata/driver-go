package af

import (
	"reflect"
	"testing"

	"github.com/taosdata/driver-go/v2/common"
	"github.com/taosdata/driver-go/v2/wrapper"
)

// @author: xftan
// @date: 2022/5/31 19:17
// @description: test ColumnTypeDatabaseTypeName
func Test_rows_ColumnTypeDatabaseTypeName(t *testing.T) {
	type fields struct {
		rowsHeader *wrapper.RowsHeader
	}
	type args struct {
		i int
	}
	tests := []struct {
		name   string
		fields fields
		want   []string
	}{
		{
			name: "all",
			fields: fields{
				rowsHeader: &wrapper.RowsHeader{
					ColTypes: []uint8{
						0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
					},
				},
			},
			want: []string{
				"NULL",
				"BOOL",
				"TINYINT",
				"SMALLINT",
				"INT",
				"BIGINT",
				"FLOAT",
				"DOUBLE",
				"BINARY",
				"TIMESTAMP",
				"NCHAR",
				"TINYINT UNSIGNED",
				"SMALLINT UNSIGNED",
				"INT UNSIGNED",
				"BIGINT UNSIGNED",
				"JSON",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rs := &rows{
				rowsHeader: tt.fields.rowsHeader,
			}
			for i := 0; i < len(tt.want); i++ {
				if got := rs.ColumnTypeDatabaseTypeName(i); got != tt.want[i] {
					t.Errorf("ColumnTypeDatabaseTypeName() = %v, want %v", got, tt.want[i])
				}
			}
		})
	}
}

// @author: xftan
// @date: 2022/5/31 19:19
// @description: test ColumnTypeLength
func Test_rows_ColumnTypeLength(t *testing.T) {
	type fields struct {
		rowsHeader *wrapper.RowsHeader
	}
	type args struct {
		i int
	}
	tests := []struct {
		name       string
		fields     fields
		args       args
		wantLength int64
		wantOk     bool
	}{
		{
			name: "normal",
			fields: fields{
				rowsHeader: &wrapper.RowsHeader{ColLength: []uint16{10}},
			},
			args: args{
				i: 0,
			},
			wantLength: 10,
			wantOk:     true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rs := &rows{
				rowsHeader: tt.fields.rowsHeader,
			}
			gotLength, gotOk := rs.ColumnTypeLength(tt.args.i)
			if gotLength != tt.wantLength {
				t.Errorf("ColumnTypeLength() gotLength = %v, want %v", gotLength, tt.wantLength)
			}
			if gotOk != tt.wantOk {
				t.Errorf("ColumnTypeLength() gotOk = %v, want %v", gotOk, tt.wantOk)
			}
		})
	}
}

// @author: xftan
// @date: 2022/5/31 19:19
// @description: test ColumnTypeScanType
func Test_rows_ColumnTypeScanType(t *testing.T) {
	type fields struct {
		rowsHeader *wrapper.RowsHeader
	}
	tests := []struct {
		name   string
		fields fields
		want   []reflect.Type
	}{
		{
			name: "all",
			fields: fields{
				rowsHeader: &wrapper.RowsHeader{ColTypes: []uint8{
					0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
				}},
			},
			want: []reflect.Type{
				common.UnknownType,
				common.NullBool,
				common.NullInt8,
				common.NullInt16,
				common.NullInt32,
				common.NullInt64,
				common.NullFloat32,
				common.NullFloat64,
				common.NullString,
				common.NullTime,
				common.NullString,
				common.NullUInt8,
				common.NullUInt16,
				common.NullUInt32,
				common.NullUInt64,
				common.NullJson,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rs := &rows{
				rowsHeader: tt.fields.rowsHeader,
			}
			for i := 0; i < len(tt.want); i++ {
				if got := rs.ColumnTypeScanType(i); !reflect.DeepEqual(got, tt.want[i]) {
					t.Errorf("ColumnTypeScanType(%d) = %v, want %v", i, got, tt.want[i])
				}
			}
		})
	}
}
