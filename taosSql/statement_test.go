package taosSql

import (
	"database/sql/driver"
	"reflect"
	"testing"
)

// @author: xftan
// @date: 2022/1/27 16:20
// @description: test convert value
func Test_converter_ConvertValue(t *testing.T) {
	ptr := int8(8)
	type args struct {
		v interface{}
	}
	tests := []struct {
		name    string
		args    args
		want    driver.Value
		wantErr bool
	}{
		{
			name: "int8",
			args: args{
				v: int8(8),
			},
			want:    int64(8),
			wantErr: false,
		},
		{
			name: "int8",
			args: args{
				v: int16(8),
			},
			want:    int64(8),
			wantErr: false,
		},
		{
			name: "int8",
			args: args{
				v: int32(8),
			},
			want:    int64(8),
			wantErr: false,
		},
		{
			name: "int8",
			args: args{
				v: int64(8),
			},
			want:    int64(8),
			wantErr: false,
		},
		{
			name: "uint8",
			args: args{
				v: uint8(8),
			},
			want:    uint64(8),
			wantErr: false,
		},
		{
			name: "uint16",
			args: args{
				v: uint16(8),
			},
			want:    uint64(8),
			wantErr: false,
		},
		{
			name: "uint32",
			args: args{
				v: uint32(8),
			},
			want:    uint64(8),
			wantErr: false,
		},
		{
			name: "uint64",
			args: args{
				v: uint64(8),
			},
			want:    uint64(8),
			wantErr: false,
		},
		{
			name: "float32",
			args: args{
				v: float32(8),
			},
			want:    float64(8),
			wantErr: false,
		},
		{
			name: "float64",
			args: args{
				v: float64(8),
			},
			want:    float64(8),
			wantErr: false,
		},
		{
			name: "bool",
			args: args{
				v: true,
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "bytes",
			args: args{
				v: []byte{'1', '2', '3'},
			},
			want:    []byte{'1', '2', '3'},
			wantErr: false,
		},
		{
			name: "string",
			args: args{
				v: "123",
			},
			want:    "123",
			wantErr: false,
		},
		{
			name: "nil",
			args: args{
				v: nil,
			},
			want:    nil,
			wantErr: false,
		},
		{
			name: "ptr",
			args: args{
				v: &ptr,
			},
			want:    int64(8),
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := converter{}
			got, err := c.ConvertValue(tt.args.v)
			if (err != nil) != tt.wantErr {
				t.Errorf("ConvertValue() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ConvertValue() got = %v, want %v", got, tt.want)
			}
		})
	}
}
