package common

import (
	"database/sql/driver"
	"testing"
	"time"
)

// @author: xftan
// @date: 2022/1/25 17:47
// @description: test sql interpolate params
func TestInterpolateParams(t *testing.T) {
	type args struct {
		query string
		args  []driver.Value
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "all type",
			args: args{
				query: "select * from t1 where " +
					"ts = ? and " +
					"i8 = ? and " +
					"i16 = ? and " +
					"i32 = ? and " +
					"i64 = ? and " +
					"ui8 = ? and " +
					"ui16 = ? and " +
					"ui32 = ? and " +
					"ui64 = ? and " +
					"f32 = ? and " +
					"f64 = ? and " +
					"i = ? and " +
					"u = ? and " +
					"b = ? and " +
					"bs = ? and " +
					"str = ? and " +
					"nil is ?",
				args: []driver.Value{
					time.Unix(1643068800, 0).UTC(),
					int8(1),
					int16(2),
					int32(3),
					int64(4),
					uint8(1),
					uint16(2),
					uint32(3),
					uint64(4),
					float32(5.2),
					float64(5.2),
					int(6),
					uint(6),
					bool(true),
					[]byte("'bytes'"),
					[]byte("'str'"),
					nil,
				},
			},
			want: "select * from t1 where " +
				"ts = '2022-01-25T00:00:00Z' and " +
				"i8 = 1 and " +
				"i16 = 2 and " +
				"i32 = 3 and " +
				"i64 = 4 and " +
				"ui8 = 1 and " +
				"ui16 = 2 and " +
				"ui32 = 3 and " +
				"ui64 = 4 and " +
				"f32 = 5.200000 and " +
				"f64 = 5.200000 and " +
				"i = 6 and " +
				"u = 6 and " +
				"b = 1 and " +
				"bs = 'bytes' and " +
				"str = 'str' and " +
				"nil is NULL",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := InterpolateParams(tt.args.query, tt.args.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("InterpolateParams() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("InterpolateParams() got = %v, want %v", got, tt.want)
			}
		})
	}
}
