package wrapper

import (
	"github.com/taosdata/driver-go/v2/common"
	"testing"
)

func TestTaosOptions(t *testing.T) {
	type args struct {
		option int
		value  string
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{
			name: "test_options",
			args: args{
				option: common.TSDB_OPTION_CONFIGDIR,
				value:  "/home/taos",
			},
			want: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := TaosOptions(tt.args.option, tt.args.value); got != tt.want {
				t.Errorf("TaosOptions() = %v, want %v", got, tt.want)
			}
		})
	}
}
