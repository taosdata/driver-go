package taosWS

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_formatBytes(t *testing.T) {
	type args struct {
		bs []byte
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "nothing",
			args: args{
				bs: nil,
			},
			want: "",
		},
		{
			name: "one byte",
			args: args{
				bs: []byte{'a'},
			},
			want: "[0x61]",
		},
		{
			name: "two byes",
			args: args{
				bs: []byte{'a', 'b'},
			},
			want: "[0x61,0x62]",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, formatBytes(tt.args.bs), "formatBytes(%v)", tt.args.bs)
		})
	}
}
