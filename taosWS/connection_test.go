package taosWS

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// @author: xftan
// @date: 2023/10/13 11:22
// @description: test format bytes
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

func TestBadConnection(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			// bad connection should not panic
			t.Fatalf("panic: %v", r)
		}
	}()

	cfg, err := parseDSN(dataSourceName)
	if err != nil {
		t.Fatalf("parseDSN error: %v", err)
	}
	conn, err := newTaosConn(cfg)
	if err != nil {
		t.Fatalf("newTaosConn error: %v", err)
	}

	// to test bad connection, we manually close the connection
	conn.Close()

	_, err = conn.Query("select 1", nil)
	if err == nil {
		t.Fatalf("query should fail")
	}
}
