package taosWS

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	taosErrors "github.com/taosdata/driver-go/v3/errors"
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

	cfg, err := ParseDSN(dataSourceName)
	if err != nil {
		t.Fatalf("ParseDSN error: %v", err)
	}
	conn, err := newTaosConn(cfg)
	if err != nil {
		t.Fatalf("newTaosConn error: %v", err)
	}

	// to test bad connection, we manually close the connection
	err = conn.Close()
	if err != nil {
		t.Fatalf("close error: %v", err)
	}

	_, err = conn.QueryContext(context.Background(), "select 1", nil)
	if err == nil {
		t.Fatalf("query should fail")
	}
}

func TestHandleResponseError(t *testing.T) {
	t.Run("Error not nil", func(t *testing.T) {
		err := errors.New("some error")
		result := handleResponseError(err, 0, "ignored message")
		assert.Equal(t, err, result, "Expected the original error to be returned")
	})

	t.Run("Error nil and non-zero code", func(t *testing.T) {
		code := 123
		msg := "some error message"
		expectedErr := taosErrors.NewError(code, msg)

		result := handleResponseError(nil, code, msg)
		assert.EqualError(t, result, expectedErr.Error(), "Expected a new error to be returned based on code and message")
	})

	t.Run("Error nil and zero code", func(t *testing.T) {
		result := handleResponseError(nil, 0, "ignored message")
		assert.Nil(t, result, "Expected nil to be returned when there is no error and code is zero")
	})
}

func TestBegin(t *testing.T) {
	cfg, err := ParseDSN(dataSourceName)
	if err != nil {
		t.Fatalf("ParseDSN error: %v", err)
	}
	conn, err := newTaosConn(cfg)
	if err != nil {
		t.Fatalf("newTaosConn error: %v", err)
	}
	defer func() {
		err = conn.Close()
		assert.NoError(t, err)
	}()

	tx, err := conn.Begin()
	assert.Error(t, err)
	assert.Nil(t, tx)
}
