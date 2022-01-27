package errors

import (
	"reflect"
	"testing"
)

// @author: xftan
// @date: 2022/1/25 17:47
// @description: test get taos error
func TestGetError(t *testing.T) {
	type args struct {
		code int
	}
	tests := []struct {
		name string
		args args
		err  error
	}{
		{
			name: "common",
			args: args{
				code: -2147483136,
			},
			err: ErrTscInvalidOperation,
		},
		{
			name: "no error",
			args: args{
				code: 0,
			},
			err: nil,
		},
		{
			name: "unknown",
			args: args{code: 0xffff},
			err: &TaosError{
				Code:   0xffff,
				ErrStr: "unknown error",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := GetError(tt.args.code)
			if err != tt.err {
				e, is := tt.err.(*TaosError)
				if !is || !err.(*TaosError).IsError(e) {
					t.Errorf("GetError() error = %v, wantErr %v", err, tt.err)
				}
			}
			if err != nil {
				_ = err.Error()
			}
		})
	}
}

// @author: xftan
// @date: 2022/1/25 17:47
// @description: test new taos error
func TestNewError(t *testing.T) {
	err := NewError(-2147483136, "test error")
	if !reflect.DeepEqual(err, &TaosError{
		Code:   0x0200,
		ErrStr: "test error",
	}) {
		t.Errorf("NewError() error = %v, wantErr %v", err, &TaosError{
			Code:   0x0200,
			ErrStr: "test error",
		})
	}
}
