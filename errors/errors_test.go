package errors

import "testing"

// @author: xftan
// @date: 2023/10/13 11:20
// @description: test new error
func TestNewError(t *testing.T) {
	type args struct {
		code   int
		errStr string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "common",
			args: args{
				code:   0,
				errStr: "success",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := NewError(tt.args.code, tt.args.errStr); (err != nil) != tt.wantErr {
				t.Errorf("NewError() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
