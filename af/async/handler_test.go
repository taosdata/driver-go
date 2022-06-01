package async

import (
	"testing"
	"time"

	"github.com/taosdata/driver-go/v2/wrapper/handler"
)

// @author: xftan
// @date: 2022/1/25 16:33
// @description: test async handler pool available
func TestHandler(t *testing.T) {
	SetHandlerSize(12)
	timer := time.NewTimer(time.Second)
	done := make(chan struct{})
	go func() {
		handler := GetHandler()
		PutHandler(handler)
		done <- struct{}{}
	}()
	select {
	case <-done:
		return
	case <-timer.C:
		timer.Stop()
		timer = nil
		t.Error("dead lock")
		return
	}
}

func TestGetHandler(t *testing.T) {
	tests := []struct {
		name string
	}{
		{
			name: "normal",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetHandler(); got.Caller == nil {
				t.Errorf("GetHandler() = %v, Caller is null", got)
			}
		})
	}
}

func TestPutHandler(t *testing.T) {
	type args struct {
		h *handler.Handler
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "nullHandlerPool",
			args: args{
				h: &handler.Handler{},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			PutHandler(tt.args.h)
		})
	}
}
