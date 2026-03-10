package taosWS

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	osexec "os/exec"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

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
	cfg.ReadTimeout = 10 * time.Second
	cfg.WriteTimeout = 10 * time.Second
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
	cfg.ReadTimeout = 10 * time.Second
	cfg.WriteTimeout = 10 * time.Second
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

func newTaosadapter(port string) *osexec.Cmd {
	command := "taosadapter"
	if runtime.GOOS == "windows" {
		command = "C:\\TDengine\\taosadapter.exe"
	}
	return osexec.Command(command, "--port", port, "--log.level", "debug")
}

func startTaosadapter(cmd *osexec.Cmd, port string) error {
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Start()
	if err != nil {
		return err
	}
	for i := 0; i < 10; i++ {
		time.Sleep(time.Millisecond * 100)
		resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%s/-/ping", port))
		if err != nil {
			continue
		}
		_ = resp.Body.Close()
		time.Sleep(time.Second)
		return nil
	}
	if cmd.Process != nil {
		_ = cmd.Process.Signal(syscall.SIGINT)
		_, _ = cmd.Process.Wait()
		cmd.Process = nil
	}
	return errors.New("taosadapter start failed")
}

func stopTaosadapter(cmd *osexec.Cmd, port string) {
	if cmd.Process == nil {
		return
	}
	_ = cmd.Process.Signal(syscall.SIGINT)
	_, _ = cmd.Process.Wait()
	cmd.Process = nil
	for i := 0; i < 10; i++ {
		time.Sleep(time.Millisecond * 100)
		resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%s/-/ping", port))
		if err != nil {
			return
		}
		_ = resp.Body.Close()
		time.Sleep(time.Second)
	}
	panic("taosadapter stop failed")
}

func TestDisconnectNoReadTimeout(t *testing.T) {
	port := "36054"
	cmd := newTaosadapter(port)
	err := startTaosadapter(cmd, port)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		stopTaosadapter(cmd, port)
	}()
	dsn := fmt.Sprintf("%s:%s@ws(%s:%s)/", user, password, host, port)
	cfg, err := ParseDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ReadTimeout = 10 * time.Second
	cfg.WriteTimeout = 3 * time.Second
	conn, err := newTaosConn(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = conn.Close()
	}()
	stopTaosadapter(cmd, port)
	start := time.Now()
	_, err = conn.QueryContext(context.Background(), "select 1", nil)
	if assert.Error(t, err) {
		assert.NotContains(t, strings.ToLower(err.Error()), "read timeout")
	}
	assert.Less(t, time.Since(start), cfg.ReadTimeout)
}
