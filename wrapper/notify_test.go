package wrapper

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/wrapper/cgo"
)

// @author: xftan
// @date: 2023/10/13 11:28
// @description: test notify callback
func TestNotify(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}

	defer TaosClose(conn)
	defer exec(conn, "drop user t_notify")
	exec(conn, "drop user t_notify")
	err = exec(conn, "create user t_notify pass 'notify'")
	assert.NoError(t, err)
	conn2, err := TaosConnect("", "t_notify", "notify", "", 0)
	if err != nil {
		t.Error(err)
		return
	}

	defer TaosClose(conn2)
	notify := make(chan int32, 1)
	handler := cgo.NewHandle(notify)
	errCode := TaosSetNotifyCB(conn2, handler, common.TAOS_NOTIFY_PASSVER)
	if errCode != 0 {
		errStr := TaosErrorStr(nil)
		t.Error(errCode, errStr)
	}
	err = exec(conn, "alter user t_notify pass 'test'")
	assert.NoError(t, err)
	timeout, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	now := time.Now()
	select {
	case version := <-notify:
		fmt.Println(time.Now().Sub(now))
		t.Log(version)
	case <-timeout.Done():
		t.Error("wait for notify callback timeout")
	}
	{
		notify := make(chan int64, 1)
		handler := cgo.NewHandle(notify)
		errCode := TaosSetNotifyCB(conn2, handler, common.TAOS_NOTIFY_WHITELIST_VER)
		if errCode != 0 {
			errStr := TaosErrorStr(nil)
			t.Error(errCode, errStr)
		}
		err = exec(conn, "ALTER USER t_notify ADD HOST '192.168.1.98/0','192.168.1.98/32'")
		assert.NoError(t, err)
		timeout, cancel = context.WithTimeout(context.Background(), time.Second*5)
		defer cancel()
		now := time.Now()
		select {
		case version := <-notify:
			fmt.Println(time.Now().Sub(now))
			t.Log(version)
		case <-timeout.Done():
			t.Error("wait for notify callback timeout")
		}
	}
	{
		notify := make(chan struct{}, 1)
		handler := cgo.NewHandle(notify)
		errCode := TaosSetNotifyCB(conn2, handler, common.TAOS_NOTIFY_USER_DROPPED)
		if errCode != 0 {
			errStr := TaosErrorStr(nil)
			t.Error(errCode, errStr)
		}
		err = exec(conn, "drop USER t_notify")
		assert.NoError(t, err)
		timeout, cancel = context.WithTimeout(context.Background(), time.Second*5)
		defer cancel()
		now := time.Now()
		select {
		case _ = <-notify:
			fmt.Println(time.Now().Sub(now))
			t.Log("user dropped")
		case <-timeout.Done():
			t.Error("wait for notify callback timeout")
		}
	}
}
