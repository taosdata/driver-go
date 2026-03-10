package stmt

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/taosdata/driver-go/v3/ws/client"
)

func pendingWSConnSendCount(c *WSConn) int {
	c.listLock.Lock()
	defer c.listLock.Unlock()
	return c.sendChanList.Len()
}

func TestSendCloseRemovesPendingChannel(t *testing.T) {
	cli := client.NewClient(nil, 1)
	conn := NewWSConn(cli, time.Second, time.Second)

	envelope := &client.Envelope{
		Msg:       bytes.NewBufferString(`{"action":"init"}`),
		ErrorChan: make(chan error, 1),
	}

	result := make(chan error, 1)
	go func() {
		_, err := conn.sendText(1, envelope)
		result <- err
	}()

	require.Eventually(t, func() bool {
		return pendingWSConnSendCount(conn) == 1
	}, time.Second, 10*time.Millisecond)

	close(conn.closeChan)
	envelope.ErrorChan <- nil

	select {
	case err := <-result:
		require.Error(t, err)
		assert.EqualError(t, err, "connection closed")
	case <-time.After(100 * time.Millisecond):
		t.Fatal("sendText did not return after close")
	}

	require.Eventually(t, func() bool {
		return pendingWSConnSendCount(conn) == 0
	}, time.Second, 10*time.Millisecond)
}
