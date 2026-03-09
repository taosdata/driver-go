package tmq

import (
	"bytes"
	"container/list"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/taosdata/driver-go/v3/ws/client"
)

func TestSendTextUsesStableClientDuringReconnect(t *testing.T) {
	oldClient := client.NewClient(nil, 1)
	newClient := client.NewClient(nil, 1)
	defer newClient.Close()

	consumer := &Consumer{
		client:         oldClient,
		sendChanList:   list.New(),
		messageTimeout: 500 * time.Millisecond,
		closeChan:      make(chan struct{}),
	}

	envelope := &client.Envelope{
		Msg:       bytes.NewBufferString(`{"action":"poll"}`),
		ErrorChan: make(chan error, 1),
	}

	result := make(chan error, 1)
	go func() {
		_, err := consumer.sendText(1, envelope)
		result <- err
	}()

	require.Eventually(t, func() bool {
		return clientSendChanLen(oldClient) == 1
	}, time.Second, 10*time.Millisecond)

	replacedClient, ok := consumer.replaceClient(newClient)
	require.True(t, ok)
	require.Same(t, oldClient, replacedClient)
	envelope.ErrorChan <- nil
	oldClient.Close()

	select {
	case err := <-result:
		require.Error(t, err)
		assert.ErrorIs(t, err, ClosedErr)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("sendText did not return after the original client closed")
	}
}

func clientSendChanLen(c *client.Client) int {
	return reflect.ValueOf(c).Elem().FieldByName("sendChan").Len()
}
