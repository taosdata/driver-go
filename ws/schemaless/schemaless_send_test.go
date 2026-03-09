package schemaless

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

func TestSendUsesStableClientDuringReconnect(t *testing.T) {
	oldClient := client.NewClient(nil, 1)
	newClient := client.NewClient(nil, 1)
	defer newClient.Close()

	s := &Schemaless{
		client:      oldClient,
		sendList:    list.New(),
		readTimeout: 500 * time.Millisecond,
		closeChan:   make(chan struct{}),
	}

	envelope := &client.Envelope{
		Msg:       bytes.NewBufferString(`{"action":"insert"}`),
		ErrorChan: make(chan error, 1),
	}

	result := make(chan error, 1)
	go func() {
		_, err := s.send(1, envelope)
		result <- err
	}()

	require.Eventually(t, func() bool {
		return clientSendChanLen(oldClient) == 1
	}, time.Second, 10*time.Millisecond)

	replacedClient, ok := s.replaceClient(newClient)
	require.True(t, ok)
	require.Same(t, oldClient, replacedClient)
	envelope.ErrorChan <- nil
	oldClient.Close()

	select {
	case err := <-result:
		require.Error(t, err)
		assert.ErrorIs(t, err, client.ClosedError)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("send did not return after the original client closed")
	}
}

func clientSendChanLen(c *client.Client) int {
	return reflect.ValueOf(c).Elem().FieldByName("sendChan").Len()
}
