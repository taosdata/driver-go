package schemaless

import (
	"bytes"
	"container/list"
	"reflect"
	"testing"
	"time"

	"github.com/gorilla/websocket"
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

func TestReconnectStaleFailureDoesNotClearActiveClient(t *testing.T) {
	staleClient := client.NewClient(nil, 1)
	defer staleClient.Close()
	activeClient := client.NewClient(nil, 1)
	defer activeClient.Close()

	s := &Schemaless{
		client:              activeClient,
		sendList:            list.New(),
		closeChan:           make(chan struct{}),
		reconnectIntervalMs: 0,
		reconnectRetryCount: 1,
		dialer:              &websocket.Dialer{},
		url:                 "://invalid-url",
	}

	err := s.reconnect(staleClient)
	require.NoError(t, err)
	assert.Same(t, activeClient, s.loadClient())
	assert.True(t, activeClient.IsRunning())
}

func TestReconnectFailureClearsMatchedClient(t *testing.T) {
	failedClient := client.NewClient(nil, 1)

	s := &Schemaless{
		client:              failedClient,
		sendList:            list.New(),
		closeChan:           make(chan struct{}),
		reconnectIntervalMs: 0,
		reconnectRetryCount: 1,
		dialer:              &websocket.Dialer{},
		url:                 "://invalid-url",
	}

	err := s.reconnect(failedClient)
	require.EqualError(t, err, "reconnect failed")
	assert.Nil(t, s.loadClient())
	assert.False(t, failedClient.IsRunning())
}

func TestReconnectDeadReplacementDoesNotShortCircuit(t *testing.T) {
	failedClient := client.NewClient(nil, 1)
	deadReplacement := client.NewClient(nil, 1)
	deadReplacement.Close()

	s := &Schemaless{
		client:              deadReplacement,
		sendList:            list.New(),
		closeChan:           make(chan struct{}),
		reconnectIntervalMs: 0,
		reconnectRetryCount: 1,
		dialer:              &websocket.Dialer{},
		url:                 "://invalid-url",
	}

	err := s.reconnect(failedClient)
	require.EqualError(t, err, "reconnect failed")
	assert.Same(t, deadReplacement, s.loadClient())
	assert.False(t, deadReplacement.IsRunning())
}
