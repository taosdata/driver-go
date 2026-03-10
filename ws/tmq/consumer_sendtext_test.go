package tmq

import (
	"bytes"
	"container/list"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/gorilla/websocket"
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

func pendingConsumerSendCount(c *Consumer) int {
	c.listLock.Lock()
	defer c.listLock.Unlock()
	return c.sendChanList.Len()
}

func TestSendTextCloseRemovesPendingChannel(t *testing.T) {
	cli := client.NewClient(nil, 1)
	consumer := &Consumer{
		client:         cli,
		sendChanList:   list.New(),
		messageTimeout: time.Second,
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
		return pendingConsumerSendCount(consumer) == 1
	}, time.Second, 10*time.Millisecond)

	close(consumer.closeChan)
	envelope.ErrorChan <- nil

	select {
	case err := <-result:
		require.Error(t, err)
		assert.Equal(t, ClosedErr, err)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("sendText did not return after close")
	}

	require.Eventually(t, func() bool {
		return pendingConsumerSendCount(consumer) == 0
	}, time.Second, 10*time.Millisecond)
}

func TestSendTextUserCloseAlwaysReturnsClosedErr(t *testing.T) {
	for i := 0; i < 20; i++ {
		cli := client.NewClient(nil, 1)
		consumer := &Consumer{
			client:         cli,
			sendChanList:   list.New(),
			messageTimeout: time.Second,
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
			return pendingConsumerSendCount(consumer) == 1
		}, time.Second, 10*time.Millisecond)

		require.NoError(t, consumer.Close())
		envelope.ErrorChan <- nil

		select {
		case err := <-result:
			require.Error(t, err)
			assert.Equal(t, ClosedErr, err)
			assert.False(t, errors.Is(err, client.ClosedError))
		case <-time.After(100 * time.Millisecond):
			t.Fatal("sendText did not return after user close")
		}
	}
}

func TestReconnectStaleFailureDoesNotClearActiveClient(t *testing.T) {
	staleClient := client.NewClient(nil, 1)
	defer staleClient.Close()
	activeClient := client.NewClient(nil, 1)
	defer activeClient.Close()

	consumer := &Consumer{
		client:              activeClient,
		reconnectIntervalMs: 0,
		reconnectRetryCount: 1,
		closeChan:           make(chan struct{}),
		dialer:              &websocket.Dialer{},
		url:                 "://invalid-url",
	}

	err := consumer.reconnect(staleClient)
	require.NoError(t, err)
	assert.Same(t, activeClient, consumer.loadClient())
	assert.True(t, activeClient.IsRunning())
}

func TestReconnectFailureClearsMatchedClient(t *testing.T) {
	failedClient := client.NewClient(nil, 1)

	consumer := &Consumer{
		client:              failedClient,
		reconnectIntervalMs: 0,
		reconnectRetryCount: 1,
		closeChan:           make(chan struct{}),
		dialer:              &websocket.Dialer{},
		url:                 "://invalid-url",
	}

	err := consumer.reconnect(failedClient)
	require.EqualError(t, err, "reconnect failed")
	assert.Nil(t, consumer.loadClient())
	assert.False(t, failedClient.IsRunning())
}

func TestReconnectDeadReplacementDoesNotShortCircuit(t *testing.T) {
	failedClient := client.NewClient(nil, 1)
	deadReplacement := client.NewClient(nil, 1)
	deadReplacement.Close()

	consumer := &Consumer{
		client:              deadReplacement,
		reconnectIntervalMs: 0,
		reconnectRetryCount: 1,
		closeChan:           make(chan struct{}),
		dialer:              &websocket.Dialer{},
		url:                 "://invalid-url",
	}

	err := consumer.reconnect(failedClient)
	require.EqualError(t, err, "reconnect failed")
	assert.Same(t, deadReplacement, consumer.loadClient())
	assert.False(t, deadReplacement.IsRunning())
}
