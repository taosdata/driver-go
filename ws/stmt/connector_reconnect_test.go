package stmt

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/taosdata/driver-go/v3/ws/client"
)

func newTestWSConn() *WSConn {
	cl := client.NewClient(nil, 1)
	return NewWSConn(cl, time.Second, time.Second)
}

func TestReconnectHealthyReplacementShortCircuit(t *testing.T) {
	failed := newTestWSConn()
	active := newTestWSConn()
	defer failed.Close()
	defer active.Close()

	connector := &Connector{
		client:              active,
		reconnectRetryCount: 0,
	}

	err := connector.reconnectWithFailed(failed)
	assert.NoError(t, err)
	assert.True(t, isWSConnRunning(active))
}

func TestReconnectDeadReplacementDoesNotShortCircuit(t *testing.T) {
	failed := newTestWSConn()
	deadReplacement := newTestWSConn()
	defer failed.Close()
	deadReplacement.Close()

	connector := &Connector{
		client:              deadReplacement,
		reconnectRetryCount: 0,
	}

	err := connector.reconnectWithFailed(failed)
	assert.EqualError(t, err, "reconnect failed")
	assert.False(t, isWSConnRunning(deadReplacement))
}

func TestReconnectFailureClosesMatchedFailedConn(t *testing.T) {
	failed := newTestWSConn()
	connector := &Connector{
		client:              failed,
		reconnectRetryCount: 0,
	}

	err := connector.reconnectWithFailed(failed)
	assert.EqualError(t, err, "reconnect failed")
	assert.False(t, isWSConnRunning(failed))
}

func TestReconnectFailureDoesNotCloseActiveReplacement(t *testing.T) {
	failed := newTestWSConn()
	failed.Close()
	active := newTestWSConn()
	defer active.Close()

	connector := &Connector{
		client:              active,
		reconnectRetryCount: 0,
	}

	err := connector.reconnectWithFailed(failed)
	assert.NoError(t, err)
	assert.True(t, isWSConnRunning(active))
}
