package schemaless

import (
	"container/list"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/taosdata/driver-go/v3/ws/client"
)

func TestSchemalessSendWithNilClient(t *testing.T) {
	sl := &Schemaless{
		sendList:    list.New(),
		readTimeout: time.Second,
		closeChan:   make(chan struct{}),
	}

	envelope := client.GlobalEnvelopePool.Get()
	defer client.GlobalEnvelopePool.Put(envelope)
	envelope.Msg.WriteString(`{"action":"insert","args":{"req_id":1}}`)

	_, err := sl.sendText(1, envelope)
	assert.Equal(t, client.ClosedError, err)
}

func TestSchemalessSendWithNilClientAfterClose(t *testing.T) {
	sl := &Schemaless{
		sendList:    list.New(),
		readTimeout: time.Second,
		closeChan:   make(chan struct{}),
	}
	close(sl.closeChan)

	envelope := client.GlobalEnvelopePool.Get()
	defer client.GlobalEnvelopePool.Put(envelope)
	envelope.Msg.WriteString(`{"action":"insert","args":{"req_id":1}}`)

	_, err := sl.sendText(1, envelope)
	assert.Equal(t, SchemalessClosedErr, err)
	assert.False(t, errors.Is(err, client.ClosedError))
}

func TestSchemalessInsertAfterCloseDoesNotReconnect(t *testing.T) {
	sl := &Schemaless{
		sendList:            list.New(),
		readTimeout:         time.Second,
		closeChan:           make(chan struct{}),
		autoReconnect:       true,
		reconnectRetryCount: 0,
	}
	close(sl.closeChan)

	err := sl.Insert(
		"measurement,host=host1 field1=2i,field2=2.0 1577837300000",
		InfluxDBLineProtocol,
		"ms",
		0,
		1,
	)
	assert.Equal(t, SchemalessClosedErr, err)
	assert.NotContains(t, err.Error(), "reconnect failed")
}
