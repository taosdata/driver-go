package unified

import (
	"github.com/gorilla/websocket"
	"github.com/taosdata/driver-go/v3/ws/client"
)

// Ping writes one websocket ping frame on the active runtime connection.
func (c *Client) Ping() error {
	runtime := c.Runtime()
	if runtime == nil {
		if c.IsClosed() {
			return ErrUnifiedClosed
		}
		return client.ClosedError
	}

	envelope := client.GlobalEnvelopePool.Get()
	defer client.GlobalEnvelopePool.Put(envelope)
	envelope.Type = websocket.PingMessage
	envelope.Msg.Reset()

	err := c.sendEnvelopeNoResponse(runtime, envelope)
	return normalizeDisconnectedError(err, "ping connection lost")
}
