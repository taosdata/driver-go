package unified

import (
	"github.com/gorilla/websocket"
	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/ws/client"
	"github.com/taosdata/driver-go/v3/ws/unified/proto"
)

// SchemalessInsert sends a schemaless insert request with automatic failover and reconnect.
func (c *Client) SchemalessInsert(lines string, protocol int, precision string, ttl int, reqID int64) error {
	if reqID == 0 {
		reqID = common.GetReqID()
	}

	req := &proto.SchemalessWriteRequest{
		ReqID:     uint64(reqID),
		Protocol:  protocol,
		Precision: precision,
		TTL:       ttl,
		Data:      lines,
	}

	args, err := client.JsonI.Marshal(req)
	if err != nil {
		return err
	}

	action := &client.WSAction{
		Action: "insert",
		Args:   args,
	}

	envelope := client.GlobalEnvelopePool.Get()
	defer client.GlobalEnvelopePool.Put(envelope)

	err = client.JsonI.NewEncoder(envelope.Msg).Encode(action)
	if err != nil {
		return err
	}

	respBytes, err := c.sendSchemalessWithReconnect(uint64(reqID), envelope)
	if err != nil {
		return err
	}

	var resp proto.SchemalessWriteResponse
	err = client.JsonI.Unmarshal(respBytes, &resp)
	return client.HandleResponseError(err, resp.Code, resp.Message)
}

// sendSchemalessWithReconnect sends a schemaless message with automatic reconnect on failure.
func (c *Client) sendSchemalessWithReconnect(reqID uint64, envelope *client.Envelope) ([]byte, error) {
	runtime := c.Runtime()
	if runtime == nil {
		if c.IsClosed() {
			return nil, ErrUnifiedClosed
		}
		return nil, client.ClosedError
	}

	envelope.Type = websocket.TextMessage
	respBytes, writeAckedToSocket, err := c.sendSchemalessWithRuntime(runtime, reqID, envelope)
	if err == nil {
		return respBytes, nil
	}

	if c.IsClosed() {
		return nil, ErrUnifiedClosed
	}
	if !c.config.AutoReconnect {
		return nil, err
	}
	// Schemaless insert is not safe to replay once the websocket write completed.
	if writeAckedToSocket {
		return nil, err
	}

	// Check if error is reconnectable
	if !isReconnectableError(err) {
		return nil, err
	}

	// Attempt reconnect with failed runtime check
	if err = c.reconnectWithBootstrap(c.defaultBootstrap, runtime); err != nil {
		return nil, err
	}

	runtime = c.Runtime()
	if runtime == nil {
		return nil, client.ClosedError
	}

	respBytes, _, err = c.sendSchemalessWithRuntime(runtime, reqID, envelope)
	if err != nil {
		return nil, err
	}

	return respBytes, nil
}

// sendSchemalessWithRuntime sends a schemaless message using the provided runtime client.
// The boolean return indicates whether websocket write has been acknowledged by WritePump.
func (c *Client) sendSchemalessWithRuntime(runtime *client.Client, reqID uint64, envelope *client.Envelope) ([]byte, bool, error) {
	respBytes, writeAcked, _, err := c.sendEnvelopeWithRuntime(runtime, reqID, envelope, c.config.ReadTimeout, ErrSchemalessMessageTimeout)
	return respBytes, writeAcked, err
}
