package unified

import (
	"github.com/gorilla/websocket"
	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/ws/client"
	"github.com/taosdata/driver-go/v3/ws/unified/proto"
)

// SchemalessInsert sends a schemaless insert request with automatic failover and reconnect.
// tableNameKey is required. Pass "" when the protocol does not use table name key.
func (c *Client) SchemalessInsert(reqID int64, lines string, protocol int, precision string, ttl int, tableNameKey string) error {
	if reqID == 0 {
		reqID = common.GetReqID()
	}

	req := &proto.SchemalessWriteRequest{
		ReqID:        uint64(reqID),
		Protocol:     protocol,
		Precision:    precision,
		TTL:          ttl,
		Data:         lines,
		TableNameKey: tableNameKey,
	}

	args, err := client.JsonI.Marshal(req)
	if err != nil {
		return err
	}

	envelope := client.GlobalEnvelopePool.Get()
	defer client.GlobalEnvelopePool.Put(envelope)
	envelope.Msg.Reset()
	err = encodeWSActionToBuffer(envelope.Msg, proto.SchemalessWrite, args, true)
	if err != nil {
		return err
	}

	respBytes, err := c.sendSchemalessWithReconnect(uint64(reqID), envelope)
	if err != nil {
		return err
	}

	var resp proto.SchemalessWriteResponse
	return decodeAndCheckJSONResponse(respBytes, &resp)
}

// sendSchemalessWithReconnect sends a schemaless message with automatic reconnect on failure.
func (c *Client) sendSchemalessWithReconnect(reqID uint64, envelope *client.Envelope) ([]byte, error) {
	runtime, err := c.runtimeOrError()
	if err != nil {
		return nil, err
	}

	envelope.Type = websocket.TextMessage
	send := func(rt *client.Client) ([]byte, bool, uint64, error) {
		respBytes, writeAcked, err := c.sendSchemalessWithRuntime(rt, reqID, envelope)
		return respBytes, writeAcked, 0, err
	}
	respBytes, _, _, err := c.sendWithReconnect(runtime, send)
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
