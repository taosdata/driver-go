package unified

import (
	"bytes"
	"errors"

	"github.com/gorilla/websocket"
	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/ws/client"
	"github.com/taosdata/driver-go/v3/ws/unified/proto"
)

// Exec executes one SQL query and returns affected rows.
func (c *Client) Exec(sql string, reqID int64) (int, error) {
	queryResp, runtime, runtimeGen, err := c.queryRaw(sql, reqID)
	if err != nil {
		return 0, err
	}
	if !queryResp.IsUpdate {
		// Exec-style calls must not leak server-side result handles.
		rs := buildResultSetFromQueryResp(c, runtime, runtimeGen, queryResp)
		if rs != nil {
			_ = rs.Close()
		}
	}
	return queryResp.AffectedRows, nil
}

// Query sends one binary query message with reconnect/failover support.
// It returns nil result for update statements.
func (c *Client) Query(sql string, reqID int64) (*ResultSet, error) {
	queryResp, runtime, runtimeGen, err := c.queryRaw(sql, reqID)
	if err != nil {
		return nil, err
	}
	return buildResultSetFromQueryResp(c, runtime, runtimeGen, queryResp), nil
}

func (c *Client) queryRaw(sql string, reqID int64) (*proto.WSQueryResp, *client.Client, uint64, error) {
	if reqID == 0 {
		reqID = common.GetReqID()
	}

	payload := BuildBinaryQueryRequest(uint64(reqID), sql)
	respBytes, runtime, runtimeGen, err := c.sendQueryWithReconnect(uint64(reqID), payload)
	if err != nil {
		return nil, nil, 0, normalizeDisconnectedError(err, "query connection lost")
	}

	var queryResp proto.WSQueryResp
	err = client.JsonI.Unmarshal(respBytes, &queryResp)
	if err != nil {
		return nil, nil, 0, &Error{
			Type:    ErrorTypeProtocol,
			Message: "invalid query response",
			Cause:   err,
		}
	}
	err = client.HandleResponseError(nil, queryResp.Code, queryResp.Message)
	if err != nil {
		return nil, nil, 0, err
	}
	return &queryResp, runtime, runtimeGen, nil
}

func buildResultSetFromQueryResp(c *Client, runtime *client.Client, runtimeGen uint64, queryResp *proto.WSQueryResp) *ResultSet {
	if c == nil || queryResp == nil || queryResp.IsUpdate {
		return nil
	}
	return &ResultSet{
		client:          c,
		runtime:         runtime,
		runtimeGen:      runtimeGen,
		resultID:        queryResp.ID,
		timezone:        c.config.Timezone,
		fieldsCount:     queryResp.FieldsCount,
		fieldsNames:     append([]string(nil), queryResp.FieldsNames...),
		fieldsTypes:     append([]uint8(nil), queryResp.FieldsTypes...),
		fieldsLengths:   append([]int64(nil), queryResp.FieldsLengths...),
		fieldsPrecision: append([]int64(nil), queryResp.FieldsPrecisions...),
		fieldsScale:     append([]int64(nil), queryResp.FieldsScales...),
		precision:       queryResp.Precision,
	}
}

func (c *Client) sendQueryWithReconnect(reqID uint64, payload []byte) ([]byte, *client.Client, uint64, error) {
	runtime := c.Runtime()
	if runtime == nil {
		if c.IsClosed() {
			return nil, nil, 0, ErrUnifiedClosed
		}
		return nil, nil, 0, client.ClosedError
	}

	envelope := client.GlobalEnvelopePool.Get()
	defer client.GlobalEnvelopePool.Put(envelope)
	envelope.Type = websocket.BinaryMessage
	envelope.Msg.Reset()
	_, _ = envelope.Msg.Write(payload)

	respBytes, writeAckedToSocket, runtimeGen, err := c.sendEnvelopeWithRuntime(runtime, reqID, envelope, c.config.ReadTimeout, ErrQueryMessageTimeout)
	if err == nil {
		return respBytes, runtime, runtimeGen, nil
	}

	if c.IsClosed() {
		return nil, nil, 0, ErrUnifiedClosed
	}
	if !c.config.AutoReconnect {
		return nil, nil, 0, err
	}
	// Query replay is unsafe after websocket write-ack because execution may already be in progress.
	if writeAckedToSocket {
		return nil, nil, 0, err
	}
	if !isReconnectableError(err) {
		return nil, nil, 0, err
	}

	if err = c.reconnectWithBootstrap(c.defaultBootstrap, runtime); err != nil {
		return nil, nil, 0, err
	}

	runtime = c.Runtime()
	if runtime == nil {
		return nil, nil, 0, client.ClosedError
	}

	respBytes, _, runtimeGen, err = c.sendEnvelopeWithRuntime(runtime, reqID, envelope, c.config.ReadTimeout, ErrQueryMessageTimeout)
	if err != nil {
		return nil, nil, 0, err
	}
	return respBytes, runtime, runtimeGen, nil
}

// BuildBinaryQueryRequest builds a unified binary query request payload.
func BuildBinaryQueryRequest(reqID uint64, sql string) []byte {
	buf := bytes.NewBuffer(make([]byte, 0, 30+len(sql)))
	writeUint64(buf, reqID)
	writeUint64(buf, 0)
	writeUint64(buf, proto.BinaryQueryMessage)
	writeUint16(buf, proto.BinaryProtocolVersion1)
	writeUint32(buf, uint32(len(sql)))
	buf.WriteString(sql)
	return buf.Bytes()
}

func normalizeDisconnectedError(err error, message string) error {
	if err == nil {
		return nil
	}
	if IsConnectionRelatedError(err) {
		return err
	}
	if errors.Is(err, client.ClosedError) || isReconnectableError(err) {
		return &Error{
			Type:                   ErrorTypeClientClosed,
			Message:                message,
			Cause:                  err,
			ConnectionRelated:      true,
			ConnectionDisconnected: true,
		}
	}
	return err
}
