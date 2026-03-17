package unified

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"strconv"
	"time"

	"github.com/gorilla/websocket"
	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/common/tdversion"
	"github.com/taosdata/driver-go/v3/ws/client"
	"github.com/taosdata/driver-go/v3/ws/unified/proto"
)

var reqIDFieldKey = []byte(`"req_id"`)

// Connect connects and initializes the client for normal connect operations.
func (c *Client) Connect() error {
	c.normalConnectLock.Lock()
	defer c.normalConnectLock.Unlock()

	c.lock.RLock()
	closed := c.closed
	connected := c.connected
	runtime := c.runtime
	c.lock.RUnlock()

	if closed {
		return ErrUnifiedClosed
	}
	if connected && runtime != nil && runtime.IsRunning() {
		return nil
	}
	// connected can become stale when runtime has been closed/replaced.
	c.lock.Lock()
	c.connected = false
	c.lock.Unlock()

	// Connect with schemaless bootstrap
	if err := c.connectWithBootstrap(c.defaultBootstrap); err != nil {
		return err
	}

	// Check if closed during connect
	if c.IsClosed() {
		return ErrUnifiedClosed
	}

	// Initialize runtime (handlers and pumps are set in connectWithCandidates)
	c.lock.Lock()
	c.connected = true
	c.lock.Unlock()
	return nil
}

// defaultBootstrap performs the normal connect handshake on a new websocket connection.
func (c *Client) defaultBootstrap(conn *websocket.Conn) error {
	// Keep legacy behavior: fail fast when server version is incompatible.
	if err := tdversion.WSCheckVersion(conn); err != nil {
		return err
	}

	tz := ""
	if c.config.Timezone != nil {
		tz = c.config.Timezone.String()
	}
	req := &proto.WSConnectReq{
		ReqID:       uint64(common.GetReqID()),
		User:        c.config.User,
		Password:    c.config.Passwd,
		DB:          c.config.DbName,
		TZ:          tz,
		TOTPCode:    c.config.TotpCode,
		BearerToken: c.config.BearerToken,
		App:         common.GetProcessName(),
		Connector:   common.GetConnectorInfo("ws"),
	}

	args, err := client.JsonI.Marshal(req)
	if err != nil {
		return err
	}

	var connectAction bytes.Buffer
	err = encodeWSActionToBuffer(&connectAction, proto.Connect, args, false)
	if err != nil {
		return err
	}

	_ = conn.SetWriteDeadline(time.Now().Add(c.config.WriteTimeout))
	err = conn.WriteMessage(websocket.TextMessage, connectAction.Bytes())
	if err != nil {
		return err
	}

	readTimeout := c.config.ReadTimeout
	if readTimeout <= 0 {
		readTimeout = c.config.MessageTimeout
	}
	if readTimeout <= 0 {
		readTimeout = common.DefaultMessageTimeout
	}
	_ = conn.SetReadDeadline(time.Now().Add(readTimeout))

	done := make(chan struct{})
	ctx, cancel := context.WithTimeout(context.Background(), readTimeout)
	defer cancel()
	var respBytes []byte
	var readErr error

	go func() {
		_, respBytes, readErr = conn.ReadMessage()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		return ErrConnectTimeout
	}
	_ = conn.SetReadDeadline(time.Time{})

	if readErr != nil {
		return readErr
	}

	var resp proto.WSConnectResp
	return decodeAndCheckJSONResponse(respBytes, &resp)
}

// handleTextMessage routes incoming text messages to pending requests by req_id.
func (c *Client) handleTextMessage(message []byte) {
	// Extract req_id from message
	reqID, err := extractReqIDFromTextMessage(message)
	if err != nil {
		// todo
		// Log or handle error - message without req_id
		return
	}

	c.handleMessage(message, reqID)
}

func (c *Client) handleBinaryMessage(message []byte) {
	// Extract req_id from message
	reqID, err := extractReqIDFromBinaryMessage(message)
	if err != nil {
		// todo
		// Log or handle error - message without req_id
		return
	}

	c.handleMessage(message, reqID)
}

func (c *Client) handleMessage(message []byte, reqID uint64) {
	req := c.removePendingRequest(reqID, nil)
	if req == nil {
		return
	}

	// Use select to avoid blocking if channel is full or closed.
	select {
	case req.channel <- message:
	default:
	}
}

// extractReqIDFromTextMessage extracts req_id from JSON text protocol message.
func extractReqIDFromTextMessage(message []byte) (uint64, error) {
	if reqID, ok := fastExtractReqIDFromTextMessage(message); ok {
		return reqID, nil
	}

	decoder := json.NewDecoder(bytes.NewReader(message))
	decoder.UseNumber()
	payload := make(map[string]interface{}, 8)
	if err := decoder.Decode(&payload); err != nil {
		return 0, err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return 0, newInvalidStateErrorf("invalid trailing data after JSON object")
		}
		return 0, err
	}
	reqIDValue, ok := payload["req_id"]
	if !ok {
		return 0, ErrReqIDNotFound
	}
	number, ok := reqIDValue.(json.Number)
	if !ok {
		return 0, newInvalidStateErrorf("req_id must be an integer number")
	}
	reqID, err := strconv.ParseUint(string(number), 10, 64)
	if err != nil {
		return 0, err
	}
	return reqID, nil
}

func fastExtractReqIDFromTextMessage(message []byte) (uint64, bool) {
	keyPos := bytes.Index(message, reqIDFieldKey)
	if keyPos < 0 {
		return 0, false
	}

	prev := keyPos - 1
	for prev >= 0 && isJSONSpace(message[prev]) {
		prev--
	}
	if prev >= 0 && message[prev] != '{' && message[prev] != ',' {
		return 0, false
	}

	i := keyPos + len(reqIDFieldKey)
	for i < len(message) && isJSONSpace(message[i]) {
		i++
	}
	if i >= len(message) || message[i] != ':' {
		return 0, false
	}
	i++
	for i < len(message) && isJSONSpace(message[i]) {
		i++
	}
	if i >= len(message) || message[i] < '0' || message[i] > '9' {
		return 0, false
	}

	var reqID uint64
	for i < len(message) && message[i] >= '0' && message[i] <= '9' {
		digit := uint64(message[i] - '0')
		if reqID > (^uint64(0)-digit)/10 {
			return 0, false
		}
		reqID = reqID*10 + digit
		i++
	}
	for i < len(message) && isJSONSpace(message[i]) {
		i++
	}
	if i >= len(message) {
		return 0, false
	}
	ch := message[i]
	if ch != ',' && ch != '}' {
		return 0, false
	}
	if ch == '}' {
		i++
		for i < len(message) && isJSONSpace(message[i]) {
			i++
		}
		if i < len(message) {
			return 0, false
		}
	}
	return reqID, true
}

func isJSONSpace(ch byte) bool {
	return ch == ' ' || ch == '\n' || ch == '\r' || ch == '\t'
}

// extractReqIDFromBinaryMessage extracts req_id from unified binary frame header.
func extractReqIDFromBinaryMessage(message []byte) (uint64, error) {
	if len(message) < 16 {
		return 0, ErrBinaryMessageTooShort
	}
	flag := binary.LittleEndian.Uint64(message[0:8])
	if flag == 0xffffffffffffffff {
		if len(message) < 34 {
			return 0, ErrBinaryMessageExtendedHeaderTooShort
		}
		return binary.LittleEndian.Uint64(message[26:34]), nil
	}
	return binary.LittleEndian.Uint64(message[8:16]), nil
}
