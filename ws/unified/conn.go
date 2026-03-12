package unified

import (
	"container/list"
	"context"
	"encoding/binary"
	"time"

	"github.com/gorilla/websocket"
	jsoniter "github.com/json-iterator/go"
	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/ws/client"
	"github.com/taosdata/driver-go/v3/ws/unified/proto"
)

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
	if err := c.ConnectWithBootstrap(c.defaultBootstrap); err != nil {
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

	action := &client.WSAction{
		Action: "conn",
		Args:   args,
	}

	connectAction, err := client.JsonI.Marshal(action)
	if err != nil {
		return err
	}

	_ = conn.SetWriteDeadline(time.Now().Add(c.config.WriteTimeout))
	err = conn.WriteMessage(websocket.TextMessage, connectAction)
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
	err = client.JsonI.Unmarshal(respBytes, &resp)
	return client.HandleResponseError(err, resp.Code, resp.Message)
}

// handleTextMessage routes incoming text messages to pending requests by req_id.
func (c *Client) handleTextMessage(message []byte) {
	// Extract req_id from message
	reqID, err := ExtractReqIDFromTextMessage(message)
	if err != nil {
		// todo
		// Log or handle error - message without req_id
		return
	}

	c.handleMessage(message, reqID)
}

func (c *Client) HandleBinaryMessage(message []byte) {
	// Extract req_id from message
	reqID, err := ExtractReqIDFromBinaryMessage(message)
	if err != nil {
		// todo
		// Log or handle error - message without req_id
		return
	}

	c.handleMessage(message, reqID)
}

func (c *Client) handleMessage(message []byte, reqID uint64) {
	// Route to pending request using list
	c.pendingLock.Lock()
	element := c.findPendingRequest(reqID)
	if element != nil {
		c.pendingRequests.Remove(element)
		ch := element.Value.(*PendingRequest).channel
		c.pendingLock.Unlock()

		// Use select to avoid blocking if channel is full or closed
		select {
		case ch <- message:
		default:
			// Channel full or closed, discard message
		}
		return
	}
	c.pendingLock.Unlock()
}

// findPendingRequest finds a pending request by req_id.
func (c *Client) findPendingRequest(reqID uint64) *list.Element {
	for e := c.pendingRequests.Front(); e != nil; e = e.Next() {
		if e.Value.(*PendingRequest).reqID == reqID {
			return e
		}
	}
	return nil
}

// ExtractReqIDFromTextMessage extracts req_id from JSON text protocol message.
func ExtractReqIDFromTextMessage(message []byte) (uint64, error) {
	iter := jsoniter.ConfigCompatibleWithStandardLibrary.BorrowIterator(message)
	var reqID uint64
	var seenReqID bool
	iter.ReadObjectCB(func(iter *jsoniter.Iterator, field string) bool {
		if field == "req_id" {
			reqID = iter.ReadUint64()
			seenReqID = true
			return false
		}
		iter.Skip()
		return iter.Error == nil
	})
	jsoniter.ConfigCompatibleWithStandardLibrary.ReturnIterator(iter)
	if iter.Error != nil {
		return 0, iter.Error
	}
	if !seenReqID {
		return 0, ErrReqIDNotFound
	}
	return reqID, nil
}

// ExtractReqIDFromBinaryMessage extracts req_id from unified binary frame header.
func ExtractReqIDFromBinaryMessage(message []byte) (uint64, error) {
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
