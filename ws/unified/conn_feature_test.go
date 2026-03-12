package unified

import (
	"container/list"
	"encoding/binary"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/ws/client"
	"github.com/taosdata/driver-go/v3/ws/unified/proto"
)

func TestDefaultBootstrapIncludesAuthAndSecurityFields(t *testing.T) {
	reqCh := make(chan proto.WSConnectReq, 1)
	errCh := make(chan error, 1)

	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := queryLifecycleUpgrader.Upgrade(w, r, nil)
		if err != nil {
			errCh <- err
			return
		}
		defer func() {
			_ = conn.Close()
		}()
		_, msg, err := conn.ReadMessage()
		if err != nil {
			errCh <- err
			return
		}
		var action client.WSAction
		if err = json.Unmarshal(msg, &action); err != nil {
			errCh <- err
			return
		}
		var req proto.WSConnectReq
		if err = json.Unmarshal(action.Args, &req); err != nil {
			errCh <- err
			return
		}
		reqCh <- req
		err = conn.WriteMessage(websocket.TextMessage, []byte(`{"code":0,"message":"","action":"conn","req_id":0}`))
		if err != nil {
			errCh <- err
		}
	}))
	defer s.Close()

	loc, err := time.LoadLocation("Asia/Shanghai")
	require.NoError(t, err)

	cfg := NewConfig([]string{wsEndpointFromHTTP(s.URL)})
	cfg.User = "u_test"
	cfg.Passwd = "p_test"
	cfg.DbName = "db_test"
	cfg.TotpCode = "123456"
	cfg.BearerToken = "token_test"
	cfg.Timezone = loc

	c, err := NewClient(cfg, "/ws")
	require.NoError(t, err)
	defer c.Close()
	require.NoError(t, c.Connect())

	select {
	case req := <-reqCh:
		assert.Equal(t, "u_test", req.User)
		assert.Equal(t, "p_test", req.Password)
		assert.Equal(t, "db_test", req.DB)
		assert.Equal(t, "123456", req.TOTPCode)
		assert.Equal(t, "token_test", req.BearerToken)
		assert.Equal(t, "Asia/Shanghai", req.TZ)
		assert.Equal(t, common.GetProcessName(), req.App)
		assert.Equal(t, common.GetConnectorInfo("ws"), req.Connector)
	case err = <-errCh:
		t.Fatalf("bootstrap server failed: %v", err)
	case <-time.After(time.Second):
		t.Fatal("timeout waiting bootstrap request")
	}
}

func TestDefaultBootstrapTimeout(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := queryLifecycleUpgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer func() {
			_ = conn.Close()
		}()
		_, _, _ = conn.ReadMessage()
		time.Sleep(200 * time.Millisecond)
	}))
	defer s.Close()

	cfg := NewConfig([]string{wsEndpointFromHTTP(s.URL)})
	cfg.User = "root"
	cfg.Passwd = "taosdata"
	cfg.ReadTimeout = 30 * time.Millisecond
	cfg.WriteTimeout = time.Second

	c, err := NewClient(cfg, "/ws")
	require.NoError(t, err)
	defer c.Close()

	err = c.Connect()
	require.Error(t, err)
	if !errors.Is(err, ErrConnectTimeout) {
		assert.True(t, strings.Contains(strings.ToLower(err.Error()), "timeout") || strings.Contains(strings.ToLower(err.Error()), "eof"))
	}
}

func TestDefaultBootstrapHandlesServerErrorResponse(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := queryLifecycleUpgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer func() {
			_ = conn.Close()
		}()
		_, _, _ = conn.ReadMessage()
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"code":65535,"message":"mock failure","action":"conn","req_id":0}`))
	}))
	defer s.Close()

	cfg := NewConfig([]string{wsEndpointFromHTTP(s.URL)})
	cfg.User = "root"
	cfg.Passwd = "taosdata"

	c, err := NewClient(cfg, "/ws")
	require.NoError(t, err)
	defer c.Close()

	err = c.Connect()
	require.Error(t, err)
	assert.Contains(t, strings.ToLower(err.Error()), "mock failure")
}

func TestDefaultBootstrapHandlesInvalidJSONResponse(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := queryLifecycleUpgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer func() {
			_ = conn.Close()
		}()
		_, _, _ = conn.ReadMessage()
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`not-json`))
	}))
	defer s.Close()

	cfg := NewConfig([]string{wsEndpointFromHTTP(s.URL)})
	cfg.User = "root"
	cfg.Passwd = "taosdata"

	c, err := NewClient(cfg, "/ws")
	require.NoError(t, err)
	defer c.Close()

	err = c.Connect()
	require.Error(t, err)
}

func TestDefaultBootstrapHandlesReadError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := queryLifecycleUpgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		_, _, _ = conn.ReadMessage()
		_ = conn.Close()
	}))
	defer s.Close()

	cfg := NewConfig([]string{wsEndpointFromHTTP(s.URL)})
	cfg.User = "root"
	cfg.Passwd = "taosdata"
	cfg.ReadTimeout = time.Second

	c, err := NewClient(cfg, "/ws")
	require.NoError(t, err)
	defer c.Close()

	err = c.Connect()
	require.Error(t, err)
}

func TestHandleBinaryMessageRoutesPendingRequest(t *testing.T) {
	respCh := make(chan []byte, 1)
	c := &Client{
		pendingRequests: list.New(),
	}
	c.pendingRequests.PushBack(&PendingRequest{
		reqID:   42,
		channel: respCh,
	})

	msg := make([]byte, 16)
	binary.LittleEndian.PutUint64(msg[8:16], 42)
	c.HandleBinaryMessage(msg)

	select {
	case got := <-respCh:
		assert.Equal(t, msg, got)
	case <-time.After(time.Second):
		t.Fatal("timeout waiting routed binary response")
	}
	assert.Equal(t, 0, c.pendingRequests.Len())
}

func TestHandleTextMessageRoutesPendingRequest(t *testing.T) {
	respCh := make(chan []byte, 1)
	c := &Client{
		pendingRequests: list.New(),
	}
	c.pendingRequests.PushBack(&PendingRequest{
		reqID:   66,
		channel: respCh,
	})

	msg := []byte(`{"action":"query","req_id":66,"code":0}`)
	c.handleTextMessage(msg)

	select {
	case got := <-respCh:
		assert.Equal(t, msg, got)
	case <-time.After(time.Second):
		t.Fatal("timeout waiting routed text response")
	}
	assert.Equal(t, 0, c.pendingRequests.Len())
}

func TestHandleTextMessageIgnoresInvalidPayload(t *testing.T) {
	respCh := make(chan []byte, 1)
	c := &Client{
		pendingRequests: list.New(),
	}
	c.pendingRequests.PushBack(&PendingRequest{
		reqID:   77,
		channel: respCh,
	})

	c.handleTextMessage([]byte(`{"action":"query","code":0}`))
	c.handleTextMessage([]byte(`{"action":"query"`))

	select {
	case <-respCh:
		t.Fatal("unexpected routed response for invalid text payload")
	default:
	}
	assert.Equal(t, 1, c.pendingRequests.Len())
}

func TestHandleBinaryMessageIgnoresInvalidFrame(t *testing.T) {
	respCh := make(chan []byte, 1)
	c := &Client{
		pendingRequests: list.New(),
	}
	c.pendingRequests.PushBack(&PendingRequest{
		reqID:   7,
		channel: respCh,
	})

	c.HandleBinaryMessage([]byte{1, 2, 3})

	select {
	case <-respCh:
		t.Fatal("unexpected routed response for invalid frame")
	default:
	}
	assert.Equal(t, 1, c.pendingRequests.Len())
}

func TestExtractReqIDFromBinaryMessageExtendedHeader(t *testing.T) {
	msg := make([]byte, 34)
	binary.LittleEndian.PutUint64(msg[0:8], 0xffffffffffffffff)
	binary.LittleEndian.PutUint64(msg[26:34], 99)

	reqID, err := ExtractReqIDFromBinaryMessage(msg)
	require.NoError(t, err)
	assert.Equal(t, uint64(99), reqID)
}

func TestExtractReqIDFromBinaryMessageErrors(t *testing.T) {
	_, err := ExtractReqIDFromBinaryMessage([]byte{1, 2, 3})
	require.ErrorIs(t, err, ErrBinaryMessageTooShort)

	msg := make([]byte, 20)
	binary.LittleEndian.PutUint64(msg[0:8], 0xffffffffffffffff)
	_, err = ExtractReqIDFromBinaryMessage(msg)
	require.ErrorIs(t, err, ErrBinaryMessageExtendedHeaderTooShort)
}

func TestExtractReqIDFromTextMessage(t *testing.T) {
	reqID, err := ExtractReqIDFromTextMessage([]byte(`{"code":0,"req_id":123,"message":""}`))
	require.NoError(t, err)
	assert.Equal(t, uint64(123), reqID)
}

func TestExtractReqIDFromTextMessageErrors(t *testing.T) {
	_, err := ExtractReqIDFromTextMessage([]byte(`{"code":0,"message":""}`))
	require.ErrorIs(t, err, ErrReqIDNotFound)

	_, err = ExtractReqIDFromTextMessage([]byte(`{"code":0,`))
	require.Error(t, err)
}
