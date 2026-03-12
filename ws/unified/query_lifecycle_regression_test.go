package unified

import (
	"encoding/binary"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/taosdata/driver-go/v3/ws/client"
	"github.com/taosdata/driver-go/v3/ws/unified/proto"
)

var queryLifecycleUpgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

func binaryAction(msg []byte) uint64 {
	if len(msg) < 24 {
		return 0
	}
	return binary.LittleEndian.Uint64(msg[16:24])
}

func binaryReqID(msg []byte) uint64 {
	if len(msg) < 8 {
		return 0
	}
	return binary.LittleEndian.Uint64(msg[0:8])
}

func writeMockQueryResponse(conn *websocket.Conn, reqID uint64, resultID uint64, isUpdate bool) error {
	resp := fmt.Sprintf(
		`{"code":0,"message":"","action":"query","req_id":%d,"id":%d,"is_update":%t,"affected_rows":1,"fields_count":1,"fields_names":["v"],"fields_types":[4],"fields_lengths":[4],"fields_precisions":[0],"fields_scales":[0],"precision":0}`,
		reqID, resultID, isUpdate,
	)
	return conn.WriteMessage(websocket.TextMessage, []byte(resp))
}

func TestQueryNoReplayAfterWriteAck(t *testing.T) {
	var connCount atomic.Int32
	var queryCount atomic.Int32

	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := queryLifecycleUpgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer func() {
			if closeErr := conn.Close(); closeErr != nil {
				t.Logf("close websocket connection: %v", closeErr)
			}
		}()
		connCount.Add(1)

		for {
			mt, msg, err := conn.ReadMessage()
			if err != nil {
				return
			}
			switch {
			case mt == websocket.TextMessage && strings.Contains(string(msg), `"action":"conn"`):
				_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"code":0,"message":"","action":"conn","req_id":0}`))
			case mt == websocket.BinaryMessage && binaryAction(msg) == proto.BinaryQueryMessage:
				queryCount.Add(1)
				_ = conn.UnderlyingConn().Close()
				return
			}
		}
	}))
	defer s.Close()

	cfg := NewConfig([]string{wsEndpointFromHTTP(s.URL)})
	cfg.User = "root"
	cfg.Passwd = "taosdata"
	cfg.AutoReconnect = true
	cfg.ReconnectRetryCount = 1
	cfg.ReconnectIntervalMs = 10

	c, err := NewClient(cfg, "/ws")
	require.NoError(t, err)
	defer c.Close()

	require.NoError(t, c.Connect())

	_, err = c.Query("select 1", 1)
	require.Error(t, err)
	assert.Equal(t, int32(1), queryCount.Load(), "query must not be replayed after write ack")
	assert.Equal(t, int32(1), connCount.Load(), "must not reconnect after write-acked query")
}

func TestQueryRespectsAutoReconnect(t *testing.T) {
	var connCount atomic.Int32

	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := queryLifecycleUpgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer func() {
			if closeErr := conn.Close(); closeErr != nil {
				t.Logf("close websocket connection: %v", closeErr)
			}
		}()
		connCount.Add(1)

		for {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				return
			}
			if strings.Contains(string(msg), `"action":"conn"`) {
				_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"code":0,"message":"","action":"conn","req_id":0}`))
			}
		}
	}))
	defer s.Close()

	cfg := NewConfig([]string{wsEndpointFromHTTP(s.URL)})
	cfg.User = "root"
	cfg.Passwd = "taosdata"
	cfg.AutoReconnect = false
	cfg.ReconnectRetryCount = 1
	cfg.ReconnectIntervalMs = 10

	c, err := NewClient(cfg, "/ws")
	require.NoError(t, err)
	defer c.Close()

	require.NoError(t, c.Connect())

	runtime := c.Runtime()
	require.NotNil(t, runtime)
	runtime.Close()

	_, err = c.Query("select 1", 2)
	require.Error(t, err)
	assert.ErrorIs(t, err, client.ClosedError)
	assert.True(t, IsConnectionDisconnectedError(err))
	assert.Equal(t, int32(1), connCount.Load(), "auto reconnect disabled should not open new connections")
}

func TestQueryResultFetchNoReconnectAfterDisconnect(t *testing.T) {
	var connCount atomic.Int32
	var fetchCount atomic.Int32
	var queryCount atomic.Int32

	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := queryLifecycleUpgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer func() {
			if closeErr := conn.Close(); closeErr != nil {
				t.Logf("close websocket connection: %v", closeErr)
			}
		}()
		connCount.Add(1)

		for {
			mt, msg, err := conn.ReadMessage()
			if err != nil {
				return
			}
			switch {
			case mt == websocket.TextMessage && strings.Contains(string(msg), `"action":"conn"`):
				_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"code":0,"message":"","action":"conn","req_id":0}`))
			case mt == websocket.BinaryMessage && binaryAction(msg) == proto.BinaryQueryMessage:
				queryCount.Add(1)
				_ = writeMockQueryResponse(conn, binaryReqID(msg), 99, false)
			case mt == websocket.BinaryMessage && binaryAction(msg) == proto.FetchRawBlockMessage:
				fetchCount.Add(1)
				_ = conn.UnderlyingConn().Close()
				return
			}
		}
	}))
	defer s.Close()

	cfg := NewConfig([]string{wsEndpointFromHTTP(s.URL)})
	cfg.User = "root"
	cfg.Passwd = "taosdata"
	cfg.AutoReconnect = true
	cfg.ReconnectRetryCount = 1
	cfg.ReconnectIntervalMs = 10
	cfg.ReadTimeout = 3 * time.Second

	c, err := NewClient(cfg, "/ws")
	require.NoError(t, err)
	defer c.Close()

	require.NoError(t, c.Connect())

	result, err := c.Query("select 1", 3)
	require.NoError(t, err)
	require.NotNil(t, result)

	start := time.Now()
	_, _, err = result.FetchRawBlock(4)
	elapsed := time.Since(start)
	require.Error(t, err)
	assert.True(t, IsConnectionDisconnectedError(err), "fetch should report disconnected result-connection")
	assert.Equal(t, int32(1), fetchCount.Load())
	assert.Equal(t, int32(1), connCount.Load(), "fetch must not trigger reconnect")
	assert.Less(t, elapsed, 2*time.Second, "disconnect should be sensed quickly")

	// Subsequent new query is stateless and should trigger reconnect successfully.
	_, err = c.Query("select 1", 5)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, connCount.Load(), int32(2))
	assert.GreaterOrEqual(t, queryCount.Load(), int32(2))
}
