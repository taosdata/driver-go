package unified

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/taosdata/driver-go/v3/ws/client"
)

var reconnectLifecycleUpgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

func wsEndpointFromHTTP(serverURL string) string {
	return "ws" + strings.TrimPrefix(serverURL, "http")
}

func TestSchemalessInsertNoReplayAfterWriteAck(t *testing.T) {
	var insertCount atomic.Int32
	var connCount atomic.Int32

	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := reconnectLifecycleUpgrader.Upgrade(w, r, nil)
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
			text := string(msg)
			switch {
			case strings.Contains(text, `"action":"conn"`):
				_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"code":0,"message":"","action":"conn","req_id":0}`))
			case strings.Contains(text, `"action":"insert"`):
				if insertCount.Add(1) == 1 {
					// Disconnect after the server has read the insert, before response.
					_ = conn.UnderlyingConn().Close()
					return
				}
				_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"code":0,"message":"","action":"insert","req_id":1}`))
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

	err = c.SchemalessInsert("measurement,host=host1 field1=2i 1577837300000", 1, "ms", 0, 1)
	require.Error(t, err)
	assert.Equal(t, int32(1), insertCount.Load(), "insert must not be replayed after write ack")
	assert.Equal(t, int32(1), connCount.Load(), "must not reconnect and replay")
}

func TestSchemalessInsertRespectsAutoReconnect(t *testing.T) {
	var connCount atomic.Int32

	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := reconnectLifecycleUpgrader.Upgrade(w, r, nil)
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
			text := string(msg)
			switch {
			case strings.Contains(text, `"action":"conn"`):
				_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"code":0,"message":"","action":"conn","req_id":0}`))
			case strings.Contains(text, `"action":"insert"`):
				_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"code":0,"message":"","action":"insert","req_id":2}`))
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

	err = c.SchemalessInsert("measurement,host=host1 field1=2i 1577837300000", 1, "ms", 0, 2)
	require.Error(t, err)
	assert.ErrorIs(t, err, client.ClosedError)
	assert.Equal(t, int32(1), connCount.Load(), "auto reconnect disabled should not open new connections")
}

func TestSetErrorHandlerBeforeConnectPersistsAfterReconnect(t *testing.T) {
	var (
		connMu sync.Mutex
		conns  []*websocket.Conn
	)

	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := reconnectLifecycleUpgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		connMu.Lock()
		conns = append(conns, conn)
		connMu.Unlock()
		defer func() {
			if closeErr := conn.Close(); closeErr != nil {
				t.Logf("close websocket connection: %v", closeErr)
			}
		}()

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
	cfg.ReconnectRetryCount = 1
	cfg.ReconnectIntervalMs = 10

	c, err := NewClient(cfg, "/ws")
	require.NoError(t, err)
	defer c.Close()

	var callbackCount atomic.Int32
	c.SetErrorHandler(func(error) {
		callbackCount.Add(1)
	})

	require.NoError(t, c.Connect())
	firstRuntime := c.Runtime()
	require.NotNil(t, firstRuntime)

	var firstConn *websocket.Conn
	require.Eventually(t, func() bool {
		connMu.Lock()
		defer connMu.Unlock()
		if len(conns) < 1 {
			return false
		}
		firstConn = conns[0]
		return true
	}, time.Second, 10*time.Millisecond)

	_ = firstConn.UnderlyingConn().Close()
	require.Eventually(t, func() bool {
		return callbackCount.Load() >= 1
	}, time.Second, 10*time.Millisecond)

	require.NoError(t, c.reconnectWithBootstrap(c.defaultBootstrap, firstRuntime))

	var secondConn *websocket.Conn
	require.Eventually(t, func() bool {
		connMu.Lock()
		defer connMu.Unlock()
		if len(conns) < 2 {
			return false
		}
		secondConn = conns[1]
		return true
	}, time.Second, 10*time.Millisecond)

	_ = secondConn.UnderlyingConn().Close()
	require.Eventually(t, func() bool {
		return callbackCount.Load() >= 2
	}, time.Second, 10*time.Millisecond)
}

func TestConnectAfterCloseReturnsClosedError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := reconnectLifecycleUpgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer func() {
			if closeErr := conn.Close(); closeErr != nil {
				t.Logf("close websocket connection: %v", closeErr)
			}
		}()

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

	c, err := NewClient(cfg, "/ws")
	require.NoError(t, err)

	require.NoError(t, c.Connect())
	c.Close()

	err = c.Connect()
	require.ErrorIs(t, err, ErrUnifiedClosed)
}
