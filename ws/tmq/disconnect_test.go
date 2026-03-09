package tmq

import (
	"container/list"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taosdata/driver-go/v3/ws/client"
)

var disconnectTestUpgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

// Simulate an abnormal disconnect by dropping the TCP connection after the first request.
func wsDropAfterFirstTMQRequest(w http.ResponseWriter, r *http.Request) {
	conn, err := disconnectTestUpgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	_, _, err = conn.ReadMessage()
	if err != nil {
		_ = conn.Close()
		return
	}
	_ = conn.UnderlyingConn().Close()
}

func wsReplyThenCloseTMQ(w http.ResponseWriter, r *http.Request) {
	conn, err := disconnectTestUpgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	_, _, err = conn.ReadMessage()
	if err != nil {
		_ = conn.Close()
		return
	}
	err = conn.WriteMessage(websocket.TextMessage, []byte(`{"code":0,"message":"","req_id":1}`))
	if err != nil {
		_ = conn.Close()
		return
	}
	_ = conn.UnderlyingConn().Close()
}

func runTMQDisconnectScenario(t *testing.T) (time.Duration, error) {
	t.Helper()
	s := httptest.NewServer(http.HandlerFunc(wsDropAfterFirstTMQRequest))
	defer s.Close()

	ep := "ws" + strings.TrimPrefix(s.URL, "http")
	ws, _, err := websocket.DefaultDialer.Dial(ep, nil)
	require.NoError(t, err)

	cl := client.NewClient(ws, 1)
	cl.PingPeriod = time.Hour
	c := &Consumer{
		client:         cl,
		sendChanList:   list.New(),
		messageTimeout: 5 * time.Second,
		closeChan:      make(chan struct{}),
	}
	c.initClient(cl)
	defer func() {
		_ = c.Close()
	}()

	envelope := client.GlobalEnvelopePool.Get()
	defer client.GlobalEnvelopePool.Put(envelope)
	envelope.Msg.WriteString(`{"action":"poll","args":{"req_id":1}}`)

	start := time.Now()
	_, err = c.sendText(1, envelope)
	return time.Since(start), err
}

func TestTMQDisconnectOldBehavior(t *testing.T) {
	t.Skip("for old-logic AB verification only; run after stashing the fix")
	elapsed, err := runTMQDisconnectScenario(t)
	require.Error(t, err)
	t.Logf("elapsed=%s err=%v", elapsed, err)
	assert.Contains(t, strings.ToLower(err.Error()), "message timeout")
	assert.GreaterOrEqual(t, elapsed, 4*time.Second)
}

func TestTMQDisconnectFixedBehavior(t *testing.T) {
	elapsed, err := runTMQDisconnectScenario(t)
	require.Error(t, err)
	t.Logf("elapsed=%s err=%v", elapsed, err)
	assert.NotContains(t, strings.ToLower(err.Error()), "message timeout")
	assert.Less(t, elapsed, 5*time.Second)
}

func TestTMQResponseBeforeServerClose(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(wsReplyThenCloseTMQ))
	defer s.Close()

	ep := "ws" + strings.TrimPrefix(s.URL, "http")
	ws, _, err := websocket.DefaultDialer.Dial(ep, nil)
	require.NoError(t, err)

	cl := client.NewClient(ws, 1)
	c := &Consumer{
		client:         cl,
		sendChanList:   list.New(),
		messageTimeout: 5 * time.Second,
		closeChan:      make(chan struct{}),
	}
	c.initClient(cl)
	defer func() {
		_ = c.Close()
	}()

	envelope := client.GlobalEnvelopePool.Get()
	defer client.GlobalEnvelopePool.Put(envelope)
	envelope.Msg.WriteString(`{"action":"poll","args":{"req_id":1}}`)

	resp, err := c.sendText(1, envelope)
	require.NoError(t, err)
	assert.Contains(t, string(resp), `"req_id":1`)
}

func TestTMQReconnectAfterCloseReturnsClosed(t *testing.T) {
	c := &Consumer{
		reconnectIntervalMs: 1000,
		reconnectRetryCount: 3,
		closeChan:           make(chan struct{}),
	}
	close(c.closeChan)

	start := time.Now()
	err := c.reconnect(nil)
	elapsed := time.Since(start)

	require.Error(t, err)
	assert.Equal(t, ClosedErr, err)
	assert.Less(t, elapsed, 100*time.Millisecond)
}
