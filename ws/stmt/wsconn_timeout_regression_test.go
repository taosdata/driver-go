package stmt

import (
	"errors"
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

var timeoutRegressionUpgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

// Simulate an abnormal disconnect: close underlying TCP without sending WS close frame.
func wsDropAfterFirstRequest(w http.ResponseWriter, r *http.Request) {
	conn, err := timeoutRegressionUpgrader.Upgrade(w, r, nil)
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

func wsReplyThenClose(w http.ResponseWriter, r *http.Request) {
	conn, err := timeoutRegressionUpgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	_, _, err = conn.ReadMessage()
	if err != nil {
		_ = conn.Close()
		return
	}
	err = conn.WriteMessage(websocket.TextMessage, []byte(`{"code":0,"message":"","action":"init","req_id":1}`))
	if err != nil {
		_ = conn.Close()
		return
	}
	_ = conn.UnderlyingConn().Close()
}

func runStmtDisconnectScenario(t *testing.T) (time.Duration, error) {
	t.Helper()
	s := httptest.NewServer(http.HandlerFunc(wsDropAfterFirstRequest))
	defer s.Close()

	ep := "ws" + strings.TrimPrefix(s.URL, "http")
	ws, _, err := websocket.DefaultDialer.Dial(ep, nil)
	require.NoError(t, err)

	cl := client.NewClient(ws, 1)
	cl.PingPeriod = time.Hour
	conn := NewWSConn(cl, 10*time.Second, 5*time.Second)
	conn.initClient()
	defer conn.Close()

	envelope := cl.GetEnvelope()
	defer cl.PutEnvelope(envelope)
	envelope.Msg.WriteString(`{"action":"init","args":{"req_id":1}}`)

	start := time.Now()
	_, err = conn.sendText(1, envelope)
	return time.Since(start), err
}

func TestStmtDisconnectOldBehavior(t *testing.T) {
	t.Skip("for old-logic AB verification only; run after stashing the fix")
	elapsed, err := runStmtDisconnectScenario(t)
	require.Error(t, err)
	t.Logf("elapsed=%s err=%v", elapsed, err)
	assert.Contains(t, strings.ToLower(err.Error()), "message timeout")
	assert.GreaterOrEqual(t, elapsed, 4*time.Second)
}

func TestStmtDisconnectFixedBehavior(t *testing.T) {
	elapsed, err := runStmtDisconnectScenario(t)
	require.Error(t, err)
	t.Logf("elapsed=%s err=%v", elapsed, err)
	assert.True(t, errors.Is(err, client.ClosedError))
	assert.NotContains(t, strings.ToLower(err.Error()), "message timeout")
	assert.Less(t, elapsed, 5*time.Second)
}

func TestStmtResponseBeforeServerClose(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(wsReplyThenClose))
	defer s.Close()

	ep := "ws" + strings.TrimPrefix(s.URL, "http")
	ws, _, err := websocket.DefaultDialer.Dial(ep, nil)
	require.NoError(t, err)

	cl := client.NewClient(ws, 1)
	conn := NewWSConn(cl, 10*time.Second, 5*time.Second)
	conn.initClient()
	defer conn.Close()

	envelope := cl.GetEnvelope()
	defer cl.PutEnvelope(envelope)
	envelope.Msg.WriteString(`{"action":"init","args":{"req_id":1}}`)

	resp, err := conn.sendText(1, envelope)
	require.NoError(t, err)
	assert.Contains(t, string(resp), `"req_id":1`)
}
