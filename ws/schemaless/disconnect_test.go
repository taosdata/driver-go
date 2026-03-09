package schemaless

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
func wsDropAfterFirstSchemalessRequest(w http.ResponseWriter, r *http.Request) {
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

func wsReplyThenCloseSchemaless(w http.ResponseWriter, r *http.Request) {
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

func runSchemalessDisconnectScenario(t *testing.T) (time.Duration, error) {
	t.Helper()
	s := httptest.NewServer(http.HandlerFunc(wsDropAfterFirstSchemalessRequest))
	defer s.Close()

	ep := "ws" + strings.TrimPrefix(s.URL, "http")
	ws, _, err := websocket.DefaultDialer.Dial(ep, nil)
	require.NoError(t, err)

	cl := client.NewClient(ws, 1)
	cl.PingPeriod = time.Hour
	sl := &Schemaless{
		client:       cl,
		sendList:     list.New(),
		readTimeout:  5 * time.Second,
		writeTimeout: 10 * time.Second,
		closeChan:    make(chan struct{}),
	}
	sl.initClient(cl)
	defer sl.Close()

	envelope := client.GlobalEnvelopePool.Get()
	defer client.GlobalEnvelopePool.Put(envelope)
	envelope.Msg.WriteString(`{"action":"insert","args":{"req_id":1}}`)

	start := time.Now()
	_, err = sl.sendText(1, envelope)
	return time.Since(start), err
}

func TestSchemalessDisconnectOldBehavior(t *testing.T) {
	t.Skip("for old-logic AB verification only; run after stashing the fix")
	elapsed, err := runSchemalessDisconnectScenario(t)
	require.Error(t, err)
	t.Logf("elapsed=%s err=%v", elapsed, err)
	assert.Contains(t, strings.ToLower(err.Error()), "message timeout")
	assert.GreaterOrEqual(t, elapsed, 4*time.Second)
}

func TestSchemalessDisconnectFixedBehavior(t *testing.T) {
	elapsed, err := runSchemalessDisconnectScenario(t)
	require.Error(t, err)
	t.Logf("elapsed=%s err=%v", elapsed, err)
	assert.NotContains(t, strings.ToLower(err.Error()), "message timeout")
	assert.Less(t, elapsed, 5*time.Second)
}

func TestSchemalessResponseBeforeServerClose(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(wsReplyThenCloseSchemaless))
	defer s.Close()

	ep := "ws" + strings.TrimPrefix(s.URL, "http")
	ws, _, err := websocket.DefaultDialer.Dial(ep, nil)
	require.NoError(t, err)

	cl := client.NewClient(ws, 1)
	sl := &Schemaless{
		client:       cl,
		sendList:     list.New(),
		readTimeout:  5 * time.Second,
		writeTimeout: 10 * time.Second,
		closeChan:    make(chan struct{}),
	}
	sl.initClient(cl)
	defer sl.Close()

	envelope := client.GlobalEnvelopePool.Get()
	defer client.GlobalEnvelopePool.Put(envelope)
	envelope.Msg.WriteString(`{"action":"insert","args":{"req_id":1}}`)

	resp, err := sl.sendText(1, envelope)
	require.NoError(t, err)
	assert.Contains(t, string(resp), `"req_id":1`)
}
