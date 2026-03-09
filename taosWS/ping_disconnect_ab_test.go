package taosWS

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taosdata/driver-go/v3/common"
)

var pingDisconnectABUpgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

type failingWriteConn struct {
	net.Conn
	failWrites atomic.Bool
}

func (c *failingWriteConn) Write(p []byte) (int, error) {
	if c.failWrites.Load() {
		return 0, io.ErrClosedPipe
	}
	return c.Conn.Write(p)
}

func taosWSSilentAfterQuery(w http.ResponseWriter, r *http.Request, queryRead chan<- struct{}) {
	conn, err := pingDisconnectABUpgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer func() {
		_ = conn.Close()
	}()

	_, _, err = conn.ReadMessage()
	if err != nil {
		return
	}
	versionResp := map[string]interface{}{
		"code":    0,
		"message": "",
		"action":  "version",
		"version": "3.3.6.0",
		"timing":  0,
	}
	versionRespBytes, err := json.Marshal(versionResp)
	if err != nil {
		return
	}
	err = conn.WriteMessage(websocket.TextMessage, versionRespBytes)
	if err != nil {
		return
	}

	_, connectPayload, err := conn.ReadMessage()
	if err != nil {
		return
	}
	var connectAction WSAction
	err = json.Unmarshal(connectPayload, &connectAction)
	if err != nil {
		return
	}
	var connectReq WSConnectReq
	err = json.Unmarshal(connectAction.Args, &connectReq)
	if err != nil {
		return
	}
	connectResp := &WSConnectResp{
		BaseResp: BaseResp{
			Code:   0,
			Action: WSConnect,
			ReqID:  connectReq.ReqID,
		},
	}
	connectRespBytes, err := json.Marshal(connectResp)
	if err != nil {
		return
	}
	err = conn.WriteMessage(websocket.TextMessage, connectRespBytes)
	if err != nil {
		return
	}

	_, _, err = conn.ReadMessage()
	if err != nil {
		return
	}
	close(queryRead)
	time.Sleep(10 * time.Second)
}

func runPingFailureWhileWaitingScenario(t *testing.T) (time.Duration, error, error) {
	t.Helper()
	queryRead := make(chan struct{})
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		taosWSSilentAfterQuery(w, r, queryRead)
	})
	s := httptest.NewServer(mux)
	defer s.Close()

	u, err := url.Parse(s.URL)
	require.NoError(t, err)
	host, portStr, err := net.SplitHostPort(u.Host)
	require.NoError(t, err)
	port, err := strconv.Atoi(portStr)
	require.NoError(t, err)

	origDialer := common.DefaultDialer
	defer func() {
		common.DefaultDialer = origDialer
	}()

	var wrappedConn *failingWriteConn
	common.DefaultDialer.NetDialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := (&net.Dialer{}).DialContext(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		wrappedConn = &failingWriteConn{Conn: conn}
		return wrappedConn, nil
	}

	cfg := NewConfig()
	cfg.User = "root"
	cfg.Passwd = "taosdata"
	cfg.Net = "ws"
	cfg.Addr = host
	cfg.Port = port
	cfg.ReadTimeout = 5 * time.Second
	cfg.WriteTimeout = 10 * time.Second

	conn, err := newTaosConn(cfg)
	require.NoError(t, err)
	defer func() {
		_ = conn.Close()
	}()
	require.NotNil(t, wrappedConn)

	type queryResult struct {
		elapsed time.Duration
		err     error
	}
	queryDone := make(chan queryResult, 1)
	go func() {
		start := time.Now()
		_, queryErr := conn.QueryContext(context.Background(), "select 1", nil)
		queryDone <- queryResult{
			elapsed: time.Since(start),
			err:     queryErr,
		}
	}()

	select {
	case <-queryRead:
	case <-time.After(2 * time.Second):
		t.Fatal("query was not sent to server")
	}

	time.Sleep(100 * time.Millisecond)
	wrappedConn.failWrites.Store(true)
	pingErr := conn.writePing()
	require.Error(t, pingErr)

	result := <-queryDone
	return result.elapsed, pingErr, result.err
}

func TestPingFailureWhileWaitingOldBehavior(t *testing.T) {
	t.Skip("for old-logic AB verification only; run after stashing the fix")
	elapsed, pingErr, queryErr := runPingFailureWhileWaitingScenario(t)
	require.Error(t, queryErr)
	t.Logf("elapsed=%s pingErr=%v queryErr=%v", elapsed, pingErr, queryErr)
	assert.Contains(t, strings.ToLower(queryErr.Error()), "read timeout")
	assert.GreaterOrEqual(t, elapsed, 4*time.Second)
}

func TestPingFailureWhileWaitingFixedBehavior(t *testing.T) {
	elapsed, pingErr, queryErr := runPingFailureWhileWaitingScenario(t)
	require.Error(t, queryErr)
	t.Logf("elapsed=%s pingErr=%v queryErr=%v", elapsed, pingErr, queryErr)
	assert.NotContains(t, strings.ToLower(queryErr.Error()), "read timeout")
	assert.Less(t, elapsed, 2*time.Second)
}

func TestReadResponsePrefersQueuedMessageOverClose(t *testing.T) {
	tc := &taosConn{
		messageChan:  make(chan *message, 1),
		messageErrCh: make(chan struct{}),
		closeCh:      make(chan struct{}),
	}
	tc.messageChan <- &message{mt: websocket.TextMessage, message: []byte("ok")}
	close(tc.messageErrCh)
	close(tc.closeCh)

	mt, resp, err := tc.readResponse()
	require.NoError(t, err)
	assert.Equal(t, websocket.TextMessage, mt)
	assert.Equal(t, []byte("ok"), resp)
}

func TestEnqueueMessageUnblocksOnCloseWhenQueueFull(t *testing.T) {
	tc := &taosConn{
		messageChan: make(chan *message, 1),
		closeCh:     make(chan struct{}),
	}
	tc.messageChan <- &message{mt: websocket.TextMessage, message: []byte("first")}

	done := make(chan bool, 1)
	go func() {
		done <- tc.enqueueMessage(&message{mt: websocket.TextMessage, message: []byte("second")})
	}()

	select {
	case <-done:
		t.Fatal("enqueueMessage should block before close when queue is full")
	case <-time.After(50 * time.Millisecond):
	}

	close(tc.closeCh)

	select {
	case ok := <-done:
		assert.False(t, ok)
	case <-time.After(time.Second):
		t.Fatal("enqueueMessage did not unblock after close")
	}
}
