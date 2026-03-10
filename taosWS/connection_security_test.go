package taosWS

import (
	"bytes"
	"context"
	"database/sql/driver"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taosdata/driver-go/v3/common"
)

func newFailingWriteWSConn(t *testing.T) (*websocket.Conn, *failingWriteConn, func()) {
	t.Helper()

	done := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := pingDisconnectABUpgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer func() {
			_ = conn.Close()
		}()
		<-done
	}))

	dialer := common.DefaultDialer
	var wrappedConn *failingWriteConn
	dialer.NetDialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := (&net.Dialer{}).DialContext(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		wrappedConn = &failingWriteConn{Conn: conn}
		return wrappedConn, nil
	}

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/ws"
	ws, _, err := dialer.Dial(wsURL, nil)
	require.NoError(t, err)
	require.NotNil(t, wrappedConn)

	cleanup := func() {
		_ = ws.Close()
		close(done)
		server.Close()
	}
	return ws, wrappedConn, cleanup
}

func TestWriteTextErrorDoesNotLeakPayload(t *testing.T) {
	ws, wrappedConn, cleanup := newFailingWriteWSConn(t)
	defer cleanup()

	tc := &taosConn{
		client:       ws,
		writeTimeout: time.Second,
		closeCh:      make(chan struct{}),
		messageErrCh: make(chan struct{}),
	}

	payload := []byte(`{"sql":"insert into log values('top-secret-value')"}`)
	atomic.StoreUint32(&wrappedConn.failWrites, 1)

	err := tc.writeText(payload)
	require.Error(t, err)
	assert.ErrorIs(t, err, driver.ErrBadConn)
	assert.Contains(t, err.Error(), "closed pipe")
	assert.NotContains(t, err.Error(), "top-secret-value")
	assert.NotContains(t, err.Error(), string(payload))

	storedErr := tc.getMessageError()
	require.Error(t, storedErr)
	assert.Equal(t, err, storedErr)
	assert.NotContains(t, storedErr.Error(), "top-secret-value")
	assert.NotContains(t, storedErr.Error(), string(payload))
}

func TestConnectWriteErrorDoesNotLeakCredentials(t *testing.T) {
	ws, wrappedConn, cleanup := newFailingWriteWSConn(t)
	defer cleanup()

	cfg := NewConfig()
	cfg.User = "root"
	cfg.Passwd = "super-secret-password"
	cfg.BearerToken = "super-secret-token"
	cfg.TotpCode = "654321"

	tc := &taosConn{
		buf:          &bytes.Buffer{},
		client:       ws,
		writeTimeout: time.Second,
		cfg:          cfg,
		closeCh:      make(chan struct{}),
		messageErrCh: make(chan struct{}),
	}

	atomic.StoreUint32(&wrappedConn.failWrites, 1)

	err := tc.connect()
	require.Error(t, err)
	assert.ErrorIs(t, err, driver.ErrBadConn)
	assert.Contains(t, err.Error(), "closed pipe")
	assert.NotContains(t, err.Error(), cfg.Passwd)
	assert.NotContains(t, err.Error(), cfg.BearerToken)
	assert.NotContains(t, err.Error(), cfg.TotpCode)

	storedErr := tc.getMessageError()
	require.Error(t, storedErr)
	assert.NotContains(t, storedErr.Error(), cfg.Passwd)
	assert.NotContains(t, storedErr.Error(), cfg.BearerToken)
	assert.NotContains(t, storedErr.Error(), cfg.TotpCode)
}
