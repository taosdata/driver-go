package taosWS

import (
	"context"
	"database/sql/driver"
	"fmt"
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

type failingConnHolder struct {
	conn *failingWriteConn
}

func setupFailingDialerServer(t *testing.T, failOnCreate bool) (*Config, *failingConnHolder, func()) {
	t.Helper()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := pingDisconnectABUpgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer func() {
			_ = conn.Close()
		}()

		// Handle connect request for successful bootstrap.
		_, _, err = conn.ReadMessage()
		if err != nil {
			return
		}
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"code":0,"message":"","action":"conn","req_id":0}`))

		// Keep connection alive for test duration.
		<-time.After(2 * time.Second)
	}))

	u, err := url.Parse(server.URL)
	require.NoError(t, err)
	host, portStr, err := net.SplitHostPort(u.Host)
	require.NoError(t, err)
	port, err := strconv.Atoi(portStr)
	require.NoError(t, err)

	origDialer := common.DefaultDialer
	dialer := common.DefaultDialer
	holder := &failingConnHolder{}
	dialer.NetDialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, dialErr := (&net.Dialer{}).DialContext(ctx, network, addr)
		if dialErr != nil {
			return nil, dialErr
		}
		wrappedConn := &failingWriteConn{Conn: conn}
		if failOnCreate {
			atomic.StoreUint32(&wrappedConn.failWrites, 1)
		}
		holder.conn = wrappedConn
		return wrappedConn, nil
	}
	common.DefaultDialer = dialer

	cfg := NewConfig()
	cfg.User = "root"
	cfg.Passwd = "taosdata"
	cfg.Net = "ws"
	cfg.Addr = host
	cfg.Port = port
	cfg.ReadTimeout = 3 * time.Second
	cfg.WriteTimeout = 3 * time.Second

	cleanup := func() {
		common.DefaultDialer = origDialer
		server.Close()
	}
	return cfg, holder, cleanup
}

func TestWriteTextErrorDoesNotLeakPayload(t *testing.T) {
	cfg, holder, cleanup := setupFailingDialerServer(t, false)
	defer cleanup()

	tc, err := newTaosConn(cfg)
	require.NoError(t, err)
	defer func() {
		_ = tc.Close()
	}()
	require.NotNil(t, holder.conn)

	atomic.StoreUint32(&holder.conn.failWrites, 1)
	secretSQL := "insert into log values('top-secret-value')"
	_, err = tc.ExecContext(context.Background(), secretSQL, nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, driver.ErrBadConn)
	assert.Contains(t, strings.ToLower(err.Error()), "closed")
	assert.NotContains(t, err.Error(), "top-secret-value")
	assert.NotContains(t, err.Error(), secretSQL)
}

func TestConnectWriteErrorDoesNotLeakCredentials(t *testing.T) {
	cfg, _, cleanup := setupFailingDialerServer(t, true)
	defer cleanup()

	cfg.User = "root"
	cfg.Passwd = "super-secret-password"
	cfg.BearerToken = "super-secret-token"
	cfg.TotpCode = "654321"

	_, err := newTaosConn(cfg)
	require.Error(t, err)
	assert.ErrorIs(t, err, driver.ErrBadConn)
	assert.Contains(t, strings.ToLower(err.Error()), "closed")
	assert.NotContains(t, fmt.Sprint(err), cfg.Passwd)
	assert.NotContains(t, fmt.Sprint(err), cfg.BearerToken)
	assert.NotContains(t, fmt.Sprint(err), cfg.TotpCode)
}
