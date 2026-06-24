package taosWS

import (
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"

	"github.com/taosdata/driver-go/v3/ws/unified/proto"
)

var skipVerifySQLUpgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

func skipVerifyBinaryAction(msg []byte) uint64 {
	if len(msg) < 24 {
		return 0
	}
	return binary.LittleEndian.Uint64(msg[16:24])
}

func skipVerifyBinaryReqID(msg []byte) uint64 {
	if len(msg) < 8 {
		return 0
	}
	return binary.LittleEndian.Uint64(msg[0:8])
}

func skipVerifyIsVersionActionText(text string) bool {
	return strings.Contains(text, `"action":"version"`) || strings.Contains(text, `"action": "version"`)
}

func skipVerifyWssEndpointFromHTTP(serverURL string) string {
	return "wss" + strings.TrimPrefix(serverURL, "https")
}

func TestSkipVerifyAllowsSQLWriteOverWSS(t *testing.T) {
	var queryCount int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := skipVerifySQLUpgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer func() {
			_ = conn.Close()
		}()

		_, msg, err := conn.ReadMessage()
		require.NoError(t, err)
		require.True(t, skipVerifyIsVersionActionText(string(msg)), "unexpected version request: %s", string(msg))
		require.NoError(t, conn.WriteMessage(websocket.TextMessage, []byte(`{"code":0,"message":"","action":"version","version":"3.3.6.0"}`)))

		_, msg, err = conn.ReadMessage()
		require.NoError(t, err)
		require.Contains(t, string(msg), `"action":"conn"`)
		require.NoError(t, conn.WriteMessage(websocket.TextMessage, []byte(`{"code":0,"message":"","action":"conn","req_id":0}`)))

		mt, msg, err := conn.ReadMessage()
		require.NoError(t, err)
		require.Equal(t, websocket.BinaryMessage, mt)
		require.Equal(t, uint64(proto.BinaryQueryMessage), skipVerifyBinaryAction(msg))
		atomic.AddInt32(&queryCount, 1)
		reqID := skipVerifyBinaryReqID(msg)
		resp := fmt.Sprintf(`{"code":0,"message":"","action":"query","req_id":%d,"id":0,"is_update":true,"affected_rows":1}`, reqID)
		require.NoError(t, conn.WriteMessage(websocket.TextMessage, []byte(resp)))
	}))
	defer server.Close()

	dsn := fmt.Sprintf("root:taosdata@wss(%s)/?skipVerify=true", strings.TrimPrefix(skipVerifyWssEndpointFromHTTP(server.URL), "wss://"))
	db, err := sql.Open("taosWS", dsn)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err = db.ExecContext(ctx, "insert into skip_verify_sql values(now, 1)")
	require.NoError(t, err)
	require.Equal(t, int32(1), atomic.LoadInt32(&queryCount))
}
