package tmq

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"

	commontmq "github.com/taosdata/driver-go/v3/common/tmq"
	"github.com/taosdata/driver-go/v3/ws/client"
)

var skipVerifyTMQUpgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

func skipVerifyTMQWSS(serverURL string) string {
	return "wss" + strings.TrimPrefix(serverURL, "https")
}

func skipVerifyTMQIsVersion(text string) bool {
	return strings.Contains(text, `"action":"version"`) || strings.Contains(text, `"action": "version"`)
}

func skipVerifyTMQReqID(t *testing.T, text string) uint64 {
	t.Helper()
	var action client.WSAction
	require.NoError(t, json.Unmarshal([]byte(text), &action))
	var args struct {
		ReqID uint64 `json:"req_id"`
	}
	require.NoError(t, json.Unmarshal(action.Args, &args))
	return args.ReqID
}

func TestSkipVerifyAllowsTMQSubscribeOverWSS(t *testing.T) {
	var subscribeCount int32
	var pollCount int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := skipVerifyTMQUpgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer func() {
			_ = conn.Close()
		}()

		_, msg, err := conn.ReadMessage()
		require.NoError(t, err)
		require.True(t, skipVerifyTMQIsVersion(string(msg)), "unexpected version request: %s", string(msg))
		require.NoError(t, conn.WriteMessage(websocket.TextMessage, []byte(`{"code":0,"message":"","action":"version","version":"3.3.6.0"}`)))

		for {
			_, msg, err = conn.ReadMessage()
			require.NoError(t, err)
			text := string(msg)
			switch {
			case strings.Contains(text, `"action":"subscribe"`):
				atomic.AddInt32(&subscribeCount, 1)
				reqID := skipVerifyTMQReqID(t, text)
				require.NoError(t, conn.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf(`{"code":0,"message":"","action":"subscribe","req_id":%d}`, reqID))))
			case strings.Contains(text, `"action":"poll"`):
				atomic.AddInt32(&pollCount, 1)
				reqID := skipVerifyTMQReqID(t, text)
				require.NoError(t, conn.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf(`{"code":0,"message":"","action":"poll","req_id":%d,"have_message":false}`, reqID))))
				return
			default:
				t.Fatalf("unexpected tmq request: %s", text)
			}
		}
	}))
	defer server.Close()

	cfg := commontmq.ConfigMap{
		"ws.url":               skipVerifyTMQWSS(server.URL),
		"ws.skipVerify":        true,
		"ws.message.timeout":   3 * time.Second,
		"ws.message.writeWait": 3 * time.Second,
		"td.connect.user":      "root",
		"td.connect.pass":      "taosdata",
		"group.id":             "skip_verify_group",
		"client.id":            "skip_verify_client",
	}
	consumer, err := NewConsumer(&cfg)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, consumer.Close())
	}()

	require.NoError(t, consumer.Subscribe("skip_verify_topic", nil))
	event := consumer.Poll(1)
	if event != nil {
		_, isErr := event.(commontmq.Error)
		require.False(t, isErr, "unexpected tmq error: %s", event.String())
	}
	require.Equal(t, int32(1), atomic.LoadInt32(&subscribeCount))
	require.Equal(t, int32(1), atomic.LoadInt32(&pollCount))
}
