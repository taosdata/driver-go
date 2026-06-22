package unified

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

func wssEndpointFromHTTP(serverURL string) string {
	return "wss" + strings.TrimPrefix(serverURL, "https")
}

func startTLSSkipVerifyServer(t *testing.T, handler http.HandlerFunc) *httptest.Server {
	t.Helper()
	server := httptest.NewTLSServer(handler)
	t.Cleanup(server.Close)
	return server
}

func readTextActionMessage(t *testing.T, conn *websocket.Conn) string {
	t.Helper()
	_, msg, err := conn.ReadMessage()
	require.NoError(t, err)
	return string(msg)
}

func writeTextResponse(t *testing.T, conn *websocket.Conn, response string) {
	t.Helper()
	require.NoError(t, conn.WriteMessage(websocket.TextMessage, []byte(response)))
}

func reqIDFromTextAction(t *testing.T, text string) uint64 {
	t.Helper()
	var action client.WSAction
	require.NoError(t, json.Unmarshal([]byte(text), &action))
	var args struct {
		ReqID uint64 `json:"req_id"`
	}
	require.NoError(t, json.Unmarshal(action.Args, &args))
	return args.ReqID
}

func handleVersionAndConnect(t *testing.T, conn *websocket.Conn) {
	t.Helper()
	text := readTextActionMessage(t, conn)
	require.True(t, isVersionActionText(text), "unexpected version request: %s", text)
	require.NoError(t, writeVersionResponse(conn))

	text = readTextActionMessage(t, conn)
	require.Contains(t, text, `"action":"conn"`)
	writeTextResponse(t, conn, `{"code":0,"message":"","action":"conn","req_id":0}`)
}

func TestSkipVerifyAllowsSchemalessWriteOverWSS(t *testing.T) {
	var insertCount int32
	server := startTLSSkipVerifyServer(t, func(w http.ResponseWriter, r *http.Request) {
		conn, err := schemalessTestUpgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer func() {
			_ = conn.Close()
		}()

		handleVersionAndConnect(t, conn)

		text := readTextActionMessage(t, conn)
		require.Contains(t, text, `"action":"insert"`)
		require.Contains(t, text, "measurement,host=host1")
		atomic.AddInt32(&insertCount, 1)
		writeTextResponse(t, conn, `{"code":0,"message":"","action":"insert","req_id":1}`)
	})

	cfg := NewConfig([]string{wssEndpointFromHTTP(server.URL)})
	cfg.User = "root"
	cfg.Passwd = "taosdata"
	cfg.SkipVerify = true

	client, err := NewClient(cfg, "/ws")
	require.NoError(t, err)
	defer client.Close()
	require.NoError(t, client.Connect())

	err = client.SchemalessInsert(1, "measurement,host=host1 field1=2i 1577837300000", 1, "ms", 0, "")
	require.NoError(t, err)
	require.Equal(t, int32(1), atomic.LoadInt32(&insertCount))
}

func TestSkipVerifyAllowsTMQSubscribeOverWSS(t *testing.T) {
	var subscribeCount int32
	var pollCount int32
	server := startTLSSkipVerifyServer(t, func(w http.ResponseWriter, r *http.Request) {
		conn, err := queryLifecycleUpgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer func() {
			_ = conn.Close()
		}()

		text := readTextActionMessage(t, conn)
		require.True(t, isVersionActionText(text), "unexpected version request: %s", text)
		require.NoError(t, writeVersionResponse(conn))

		for {
			text = readTextActionMessage(t, conn)
			switch {
			case strings.Contains(text, `"action":"subscribe"`):
				atomic.AddInt32(&subscribeCount, 1)
				reqID := reqIDFromTextAction(t, text)
				writeTextResponse(t, conn, fmt.Sprintf(`{"code":0,"message":"","action":"subscribe","req_id":%d}`, reqID))
			case strings.Contains(text, `"action":"poll"`):
				atomic.AddInt32(&pollCount, 1)
				reqID := reqIDFromTextAction(t, text)
				writeTextResponse(t, conn, fmt.Sprintf(`{"code":0,"message":"","action":"poll","req_id":%d,"have_message":false}`, reqID))
				return
			default:
				t.Fatalf("unexpected tmq request: %s", text)
			}
		}
	})

	cfg := commontmq.ConfigMap{
		"ws.url":               wssEndpointFromHTTP(server.URL),
		"ws.skipVerify":        true,
		"ws.message.timeout":   3 * time.Second,
		"ws.message.writeWait": 3 * time.Second,
		"td.connect.user":      "root",
		"td.connect.pass":      "taosdata",
		"group.id":             "skip_verify_group",
		"client.id":            "skip_verify_client",
	}
	consumer, err := NewTMQConsumer(&cfg)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, consumer.Close())
	}()

	require.NoError(t, consumer.Subscribe("skip_verify_topic", nil))
	event := consumer.Poll(1)
	_, isErr := event.(commontmq.Error)
	if event != nil {
		require.False(t, isErr, "unexpected tmq error: %s", event.String())
	}
	require.Equal(t, int32(1), atomic.LoadInt32(&subscribeCount))
	require.Equal(t, int32(1), atomic.LoadInt32(&pollCount))
}
