package unified

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commontmq "github.com/taosdata/driver-go/v3/common/tmq"
	"github.com/taosdata/driver-go/v3/ws/client"
	"github.com/taosdata/driver-go/v3/ws/unified/proto"
)

// TestNewTMQConsumerNilConfig verifies the expected behavior for this scenario.
func TestNewTMQConsumerNilConfig(t *testing.T) {
	consumer, err := NewTMQConsumer(nil)
	require.ErrorIs(t, err, ErrNilConfig)
	require.Nil(t, consumer)
}

// TestNewTMQConsumerConfigValidationError verifies the expected behavior for this scenario.
func TestNewTMQConsumerConfigValidationError(t *testing.T) {
	cfg := commontmq.ConfigMap{}
	consumer, err := NewTMQConsumer(&cfg)
	require.EqualError(t, err, "ws.url required")
	require.Nil(t, consumer)
}

// TestTMQConsumerNilReceiver verifies the expected behavior for this scenario.
func TestTMQConsumerNilReceiver(t *testing.T) {
	var consumer *TMQConsumer
	require.ErrorIs(t, consumer.Close(), ErrTMQConsumerUninitialized)
	ev := consumer.Poll(100)
	require.NotNil(t, ev)
}

// TestTMQParseEndpointsFromURLList verifies the expected behavior for this scenario.
func TestTMQParseEndpointsFromURLList(t *testing.T) {
	endpoints, err := parseTMQEndpoints("ws://127.0.0.1:6041, ws://127.0.0.1:6042/ws?token=abc, ws://127.0.0.1:6041")
	require.NoError(t, err)
	require.Equal(t, []string{
		"ws://127.0.0.1:6041/rest/tmq",
		"ws://127.0.0.1:6042/rest/tmq?token=abc",
	}, endpoints)
}

// TestTMQConfigMapParsesMultipleEndpoints verifies the expected behavior for this scenario.
func TestTMQConfigMapParsesMultipleEndpoints(t *testing.T) {
	cfg, err := configMapToConfig(commontmq.ConfigMap{
		"ws.url": "ws://127.0.0.1:6041,ws://127.0.0.1:6042",
	})
	require.NoError(t, err)
	require.Equal(t, []string{
		"ws://127.0.0.1:6041/rest/tmq",
		"ws://127.0.0.1:6042/rest/tmq",
	}, cfg.Endpoints)
	require.Equal(t, "ws://127.0.0.1:6041/rest/tmq", cfg.Url)
}

func TestTMQSubscribeAdapterHARequestsInstancesOnlyOnce(t *testing.T) {
	withFreshClusterRegistry(t)
	listInstancesCh := make(chan bool, 2)
	errCh := make(chan error, 2)

	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := queryLifecycleUpgrader.Upgrade(w, r, nil)
		if err != nil {
			errCh <- err
			return
		}
		defer func() {
			_ = conn.Close()
		}()

		for {
			_, msg, readErr := conn.ReadMessage()
			if readErr != nil {
				return
			}
			if isVersionActionText(string(msg)) {
				if writeErr := writeVersionResponse(conn); writeErr != nil {
					errCh <- writeErr
					return
				}
				continue
			}
			var action client.WSAction
			if err = json.Unmarshal(msg, &action); err != nil {
				errCh <- err
				return
			}
			if strings.ToLower(action.Action) != proto.TMQActionSubscribe {
				continue
			}
			var req proto.SubscribeReq
			if err = json.Unmarshal(action.Args, &req); err != nil {
				errCh <- err
				return
			}
			listInstancesCh <- req.ListInstances
			resp := []byte(`{"code":0,"message":"","action":"subscribe","req_id":` + strconv.FormatUint(req.ReqID, 10) + `,"list_instances":["peer:6041"]}`)
			if writeErr := conn.WriteMessage(websocket.TextMessage, resp); writeErr != nil {
				errCh <- writeErr
				return
			}
		}
	}))
	defer s.Close()

	cfg := commontmq.ConfigMap{
		"ws.url":       wsEndpointFromHTTP(s.URL),
		"ws.adapterHa": true,
	}
	consumer, err := NewTMQConsumer(&cfg)
	require.NoError(t, err)
	defer func() {
		_ = consumer.Close()
	}()

	require.NoError(t, consumer.Subscribe("topic_a", nil))
	select {
	case got := <-listInstancesCh:
		assert.True(t, got, "first subscribe should request list_instances")
	case err = <-errCh:
		t.Fatalf("tmq server failed: %v", err)
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for first subscribe")
	}
	require.Len(t, consumer.client.failover.endpointsCopy(), 2)

	require.NoError(t, consumer.Subscribe("topic_b", nil))
	select {
	case got := <-listInstancesCh:
		assert.False(t, got, "second subscribe should not request list_instances")
	case err = <-errCh:
		t.Fatalf("tmq server failed: %v", err)
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for second subscribe")
	}
}

// TestBuildTMQTimeoutMessageRedactsSensitiveArgs verifies timeout message keeps context while masking secrets.
func TestBuildTMQTimeoutMessageRedactsSensitiveArgs(t *testing.T) {
	args := []byte(`{
		"user":"root",
		"password":"raw-pass",
		"config":{
			"td.connect.pass":"raw-td-pass",
			"api_token":"raw-token",
			"safe":"ok"
		},
		"endpoint":"ws://127.0.0.1:6041/rest/tmq?token=raw-query-token&x=1"
	}`)
	message := buildTMQTimeoutMessage("subscribe", 12345, args)

	require.Contains(t, message, "tmq message timeout")
	require.Contains(t, message, "action=subscribe")
	require.Contains(t, message, "req_id=12345")
	require.Contains(t, message, `"password":"***"`)
	require.Contains(t, message, `"td.connect.pass":"***"`)
	require.Contains(t, message, `"api_token":"***"`)
	require.Contains(t, message, `"safe":"ok"`)
	require.NotContains(t, message, "raw-pass")
	require.NotContains(t, message, "raw-td-pass")
	require.NotContains(t, message, "raw-token")
	require.NotContains(t, message, "raw-query-token")
}

// TestBuildTMQTimeoutMessageHandlesInvalidJSONArgs verifies malformed args are handled without leaking payload.
func TestBuildTMQTimeoutMessageHandlesInvalidJSONArgs(t *testing.T) {
	message := buildTMQTimeoutMessage("poll", 9, []byte(`{"bad"`))
	require.Contains(t, message, "action=poll")
	require.Contains(t, message, "req_id=9")
	require.Contains(t, message, "<invalid_json")
}
