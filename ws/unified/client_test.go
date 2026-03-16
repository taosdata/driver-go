package unified

import (
	"reflect"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/taosdata/driver-go/v3/ws/client"
)

// TestNewClientNormalizesEndpoints verifies the expected behavior for this scenario.
func TestNewClientNormalizesEndpoints(t *testing.T) {
	c, err := NewClient(NewConfig([]string{"ws://127.0.0.1:6041"}), "/ws")
	if err != nil {
		t.Fatal(err)
	}
	cfg := c.Config()
	if len(cfg.Endpoints) != 1 || cfg.Endpoints[0] != "ws://127.0.0.1:6041/ws" {
		t.Fatalf("unexpected endpoints: %+v", cfg.Endpoints)
	}
}

// TestClientConnectFailoverToNextEndpoint verifies the expected behavior for this scenario.
func TestClientConnectFailoverToNextEndpoint(t *testing.T) {
	cfg := NewConfig([]string{"ws://a:1", "ws://b:2"})
	attempts := make([]string, 0, 2)
	c, err := NewClient(cfg, "/ws",
		WithDialFunc(func(endpoint string) (*websocket.Conn, error) {
			attempts = append(attempts, endpoint)
			if endpoint == "ws://a:1/ws" {
				return nil, newInvalidStateErrorf("dial failed")
			}
			return nil, nil
		}),
		WithClientFactory(func(_ *websocket.Conn, chanLength uint) *client.Client {
			return client.NewClient(nil, chanLength)
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	err = c.connectWithBootstrap(nil)
	if err != nil {
		t.Fatal(err)
	}

	// InitialCandidates uses random start, so we need to check both possible orders
	// Either it starts with a:1 and fails over to b:2, or starts with b:2 and succeeds
	validOrders := [][]string{
		{"ws://a:1/ws", "ws://b:2/ws"}, // Started with a:1, failed, tried b:2
		{"ws://b:2/ws"},                // Started with b:2, succeeded immediately
	}

	validOrder := false
	for _, order := range validOrders {
		if reflect.DeepEqual(order, attempts) {
			validOrder = true
			break
		}
	}

	if !validOrder {
		t.Fatalf("unexpected attempt order: %v (expected one of %v)", attempts, validOrders)
	}

	// Active endpoint should be b:2 regardless of order
	active := c.failover.Active()
	if active.Index != 1 || active.URL != "ws://b:2/ws" {
		t.Fatalf("unexpected active endpoint: %+v", active)
	}
}

// TestClientReconnectStartsFromNextEndpoint verifies the expected behavior for this scenario.
func TestClientReconnectStartsFromNextEndpoint(t *testing.T) {
	cfg := NewConfig([]string{"ws://a:1", "ws://b:2", "ws://c:3"})
	attempts := make([]string, 0, 4)
	c, err := NewClient(cfg, "/ws",
		WithDialFunc(func(endpoint string) (*websocket.Conn, error) {
			attempts = append(attempts, endpoint)
			return nil, nil
		}),
		WithClientFactory(func(_ *websocket.Conn, chanLength uint) *client.Client {
			return client.NewClient(nil, chanLength)
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err = c.connectWithBootstrap(nil); err != nil {
		t.Fatal(err)
	}

	// Get the active endpoint after first connect
	active := c.failover.Active()
	firstConnectEndpoint := active.URL

	if err = c.reconnectWithBootstrap(nil, nil); err != nil {
		t.Fatal(err)
	}

	if len(attempts) < 2 {
		t.Fatalf("unexpected attempts: %v", attempts)
	}

	// First connect should match the active endpoint
	if attempts[0] != firstConnectEndpoint {
		t.Fatalf("first connect endpoint %s doesn't match active %s", attempts[0], firstConnectEndpoint)
	}

	// Reconnect should start from next endpoint (not the same as first)
	if attempts[1] == firstConnectEndpoint {
		t.Fatalf("reconnect should not start from same endpoint as first connect: %s", attempts[1])
	}

	// Active endpoint should have changed after reconnect
	activeAfterReconnect := c.failover.Active()
	if activeAfterReconnect.URL == firstConnectEndpoint {
		t.Fatalf("active endpoint should change after reconnect, but still: %s", activeAfterReconnect.URL)
	}
}

// TestClientCloseRejectsConnect verifies the expected behavior for this scenario.
func TestClientCloseRejectsConnect(t *testing.T) {
	cfg := NewConfig([]string{"ws://a:1"})
	c, err := NewClient(cfg, "/ws",
		WithDialFunc(func(endpoint string) (*websocket.Conn, error) {
			return nil, nil
		}),
		WithClientFactory(func(_ *websocket.Conn, chanLength uint) *client.Client {
			return client.NewClient(nil, chanLength)
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	c.Close()
	if err = c.connectWithBootstrap(nil); err == nil {
		t.Fatal("expect close error")
	}
}

// TestNewClientFromDSN verifies the expected behavior for this scenario.
func TestNewClientFromDSN(t *testing.T) {
	c, err := NewClientFromDSN("user:passwd@ws(127.0.0.1:6041)/db?token=abc", "/ws")
	if err != nil {
		t.Fatal(err)
	}
	cfg := c.Config()
	if len(cfg.Endpoints) != 1 || cfg.Endpoints[0] != "ws://127.0.0.1:6041/ws?token=abc" {
		t.Fatalf("unexpected endpoints: %+v", cfg.Endpoints)
	}
	if cfg.User != "user" || cfg.Passwd != "passwd" || cfg.DbName != "db" {
		t.Fatalf("unexpected cfg auth/db: %+v", cfg)
	}
}

// TestClearPendingRequestsNotifiesWaiters verifies the expected behavior for this scenario.
func TestClearPendingRequestsNotifiesWaiters(t *testing.T) {
	c := &Client{}
	waiters := []*pendingRequest{
		{reqID: 1, channel: make(chan []byte, 1)},
		{reqID: 34, channel: make(chan []byte, 1)},
	}
	for i := 0; i < len(waiters); i++ {
		registerPendingRequestForTest(c, waiters[i])
	}

	clearPendingRequestsForTest(c)

	for i := 0; i < len(waiters); i++ {
		select {
		case msg := <-waiters[i].channel:
			if msg != nil {
				t.Fatalf("waiter %d should receive nil, got %v", i, msg)
			}
		default:
			t.Fatalf("waiter %d did not receive cleanup notification", i)
		}
		if pendingRequestExistsForTest(c, waiters[i].reqID) {
			t.Fatalf("pending request %d still exists after cleanup", waiters[i].reqID)
		}
	}
}

// TestRemovePendingRequestExpectedPointerMatch verifies pointer identity protects
// against removing a different request with the same req_id.
func TestRemovePendingRequestExpectedPointerMatch(t *testing.T) {
	c := &Client{
		pendingRequests: make(map[uint64]*pendingRequest),
	}
	reqID := uint64(77)
	registered := &pendingRequest{reqID: reqID, channel: make(chan []byte, 1)}
	other := &pendingRequest{reqID: reqID, channel: make(chan []byte, 1)}
	c.pendingRequests[reqID] = registered

	if removed := c.removePendingRequest(reqID, other); removed != nil {
		t.Fatalf("unexpected removal when expected pointer mismatched: %+v", removed)
	}
	if !pendingRequestExistsForTest(c, reqID) {
		t.Fatalf("request %d should still be present after mismatch remove", reqID)
	}

	removed := c.removePendingRequest(reqID, registered)
	if removed != registered {
		t.Fatalf("expected registered request to be removed, got %+v", removed)
	}
	if pendingRequestExistsForTest(c, reqID) {
		t.Fatalf("request %d should be removed after pointer match", reqID)
	}
}
