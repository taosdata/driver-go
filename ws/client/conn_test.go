package client

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
)

func TestEnvelopePool(t *testing.T) {
	pool := &EnvelopePool{}

	// Test Get method
	env := pool.Get()
	assert.NotNil(t, env)
	assert.NotNil(t, env.Msg)

	// Test Put method
	env.Msg.WriteString("test")
	pool.Put(env)

	// Test if the envelope is reset after put
	env = pool.Get()
	assert.Equal(t, 0, env.Msg.Len())
}

func TestEnvelope_Reset(t *testing.T) {
	env := &Envelope{
		Type: 1,
		Msg:  bytes.NewBufferString("test"),
	}

	env.Reset()

	assert.Equal(t, 0, env.Msg.Len())
}

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

func wsEchoServer(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()

	for {
		messageType, message, err := conn.ReadMessage()
		if err != nil {
			return
		}

		if err := conn.WriteMessage(messageType, message); err != nil {
			return
		}
	}
}

func TestClient(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(wsEchoServer))
	defer s.Close()
	t.Log(s.URL)
	ep := "ws" + strings.TrimPrefix(s.URL, "http")
	ws, _, err := websocket.DefaultDialer.Dial(ep, nil)
	assert.NoError(t, err)
	c := NewClient(ws, 1)
	gotMessage := make(chan struct{})
	c.TextMessageHandler = func(message []byte) {
		assert.Equal(t, "test", string(message))
		gotMessage <- struct{}{}
	}
	running := c.IsRunning()
	assert.True(t, running)
	defer c.Close()
	go c.ReadPump()
	go c.WritePump()
	env := c.GetEnvelope()
	env.Type = websocket.TextMessage
	env.Msg.WriteString("test")
	c.Send(env)
	env = c.GetEnvelope()
	c.PutEnvelope(env)
	timeout := time.NewTimer(time.Second * 3)
	select {
	case <-gotMessage:
		t.Log("got message")
	case <-timeout.C:
		t.Error("timeout")
	}
}
