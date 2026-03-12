package unified

import (
	"testing"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSwapRuntimeNilDoesNotPanicWhenClosed(t *testing.T) {
	cfg := NewConfig([]string{"ws://localhost:6041"})
	c, err := NewClient(cfg, "/ws")
	require.NoError(t, err)

	c.Close()

	assert.NotPanics(t, func() {
		oldRuntime, swapErr := c.swapRuntime(nil, 0)
		assert.Nil(t, oldRuntime)
		assert.ErrorIs(t, swapErr, ErrUnifiedClosed)
	})
}

func TestSwapRuntimeNilOnOpenClientReturnsInvalidState(t *testing.T) {
	cfg := NewConfig([]string{"ws://localhost:6041"})
	c, err := NewClient(cfg, "/ws")
	require.NoError(t, err)
	defer c.Close()

	oldRuntime, swapErr := c.swapRuntime(nil, 0)
	assert.Nil(t, oldRuntime)
	assert.ErrorIs(t, swapErr, ErrNilRuntime)
}

func TestIsReconnectableErrorWithWebsocketCloseError(t *testing.T) {
	wsErr := &websocket.CloseError{
		Code: websocket.CloseAbnormalClosure,
		Text: "connection lost",
	}
	assert.True(t, isReconnectableError(wsErr))
	assert.True(t, isReconnectableError(&Error{Type: ErrorTypeUnknown, Cause: wsErr}))
}

func TestReconnectFailureErrorType(t *testing.T) {
	cfg := NewConfig([]string{"ws://a:1"})
	c, err := NewClient(cfg, "/ws",
		WithDialFunc(func(string) (*websocket.Conn, error) {
			return nil, newInvalidStateErrorf("dial failed")
		}),
	)
	require.NoError(t, err)
	defer c.Close()

	err = c.ReconnectWithBootstrap(nil)
	require.Error(t, err)
	assert.True(t, IsReconnectFailedError(err))
	assert.True(t, IsConnectionRelatedError(err))
	assert.Equal(t, ErrorTypeReconnectFailed, ErrorTypeOf(err))
	assert.ErrorIs(t, err, ErrUnifiedConnectFailed)
}
