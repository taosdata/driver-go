package unified

import (
	"testing"

	"github.com/stretchr/testify/require"
	commontmq "github.com/taosdata/driver-go/v3/common/tmq"
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
