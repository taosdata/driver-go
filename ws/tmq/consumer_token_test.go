package tmq

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/common/testenv"
	"github.com/taosdata/driver-go/v3/common/testtool"
	"github.com/taosdata/driver-go/v3/common/tmq"
)

// TestConsumerWithToken tests TMQ consumer with token authentication
func TestConsumerWithToken(t *testing.T) {
	if !testenv.IsEnterpriseTest() {
		t.Skip("Skip token test for community edition")
	}
	dbName := "test_ws_tmq_token_valid"
	topicName := "test_ws_tmq_token_topic_valid"
	user := "root"
	// Prepare environment
	err := prepareTokenEnv(t, dbName, topicName)
	if !assert.NoError(t, err) {
		return
	}
	defer func() {
		err = cleanTokenEnv(t, dbName, topicName)
		assert.NoError(t, err)
	}()

	// Create a token for testing and get token value
	tokenName := fmt.Sprintf("ws_tk_v_%d", time.Now().Unix())
	result, err := testtool.HTTPQuery(fmt.Sprintf("CREATE TOKEN %s FROM USER root", tokenName))
	if !assert.NoError(t, err, "Token feature not available") {
		return
	}

	defer func() {
		_ = doRequest(fmt.Sprintf("DROP TOKEN %s", tokenName))
	}()

	if !assert.NotEmpty(t, result.Data, "token query returned no rows") {
		return
	}
	rawToken, ok := result.Data[0][0].(string)
	if !assert.True(t, ok, "token value should be string") {
		return
	}
	token := rawToken
	if !assert.NotEmpty(t, token, "Failed to get token value") {
		return
	}

	// Grant permissions to the token
	err = doRequest(fmt.Sprintf("GRANT SUBSCRIBE ON TOPIC %s.%s TO %s", dbName, topicName, user))
	assert.NoError(t, err, "grant subscribe permission failed")

	err = doRequest(fmt.Sprintf("GRANT ALL ON DATABASE %s TO %s", dbName, user))
	assert.NoError(t, err, "grant database permission failed")

	now := time.Now()
	err = doRequest(fmt.Sprintf("create table %s.t1(ts timestamp, v int)", dbName))
	if !assert.NoError(t, err) {
		return
	}

	// Test: Consumer with valid token
	consumer, err := NewConsumer(&tmq.ConfigMap{
		"ws.url":                "ws://127.0.0.1:6041",
		"ws.message.channelLen": uint(0),
		"ws.message.timeout":    common.DefaultMessageTimeout,
		"ws.message.writeWait":  common.DefaultWriteWait,
		"td.connect.token":      token, // Token will be passed in Config map
		"td.connect.user":       "root",
		"td.connect.pass":       "wrong_password", // Ensure auth succeeds via token instead of password
		"group.id":              "test_token",
		"client.id":             "test_consumer_token",
		"auto.offset.reset":     "earliest",
		"enable.auto.commit":    "false",
		"msg.with.table.name":   "true",
	})
	if !assert.NoError(t, err) {
		return
	}
	defer func() {
		err = consumer.Unsubscribe()
		assert.NoError(t, err)
		err = consumer.Close()
		assert.NoError(t, err)
	}()

	err = consumer.SubscribeTopics([]string{topicName}, nil)
	if !assert.NoError(t, err) {
		return
	}

	// Insert test data
	err = doRequest(fmt.Sprintf("insert into %s.t1 values(%d, 1)", dbName, now.UnixNano()/1e6))
	assert.NoError(t, err)

	// Poll for messages
	for i := 0; i < 10; i++ {
		event := consumer.Poll(500)
		if event == nil {
			continue
		}
		switch e := event.(type) {
		case *tmq.DataMessage:
			data := e.Value().([]*tmq.Data)
			assert.Equal(t, dbName, e.DBName())
			assert.Equal(t, 1, len(data))
			assert.Equal(t, "t1", data[0].TableName)
			assert.Equal(t, 1, len(data[0].Data))
			assert.Equal(t, 2, len(data[0].Data[0]))
			assert.Equal(t, int32(1), data[0].Data[0][1].(int32))
			return
		case tmq.Error:
			assert.Failf(t, "Received error", "%v", e)
			return
		}
	}
	assert.Fail(t, "no message got")
}

// TestConsumerWithInvalidToken tests TMQ consumer with invalid token
func TestConsumerWithInvalidToken(t *testing.T) {
	if !testenv.IsEnterpriseTest() {
		t.Skip("Skip token test for community edition")
	}
	dbName := "test_ws_tmq_token_invalid"
	topicName := "test_ws_tmq_token_topic_invalid"
	err := prepareTokenEnv(t, dbName, topicName)
	if !assert.NoError(t, err) {
		return
	}
	defer func() {
		err = cleanTokenEnv(t, dbName, topicName)
		assert.NoError(t, err)
	}()
	// Test with invalid token - should fail at subscription
	consumer, err := NewConsumer(&tmq.ConfigMap{
		"ws.url":                "ws://127.0.0.1:6041",
		"ws.message.channelLen": uint(0),
		"ws.message.timeout":    common.DefaultMessageTimeout,
		"ws.message.writeWait":  common.DefaultWriteWait,
		"td.connect.token":      "invalid_token_12345",
		"group.id":              "test_invalid_token",
		"client.id":             "test_consumer_invalid",
		"auto.offset.reset":     "earliest",
	})
	if err == nil {
		defer func() {
			err = consumer.Close()
			assert.NoError(t, err)
		}()
		// Try to subscribe - should fail
		err = consumer.SubscribeTopics([]string{topicName}, nil)
	}
	assert.Error(t, err, "invalid token should fail in NewConsumer or Subscribe")
}

// TestConsumerTokenPriority tests that token has higher priority than user/password
func TestConsumerTokenPriority(t *testing.T) {
	if !testenv.IsEnterpriseTest() {
		t.Skip("Skip token test for community edition")
	}
	dbName := "test_ws_tmq_token_priority"
	topicName := "test_ws_tmq_token_topic_priority"
	// This test verifies that when both token and user/password are provided,
	// token takes priority
	err := prepareTokenEnv(t, dbName, topicName)
	if !assert.NoError(t, err) {
		return
	}
	defer func() {
		err = cleanTokenEnv(t, dbName, topicName)
		assert.NoError(t, err)
	}()
	user := "root"
	tokenName := fmt.Sprintf("ws_tk_p_%d", time.Now().Unix())
	result, err := testtool.HTTPQuery(fmt.Sprintf("CREATE TOKEN %s FROM USER root", tokenName))
	if !assert.NoError(t, err) {
		return
	}

	defer func() {
		_ = doRequest(fmt.Sprintf("DROP TOKEN %s", tokenName))
	}()

	if !assert.NotEmpty(t, result.Data, "token query returned no rows") {
		return
	}
	rawToken, ok := result.Data[0][0].(string)
	if !assert.True(t, ok, "token value should be string") {
		return
	}
	token := rawToken
	if !assert.NotEmpty(t, token, "Failed to get token value") {
		return
	}

	// Grant permissions to the token
	err = doRequest(fmt.Sprintf("GRANT SUBSCRIBE ON TOPIC %s.%s TO %s", dbName, topicName, user))
	assert.NoError(t, err, "grant subscribe permission failed")

	err = doRequest(fmt.Sprintf("GRANT ALL ON DATABASE %s TO %s", dbName, user))
	assert.NoError(t, err, "grant database permission failed")

	// Create consumer with both token and wrong password
	// Token should take priority, so connection should succeed
	consumer, err := NewConsumer(&tmq.ConfigMap{
		"ws.url":                "ws://127.0.0.1:6041",
		"ws.message.channelLen": uint(0),
		"ws.message.timeout":    common.DefaultMessageTimeout,
		"ws.message.writeWait":  common.DefaultWriteWait,
		"td.connect.token":      token,
		"td.connect.user":       "root",
		"td.connect.pass":       "wrong_password", // Wrong password, but token should work
		"group.id":              "test_priority",
		"client.id":             "test_consumer_priority",
		"auto.offset.reset":     "earliest",
	})

	if !assert.NoError(t, err) {
		return
	}
	defer func() {
		err = consumer.Unsubscribe()
		assert.NoError(t, err)
		err = consumer.Close()
		assert.NoError(t, err)
	}()

	// Should be able to subscribe successfully using token
	err = consumer.SubscribeTopics([]string{topicName}, nil)
	assert.NoError(t, err)
}

func prepareTokenEnv(t *testing.T, dbName, topicName string) error {
	steps := []string{
		fmt.Sprintf("drop topic if exists %s", topicName),
		fmt.Sprintf("drop database if exists %s", dbName),
		fmt.Sprintf("create database %s WAL_RETENTION_PERIOD 86400", dbName),
		fmt.Sprintf("create topic %s as database %s", topicName, dbName),
	}
	return executeStepsEventually(
		t,
		steps,
		doRequest,
		6*time.Second,
		500*time.Millisecond,
		fmt.Sprintf("prepare token env timeout for db %s and topic %s", dbName, topicName),
	)
}

func cleanTokenEnv(t *testing.T, dbName, topicName string) error {
	steps := []string{
		fmt.Sprintf("drop topic if exists %s", topicName),
		fmt.Sprintf("drop database if exists %s", dbName),
	}
	return executeStepsEventually(
		t,
		steps,
		doRequest,
		6*time.Second,
		500*time.Millisecond,
		fmt.Sprintf("clean token env timeout for db %s and topic %s", dbName, topicName),
	)
}

func executeStepsEventually(t *testing.T, steps []string, exec func(string) error, timeout, interval time.Duration, timeoutErrMsg string) error {
	var lastErr error
	ok := assert.Eventually(t, func() bool {
		for _, step := range steps {
			if err := exec(step); err != nil {
				lastErr = err
				return false
			}
		}
		lastErr = nil
		return true
	}, timeout, interval)
	if !ok && lastErr == nil {
		lastErr = errors.New(timeoutErrMsg)
	}
	return lastErr
}
