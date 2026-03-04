package tmq

import (
	"fmt"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/taosdata/driver-go/v3/common/testenv"
	"github.com/taosdata/driver-go/v3/common/tmq"
	"github.com/taosdata/driver-go/v3/wrapper"
)

func TestConsumerWithToken(t *testing.T) {
	if !testenv.IsEnterpriseTest() {
		t.Skip("Skip token test for community edition")
	}
	conn, err := wrapper.TaosConnect("", "root", "taosdata", "", 0)
	if !assert.NoError(t, err) {
		return
	}
	defer wrapper.TaosClose(conn)

	dbName := "test_af_tmq_token_valid"
	topicName := "test_af_tmq_token_topic_valid"
	user := "root"

	err = prepareTokenEnv(t, conn, dbName, topicName)
	if !assert.NoError(t, err) {
		return
	}
	defer func() {
		err = cleanTokenEnv(t, conn, dbName, topicName)
		assert.NoError(t, err)
	}()

	tokenName := fmt.Sprintf("af_tk_v_%d", time.Now().Unix())
	token, err := createToken(conn, tokenName)
	if !assert.NoError(t, err) {
		return
	}
	defer func() {
		_ = execWithoutResult(conn, fmt.Sprintf("DROP TOKEN %s", tokenName))
	}()

	err = execWithoutResult(conn, fmt.Sprintf("GRANT SUBSCRIBE ON TOPIC %s.%s TO %s", dbName, topicName, user))
	assert.NoError(t, err, "grant subscribe permission failed")
	err = execWithoutResult(conn, fmt.Sprintf("GRANT ALL ON DATABASE %s TO %s", dbName, user))
	assert.NoError(t, err, "grant database permission failed")

	now := time.Now()
	err = execWithoutResult(conn, fmt.Sprintf("create table %s.t1(ts timestamp, v int)", dbName))
	if !assert.NoError(t, err) {
		return
	}

	consumer, err := NewConsumer(&tmq.ConfigMap{
		"td.connect.ip":       "127.0.0.1",
		"td.connect.port":     "6030",
		"td.connect.token":    token,
		"td.connect.user":     "root",
		"td.connect.pass":     "wrong_password", // Ensure auth succeeds via token instead of password
		"group.id":            "test_token",
		"client.id":           "test_consumer_token",
		"auto.offset.reset":   "earliest",
		"enable.auto.commit":  "false",
		"msg.with.table.name": "true",
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

	err = consumer.Subscribe(topicName, nil)
	if !assert.NoError(t, err) {
		return
	}

	err = execWithoutResult(conn, fmt.Sprintf("insert into %s.t1 values(%d, 1)", dbName, now.UnixNano()/1e6))
	assert.NoError(t, err)

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

func TestConsumerWithInvalidToken(t *testing.T) {
	if !testenv.IsEnterpriseTest() {
		t.Skip("Skip token test for community edition")
	}
	conn, err := wrapper.TaosConnect("", "root", "taosdata", "", 0)
	if !assert.NoError(t, err) {
		return
	}
	defer wrapper.TaosClose(conn)

	dbName := "test_af_tmq_token_invalid"
	topicName := "test_af_tmq_token_topic_invalid"
	err = prepareTokenEnv(t, conn, dbName, topicName)
	if !assert.NoError(t, err) {
		return
	}
	defer func() {
		err = cleanTokenEnv(t, conn, dbName, topicName)
		assert.NoError(t, err)
	}()

	consumer, err := NewConsumer(&tmq.ConfigMap{
		"td.connect.ip":     "127.0.0.1",
		"td.connect.port":   "6030",
		"td.connect.token":  "invalid_token_12345",
		"group.id":          "test_invalid_token",
		"client.id":         "test_consumer_invalid",
		"auto.offset.reset": "earliest",
	})
	if err == nil {
		defer func() {
			err = consumer.Close()
			assert.NoError(t, err)
		}()
		err = consumer.Subscribe(topicName, nil)
	}
	assert.Error(t, err, "invalid token should fail in NewConsumer or Subscribe")
}

func TestConsumerTokenPriority(t *testing.T) {
	if !testenv.IsEnterpriseTest() {
		t.Skip("Skip token test for community edition")
	}
	conn, err := wrapper.TaosConnect("", "root", "taosdata", "", 0)
	if !assert.NoError(t, err) {
		return
	}
	defer wrapper.TaosClose(conn)

	dbName := "test_af_tmq_token_priority"
	topicName := "test_af_tmq_token_topic_priority"
	user := "root"

	err = prepareTokenEnv(t, conn, dbName, topicName)
	if !assert.NoError(t, err) {
		return
	}
	defer func() {
		err = cleanTokenEnv(t, conn, dbName, topicName)
		assert.NoError(t, err)
	}()

	tokenName := fmt.Sprintf("af_tk_p_%d", time.Now().Unix())
	token, err := createToken(conn, tokenName)
	if !assert.NoError(t, err) {
		return
	}
	defer func() {
		_ = execWithoutResult(conn, fmt.Sprintf("DROP TOKEN %s", tokenName))
	}()

	err = execWithoutResult(conn, fmt.Sprintf("GRANT SUBSCRIBE ON TOPIC %s.%s TO %s", dbName, topicName, user))
	assert.NoError(t, err, "grant subscribe permission failed")
	err = execWithoutResult(conn, fmt.Sprintf("GRANT ALL ON DATABASE %s TO %s", dbName, user))
	assert.NoError(t, err, "grant database permission failed")

	consumer, err := NewConsumer(&tmq.ConfigMap{
		"td.connect.ip":       "127.0.0.1",
		"td.connect.port":     "6030",
		"td.connect.token":    token,
		"td.connect.user":     "root",
		"td.connect.pass":     "wrong_password",
		"group.id":            "test_priority",
		"client.id":           "test_consumer_priority",
		"auto.offset.reset":   "earliest",
		"enable.auto.commit":  "false",
		"msg.with.table.name": "true",
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

	err = consumer.Subscribe(topicName, nil)
	assert.NoError(t, err)
}

func prepareTokenEnv(t *testing.T, conn unsafe.Pointer, dbName, topicName string) error {
	steps := []string{
		fmt.Sprintf("drop topic if exists %s", topicName),
		fmt.Sprintf("drop database if exists %s", dbName),
		fmt.Sprintf("create database %s WAL_RETENTION_PERIOD 86400", dbName),
		fmt.Sprintf("create topic %s as database %s", topicName, dbName),
	}
	return executeStepsEventually(
		t,
		steps,
		func(step string) error { return execWithoutResult(conn, step) },
		6*time.Second,
		500*time.Millisecond,
		fmt.Sprintf("prepare token env timeout for db %s and topic %s", dbName, topicName),
	)
}

func cleanTokenEnv(t *testing.T, conn unsafe.Pointer, dbName, topicName string) error {
	steps := []string{
		fmt.Sprintf("drop topic if exists %s", topicName),
		fmt.Sprintf("drop database if exists %s", dbName),
	}
	return executeStepsEventually(
		t,
		steps,
		func(step string) error { return execWithoutResult(conn, step) },
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
		lastErr = fmt.Errorf(timeoutErrMsg)
	}
	return lastErr
}

func createToken(conn unsafe.Pointer, tokenName string) (string, error) {
	token, err := queryOneString(conn, fmt.Sprintf("CREATE TOKEN %s FROM USER root", tokenName))
	if err != nil {
		return "", err
	}
	if token == "" {
		return "", fmt.Errorf("failed to get token value")
	}
	return token, nil
}

func queryOneString(conn unsafe.Pointer, sql string) (string, error) {
	res := wrapper.TaosQuery(conn, sql)
	if code := wrapper.TaosError(res); code != 0 {
		errStr := wrapper.TaosErrorStr(res)
		wrapper.TaosFreeResult(res)
		return "", fmt.Errorf("[%#x] %s", code&0xffff, errStr)
	}
	defer wrapper.TaosFreeResult(res)

	numFields := wrapper.TaosFieldCount(res)
	if numFields < 1 {
		return "", fmt.Errorf("query returned no columns")
	}
	headers, err := wrapper.ReadColumn(res, numFields)
	if err != nil {
		return "", err
	}
	row := wrapper.TaosFetchRow(res)
	if row == nil {
		return "", fmt.Errorf("query returned no rows")
	}
	precision := wrapper.TaosResultPrecision(res)
	lengths := wrapper.FetchLengths(res, numFields)
	value := wrapper.FetchRow(row, 0, headers.ColTypes[0], lengths[0], precision)
	switch v := value.(type) {
	case string:
		return v, nil
	case []byte:
		return string(v), nil
	default:
		return fmt.Sprintf("%v", v), nil
	}
}
