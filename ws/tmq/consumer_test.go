package tmq

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"testing"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/assert"
	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/common/tmq"
	taosErrors "github.com/taosdata/driver-go/v3/errors"
	"github.com/taosdata/driver-go/v3/ws/client"
)

func prepareEnv() error {
	var err error
	steps := []string{
		"drop topic if exists test_ws_tmq_topic",
		"drop database if exists test_ws_tmq",
		"create database test_ws_tmq WAL_RETENTION_PERIOD 86400",
		"create topic test_ws_tmq_topic as database test_ws_tmq",
	}
	for _, step := range steps {
		err = doRequest(step)
		if err != nil {
			return err
		}
	}
	return nil
}

func cleanEnv() error {
	var err error
	time.Sleep(2 * time.Second)
	steps := []string{
		"drop topic if exists test_ws_tmq_topic",
		"drop database if exists test_ws_tmq",
	}
	for _, step := range steps {
		err = doRequest(step)
		if err != nil {
			return err
		}
	}
	return nil
}

func doRequest(payload string) error {
	body := strings.NewReader(payload)
	req, _ := http.NewRequest(http.MethodPost, "http://127.0.0.1:6041/rest/sql", body)
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("http code: %d", resp.StatusCode)
	}
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	iter := client.JsonI.BorrowIterator(data)
	code := int32(0)
	desc := ""
	iter.ReadObjectCB(func(iter *jsoniter.Iterator, s string) bool {
		switch s {
		case "code":
			code = iter.ReadInt32()
		case "desc":
			desc = iter.ReadString()
		default:
			iter.Skip()
		}
		return iter.Error == nil
	})
	client.JsonI.ReturnIterator(iter)
	if code != 0 {
		return taosErrors.NewError(int(code), desc)
	}
	return nil
}

// @author: xftan
// @date: 2023/10/13 11:36
// @description: test tmq subscribe over websocket
func TestConsumer(t *testing.T) {
	err := prepareEnv()
	if err != nil {
		t.Error(err)
		return
	}
	defer cleanEnv()
	now := time.Now()
	go func() {
		err = doRequest("create table test_ws_tmq.t_all(ts timestamp," +
			"c1 bool," +
			"c2 tinyint," +
			"c3 smallint," +
			"c4 int," +
			"c5 bigint," +
			"c6 tinyint unsigned," +
			"c7 smallint unsigned," +
			"c8 int unsigned," +
			"c9 bigint unsigned," +
			"c10 float," +
			"c11 double," +
			"c12 binary(20)," +
			"c13 nchar(20)" +
			")")
		if err != nil {
			t.Error(err)
			return
		}
		err = doRequest(fmt.Sprintf("insert into test_ws_tmq.t_all values('%s',true,2,3,4,5,6,7,8,9,10.123,11.123,'binary','nchar')", now.Format(time.RFC3339Nano)))
		if err != nil {
			t.Error(err)
			return
		}
	}()
	consumer, err := NewConsumer(&tmq.ConfigMap{
		"ws.url":                  "ws://127.0.0.1:6041/rest/tmq",
		"ws.message.channelLen":   uint(0),
		"ws.message.timeout":      common.DefaultMessageTimeout,
		"ws.message.writeWait":    common.DefaultWriteWait,
		"td.connect.user":         "root",
		"td.connect.pass":         "taosdata",
		"group.id":                "test",
		"client.id":               "test_consumer",
		"auto.offset.reset":       "earliest",
		"enable.auto.commit":      "true",
		"auto.commit.interval.ms": "5000",
		"msg.with.table.name":     "true",
	})
	if err != nil {
		t.Error(err)
		return
	}
	defer consumer.Close()
	topic := []string{"test_ws_tmq_topic"}
	err = consumer.SubscribeTopics(topic, nil)
	if err != nil {
		t.Error(err)
		return
	}
	gotData := false
	for i := 0; i < 5; i++ {
		if gotData {
			return
		}
		ev := consumer.Poll(0)
		if ev != nil {
			switch e := ev.(type) {
			case *tmq.DataMessage:
				gotData = true
				data := e.Value().([]*tmq.Data)
				assert.Equal(t, "test_ws_tmq", e.DBName())
				assert.Equal(t, 1, len(data))
				assert.Equal(t, "t_all", data[0].TableName)
				assert.Equal(t, 1, len(data[0].Data))
				assert.Equal(t, now.Unix(), data[0].Data[0][0].(time.Time).Unix())
				var v = data[0].Data[0]
				assert.Equal(t, true, v[1].(bool))
				assert.Equal(t, int8(2), v[2].(int8))
				assert.Equal(t, int16(3), v[3].(int16))
				assert.Equal(t, int32(4), v[4].(int32))
				assert.Equal(t, int64(5), v[5].(int64))
				assert.Equal(t, uint8(6), v[6].(uint8))
				assert.Equal(t, uint16(7), v[7].(uint16))
				assert.Equal(t, uint32(8), v[8].(uint32))
				assert.Equal(t, uint64(9), v[9].(uint64))
				assert.Equal(t, float32(10.123), v[10].(float32))
				assert.Equal(t, float64(11.123), v[11].(float64))
				assert.Equal(t, "binary", v[12].(string))
				assert.Equal(t, "nchar", v[13].(string))
				t.Log(e.Offset())
				ass, err := consumer.Assignment()
				t.Log(ass)
				committed, err := consumer.Committed(ass, 0)
				t.Log(committed)
				position, _ := consumer.Position(ass)
				t.Log(position)
				offsets, err := consumer.Position([]tmq.TopicPartition{e.TopicPartition})
				assert.NoError(t, err)
				_, err = consumer.CommitOffsets(offsets)
				assert.NoError(t, err)
				ass, err = consumer.Assignment()
				t.Log(ass)
				committed, err = consumer.Committed(ass, 0)
				t.Log(committed)
				position, _ = consumer.Position(ass)
				t.Log(position)
			case tmq.Error:
				t.Error(e)
				return
			default:
				t.Error("unexpected", e)
				return
			}

		}

		if err != nil {
			t.Error(err)
			return
		}
	}
	if !gotData {
		t.Error("no data got")
	}
	err = consumer.Unsubscribe()
	if err != nil {
		t.Error(err)
		return
	}
}

func prepareSeekEnv() error {
	var err error
	steps := []string{
		"drop topic if exists test_ws_tmq_seek_topic",
		"drop database if exists test_ws_tmq_seek",
		"create database test_ws_tmq_seek vgroups 1 WAL_RETENTION_PERIOD 86400",
		"create topic test_ws_tmq_seek_topic as database test_ws_tmq_seek",
		"create table test_ws_tmq_seek.t1(ts timestamp,v int)",
		"insert into test_ws_tmq_seek.t1 values (now,1)",
	}
	for _, step := range steps {
		err = doRequest(step)
		if err != nil {
			return err
		}
	}
	return nil
}

func cleanSeekEnv() error {
	var err error
	time.Sleep(2 * time.Second)
	steps := []string{
		"drop topic if exists test_ws_tmq_seek_topic",
		"drop database if exists test_ws_tmq_seek",
	}
	for _, step := range steps {
		err = doRequest(step)
		if err != nil {
			return err
		}
	}
	return nil
}

// @author: xftan
// @date: 2023/10/13 11:36
// @description: test tmq seek over websocket
func TestSeek(t *testing.T) {
	err := prepareSeekEnv()
	if err != nil {
		t.Error(err)
		return
	}
	defer cleanSeekEnv()
	consumer, err := NewConsumer(&tmq.ConfigMap{
		"ws.url":                       "ws://127.0.0.1:6041/rest/tmq",
		"ws.message.channelLen":        uint(0),
		"ws.message.timeout":           common.DefaultMessageTimeout,
		"ws.message.writeWait":         common.DefaultWriteWait,
		"td.connect.user":              "root",
		"td.connect.pass":              "taosdata",
		"group.id":                     "test",
		"client.id":                    "test_consumer",
		"auto.offset.reset":            "earliest",
		"enable.auto.commit":           "false",
		"experimental.snapshot.enable": "false",
		"msg.with.table.name":          "true",
	})
	if err != nil {
		t.Error(err)
		return
	}
	defer consumer.Close()
	topic := []string{"test_ws_tmq_seek_topic"}
	err = consumer.SubscribeTopics(topic, nil)
	if err != nil {
		t.Error(err)
		return
	}
	partitions, err := consumer.Assignment()
	assert.NoError(t, err)
	assert.Equal(t, 1, len(partitions))
	assert.Equal(t, "test_ws_tmq_seek_topic", *partitions[0].Topic)
	assert.Equal(t, tmq.Offset(0), partitions[0].Offset)

	//poll
	messageOffset := tmq.Offset(0)
	haveMessage := false
	for i := 0; i < 5; i++ {
		event := consumer.Poll(500)
		if event != nil {
			haveMessage = true
			_, err = consumer.Commit()
			assert.NoError(t, err)
			messageOffset = event.(*tmq.DataMessage).Offset()
		}
	}
	assert.True(t, haveMessage)
	partitions, err = consumer.Assignment()
	assert.NoError(t, err)
	assert.Equal(t, 1, len(partitions))
	assert.Equal(t, "test_ws_tmq_seek_topic", *partitions[0].Topic)
	assert.GreaterOrEqual(t, partitions[0].Offset, messageOffset)

	//seek
	tmpTopic := "test_ws_tmq_seek_topic"
	err = consumer.Seek(tmq.TopicPartition{
		Topic:     &tmpTopic,
		Partition: partitions[0].Partition,
		Offset:    0,
	}, 0)
	assert.NoError(t, err)

	//assignment
	partitions, err = consumer.Assignment()
	assert.NoError(t, err)
	assert.Equal(t, 1, len(partitions))
	assert.Equal(t, "test_ws_tmq_seek_topic", *partitions[0].Topic)
	assert.Equal(t, tmq.Offset(0), partitions[0].Offset)

	//poll
	messageOffset = tmq.Offset(0)
	haveMessage = false
	for i := 0; i < 5; i++ {
		event := consumer.Poll(500)
		if event != nil {
			haveMessage = true
			messageOffset = event.(*tmq.DataMessage).Offset()
			_, err = consumer.Commit()
			assert.NoError(t, err)
		}
	}
	partitions, err = consumer.Assignment()
	assert.True(t, haveMessage)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(partitions))
	assert.Equal(t, "test_ws_tmq_seek_topic", *partitions[0].Topic)
	assert.GreaterOrEqual(t, partitions[0].Offset, messageOffset)
}
