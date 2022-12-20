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
	taosErrors "github.com/taosdata/driver-go/v3/errors"
	"github.com/taosdata/driver-go/v3/ws/client"
)

func prepareEnv() error {
	var err error
	steps := []string{
		"drop topic if exists test_ws_tmq_topic",
		"drop database if exists test_ws_tmq",
		"create database test_ws_tmq",
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
	config := NewConfig("ws://127.0.0.1:6041/rest/tmq", 0)
	config.SetConnectUser("root")
	config.SetConnectPass("taosdata")
	config.SetGroupID("test")
	config.SetClientID("test_consumer")
	config.SetAutoOffsetReset("earliest")
	config.SetMessageTimeout(common.DefaultMessageTimeout)
	config.SetWriteWait(common.DefaultWriteWait)
	config.SetErrorHandler(func(consumer *Consumer, err error) {
		t.Log(err)
	})
	config.SetCloseHandler(func() {
		t.Log("tmq websocket closed")
	})
	consumer, err := NewConsumer(config)
	if err != nil {
		t.Error(err)
		return
	}
	defer consumer.Close()
	topic := []string{"test_ws_tmq_topic"}
	err = consumer.Subscribe(topic)
	if err != nil {
		t.Error(err)
		return
	}

	gotData := false
	for i := 0; i < 5; i++ {
		if gotData {
			return
		}
		result, err := consumer.Poll(0)
		if err != nil {
			t.Error(err)
			return
		}
		if result != nil {
			switch result.Type {
			case common.TMQ_RES_DATA:
				gotData = true
				assert.Equal(t, "test_ws_tmq", result.DBName)
				assert.Equal(t, 1, len(result.Data))
				assert.Equal(t, "t_all", result.Data[0].TableName)
				assert.Equal(t, 1, len(result.Data[0].Data))
				assert.Equal(t, now.Unix(), result.Data[0].Data[0][0].(time.Time).Unix())
				var v = result.Data[0].Data[0]
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
			case common.TMQ_RES_TABLE_META:
				assert.Equal(t, "test_ws_tmq", result.DBName)
				assert.Equal(t, "create", result.Meta.Type)
				assert.Equal(t, "t_all", result.Meta.TableName)
				assert.Equal(t, "normal", result.Meta.TableType)
				assert.Equal(t, []*common.Column{
					{
						Name:   "ts",
						Type:   9,
						Length: 0,
					},
					{
						Name:   "c1",
						Type:   1,
						Length: 0,
					},
					{
						Name:   "c2",
						Type:   2,
						Length: 0,
					},
					{
						Name:   "c3",
						Type:   3,
						Length: 0,
					},
					{
						Name:   "c4",
						Type:   4,
						Length: 0,
					},
					{
						Name:   "c5",
						Type:   5,
						Length: 0,
					},
					{
						Name:   "c6",
						Type:   11,
						Length: 0,
					},
					{
						Name:   "c7",
						Type:   12,
						Length: 0,
					},
					{
						Name:   "c8",
						Type:   13,
						Length: 0,
					},
					{
						Name:   "c9",
						Type:   14,
						Length: 0,
					},
					{
						Name:   "c10",
						Type:   6,
						Length: 0,
					},
					{
						Name:   "c11",
						Type:   7,
						Length: 0,
					},
					{
						Name:   "c12",
						Type:   8,
						Length: 20,
					},
					{
						Name:   "c13",
						Type:   10,
						Length: 20,
					}}, result.Meta.Columns)
			}
		}
	}
	if !gotData {
		t.Error("no data got")
	}
}
