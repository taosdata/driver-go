package wrapper

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/taosdata/driver-go/v2/errors"
	"github.com/taosdata/driver-go/v2/wrapper/cgo"
)

func TestTMQ(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}

	result := TaosQuery(conn, "create database if not exists abc1 vgroups 2")
	code := TaosError(result)
	if code != 0 {
		errStr := TaosErrorStr(result)
		TaosFreeResult(result)
		t.Error(errors.TaosError{Code: int32(code), ErrStr: errStr})
		return
	}
	TaosFreeResult(result)

	result = TaosQuery(conn, "use abc1")
	code = TaosError(result)
	if code != 0 {
		errStr := TaosErrorStr(result)
		TaosFreeResult(result)
		t.Error(errors.TaosError{Code: int32(code), ErrStr: errStr})
		return
	}
	TaosFreeResult(result)

	result = TaosQuery(conn, "create stable if not exists st1 (ts timestamp, c1 int, c2 float, c3 binary(10)) tags(t1 int)")
	code = TaosError(result)
	if code != 0 {
		errStr := TaosErrorStr(result)
		TaosFreeResult(result)
		t.Error(errors.TaosError{Code: int32(code), ErrStr: errStr})
		return
	}
	TaosFreeResult(result)

	result = TaosQuery(conn, "create table if not exists ct0 using st1 tags(1000)")
	code = TaosError(result)
	if code != 0 {
		errStr := TaosErrorStr(result)
		TaosFreeResult(result)
		t.Error(errors.TaosError{Code: int32(code), ErrStr: errStr})
		return
	}
	TaosFreeResult(result)

	result = TaosQuery(conn, "create table if not exists ct1 using st1 tags(2000)")
	code = TaosError(result)
	if code != 0 {
		errStr := TaosErrorStr(result)
		TaosFreeResult(result)
		t.Error(errors.TaosError{Code: int32(code), ErrStr: errStr})
		return
	}
	TaosFreeResult(result)

	result = TaosQuery(conn, "create table if not exists ct3 using st1 tags(3000)")
	code = TaosError(result)
	if code != 0 {
		errStr := TaosErrorStr(result)
		TaosFreeResult(result)
		t.Error(errors.TaosError{Code: int32(code), ErrStr: errStr})
		return
	}
	TaosFreeResult(result)

	//create topic
	result = TaosQuery(conn, "create topic if not exists topic_ctb_column as select ts, c1 from ct1")
	code = TaosError(result)
	if code != 0 {
		errStr := TaosErrorStr(result)
		TaosFreeResult(result)
		t.Error(errors.TaosError{Code: int32(code), ErrStr: errStr})
		return
	}
	TaosFreeResult(result)

	result = TaosQuery(conn, "insert into ct1 values(now,1,2,'1')")
	code = TaosError(result)
	if code != 0 {
		errStr := TaosErrorStr(result)
		TaosFreeResult(result)
		t.Error(errors.TaosError{Code: int32(code), ErrStr: errStr})
		return
	}
	TaosFreeResult(result)
	//build consumer
	conf := TMQConfNew()
	TMQConfSet(conf, "group.id", "tg2")
	c := make(chan *TMQCommitCallbackResult, 1)
	h := cgo.NewHandle(c)
	TMQConfSetOffsetCommitCB(conf, h)
	tmq, err := TMQConsumerNew(conf)
	if err != nil {
		t.Error(err)
	}
	TMQConfDestroy(conf)
	//build_topic_list
	topicList := TMQListNew()
	TMQListAppend(topicList, "topic_ctb_column")

	//sync_consume_loop
	errCode := TMQSubscribe(tmq, topicList)
	if errCode != 0 {
		errStr := TMQErr2Str(errCode)
		t.Error(errors.NewError(int(errCode), errStr))
		return
	}
	errCode, list := TMQSubscription(tmq)
	if errCode != 0 {
		errStr := TMQErr2Str(errCode)
		t.Error(errors.NewError(int(errCode), errStr))
		return
	}
	size := TMQListGetSize(list)
	r := TMQListToCArray(list, int(size))
	assert.Equal(t, []string{"1.topic_ctb_column"}, r)
	for i := 0; i < 10; i++ {
		message := TMQConsumerPoll(tmq, 500)
		if message != nil {
			t.Log(message)
			topic := TMQGetTopicName(message)
			assert.Equal(t, "topic_ctb_column", topic)
			fileCount := TaosNumFields(message)
			rh, err := ReadColumn(message, fileCount)
			if err != nil {
				t.Error(err)
				return
			}
			precision := TaosResultPrecision(message)
			for {
				blockSize, errCode, block := TaosFetchRawBlock(message)
				if errCode != int(errors.SUCCESS) {
					errStr := TaosErrorStr(message)
					err := errors.NewError(errCode, errStr)
					t.Error(err)
					TaosFreeResult(message)
					return
				}
				if blockSize == 0 {
					break
				}
				data := ReadBlock(block, blockSize, rh.ColTypes, precision)
				t.Log(data)
			}
			TaosFreeResult(message)

			TMQCommit(tmq, nil, true)
			timer := time.NewTimer(time.Minute)
			select {
			case d := <-c:
				assert.Equal(t, int32(0), d.ErrCode)
				PutTMQCommitCallbackResult(d)
				timer.Stop()
				break
			case <-timer.C:
				timer.Stop()
				t.Error("wait tmq commit callback timeout")
				return
			}
		}
	}

	errCode = TMQConsumerClose(tmq)
	if errCode != 0 {
		errStr := TMQErr2Str(errCode)
		t.Error(errors.NewError(int(errCode), errStr))
		return
	}
}

func TestTMQList(t *testing.T) {
	list := TMQListNew()
	TMQListAppend(list, "1")
	TMQListAppend(list, "2")
	TMQListAppend(list, "3")
	size := TMQListGetSize(list)
	r := TMQListToCArray(list, int(size))
	assert.Equal(t, []string{"1", "2", "3"}, r)
	TMQListDestroy(list)
}
