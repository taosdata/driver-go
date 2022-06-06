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
	go func() {
		for i := 0; i < 5; i++ {
			result = TaosQuery(conn, "insert into ct1 values(now,1,2,'1')")
			code = TaosError(result)
			if code != 0 {
				errStr := TaosErrorStr(result)
				TaosFreeResult(result)
				t.Error(errors.TaosError{Code: int32(code), ErrStr: errStr})
				return
			}
			TaosFreeResult(result)
			time.Sleep(time.Millisecond)
		}
	}()
	//build consumer
	conf := TMQConfNew()
	TMQConfSet(conf, "msg.with.table.name", "true")
	// auto commit default is true then the commitCallback function will be called after 5 seconds
	TMQConfSet(conf, "enable.auto.commit", "true")
	TMQConfSet(conf, "group.id", "tg2")
	c := make(chan *TMQCommitCallbackResult, 1)
	h := cgo.NewHandle(c)
	TMQConfSetAutoCommitCB(conf, h)
	go func() {
		for {
			select {
			case r := <-c:
				t.Log("auto commit", r)
				PutTMQCommitCallbackResult(r)
			}
		}
	}()
	tmq, err := TMQConsumerNew(conf)
	if err != nil {
		t.Error(err)
	}
	TMQConfDestroy(conf)
	//build_topic_list
	topicList := TMQListNew()
	TMQListAppend(topicList, "topic_ctb_column")

	//sync_consume_loop
	s := time.Now()
	errCode := TMQSubscribe(tmq, topicList)
	if errCode != 0 {
		errStr := TMQErr2Str(errCode)
		t.Error(errors.NewError(int(errCode), errStr))
		return
	}
	t.Log("sub", time.Now().Sub(s))
	errCode, list := TMQSubscription(tmq)
	if errCode != 0 {
		errStr := TMQErr2Str(errCode)
		t.Error(errors.NewError(int(errCode), errStr))
		return
	}
	size := TMQListGetSize(list)
	r := TMQListToCArray(list, int(size))
	assert.Equal(t, []string{"topic_ctb_column"}, r)
	c2 := make(chan *TMQCommitCallbackResult, 1)
	h2 := cgo.NewHandle(c2)
	for i := 0; i < 5; i++ {

		message := TMQConsumerPoll(tmq, 500)
		if message != nil {
			t.Log(message)
			topic := TMQGetTopicName(message)
			assert.Equal(t, "topic_ctb_column", topic)
			vgroupID := TMQGetVgroupID(message)
			t.Log("vgroupID", vgroupID)

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
				filedCount := TaosNumFields(message)
				rh, err := ReadColumn(message, filedCount)
				if err != nil {
					t.Error(err)
					return
				}
				precision := TaosResultPrecision(message)
				tableName := TMQGetTableName(message)
				assert.Equal(t, "ct1", tableName)
				data := ReadBlock(block, blockSize, rh.ColTypes, precision)
				t.Log(data)
			}
			TaosFreeResult(message)
			TMQCommitAsync(tmq, nil, h2)
			timer := time.NewTimer(time.Minute)
			select {
			case d := <-c2:
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

func TestTMQDB(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}

	result := TaosQuery(conn, "create database if not exists tmq_test_db vgroups 2")
	code := TaosError(result)
	if code != 0 {
		errStr := TaosErrorStr(result)
		TaosFreeResult(result)
		t.Error(errors.TaosError{Code: int32(code), ErrStr: errStr})
		return
	}
	TaosFreeResult(result)

	result = TaosQuery(conn, "use tmq_test_db")
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
	result = TaosQuery(conn, "create topic if not exists test_tmq_db_topic as DATABASE tmq_test_db")
	code = TaosError(result)
	if code != 0 {
		errStr := TaosErrorStr(result)
		TaosFreeResult(result)
		t.Error(errors.TaosError{Code: int32(code), ErrStr: errStr})
		return
	}
	TaosFreeResult(result)
	go func() {
		for i := 0; i < 5; i++ {
			t.Log("start insert")
			result = TaosQuery(conn, "insert into ct1 values(now,1,2,'1')")
			t.Log("finish insert")
			code = TaosError(result)
			t.Log("get error", code)
			if code != 0 {
				errStr := TaosErrorStr(result)
				TaosFreeResult(result)
				t.Error(errors.TaosError{Code: int32(code), ErrStr: errStr})
				return
			}
			t.Log("start free result")
			TaosFreeResult(result)
			t.Log("finish free result")
			time.Sleep(time.Millisecond)
		}
	}()
	//build consumer
	conf := TMQConfNew()
	// auto commit default is true then the commitCallback function will be called after 5 seconds
	TMQConfSet(conf, "enable.auto.commit", "true")
	TMQConfSet(conf, "group.id", "tg2")
	TMQConfSet(conf, "msg.with.table.name", "true")
	c := make(chan *TMQCommitCallbackResult, 1)
	h := cgo.NewHandle(c)
	TMQConfSetAutoCommitCB(conf, h)
	go func() {
		for {
			select {
			case r := <-c:
				t.Log("auto commit", r)
				PutTMQCommitCallbackResult(r)
			}
		}
	}()
	tmq, err := TMQConsumerNew(conf)
	if err != nil {
		t.Error(err)
	}
	TMQConfDestroy(conf)
	//build_topic_list
	topicList := TMQListNew()
	TMQListAppend(topicList, "test_tmq_db_topic")

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
	assert.Equal(t, []string{"test_tmq_db_topic"}, r)
	totalCount := 0
	c2 := make(chan *TMQCommitCallbackResult, 1)
	h2 := cgo.NewHandle(c2)
	for i := 0; i < 5; i++ {
		message := TMQConsumerPoll(tmq, 500)
		if message != nil {
			t.Log(message)
			topic := TMQGetTopicName(message)
			assert.Equal(t, "test_tmq_db_topic", topic)
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
				tableName := TMQGetTableName(message)
				assert.Equal(t, "ct1", tableName)
				filedCount := TaosNumFields(message)
				rh, err := ReadColumn(message, filedCount)
				if err != nil {
					t.Error(err)
					return
				}
				precision := TaosResultPrecision(message)
				totalCount += blockSize
				data := ReadBlock(block, blockSize, rh.ColTypes, precision)
				t.Log(data)
			}
			TaosFreeResult(message)

			TMQCommitAsync(tmq, nil, h2)
			timer := time.NewTimer(time.Minute)
			select {
			case d := <-c2:
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
	assert.GreaterOrEqual(t, totalCount, 5)
}

func TestTMQDBMultiTable(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}

	result := TaosQuery(conn, "create database if not exists tmq_test_db_multi vgroups 2")
	code := TaosError(result)
	if code != 0 {
		errStr := TaosErrorStr(result)
		TaosFreeResult(result)
		t.Error(errors.TaosError{Code: int32(code), ErrStr: errStr})
		return
	}
	TaosFreeResult(result)

	result = TaosQuery(conn, "use tmq_test_db_multi")
	code = TaosError(result)
	if code != 0 {
		errStr := TaosErrorStr(result)
		TaosFreeResult(result)
		t.Error(errors.TaosError{Code: int32(code), ErrStr: errStr})
		return
	}
	TaosFreeResult(result)

	result = TaosQuery(conn, "create table if not exists ct0 (ts timestamp, c1 int)")
	code = TaosError(result)
	if code != 0 {
		errStr := TaosErrorStr(result)
		TaosFreeResult(result)
		t.Error(errors.TaosError{Code: int32(code), ErrStr: errStr})
		return
	}
	TaosFreeResult(result)

	result = TaosQuery(conn, "create table if not exists ct1 (ts timestamp, c1 int, c2 float)")
	code = TaosError(result)
	if code != 0 {
		errStr := TaosErrorStr(result)
		TaosFreeResult(result)
		t.Error(errors.TaosError{Code: int32(code), ErrStr: errStr})
		return
	}
	TaosFreeResult(result)

	result = TaosQuery(conn, "create table if not exists ct2 (ts timestamp, c1 int, c2 float, c3 binary(10))")
	code = TaosError(result)
	if code != 0 {
		errStr := TaosErrorStr(result)
		TaosFreeResult(result)
		t.Error(errors.TaosError{Code: int32(code), ErrStr: errStr})
		return
	}
	TaosFreeResult(result)

	//create topic
	result = TaosQuery(conn, "create topic if not exists test_tmq_db_multi_topic as DATABASE tmq_test_db_multi")
	code = TaosError(result)
	if code != 0 {
		errStr := TaosErrorStr(result)
		TaosFreeResult(result)
		t.Error(errors.TaosError{Code: int32(code), ErrStr: errStr})
		return
	}
	TaosFreeResult(result)
	{
		result = TaosQuery(conn, "insert into ct0 values(now,1)")
		code = TaosError(result)
		if code != 0 {
			errStr := TaosErrorStr(result)
			TaosFreeResult(result)
			t.Error(errors.TaosError{Code: int32(code), ErrStr: errStr})
			return
		}
		TaosFreeResult(result)
	}
	{
		result = TaosQuery(conn, "insert into ct1 values(now,1,2)")
		code = TaosError(result)
		if code != 0 {
			errStr := TaosErrorStr(result)
			TaosFreeResult(result)
			t.Error(errors.TaosError{Code: int32(code), ErrStr: errStr})
			return
		}
		TaosFreeResult(result)
	}
	{
		result = TaosQuery(conn, "insert into ct2 values(now,1,2,'3')")
		code = TaosError(result)
		if code != 0 {
			errStr := TaosErrorStr(result)
			TaosFreeResult(result)
			t.Error(errors.TaosError{Code: int32(code), ErrStr: errStr})
			return
		}
		TaosFreeResult(result)
	}
	//build consumer
	conf := TMQConfNew()
	// auto commit default is true then the commitCallback function will be called after 5 seconds
	TMQConfSet(conf, "enable.auto.commit", "true")
	TMQConfSet(conf, "group.id", "tg2")
	TMQConfSet(conf, "msg.with.table.name", "true")
	c := make(chan *TMQCommitCallbackResult, 1)
	h := cgo.NewHandle(c)
	TMQConfSetAutoCommitCB(conf, h)
	go func() {
		for {
			select {
			case r := <-c:
				t.Log("auto commit", r)
				PutTMQCommitCallbackResult(r)
			}
		}
	}()
	tmq, err := TMQConsumerNew(conf)
	if err != nil {
		t.Error(err)
	}
	TMQConfDestroy(conf)
	//build_topic_list
	topicList := TMQListNew()
	TMQListAppend(topicList, "test_tmq_db_multi_topic")

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
	assert.Equal(t, []string{"test_tmq_db_multi_topic"}, r)
	totalCount := 0
	tables := map[string]struct{}{
		"ct0": {},
		"ct1": {},
		"ct2": {},
	}
	c2 := make(chan *TMQCommitCallbackResult, 1)
	h2 := cgo.NewHandle(c2)
	for i := 0; i < 5; i++ {
		message := TMQConsumerPoll(tmq, 500)
		if message != nil {
			t.Log(message)
			topic := TMQGetTopicName(message)
			assert.Equal(t, "test_tmq_db_multi_topic", topic)
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
				tableName := TMQGetTableName(message)
				delete(tables, tableName)
				filedCount := TaosNumFields(message)
				rh, err := ReadColumn(message, filedCount)
				if err != nil {
					t.Error(err)
					return
				}
				precision := TaosResultPrecision(message)
				totalCount += blockSize
				data := ReadBlock(block, blockSize, rh.ColTypes, precision)
				t.Log(data)
			}
			TaosFreeResult(message)

			TMQCommitAsync(tmq, nil, h2)
			timer := time.NewTimer(time.Minute)
			select {
			case d := <-c2:
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
	assert.GreaterOrEqual(t, totalCount, 3)
	assert.Emptyf(t, tables, "tables name not empty", tables)
}

func TestTMQDBMultiInsert(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}

	result := TaosQuery(conn, "create database if not exists tmq_test_db_multi_insert vgroups 2")
	code := TaosError(result)
	if code != 0 {
		errStr := TaosErrorStr(result)
		TaosFreeResult(result)
		t.Error(errors.TaosError{Code: int32(code), ErrStr: errStr})
		return
	}
	TaosFreeResult(result)

	result = TaosQuery(conn, "use tmq_test_db_multi_insert")
	code = TaosError(result)
	if code != 0 {
		errStr := TaosErrorStr(result)
		TaosFreeResult(result)
		t.Error(errors.TaosError{Code: int32(code), ErrStr: errStr})
		return
	}
	TaosFreeResult(result)

	result = TaosQuery(conn, "create table if not exists ct0 (ts timestamp, c1 int)")
	code = TaosError(result)
	if code != 0 {
		errStr := TaosErrorStr(result)
		TaosFreeResult(result)
		t.Error(errors.TaosError{Code: int32(code), ErrStr: errStr})
		return
	}
	TaosFreeResult(result)

	result = TaosQuery(conn, "create table if not exists ct1 (ts timestamp, c1 int, c2 float)")
	code = TaosError(result)
	if code != 0 {
		errStr := TaosErrorStr(result)
		TaosFreeResult(result)
		t.Error(errors.TaosError{Code: int32(code), ErrStr: errStr})
		return
	}
	TaosFreeResult(result)

	result = TaosQuery(conn, "create table if not exists ct2 (ts timestamp, c1 int, c2 float, c3 binary(10))")
	code = TaosError(result)
	if code != 0 {
		errStr := TaosErrorStr(result)
		TaosFreeResult(result)
		t.Error(errors.TaosError{Code: int32(code), ErrStr: errStr})
		return
	}
	TaosFreeResult(result)

	//create topic
	result = TaosQuery(conn, "create topic if not exists tmq_test_db_multi_insert_topic as DATABASE tmq_test_db_multi_insert")
	code = TaosError(result)
	if code != 0 {
		errStr := TaosErrorStr(result)
		TaosFreeResult(result)
		t.Error(errors.TaosError{Code: int32(code), ErrStr: errStr})
		return
	}
	TaosFreeResult(result)
	{
		result = TaosQuery(conn, "insert into ct0 values(now,1) ct1 values(now,1,2) ct2 values(now,1,2,'3')")
		code = TaosError(result)
		if code != 0 {
			errStr := TaosErrorStr(result)
			TaosFreeResult(result)
			t.Error(errors.TaosError{Code: int32(code), ErrStr: errStr})
			return
		}
		TaosFreeResult(result)
	}
	//build consumer
	conf := TMQConfNew()
	// auto commit default is true then the commitCallback function will be called after 5 seconds
	TMQConfSet(conf, "enable.auto.commit", "true")
	TMQConfSet(conf, "group.id", "tg2")
	TMQConfSet(conf, "msg.with.table.name", "true")
	c := make(chan *TMQCommitCallbackResult, 1)
	h := cgo.NewHandle(c)
	TMQConfSetAutoCommitCB(conf, h)
	go func() {
		for {
			select {
			case r := <-c:
				t.Log("auto commit", r)
				PutTMQCommitCallbackResult(r)
			}
		}
	}()
	tmq, err := TMQConsumerNew(conf)
	if err != nil {
		t.Error(err)
	}
	TMQConfDestroy(conf)
	//build_topic_list
	topicList := TMQListNew()
	TMQListAppend(topicList, "tmq_test_db_multi_insert_topic")

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
	assert.Equal(t, []string{"tmq_test_db_multi_insert_topic"}, r)
	totalCount := 0
	var tables [][]string
	c2 := make(chan *TMQCommitCallbackResult, 1)
	h2 := cgo.NewHandle(c2)
	for i := 0; i < 5; i++ {
		message := TMQConsumerPoll(tmq, 500)
		if message != nil {
			t.Log(message)
			topic := TMQGetTopicName(message)
			assert.Equal(t, "tmq_test_db_multi_insert_topic", topic)
			var table []string
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
				tableName := TMQGetTableName(message)
				table = append(table, tableName)
				filedCount := TaosNumFields(message)
				rh, err := ReadColumn(message, filedCount)
				if err != nil {
					t.Error(err)
					return
				}
				precision := TaosResultPrecision(message)
				totalCount += blockSize
				data := ReadBlock(block, blockSize, rh.ColTypes, precision)
				t.Log(data)
			}
			TaosFreeResult(message)

			TMQCommitAsync(tmq, nil, h2)
			timer := time.NewTimer(time.Minute)
			select {
			case d := <-c2:
				assert.Equal(t, int32(0), d.ErrCode)
				PutTMQCommitCallbackResult(d)
				timer.Stop()
				break
			case <-timer.C:
				timer.Stop()
				t.Error("wait tmq commit callback timeout")
				return
			}
			tables = append(tables, table)
		}
	}

	errCode = TMQConsumerClose(tmq)
	if errCode != 0 {
		errStr := TMQErr2Str(errCode)
		t.Error(errors.NewError(int(errCode), errStr))
		return
	}
	assert.GreaterOrEqual(t, totalCount, 3)
	t.Log(tables)
}
