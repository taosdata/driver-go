package wrapper

import (
	"database/sql/driver"
	"testing"
	"time"
	"unsafe"

	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/assert"
	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/common/parser"
	tmqcommon "github.com/taosdata/driver-go/v3/common/tmq"
	"github.com/taosdata/driver-go/v3/errors"
	"github.com/taosdata/driver-go/v3/wrapper/cgo"
)

func TestTMQ(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer TaosClose(conn)
	defer func() {
		result := TaosQuery(conn, "drop database if exists abc1")
		code := TaosError(result)
		if code != 0 {
			errStr := TaosErrorStr(result)
			TaosFreeResult(result)
			t.Error(errors.TaosError{Code: int32(code), ErrStr: errStr})
			return
		}
		TaosFreeResult(result)
	}()
	result := TaosQuery(conn, "create database if not exists abc1 vgroups 2 WAL_RETENTION_PERIOD 86400")
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
	defer func() {
		result = TaosQuery(conn, "drop topic if exists topic_ctb_column")
		code = TaosError(result)
		if code != 0 {
			errStr := TaosErrorStr(result)
			TaosFreeResult(result)
			t.Error(errors.TaosError{Code: int32(code), ErrStr: errStr})
			return
		}
		TaosFreeResult(result)
	}()
	result = TaosQuery(conn, "create topic if not exists topic_ctb_column as select ts, c1 from ct1")
	code = TaosError(result)
	if code != 0 {
		errStr := TaosErrorStr(result)
		TaosFreeResult(result)
		t.Error(errors.TaosError{Code: int32(code), ErrStr: errStr})
		return
	}
	TaosFreeResult(result)
	defer func() {
		result = TaosQuery(conn, "drop topic if exists topic_ctb_column")
		code = TaosError(result)
		if code != 0 {
			errStr := TaosErrorStr(result)
			TaosFreeResult(result)
			t.Error(errors.TaosError{Code: int32(code), ErrStr: errStr})
			return
		}
		TaosFreeResult(result)
	}()
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
		for r := range c {
			t.Log("auto commit", r)
			PutTMQCommitCallbackResult(r)
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
	t.Log("sub", time.Since(s))
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
				//tableName := TMQGetTableName(message)
				//assert.Equal(t, "ct1", tableName)
				dbName := TMQGetDBName(message)
				assert.Equal(t, "abc1", dbName)
				data := parser.ReadBlock(block, blockSize, rh.ColTypes, precision)
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
	defer TaosClose(conn)
	defer func() {
		result := TaosQuery(conn, "drop database if exists tmq_test_db")
		code := TaosError(result)
		if code != 0 {
			errStr := TaosErrorStr(result)
			TaosFreeResult(result)
			t.Error(errors.TaosError{Code: int32(code), ErrStr: errStr})
			return
		}
		TaosFreeResult(result)
	}()
	result := TaosQuery(conn, "create database if not exists tmq_test_db vgroups 2 WAL_RETENTION_PERIOD 86400")
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
	defer func() {
		result = TaosQuery(conn, "drop topic if exists test_tmq_db_topic")
		code = TaosError(result)
		if code != 0 {
			errStr := TaosErrorStr(result)
			TaosFreeResult(result)
			t.Error(errors.TaosError{Code: int32(code), ErrStr: errStr})
			return
		}
		TaosFreeResult(result)
	}()
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
	// auto commit default is true then the commitCallback function will be called after 5 seconds
	TMQConfSet(conf, "enable.auto.commit", "true")
	TMQConfSet(conf, "group.id", "tg2")
	TMQConfSet(conf, "msg.with.table.name", "true")
	c := make(chan *TMQCommitCallbackResult, 1)
	h := cgo.NewHandle(c)
	TMQConfSetAutoCommitCB(conf, h)
	go func() {
		for r := range c {
			t.Log("auto commit", r)
			PutTMQCommitCallbackResult(r)
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
				data := parser.ReadBlock(block, blockSize, rh.ColTypes, precision)
				t.Log(data)
			}
			TaosFreeResult(message)

			TMQCommitAsync(tmq, nil, h2)
			timer := time.NewTimer(time.Minute)
			select {
			case d := <-c2:
				assert.Nil(t, d.GetError())
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
	defer TaosClose(conn)
	defer func() {
		result := TaosQuery(conn, "drop database if exists tmq_test_db_multi")
		code := TaosError(result)
		if code != 0 {
			errStr := TaosErrorStr(result)
			TaosFreeResult(result)
			t.Error(errors.TaosError{Code: int32(code), ErrStr: errStr})
			return
		}
		TaosFreeResult(result)
	}()
	result := TaosQuery(conn, "create database if not exists tmq_test_db_multi vgroups 2 WAL_RETENTION_PERIOD 86400")
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
	defer func() {
		result = TaosQuery(conn, "drop topic if exists test_tmq_db_multi_topic")
		code = TaosError(result)
		if code != 0 {
			errStr := TaosErrorStr(result)
			TaosFreeResult(result)
			t.Error(errors.TaosError{Code: int32(code), ErrStr: errStr})
			return
		}
		TaosFreeResult(result)
	}()
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
		for r := range c {
			t.Log("auto commit", r)
			PutTMQCommitCallbackResult(r)
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
				data := parser.ReadBlock(block, blockSize, rh.ColTypes, precision)
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
	errCode = TMQUnsubscribe(tmq)
	if errCode != 0 {
		errStr := TMQErr2Str(errCode)
		t.Error(errors.NewError(int(errCode), errStr))
		return
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
	defer TaosClose(conn)
	defer func() {
		result := TaosQuery(conn, "drop database if exists tmq_test_db_multi_insert")
		code := TaosError(result)
		if code != 0 {
			errStr := TaosErrorStr(result)
			TaosFreeResult(result)
			t.Error(errors.TaosError{Code: int32(code), ErrStr: errStr})
			return
		}
		TaosFreeResult(result)
	}()
	result := TaosQuery(conn, "create database if not exists tmq_test_db_multi_insert vgroups 2 WAL_RETENTION_PERIOD 86400")
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
	defer func() {
		result = TaosQuery(conn, "drop topic if exists tmq_test_db_multi_insert_topic")
		code = TaosError(result)
		if code != 0 {
			errStr := TaosErrorStr(result)
			TaosFreeResult(result)
			t.Error(errors.TaosError{Code: int32(code), ErrStr: errStr})
			return
		}
		TaosFreeResult(result)
	}()
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
		for r := range c {
			t.Log("auto commit", r)
			PutTMQCommitCallbackResult(r)
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
				data := parser.ReadBlock(block, blockSize, rh.ColTypes, precision)
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

func TestTMQModify(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer TaosClose(conn)
	defer func() {
		result := TaosQuery(conn, "drop database if exists tmq_test_db_modify")
		code := TaosError(result)
		if code != 0 {
			errStr := TaosErrorStr(result)
			TaosFreeResult(result)
			t.Error(errors.TaosError{Code: int32(code), ErrStr: errStr})
			return
		}
		TaosFreeResult(result)
		result = TaosQuery(conn, "drop database if exists tmq_test_db_modify_target")
		code = TaosError(result)
		if code != 0 {
			errStr := TaosErrorStr(result)
			TaosFreeResult(result)
			t.Error(errors.TaosError{Code: int32(code), ErrStr: errStr})
			return
		}
		TaosFreeResult(result)
	}()

	result := TaosQuery(conn, "drop database if exists tmq_test_db_modify_target")
	code := TaosError(result)
	if code != 0 {
		errStr := TaosErrorStr(result)
		TaosFreeResult(result)
		t.Error(errors.TaosError{Code: int32(code), ErrStr: errStr})
		return
	}
	TaosFreeResult(result)

	result = TaosQuery(conn, "drop database if exists tmq_test_db_modify")
	code = TaosError(result)
	if code != 0 {
		errStr := TaosErrorStr(result)
		TaosFreeResult(result)
		t.Error(errors.TaosError{Code: int32(code), ErrStr: errStr})
		return
	}
	TaosFreeResult(result)

	result = TaosQuery(conn, "create database if not exists tmq_test_db_modify_target vgroups 2 WAL_RETENTION_PERIOD 86400")
	code = TaosError(result)
	if code != 0 {
		errStr := TaosErrorStr(result)
		TaosFreeResult(result)
		t.Error(errors.TaosError{Code: int32(code), ErrStr: errStr})
		return
	}
	TaosFreeResult(result)

	result = TaosQuery(conn, "create database if not exists tmq_test_db_modify vgroups 5 WAL_RETENTION_PERIOD 86400")
	code = TaosError(result)
	if code != 0 {
		errStr := TaosErrorStr(result)
		TaosFreeResult(result)
		t.Error(errors.TaosError{Code: int32(code), ErrStr: errStr})
		return
	}
	TaosFreeResult(result)

	result = TaosQuery(conn, "use tmq_test_db_modify")
	code = TaosError(result)
	if code != 0 {
		errStr := TaosErrorStr(result)
		TaosFreeResult(result)
		t.Error(errors.TaosError{Code: int32(code), ErrStr: errStr})
		return
	}
	TaosFreeResult(result)

	//create topic
	result = TaosQuery(conn, "create topic if not exists tmq_test_db_modify_topic with meta as DATABASE tmq_test_db_modify")
	code = TaosError(result)
	if code != 0 {
		errStr := TaosErrorStr(result)
		TaosFreeResult(result)
		t.Error(errors.TaosError{Code: int32(code), ErrStr: errStr})
		return
	}
	TaosFreeResult(result)
	defer func() {
		result = TaosQuery(conn, "drop topic if exists tmq_test_db_modify_topic")
		code = TaosError(result)
		if code != 0 {
			errStr := TaosErrorStr(result)
			TaosFreeResult(result)
			t.Error(errors.TaosError{Code: int32(code), ErrStr: errStr})
			return
		}
		TaosFreeResult(result)
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
		for r := range c {
			t.Log("auto commit", r)
			PutTMQCommitCallbackResult(r)
		}
	}()
	tmq, err := TMQConsumerNew(conf)
	if err != nil {
		t.Error(err)
	}
	TMQConfDestroy(conf)
	//build_topic_list
	topicList := TMQListNew()
	TMQListAppend(topicList, "tmq_test_db_modify_topic")

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
	assert.Equal(t, []string{"tmq_test_db_modify_topic"}, r)
	c2 := make(chan *TMQCommitCallbackResult, 1)
	h2 := cgo.NewHandle(c2)
	targetConn, err := TaosConnect("", "root", "taosdata", "tmq_test_db_modify_target", 0)
	assert.NoError(t, err)
	defer TaosFreeResult(targetConn)
	result = TaosQuery(conn, "create table stb (ts timestamp,"+
		"c1 bool,"+
		"c2 tinyint,"+
		"c3 smallint,"+
		"c4 int,"+
		"c5 bigint,"+
		"c6 tinyint unsigned,"+
		"c7 smallint unsigned,"+
		"c8 int unsigned,"+
		"c9 bigint unsigned,"+
		"c10 float,"+
		"c11 double,"+
		"c12 binary(20),"+
		"c13 nchar(20)"+
		")"+
		"tags(tts timestamp,"+
		"tc1 bool,"+
		"tc2 tinyint,"+
		"tc3 smallint,"+
		"tc4 int,"+
		"tc5 bigint,"+
		"tc6 tinyint unsigned,"+
		"tc7 smallint unsigned,"+
		"tc8 int unsigned,"+
		"tc9 bigint unsigned,"+
		"tc10 float,"+
		"tc11 double,"+
		"tc12 binary(20),"+
		"tc13 nchar(20)"+
		")")
	code = TaosError(result)
	if code != 0 {
		errStr := TaosErrorStr(result)
		TaosFreeResult(result)
		t.Error(errors.TaosError{Code: int32(code), ErrStr: errStr})
		return
	}
	TaosFreeResult(result)

	pool := func(cb func(*tmqcommon.Meta, unsafe.Pointer)) {
		message := TMQConsumerPoll(tmq, 500)
		assert.NotNil(t, message)
		topic := TMQGetTopicName(message)
		assert.Equal(t, "tmq_test_db_modify_topic", topic)
		messageType := TMQGetResType(message)
		assert.Equal(t, int32(common.TMQ_RES_TABLE_META), messageType)
		pointer := TMQGetJsonMeta(message)
		assert.NotNil(t, pointer)
		data := ParseJsonMeta(pointer)
		var meta tmqcommon.Meta
		err = jsoniter.Unmarshal(data, &meta)
		assert.NoError(t, err)

		defer TaosFreeResult(message)

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
			cb(nil, nil)
			return
		}
		errCode, rawMeta := TMQGetRaw(message)
		if errCode != errors.SUCCESS {
			errStr := TaosErrorStr(result)
			TaosFreeResult(result)
			t.Error(errors.NewError(int(errCode), errStr))
			return
		}
		cb(&meta, rawMeta)
		TMQFreeRaw(rawMeta)
	}

	pool(func(meta *tmqcommon.Meta, rawMeta unsafe.Pointer) {
		assert.Equal(t, "create", meta.Type)
		assert.Equal(t, "stb", meta.TableName)
		assert.Equal(t, "super", meta.TableType)
		assert.NoError(t, err)
		length, metaType, data := ParseRawMeta(rawMeta)
		r2 := BuildRawMeta(length, metaType, data)
		errCode = TMQWriteRaw(targetConn, r2)
		if errCode != 0 {
			errStr := TMQErr2Str(errCode)
			t.Error(errors.NewError(int(errCode), errStr))
			return
		}
		d, err := query(targetConn, "describe stb")
		assert.NoError(t, err)
		assert.Equal(t, [][]driver.Value{
			{"ts", "TIMESTAMP", int32(8), ""},
			{"c1", "BOOL", int32(1), ""},
			{"c2", "TINYINT", int32(1), ""},
			{"c3", "SMALLINT", int32(2), ""},
			{"c4", "INT", int32(4), ""},
			{"c5", "BIGINT", int32(8), ""},
			{"c6", "TINYINT UNSIGNED", int32(1), ""},
			{"c7", "SMALLINT UNSIGNED", int32(2), ""},
			{"c8", "INT UNSIGNED", int32(4), ""},
			{"c9", "BIGINT UNSIGNED", int32(8), ""},
			{"c10", "FLOAT", int32(4), ""},
			{"c11", "DOUBLE", int32(8), ""},
			{"c12", "VARCHAR", int32(20), ""},
			{"c13", "NCHAR", int32(20), ""},
			{"tts", "TIMESTAMP", int32(8), "TAG"},
			{"tc1", "BOOL", int32(1), "TAG"},
			{"tc2", "TINYINT", int32(1), "TAG"},
			{"tc3", "SMALLINT", int32(2), "TAG"},
			{"tc4", "INT", int32(4), "TAG"},
			{"tc5", "BIGINT", int32(8), "TAG"},
			{"tc6", "TINYINT UNSIGNED", int32(1), "TAG"},
			{"tc7", "SMALLINT UNSIGNED", int32(2), "TAG"},
			{"tc8", "INT UNSIGNED", int32(4), "TAG"},
			{"tc9", "BIGINT UNSIGNED", int32(8), "TAG"},
			{"tc10", "FLOAT", int32(4), "TAG"},
			{"tc11", "DOUBLE", int32(8), "TAG"},
			{"tc12", "VARCHAR", int32(20), "TAG"},
			{"tc13", "NCHAR", int32(20), "TAG"},
		}, d)
	})

	TMQUnsubscribe(tmq)
	errCode = TMQConsumerClose(tmq)
	if errCode != 0 {
		errStr := TMQErr2Str(errCode)
		t.Error(errors.NewError(int(errCode), errStr))
		return
	}
}

func TestTMQAutoCreateTable(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer TaosClose(conn)
	defer func() {
		result := TaosQuery(conn, "drop database if exists tmq_test_auto_create")
		code := TaosError(result)
		if code != 0 {
			errStr := TaosErrorStr(result)
			TaosFreeResult(result)
			t.Error(errors.TaosError{Code: int32(code), ErrStr: errStr})
			return
		}
		TaosFreeResult(result)
	}()
	result := TaosQuery(conn, "create database if not exists tmq_test_auto_create vgroups 2 WAL_RETENTION_PERIOD 86400")
	code := TaosError(result)
	if code != 0 {
		errStr := TaosErrorStr(result)
		TaosFreeResult(result)
		t.Error(errors.TaosError{Code: int32(code), ErrStr: errStr})
		return
	}
	TaosFreeResult(result)

	result = TaosQuery(conn, "use tmq_test_auto_create")
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

	//create topic
	result = TaosQuery(conn, "create topic if not exists test_tmq_auto_topic with meta as DATABASE tmq_test_auto_create")
	code = TaosError(result)
	if code != 0 {
		errStr := TaosErrorStr(result)
		TaosFreeResult(result)
		t.Error(errors.TaosError{Code: int32(code), ErrStr: errStr})
		return
	}
	TaosFreeResult(result)
	defer func() {
		result = TaosQuery(conn, "drop topic if exists test_tmq_auto_topic")
		code = TaosError(result)
		if code != 0 {
			errStr := TaosErrorStr(result)
			TaosFreeResult(result)
			t.Error(errors.TaosError{Code: int32(code), ErrStr: errStr})
			return
		}
		TaosFreeResult(result)
	}()
	result = TaosQuery(conn, "insert into ct1 using st1 tags(2000) values(now,1,2,'1')")
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
	// auto commit default is true then the commitCallback function will be called after 5 seconds
	TMQConfSet(conf, "enable.auto.commit", "true")
	TMQConfSet(conf, "group.id", "tg2")
	TMQConfSet(conf, "msg.with.table.name", "true")
	c := make(chan *TMQCommitCallbackResult, 1)
	h := cgo.NewHandle(c)
	TMQConfSetAutoCommitCB(conf, h)
	go func() {
		for r := range c {
			t.Log("auto commit", r)
			PutTMQCommitCallbackResult(r)
		}
	}()
	tmq, err := TMQConsumerNew(conf)
	if err != nil {
		t.Error(err)
	}
	TMQConfDestroy(conf)
	//build_topic_list
	topicList := TMQListNew()
	TMQListAppend(topicList, "test_tmq_auto_topic")

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
	assert.Equal(t, []string{"test_tmq_auto_topic"}, r)
	totalCount := 0
	c2 := make(chan *TMQCommitCallbackResult, 1)
	h2 := cgo.NewHandle(c2)
	for i := 0; i < 5; i++ {
		message := TMQConsumerPoll(tmq, 500)
		if message != nil {
			t.Log(message)
			topic := TMQGetTopicName(message)
			assert.Equal(t, "test_tmq_auto_topic", topic)
			messageType := TMQGetResType(message)
			if messageType != common.TMQ_RES_METADATA {
				continue
			}
			pointer := TMQGetJsonMeta(message)
			data := ParseJsonMeta(pointer)
			t.Log(string(data))
			var meta tmqcommon.Meta
			err = jsoniter.Unmarshal(data, &meta)
			assert.NoError(t, err)
			assert.Equal(t, "create", meta.Type)
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
				data := parser.ReadBlock(block, blockSize, rh.ColTypes, precision)
				t.Log(data)
			}
			TaosFreeResult(message)

			TMQCommitAsync(tmq, nil, h2)
			timer := time.NewTimer(time.Minute)
			select {
			case d := <-c2:
				assert.Nil(t, d.GetError())
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
	assert.GreaterOrEqual(t, totalCount, 1)
}
