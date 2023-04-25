package tmq

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/taosdata/driver-go/v3/common/tmq"
	"github.com/taosdata/driver-go/v3/errors"
	"github.com/taosdata/driver-go/v3/wrapper"
)

func TestTmq(t *testing.T) {
	conn, err := wrapper.TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}

	defer func() {
		result := wrapper.TaosQuery(conn, "drop database if exists af_test_tmq")
		code := wrapper.TaosError(result)
		if code != 0 {
			errStr := wrapper.TaosErrorStr(result)
			wrapper.TaosFreeResult(result)
			t.Error(errors.TaosError{Code: int32(code), ErrStr: errStr})
			return
		}
		wrapper.TaosFreeResult(result)
	}()

	result := wrapper.TaosQuery(conn, "create database if not exists af_test_tmq vgroups 2 WAL_RETENTION_PERIOD 86400")
	code := wrapper.TaosError(result)
	if code != 0 {
		errStr := wrapper.TaosErrorStr(result)
		wrapper.TaosFreeResult(result)
		t.Error(errors.TaosError{Code: int32(code), ErrStr: errStr})
		return
	}
	wrapper.TaosFreeResult(result)

	result = wrapper.TaosQuery(conn, "use af_test_tmq")
	code = wrapper.TaosError(result)
	if code != 0 {
		errStr := wrapper.TaosErrorStr(result)
		wrapper.TaosFreeResult(result)
		t.Error(errors.TaosError{Code: int32(code), ErrStr: errStr})
		return
	}
	wrapper.TaosFreeResult(result)
	result = wrapper.TaosQuery(conn, "create stable if not exists all_type (ts timestamp,"+
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
		") tags(t1 int)")
	code = wrapper.TaosError(result)
	if code != 0 {
		errStr := wrapper.TaosErrorStr(result)
		wrapper.TaosFreeResult(result)
		t.Error(errors.TaosError{Code: int32(code), ErrStr: errStr})
		return
	}
	wrapper.TaosFreeResult(result)

	result = wrapper.TaosQuery(conn, "create table if not exists ct0 using all_type tags(1000)")
	code = wrapper.TaosError(result)
	if code != 0 {
		errStr := wrapper.TaosErrorStr(result)
		wrapper.TaosFreeResult(result)
		t.Error(errors.TaosError{Code: int32(code), ErrStr: errStr})
		return
	}
	wrapper.TaosFreeResult(result)

	result = wrapper.TaosQuery(conn, "create table if not exists ct1 using all_type tags(2000)")
	code = wrapper.TaosError(result)
	if code != 0 {
		errStr := wrapper.TaosErrorStr(result)
		wrapper.TaosFreeResult(result)
		t.Error(errors.TaosError{Code: int32(code), ErrStr: errStr})
		return
	}
	wrapper.TaosFreeResult(result)

	result = wrapper.TaosQuery(conn, "create table if not exists ct3 using all_type tags(3000)")
	code = wrapper.TaosError(result)
	if code != 0 {
		errStr := wrapper.TaosErrorStr(result)
		wrapper.TaosFreeResult(result)
		t.Error(errors.TaosError{Code: int32(code), ErrStr: errStr})
		return
	}
	wrapper.TaosFreeResult(result)

	//create topic
	result = wrapper.TaosQuery(conn, "create topic if not exists test_tmq_common as select ts,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13 from ct1")
	code = wrapper.TaosError(result)
	if code != 0 {
		errStr := wrapper.TaosErrorStr(result)
		wrapper.TaosFreeResult(result)
		t.Error(errors.TaosError{Code: int32(code), ErrStr: errStr})
		return
	}
	wrapper.TaosFreeResult(result)
	defer func() {
		result = wrapper.TaosQuery(conn, "drop topic if exists test_tmq_common")
		code = wrapper.TaosError(result)
		if code != 0 {
			errStr := wrapper.TaosErrorStr(result)
			wrapper.TaosFreeResult(result)
			t.Error(errors.TaosError{Code: int32(code), ErrStr: errStr})
			return
		}
		wrapper.TaosFreeResult(result)
	}()
	now := time.Now()
	result = wrapper.TaosQuery(conn, fmt.Sprintf("insert into ct1 values('%s',true,2,3,4,5,6,7,8,9,10,11,'1','2')", now.Format(time.RFC3339Nano)))
	code = wrapper.TaosError(result)
	if code != 0 {
		errStr := wrapper.TaosErrorStr(result)
		wrapper.TaosFreeResult(result)
		t.Error(errors.TaosError{Code: int32(code), ErrStr: errStr})
		return
	}
	wrapper.TaosFreeResult(result)

	assert.NoError(t, err)
	consumer, err := NewConsumer(&tmq.ConfigMap{
		"group.id":                     "test",
		"auto.offset.reset":            "earliest",
		"td.connect.ip":                "127.0.0.1",
		"td.connect.user":              "root",
		"td.connect.pass":              "taosdata",
		"td.connect.port":              "6030",
		"client.id":                    "test_tmq_c",
		"enable.auto.commit":           "false",
		"experimental.snapshot.enable": "true",
		"msg.with.table.name":          "true",
	})
	if err != nil {
		t.Error(err)
		return
	}
	err = consumer.Subscribe("test_tmq_common", nil)
	if err != nil {
		t.Error(err)
		return
	}
	for i := 0; i < 5; i++ {
		ev := consumer.Poll(500)
		switch e := ev.(type) {
		case *tmq.DataMessage:
			row1 := e.Value().([]*tmq.Data)[0].Data[0]
			assert.Equal(t, "af_test_tmq", e.DBName())
			assert.Equal(t, now.UnixNano()/1e6, row1[0].(time.Time).UnixNano()/1e6)
			assert.Equal(t, true, row1[1].(bool))
			assert.Equal(t, int8(2), row1[2].(int8))
			assert.Equal(t, int16(3), row1[3].(int16))
			assert.Equal(t, int32(4), row1[4].(int32))
			assert.Equal(t, int64(5), row1[5].(int64))
			assert.Equal(t, uint8(6), row1[6].(uint8))
			assert.Equal(t, uint16(7), row1[7].(uint16))
			assert.Equal(t, uint32(8), row1[8].(uint32))
			assert.Equal(t, uint64(9), row1[9].(uint64))
			assert.Equal(t, float32(10), row1[10].(float32))
			assert.Equal(t, float64(11), row1[11].(float64))
			assert.Equal(t, "1", row1[12].(string))
			assert.Equal(t, "2", row1[13].(string))
			_, err = consumer.Commit()
			assert.NoError(t, err)
			err = consumer.Unsubscribe()
			assert.NoError(t, err)
			err = consumer.Close()
			assert.NoError(t, err)
			return
		case tmq.Error:
			t.Error(e)
			return
		default:
			t.Error("unexpected", e)
			return
		}
	}
}
