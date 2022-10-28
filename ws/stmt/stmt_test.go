package stmt

import (
	"database/sql/driver"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"testing"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/assert"
	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/common/param"
	taosErrors "github.com/taosdata/driver-go/v3/errors"
	"github.com/taosdata/driver-go/v3/ws/client"
)

func prepareEnv() error {
	var err error
	steps := []string{
		"drop database if exists test_ws_stmt",
		"create database test_ws_stmt",
		"create table test_ws_stmt.all_json(ts timestamp," +
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
			")" +
			"tags(t json)",
		"create table test_ws_stmt.all_all(" +
			"ts timestamp," +
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
			")" +
			"tags(" +
			"tts timestamp," +
			"tc1 bool," +
			"tc2 tinyint," +
			"tc3 smallint," +
			"tc4 int," +
			"tc5 bigint," +
			"tc6 tinyint unsigned," +
			"tc7 smallint unsigned," +
			"tc8 int unsigned," +
			"tc9 bigint unsigned," +
			"tc10 float," +
			"tc11 double," +
			"tc12 binary(20)," +
			"tc13 nchar(20))",
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
		"drop database if exists test_ws_stmt",
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

func query(payload string) (*common.TDEngineRestfulResp, error) {
	body := strings.NewReader(payload)
	req, _ := http.NewRequest(http.MethodPost, "http://127.0.0.1:6041/rest/sql", body)
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("http code: %d", resp.StatusCode)
	}
	return marshalBody(resp.Body, 512)
}

func TestStmt(t *testing.T) {
	err := prepareEnv()
	if err != nil {
		t.Error(err)
		return
	}
	defer cleanEnv()
	now := time.Now()
	config := NewConfig("ws://127.0.0.1:6041/rest/stmt", 0)
	config.SetConnectUser("root")
	config.SetConnectPass("taosdata")
	config.SetConnectDB("test_ws_stmt")
	config.SetMessageTimeout(common.DefaultMessageTimeout)
	config.SetWriteWait(common.DefaultWriteWait)
	config.SetErrorHandler(func(connector *Connector, err error) {
		t.Log(err)
	})
	config.SetCloseHandler(func() {
		t.Log("stmt websocket closed")
	})
	connector, err := NewConnector(config)
	if err != nil {
		t.Error(err)
		return
	}
	defer connector.Close()
	{
		stmt, err := connector.Init()
		if err != nil {
			t.Error(err)
			return
		}
		err = stmt.Prepare("insert into ? using all_json tags(?) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
		if err != nil {
			t.Error(err)
			return
		}
		err = stmt.SetTableName("tb1")
		if err != nil {
			t.Error(err)
			return
		}
		err = stmt.SetTags(param.NewParam(1).AddJson([]byte(`{"tb":1}`)), param.NewColumnType(1).AddJson(0))
		if err != nil {
			t.Error(err)
			return
		}
		params := []*param.Param{
			param.NewParam(3).AddTimestamp(now, 0).AddTimestamp(now.Add(time.Second), 0).AddTimestamp(now.Add(time.Second*2), 0),
			param.NewParam(3).AddBool(true).AddNull().AddBool(true),
			param.NewParam(3).AddTinyint(1).AddNull().AddTinyint(1),
			param.NewParam(3).AddSmallint(1).AddNull().AddSmallint(1),
			param.NewParam(3).AddInt(1).AddNull().AddInt(1),
			param.NewParam(3).AddBigint(1).AddNull().AddBigint(1),
			param.NewParam(3).AddUTinyint(1).AddNull().AddUTinyint(1),
			param.NewParam(3).AddUSmallint(1).AddNull().AddUSmallint(1),
			param.NewParam(3).AddUInt(1).AddNull().AddUInt(1),
			param.NewParam(3).AddUBigint(1).AddNull().AddUBigint(1),
			param.NewParam(3).AddFloat(1).AddNull().AddFloat(1),
			param.NewParam(3).AddDouble(1).AddNull().AddDouble(1),
			param.NewParam(3).AddBinary([]byte("test_binary")).AddNull().AddBinary([]byte("test_binary")),
			param.NewParam(3).AddNchar("test_nchar").AddNull().AddNchar("test_nchar"),
		}
		paramTypes := param.NewColumnType(14).
			AddTimestamp().
			AddBool().
			AddTinyint().
			AddSmallint().
			AddInt().
			AddBigint().
			AddUTinyint().
			AddUSmallint().
			AddUInt().
			AddUBigint().
			AddFloat().
			AddDouble().
			AddBinary(0).
			AddNchar(0)
		err = stmt.BindParam(params, paramTypes)
		if err != nil {
			t.Error(err)
			return
		}
		err = stmt.AddBatch()
		if err != nil {
			t.Error(err)
			return
		}
		err = stmt.Exec()
		if err != nil {
			t.Error(err)
			return
		}
		affected := stmt.GetAffectedRows()
		if !assert.Equal(t, 3, affected) {
			return
		}
		err = stmt.Close()
		if err != nil {
			t.Error(err)
			return
		}
		result, err := query("select * from test_ws_stmt.all_json order by ts")
		if err != nil {
			t.Error(err)
			return
		}
		assert.Equal(t, 0, result.Code, result)
		assert.Equal(t, 3, len(result.Data))
		assert.Equal(t, 15, len(result.ColTypes))
		row1 := result.Data[0]
		assert.Equal(t, now.UnixNano()/1e6, row1[0].(time.Time).UnixNano()/1e6)
		assert.Equal(t, true, row1[1])
		assert.Equal(t, int8(1), row1[2])
		assert.Equal(t, int16(1), row1[3])
		assert.Equal(t, int32(1), row1[4])
		assert.Equal(t, int64(1), row1[5])
		assert.Equal(t, uint8(1), row1[6])
		assert.Equal(t, uint16(1), row1[7])
		assert.Equal(t, uint32(1), row1[8])
		assert.Equal(t, uint64(1), row1[9])
		assert.Equal(t, float32(1), row1[10])
		assert.Equal(t, float64(1), row1[11])
		assert.Equal(t, "test_binary", row1[12])
		assert.Equal(t, "test_nchar", row1[13])
		assert.Equal(t, []byte(`{"tb":1}`), row1[14])
		row2 := result.Data[1]
		assert.Equal(t, now.Add(time.Second).UnixNano()/1e6, row2[0].(time.Time).UnixNano()/1e6)
		for i := 1; i < 14; i++ {
			assert.Nil(t, row2[i])
		}
		assert.Equal(t, []byte(`{"tb":1}`), row2[14])
		row3 := result.Data[2]
		assert.Equal(t, now.Add(time.Second*2).UnixNano()/1e6, row3[0].(time.Time).UnixNano()/1e6)
		assert.Equal(t, true, row3[1])
		assert.Equal(t, int8(1), row3[2])
		assert.Equal(t, int16(1), row3[3])
		assert.Equal(t, int32(1), row3[4])
		assert.Equal(t, int64(1), row3[5])
		assert.Equal(t, uint8(1), row3[6])
		assert.Equal(t, uint16(1), row3[7])
		assert.Equal(t, uint32(1), row3[8])
		assert.Equal(t, uint64(1), row3[9])
		assert.Equal(t, float32(1), row3[10])
		assert.Equal(t, float64(1), row3[11])
		assert.Equal(t, "test_binary", row3[12])
		assert.Equal(t, "test_nchar", row3[13])
		assert.Equal(t, []byte(`{"tb":1}`), row3[14])
	}
	{
		stmt, err := connector.Init()
		if err != nil {
			t.Error(err)
			return
		}
		err = stmt.Prepare("insert into ? using all_all tags(?,?,?,?,?,?,?,?,?,?,?,?,?,?) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
		err = stmt.SetTableName("tb1")
		if err != nil {
			t.Error(err)
			return
		}

		err = stmt.SetTableName("tb2")
		if err != nil {
			t.Error(err)
			return
		}
		err = stmt.SetTags(
			param.NewParam(14).
				AddTimestamp(now, 0).
				AddBool(true).
				AddTinyint(2).
				AddSmallint(2).
				AddInt(2).
				AddBigint(2).
				AddUTinyint(2).
				AddUSmallint(2).
				AddUInt(2).
				AddUBigint(2).
				AddFloat(2).
				AddDouble(2).
				AddBinary([]byte("tb2")).
				AddNchar("tb2"),
			param.NewColumnType(14).
				AddTimestamp().
				AddBool().
				AddTinyint().
				AddSmallint().
				AddInt().
				AddBigint().
				AddUTinyint().
				AddUSmallint().
				AddUInt().
				AddUBigint().
				AddFloat().
				AddDouble().
				AddBinary(0).
				AddNchar(0),
		)
		if err != nil {
			t.Error(err)
			return
		}
		params := []*param.Param{
			param.NewParam(3).AddTimestamp(now, 0).AddTimestamp(now.Add(time.Second), 0).AddTimestamp(now.Add(time.Second*2), 0),
			param.NewParam(3).AddBool(true).AddNull().AddBool(true),
			param.NewParam(3).AddTinyint(1).AddNull().AddTinyint(1),
			param.NewParam(3).AddSmallint(1).AddNull().AddSmallint(1),
			param.NewParam(3).AddInt(1).AddNull().AddInt(1),
			param.NewParam(3).AddBigint(1).AddNull().AddBigint(1),
			param.NewParam(3).AddUTinyint(1).AddNull().AddUTinyint(1),
			param.NewParam(3).AddUSmallint(1).AddNull().AddUSmallint(1),
			param.NewParam(3).AddUInt(1).AddNull().AddUInt(1),
			param.NewParam(3).AddUBigint(1).AddNull().AddUBigint(1),
			param.NewParam(3).AddFloat(1).AddNull().AddFloat(1),
			param.NewParam(3).AddDouble(1).AddNull().AddDouble(1),
			param.NewParam(3).AddBinary([]byte("test_binary")).AddNull().AddBinary([]byte("test_binary")),
			param.NewParam(3).AddNchar("test_nchar").AddNull().AddNchar("test_nchar"),
		}
		paramTypes := param.NewColumnType(14).
			AddTimestamp().
			AddBool().
			AddTinyint().
			AddSmallint().
			AddInt().
			AddBigint().
			AddUTinyint().
			AddUSmallint().
			AddUInt().
			AddUBigint().
			AddFloat().
			AddDouble().
			AddBinary(0).
			AddNchar(0)
		err = stmt.BindParam(params, paramTypes)
		if err != nil {
			t.Error(err)
			return
		}
		err = stmt.AddBatch()
		if err != nil {
			t.Error(err)
			return
		}
		err = stmt.Exec()
		if err != nil {
			t.Error(err)
			return
		}
		affected := stmt.GetAffectedRows()
		if !assert.Equal(t, 3, affected) {
			return
		}
		err = stmt.Close()
		if err != nil {
			t.Error(err)
			return
		}
		result, err := query("select * from test_ws_stmt.all_all order by ts")
		if err != nil {
			t.Error(err)
			return
		}
		assert.Equal(t, 3, affected)
		assert.Equal(t, 0, result.Code, result)
		assert.Equal(t, 3, len(result.Data))
		assert.Equal(t, 28, len(result.ColTypes))
		row1 := result.Data[0]
		assert.Equal(t, now.UnixNano()/1e6, row1[0].(time.Time).UnixNano()/1e6)
		assert.Equal(t, true, row1[1])
		assert.Equal(t, int8(1), row1[2])
		assert.Equal(t, int16(1), row1[3])
		assert.Equal(t, int32(1), row1[4])
		assert.Equal(t, int64(1), row1[5])
		assert.Equal(t, uint8(1), row1[6])
		assert.Equal(t, uint16(1), row1[7])
		assert.Equal(t, uint32(1), row1[8])
		assert.Equal(t, uint64(1), row1[9])
		assert.Equal(t, float32(1), row1[10])
		assert.Equal(t, float64(1), row1[11])
		assert.Equal(t, "test_binary", row1[12])
		assert.Equal(t, "test_nchar", row1[13])
		assert.Equal(t, now.UnixNano()/1e6, row1[14].(time.Time).UnixNano()/1e6)
		assert.Equal(t, true, row1[15])
		assert.Equal(t, int8(2), row1[16])
		assert.Equal(t, int16(2), row1[17])
		assert.Equal(t, int32(2), row1[18])
		assert.Equal(t, int64(2), row1[19])
		assert.Equal(t, uint8(2), row1[20])
		assert.Equal(t, uint16(2), row1[21])
		assert.Equal(t, uint32(2), row1[22])
		assert.Equal(t, uint64(2), row1[23])
		assert.Equal(t, float32(2), row1[24])
		assert.Equal(t, float64(2), row1[25])
		assert.Equal(t, "tb2", row1[26])
		assert.Equal(t, "tb2", row1[27])
		row2 := result.Data[1]
		assert.Equal(t, now.Add(time.Second).UnixNano()/1e6, row2[0].(time.Time).UnixNano()/1e6)
		for i := 1; i < 14; i++ {
			assert.Nil(t, row2[i])
		}
		assert.Equal(t, now.UnixNano()/1e6, row1[14].(time.Time).UnixNano()/1e6)
		assert.Equal(t, true, row1[15])
		assert.Equal(t, int8(2), row1[16])
		assert.Equal(t, int16(2), row1[17])
		assert.Equal(t, int32(2), row1[18])
		assert.Equal(t, int64(2), row1[19])
		assert.Equal(t, uint8(2), row1[20])
		assert.Equal(t, uint16(2), row1[21])
		assert.Equal(t, uint32(2), row1[22])
		assert.Equal(t, uint64(2), row1[23])
		assert.Equal(t, float32(2), row1[24])
		assert.Equal(t, float64(2), row1[25])
		assert.Equal(t, "tb2", row1[26])
		assert.Equal(t, "tb2", row1[27])
		row3 := result.Data[2]
		assert.Equal(t, now.Add(time.Second*2).UnixNano()/1e6, row3[0].(time.Time).UnixNano()/1e6)
		assert.Equal(t, true, row3[1])
		assert.Equal(t, int8(1), row3[2])
		assert.Equal(t, int16(1), row3[3])
		assert.Equal(t, int32(1), row3[4])
		assert.Equal(t, int64(1), row3[5])
		assert.Equal(t, uint8(1), row3[6])
		assert.Equal(t, uint16(1), row3[7])
		assert.Equal(t, uint32(1), row3[8])
		assert.Equal(t, uint64(1), row3[9])
		assert.Equal(t, float32(1), row3[10])
		assert.Equal(t, float64(1), row3[11])
		assert.Equal(t, "test_binary", row3[12])
		assert.Equal(t, "test_nchar", row3[13])
		assert.Equal(t, now.UnixNano()/1e6, row3[14].(time.Time).UnixNano()/1e6)
		assert.Equal(t, true, row3[15])
		assert.Equal(t, int8(2), row3[16])
		assert.Equal(t, int16(2), row3[17])
		assert.Equal(t, int32(2), row3[18])
		assert.Equal(t, int64(2), row3[19])
		assert.Equal(t, uint8(2), row3[20])
		assert.Equal(t, uint16(2), row3[21])
		assert.Equal(t, uint32(2), row3[22])
		assert.Equal(t, uint64(2), row3[23])
		assert.Equal(t, float32(2), row3[24])
		assert.Equal(t, float64(2), row3[25])
		assert.Equal(t, "tb2", row3[26])
		assert.Equal(t, "tb2", row3[27])
	}
}

func marshalBody(body io.Reader, bufferSize int) (*common.TDEngineRestfulResp, error) {
	var result common.TDEngineRestfulResp
	iter := client.JsonI.BorrowIterator(make([]byte, bufferSize))
	defer client.JsonI.ReturnIterator(iter)
	iter.Reset(body)
	timeFormat := time.RFC3339Nano
	iter.ReadObjectCB(func(iter *jsoniter.Iterator, s string) bool {
		switch s {
		case "code":
			result.Code = iter.ReadInt()
		case "desc":
			result.Desc = iter.ReadString()
		case "column_meta":
			iter.ReadArrayCB(func(iter *jsoniter.Iterator) bool {
				index := 0
				iter.ReadArrayCB(func(iter *jsoniter.Iterator) bool {
					switch index {
					case 0:
						result.ColNames = append(result.ColNames, iter.ReadString())
						index = 1
					case 1:
						typeStr := iter.ReadString()
						t, exist := common.NameTypeMap[typeStr]
						if exist {
							result.ColTypes = append(result.ColTypes, t)
						} else {
							iter.ReportError("unsupported type in column_meta", typeStr)
						}
						index = 2
					case 2:
						result.ColLength = append(result.ColLength, iter.ReadInt64())
						index = 0
					}
					return true
				})
				return true
			})
		case "data":
			columnCount := len(result.ColTypes)
			column := 0
			iter.ReadArrayCB(func(iter *jsoniter.Iterator) bool {
				column = 0
				var row = make([]driver.Value, columnCount)
				iter.ReadArrayCB(func(iter *jsoniter.Iterator) bool {
					defer func() {
						column += 1
					}()
					columnType := result.ColTypes[column]
					if columnType == common.TSDB_DATA_TYPE_JSON {
						row[column] = iter.SkipAndReturnBytes()
						return true
					}
					if iter.ReadNil() {
						row[column] = nil
						return true
					}
					var err error
					switch columnType {
					case common.TSDB_DATA_TYPE_NULL:
						iter.Skip()
						row[column] = nil
					case common.TSDB_DATA_TYPE_BOOL:
						row[column] = iter.ReadAny().ToBool()
					case common.TSDB_DATA_TYPE_TINYINT:
						row[column] = iter.ReadInt8()
					case common.TSDB_DATA_TYPE_SMALLINT:
						row[column] = iter.ReadInt16()
					case common.TSDB_DATA_TYPE_INT:
						row[column] = iter.ReadInt32()
					case common.TSDB_DATA_TYPE_BIGINT:
						row[column] = iter.ReadInt64()
					case common.TSDB_DATA_TYPE_FLOAT:
						row[column] = iter.ReadFloat32()
					case common.TSDB_DATA_TYPE_DOUBLE:
						row[column] = iter.ReadFloat64()
					case common.TSDB_DATA_TYPE_BINARY:
						row[column] = iter.ReadString()
					case common.TSDB_DATA_TYPE_TIMESTAMP:
						b := iter.ReadString()
						row[column], err = time.Parse(timeFormat, b)
						if err != nil {
							iter.ReportError("parse time", err.Error())
						}
					case common.TSDB_DATA_TYPE_NCHAR:
						row[column] = iter.ReadString()
					case common.TSDB_DATA_TYPE_UTINYINT:
						row[column] = iter.ReadUint8()
					case common.TSDB_DATA_TYPE_USMALLINT:
						row[column] = iter.ReadUint16()
					case common.TSDB_DATA_TYPE_UINT:
						row[column] = iter.ReadUint32()
					case common.TSDB_DATA_TYPE_UBIGINT:
						row[column] = iter.ReadUint64()
					default:
						row[column] = nil
						iter.Skip()
					}
					return iter.Error == nil
				})
				if iter.Error != nil {
					return false
				}
				result.Data = append(result.Data, row)
				return true
			})
		case "rows":
			result.Rows = iter.ReadInt()
		default:
			iter.Skip()
		}
		return iter.Error == nil
	})
	if iter.Error != nil && iter.Error != io.EOF {
		return nil, iter.Error
	}
	return &result, nil
}
