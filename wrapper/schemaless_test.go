package wrapper_test

import (
	"testing"
	"time"
	"unsafe"

	"github.com/taosdata/driver-go/v3/errors"
	"github.com/taosdata/driver-go/v3/wrapper"
)

func prepareEnv() unsafe.Pointer {
	conn, err := wrapper.TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		panic(err)
		return nil
	}
	res := wrapper.TaosQuery(conn, "create database if not exists test_schemaless_common")
	if wrapper.TaosError(res) != 0 {
		errStr := wrapper.TaosErrorStr(res)
		wrapper.TaosFreeResult(res)
		panic(errStr)
	}
	wrapper.TaosFreeResult(res)
	code := wrapper.TaosSelectDB(conn, "test_schemaless_common")
	if code != 0 {
		panic("use db test_schemaless_common fail")
	}
	return conn
}

func cleanEnv(conn unsafe.Pointer) {
	res := wrapper.TaosQuery(conn, "drop database if exists test_schemaless_common")
	if wrapper.TaosError(res) != 0 {
		errStr := wrapper.TaosErrorStr(res)
		wrapper.TaosFreeResult(res)
		panic(errStr)
	}
	wrapper.TaosFreeResult(res)
}

func BenchmarkTelnetSchemaless(b *testing.B) {
	conn := prepareEnv()
	defer wrapper.TaosClose(conn)
	defer cleanEnv(conn)
	for i := 0; i < b.N; i++ {
		result := wrapper.TaosSchemalessInsert(conn, []string{
			"sys_if_bytes_out 1636626444 1.3E3 host=web01 interface=eth0",
		}, wrapper.OpenTSDBTelnetLineProtocol, "")
		code := wrapper.TaosError(result)
		if code != 0 {
			errStr := wrapper.TaosErrorStr(result)
			wrapper.TaosFreeResult(result)
			b.Error(errors.NewError(code, errStr))
			return
		}
		wrapper.TaosFreeResult(result)
	}
}

// @author: xftan
// @date: 2022/1/27 17:26
// @description: test schemaless opentsdb telnet
func TestSchemalessTelnet(t *testing.T) {
	conn := prepareEnv()
	defer wrapper.TaosClose(conn)
	defer cleanEnv(conn)
	result := wrapper.TaosSchemalessInsert(conn, []string{
		"sys_if_bytes_out 1636626444 1.3E3 host=web01 interface=eth0",
	}, wrapper.OpenTSDBTelnetLineProtocol, "")
	code := wrapper.TaosError(result)
	if code != 0 {
		errStr := wrapper.TaosErrorStr(result)
		wrapper.TaosFreeResult(result)
		t.Error(errors.NewError(code, errStr))
		return
	}
	wrapper.TaosFreeResult(result)
	s := time.Now()
	result = wrapper.TaosSchemalessInsert(conn, []string{
		"sys_if_bytes_out 1636626444 1.3E3 host=web01 interface=eth0",
	}, wrapper.OpenTSDBTelnetLineProtocol, "")
	code = wrapper.TaosError(result)
	if code != 0 {
		errStr := wrapper.TaosErrorStr(result)
		wrapper.TaosFreeResult(result)
		t.Error(errors.NewError(code, errStr))
		return
	}
	wrapper.TaosFreeResult(result)
	t.Log("finish ", time.Now().Sub(s))
}

// @author: xftan
// @date: 2022/1/27 17:26
// @description: test schemaless influxDB
func TestSchemalessInfluxDB(t *testing.T) {
	conn := prepareEnv()
	defer wrapper.TaosClose(conn)
	defer cleanEnv(conn)
	{
		result := wrapper.TaosSchemalessInsert(conn, []string{
			"measurement,host=host1 field1=2i,field2=2.0 1577836800000000000",
		}, wrapper.InfluxDBLineProtocol, "")
		code := wrapper.TaosError(result)
		if code != 0 {
			errStr := wrapper.TaosErrorStr(result)
			wrapper.TaosFreeResult(result)
			t.Error(errors.NewError(code, errStr))
			return
		}
		wrapper.TaosFreeResult(result)
	}
	{
		result := wrapper.TaosSchemalessInsert(conn, []string{
			"measurement,host=host1 field1=2i,field2=2.0 1577836800000000000",
		}, wrapper.InfluxDBLineProtocol, "ns")
		code := wrapper.TaosError(result)
		if code != 0 {
			errStr := wrapper.TaosErrorStr(result)
			wrapper.TaosFreeResult(result)
			t.Error(errors.NewError(code, errStr))
			return
		}
		wrapper.TaosFreeResult(result)
	}
	{
		result := wrapper.TaosSchemalessInsert(conn, []string{
			"measurement,host=host1 field1=2i,field2=2.0 1577836800000000",
		}, wrapper.InfluxDBLineProtocol, "u")
		code := wrapper.TaosError(result)
		if code != 0 {
			errStr := wrapper.TaosErrorStr(result)
			wrapper.TaosFreeResult(result)
			t.Error(errors.NewError(code, errStr))
			return
		}
		wrapper.TaosFreeResult(result)
	}
	{
		result := wrapper.TaosSchemalessInsert(conn, []string{
			"measurement,host=host1 field1=2i,field2=2.0 1577836800000000",
		}, wrapper.InfluxDBLineProtocol, "μ")
		code := wrapper.TaosError(result)
		if code != 0 {
			errStr := wrapper.TaosErrorStr(result)
			wrapper.TaosFreeResult(result)
			t.Error(errors.NewError(code, errStr))
			return
		}
		wrapper.TaosFreeResult(result)
	}
	{
		result := wrapper.TaosSchemalessInsert(conn, []string{
			"measurement,host=host1 field1=2i,field2=2.0 1577836800000",
		}, wrapper.InfluxDBLineProtocol, "ms")
		code := wrapper.TaosError(result)
		if code != 0 {
			errStr := wrapper.TaosErrorStr(result)
			wrapper.TaosFreeResult(result)
			t.Error(errors.NewError(code, errStr))
			return
		}
		wrapper.TaosFreeResult(result)
	}
	{
		result := wrapper.TaosSchemalessInsert(conn, []string{
			"measurement,host=host1 field1=2i,field2=2.0 1577836800",
		}, wrapper.InfluxDBLineProtocol, "s")
		code := wrapper.TaosError(result)
		if code != 0 {
			errStr := wrapper.TaosErrorStr(result)
			wrapper.TaosFreeResult(result)
			t.Error(errors.NewError(code, errStr))
			return
		}
		wrapper.TaosFreeResult(result)
	}
	{
		result := wrapper.TaosSchemalessInsert(conn, []string{
			"measurement,host=host1 field1=2i,field2=2.0 26297280",
		}, wrapper.InfluxDBLineProtocol, "m")
		code := wrapper.TaosError(result)
		if code != 0 {
			errStr := wrapper.TaosErrorStr(result)
			wrapper.TaosFreeResult(result)
			t.Error(errors.NewError(code, errStr))
			return
		}
		wrapper.TaosFreeResult(result)
	}
	{
		result := wrapper.TaosSchemalessInsert(conn, []string{
			"measurement,host=host1 field1=2i,field2=2.0 438288",
		}, wrapper.InfluxDBLineProtocol, "h")
		code := wrapper.TaosError(result)
		if code != 0 {
			errStr := wrapper.TaosErrorStr(result)
			wrapper.TaosFreeResult(result)
			t.Error(errors.NewError(code, errStr))
			return
		}
		wrapper.TaosFreeResult(result)
	}
}

func TestTaosSchemalessInsertRaw(t *testing.T) {
	conn := prepareEnv()
	defer wrapper.TaosClose(conn)
	//defer cleanEnv(conn)
	cases := []struct {
		name      string
		data      string
		rows      int
		code      int
		protocol  int
		precision string
	}{
		{
			name:      "schemaless_1",
			data:      "measurement,host=host1 field1=2i,field2=2.0 26297280",
			rows:      1,
			code:      0,
			protocol:  wrapper.InfluxDBLineProtocol,
			precision: "m",
		},
		{
			name: "schemaless_2",
			data: "measurement,host=host1 field1=2i,field2=2.0 438288\n" +
				"measurement,host=host1 field1=2i,field2=2.0 438289\n" +
				"measurement,host=host1 field1=2i,field2=2.0 438290\n" +
				"measurement,host=host1 field1=2i,field2=2.0 438291\n",
			rows:      4,
			code:      0,
			protocol:  wrapper.InfluxDBLineProtocol,
			precision: "h",
		},
		{
			name:      "schemaless_3",
			data:      "measurement,host=host2\\0 field1=2i,field2=2.0 438292",
			rows:      1,
			code:      0,
			protocol:  wrapper.InfluxDBLineProtocol,
			precision: "h",
		},
		{
			name: "schemaless_4",
			data: "measurement,host=host2\\0 field1=2i,field2=2.0 438293\n" +
				"measurement,host=host2\\0 field1=2i,field2=2.0 438294\n" +
				"measurement,host=host2\\0 field1=2i,field2=2.0,field3=\"test\\0\" 438295\n" +
				"measurement,host=host2\\0 field1=2i,field2=2.0,field3=\"测试\\0\" 438296\n",
			rows:      4,
			code:      0,
			protocol:  wrapper.InfluxDBLineProtocol,
			precision: "h",
		},
		{
			name: "schemaless_4",
			data: `measurement,host=host2\\0 field1=2i,field2=2.0 438293
measurement,host=host2\\0 field1=2i,field2=2.0 438294
measurement,host=host2\\0 field1=2i,field2=2.0,field3="test\0" 438295
measurement,host=host2\\0 field1=2i,field2=2.0,field3="测试\0" 438296`,
			rows:      4,
			code:      0,
			protocol:  wrapper.InfluxDBLineProtocol,
			precision: "h",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(tt *testing.T) {
			result, rows := wrapper.TaosSchemalessInsertRaw(conn, c.data, c.protocol, c.precision)
			if code := wrapper.TaosError(result); code != c.code || rows != c.rows {
				tt.Fatal(errors.NewError(code, wrapper.TaosErrorStr(result)))
			}
			wrapper.TaosFreeResult(result)
		})
	}
}
