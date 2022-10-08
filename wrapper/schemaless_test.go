package wrapper_test

import (
	"strings"
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

func TestSchemalessRawTelnet(t *testing.T) {
	conn := prepareEnv()
	defer wrapper.TaosClose(conn)
	defer cleanEnv(conn)
	type in struct {
		rows []string
	}
	data := []in{
		{
			rows: []string{"sys_if_bytes_out 1636626444 1.3E3 host=web01 interface=eth0"},
		},
		{
			rows: []string{"sys_if_bytes_out 1636626444 1.3E3 host=web01 interface=eth0"},
		},
	}
	for _, d := range data {
		row := strings.Join(d.rows, "\n")
		totalRows, result := wrapper.TaosSchemalessInsertRaw(conn, row, wrapper.OpenTSDBTelnetLineProtocol, "")
		code := wrapper.TaosError(result)
		if code != 0 {
			errStr := wrapper.TaosErrorStr(result)
			wrapper.TaosFreeResult(result)
			t.Log(row)
			t.Error(errors.NewError(code, errStr))
			return
		}
		if int(totalRows) != len(d.rows) {
			t.Log(row)
			t.Errorf("expect rows %d got %d", len(d.rows), totalRows)
		}
		affected := wrapper.TaosAffectedRows(result)
		if affected != len(d.rows) {
			t.Log(row)
			t.Errorf("expect affected %d got %d", len(d.rows), affected)
		}
		wrapper.TaosFreeResult(result)
	}
}

func TestSchemalessRawInfluxDB(t *testing.T) {
	conn := prepareEnv()
	defer wrapper.TaosClose(conn)
	defer cleanEnv(conn)
	type in struct {
		rows      []string
		precision string
	}
	data := []in{
		{
			rows:      []string{"measurement,host=host1 field1=2i,field2=2.0 1577836800000000000"},
			precision: "",
		},
		{
			rows:      []string{"measurement,host=host1 field1=2i,field2=2.0 1577836800000000000"},
			precision: "ns",
		},
		{
			rows:      []string{"measurement,host=host1 field1=2i,field2=2.0 1577836800000000"},
			precision: "u",
		},
		{
			rows:      []string{"measurement,host=host1 field1=2i,field2=2.0 1577836800000000"},
			precision: "μ",
		},
		{
			rows:      []string{"measurement,host=host1 field1=2i,field2=2.0 1577836800000"},
			precision: "ms",
		},
		{
			rows:      []string{"measurement,host=host1 field1=2i,field2=2.0 1577836800"},
			precision: "s",
		},
		{
			rows:      []string{"measurement,host=host1 field1=2i,field2=2.0 26297280"},
			precision: "m",
		},
		{
			rows:      []string{"measurement,host=host1 field1=2i,field2=2.0 438288"},
			precision: "h",
		},
		{
			rows:      []string{"cpu_value,host=xyzzy,instance=0,type=cpu,type_instance=user value=63843347 1665212955372077566\n"},
			precision: "ns",
		},
	}
	for _, d := range data {
		row := strings.Join(d.rows, "\n")
		totalRows, result := wrapper.TaosSchemalessInsertRaw(conn, row, wrapper.InfluxDBLineProtocol, d.precision)
		code := wrapper.TaosError(result)
		if code != 0 {
			errStr := wrapper.TaosErrorStr(result)
			wrapper.TaosFreeResult(result)
			t.Log(row)
			t.Error(errors.NewError(code, errStr))
			return
		}
		if int(totalRows) != len(d.rows) {
			t.Log(row)
			t.Errorf("expect rows %d got %d", len(d.rows), totalRows)
		}
		affected := wrapper.TaosAffectedRows(result)
		if affected != len(d.rows) {
			t.Log(row)
			t.Errorf("expect affected %d got %d", len(d.rows), affected)
		}
		wrapper.TaosFreeResult(result)
	}
}
