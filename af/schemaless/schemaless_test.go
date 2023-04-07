package schemaless

import (
	"context"
	"testing"
	"time"

	"github.com/taosdata/driver-go/v3/af"
	"github.com/taosdata/driver-go/v3/wrapper"
)

func TestSchemaless_Insert_by_native(t *testing.T) {
	before()
	defer after()

	conn, err := NewNativeConnection("root", "taosdata", "", 6030, "test_schemaless")
	if err != nil {
		t.Fatal(err)
	}
	doTest(t, conn)
}

func TestSchemaless_Insert_by_ws(t *testing.T) {
	before()
	defer after()

	conn, err := NewWsConnection(false, "root", "taosdata", "", "", 6041, "test_schemaless", time.Second, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	doTest(t, conn)
}

var cases = []struct {
	name      string
	db        string
	protocol  int
	precision string
	data      string
	ttl       int
	code      int
}{
	{
		name:      "influxdb",
		db:        "test_schemaless_ws",
		protocol:  wrapper.InfluxDBLineProtocol,
		precision: "ms",
		data: "measurement,host=host1 field1=2i,field2=2.0 1577837300000\n" +
			"measurement,host=host1 field1=2i,field2=2.0 1577837400000\n" +
			"measurement,host=host1 field1=2i,field2=2.0 1577837500000\n" +
			"measurement,host=host1 field1=2i,field2=2.0 1577837600000",
		ttl:  1000,
		code: 0,
	},
	{
		name:      "opentsdb_telnet",
		db:        "test_schemaless_ws",
		protocol:  wrapper.OpenTSDBTelnetLineProtocol,
		precision: "ms",
		data: "meters.current 1648432611249 10.3 location=California.SanFrancisco group=2\n" +
			"meters.current 1648432611250 12.6 location=California.SanFrancisco group=2\n" +
			"meters.current 1648432611251 10.8 location=California.LosAngeles group=3\n" +
			"meters.current 1648432611252 11.3 location=California.LosAngeles group=3\n",
		ttl:  1000,
		code: 0,
	},
	{
		name:      "opentsdb_json",
		db:        "test_schemaless_ws",
		protocol:  wrapper.OpenTSDBJsonFormatProtocol,
		precision: "ms",
		data: "[{\"metric\": \"meters.current\", \"timestamp\": 1648432611253, \"value\": 10.2, \"tags\": " +
			"{\"location\": \"California.LosAngeles\", \"group\": \"1\" } }, {\"metric\": \"meters.current\", " +
			"\"timestamp\": 1648432611254, \"value\": 12.1, \"tags\": {\"location\": \"California.LosAngeles\", " +
			"\"group\": \"1\" } }]",
		ttl:  100,
		code: 0,
	},
}

func before() {
	afConn, err := af.Open("", "root", "taosdata", "", 0)
	if err != nil {
		panic(err)
	}
	_, err = afConn.Exec("drop database if exists test_schemaless")
	if err != nil {
		panic(err)
	}
	_, err = afConn.Exec("create database if not exists test_schemaless")
	if err != nil {
		panic(err)
	}
}

func after() {
	afConn, err := af.Open("", "root", "taosdata", "", 0)
	if err != nil {
		panic(err)
	}
	_, _ = afConn.Exec("drop database if exists test_schemaless")
}

func doTest(t *testing.T, conn Connection) {
	s := NewSchemaless(conn)
	defer s.Close(context.Background())
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if err := s.Insert(context.Background(), c.data, c.protocol, c.precision, c.ttl, 0); err != nil {
				t.Fatal(err)
			}
		})
	}
}
