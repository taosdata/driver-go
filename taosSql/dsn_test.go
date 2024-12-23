package taosSql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// @author: xftan
// @date: 2022/1/27 16:18
// @description: test dsn parse
func TestParseDsn(t *testing.T) {
	tests := []struct {
		name                    string
		dsn                     string
		errs                    string
		user                    string
		passwd                  string
		net                     string
		addr                    string
		port                    int
		dbName                  string
		configPath              string
		cgoThread               int
		cgoAsyncHandlerPoolSize int
	}{
		{name: "invalid", dsn: "abcd", errs: "invalid DSN: missing the slash separating the database name"},
		{name: "normal", dsn: "user:passwd@net(fqdn:6030)/dbname", user: "user", passwd: "passwd", net: "net", addr: "fqdn", port: 6030, dbName: "dbname"},
		{name: "missing closing brace", dsn: "user:passwd@net()/dbname", errs: "invalid DSN: network address not terminated (missing closing brace)"},
		{name: "default addr", dsn: "user:passwd@net(:)/dbname", user: "user", passwd: "passwd", net: "net", dbName: "dbname"},
		{name: "0port", dsn: "user:passwd@net(:0)/dbname", user: "user", passwd: "passwd", net: "net", dbName: "dbname"},
		{name: "no dbname", dsn: "user:passwd@net(:0)/", user: "user", passwd: "passwd", net: "net"},
		{name: "no auth", dsn: "net(:0)/wo", net: "net", dbName: "wo"},
		{name: "cfg", dsn: "user:passwd@cfg(/home/taos)/db", user: "user", passwd: "passwd", net: "cfg", configPath: "/home/taos", dbName: "db"},
		{name: "no addr", dsn: "user:passwd@cfg/db", user: "user", passwd: "passwd", net: "cfg", configPath: "", dbName: "db"},
		{name: "options", dsn: "net(:0)/wo?firstEp=LAPTOP-NNKFTLTG.localdomain%3A6030&secondEp=LAPTOP-NNKFTLTG.localdomain%3A6030&fqdn=LAPTOP-NNKFTLTG.localdomain&serverPort=6030&configDir=%2Fetc%2Ftaos&logDir=%2Fvar%2Flog%2Ftaos&scriptDir=%2Fetc%2Ftaos&arbitrator=&numOfThreadsPerCore=1.000000&maxNumOfDistinctRes=10000000&rpcTimer=300&rpcForceTcp=0&rpcMaxTime=600&shellActivityTimer=3&compressMsgSize=-1&maxSQLLength=1048576&maxWildCardsLength=100&maxNumOfOrderedRes=100000&keepColumnName=0&timezone=Asia%2FShanghai+%28CST%2C+%2B0800%29&locale=C.UTF-8&charset=UTF-8&numOfLogLines=10000000&logKeepDays=0&asyncLog=1&debugFlag=0&rpcDebugFlag=131&tmrDebugFlag=131&cDebugFlag=131&jniDebugFlag=131&odbcDebugFlag=131&uDebugFlag=131&qDebugFlag=131&tsdbDebugFlag=131&gitinfo=TAOS_CFG_VTYPE_STRING&gitinfoOfInternal=TAOS_CFG_VTYPE_STRING&buildinfo=TAOS_CFG_VTYPE_STRING&version=TAOS_CFG_VTYPE_STRING&maxBinaryDisplayWidth=30&tempDir=%2Ftmp%2F", net: "net", dbName: "wo"},
		{name: "cgoThread", dsn: "net(:0)/wo?cgoThread=8", net: "net", dbName: "wo", cgoThread: 8},
		{name: "cgoAsyncHandlerPoolSize", dsn: "net(:0)/wo?cgoThread=8&cgoAsyncHandlerPoolSize=10000", net: "net", dbName: "wo", cgoThread: 8, cgoAsyncHandlerPoolSize: 10000},
		{
			name:   "special char",
			dsn:    "!%40%23%24%25%5E%26*()-_%2B%3D%5B%5D%7B%7D%3A%3B%3E%3C%3F%7C~%2C.:!%40%23%24%25%5E%26*()-_%2B%3D%5B%5D%7B%7D%3A%3B%3E%3C%3F%7C~%2C.@net(:)/dbname",
			user:   "!@#$%^&*()-_+=[]{}:;><?|~,.",
			passwd: "!@#$%^&*()-_+=[]{}:;><?|~,.",
			net:    "net",
			dbName: "dbname",
		},
		//encodeURIComponent('!q@w#a$1%3^&*()-_+=[]{}:;><?|~,.')
		{
			name:   "special char2",
			dsn:    "!q%40w%23a%241%253%5E%26*()-_%2B%3D%5B%5D%7B%7D%3A%3B%3E%3C%3F%7C~%2C.:!q%40w%23a%241%253%5E%26*()-_%2B%3D%5B%5D%7B%7D%3A%3B%3E%3C%3F%7C~%2C.@net(:)/dbname",
			user:   "!q@w#a$1%3^&*()-_+=[]{}:;><?|~,.",
			passwd: "!q@w#a$1%3^&*()-_+=[]{}:;><?|~,.",
			net:    "net",
			dbName: "dbname",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := ParseDSN(tc.dsn)
			if err != nil {
				if errs := err.Error(); errs != tc.errs {
					t.Fatal(tc.errs, "\n", errs)
				}
				return
			}
			assert.Equal(t, tc.user, cfg.User)
			assert.Equal(t, tc.dbName, cfg.DbName)
			assert.Equal(t, tc.passwd, cfg.Passwd)
			assert.Equal(t, tc.net, cfg.Net)
			assert.Equal(t, tc.addr, cfg.Addr)
			assert.Equal(t, tc.configPath, cfg.ConfigPath)
			assert.Equal(t, tc.port, cfg.Port)
			assert.Equal(t, tc.cgoThread, cfg.CgoThread)
			assert.Equal(t, tc.cgoAsyncHandlerPoolSize, cfg.CgoAsyncHandlerPoolSize)
		})
	}
}

func TestTryUnescape(t *testing.T) {
	escaped := tryUnescape("%3F") // ?
	assert.Equal(t, "?", escaped)
	escaped = tryUnescape("%3f") // ?
	assert.Equal(t, "?", escaped)
	escaped = tryUnescape("%25") // %
	assert.Equal(t, "%", escaped)
	escaped = tryUnescape("%")
	assert.Equal(t, "%", escaped)
}
