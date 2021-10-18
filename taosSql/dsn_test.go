package taosSql

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestParseDsn(t *testing.T) {
	tcs := []struct {
		dsn        string
		errs       string
		user       string
		passwd     string
		net        string
		addr       string
		port       int
		dbName     string
		configPath string
	}{{},
		{dsn: "abcd", errs: "invalid DSN: missing the slash separating the database name"},
		{dsn: "user:passwd@net(fqdn:6030)/dbname", user: "user", passwd: "passwd", net: "net", addr: "fqdn", port: 6030, dbName: "dbname"},
		{dsn: "user:passwd@net()/dbname", errs: "invalid DSN: network address not terminated (missing closing brace)"},
		{dsn: "user:passwd@net(:)/dbname", user: "user", passwd: "passwd", net: "net", dbName: "dbname"},
		{dsn: "user:passwd@net(:0)/dbname", user: "user", passwd: "passwd", net: "net", dbName: "dbname"},
		{dsn: "user:passwd@net(:0)/", user: "user", passwd: "passwd", net: "net"},
		{dsn: "net(:0)/wo", net: "net", dbName: "wo"},
		{dsn: "user:passwd@cfg(/home/taos)/db", user: "user", passwd: "passwd", net: "cfg", configPath: "/home/taos", dbName: "db"},
		{dsn: "user:passwd@cfg/db", user: "user", passwd: "passwd", net: "cfg", configPath: "", dbName: "db"},
		{dsn: "net(:0)/wo?firstEp=LAPTOP-NNKFTLTG.localdomain%3A6030&secondEp=LAPTOP-NNKFTLTG.localdomain%3A6030&fqdn=LAPTOP-NNKFTLTG.localdomain&serverPort=6030&configDir=%2Fetc%2Ftaos&logDir=%2Fvar%2Flog%2Ftaos&scriptDir=%2Fetc%2Ftaos&arbitrator=&numOfThreadsPerCore=1.000000&maxNumOfDistinctRes=10000000&rpcTimer=300&rpcForceTcp=0&rpcMaxTime=600&shellActivityTimer=3&compressMsgSize=-1&maxSQLLength=1048576&maxWildCardsLength=100&maxNumOfOrderedRes=100000&keepColumnName=0&timezone=Asia%2FShanghai+%28CST%2C+%2B0800%29&locale=C.UTF-8&charset=UTF-8&numOfLogLines=10000000&logKeepDays=0&asyncLog=1&debugFlag=0&rpcDebugFlag=131&tmrDebugFlag=131&cDebugFlag=131&jniDebugFlag=131&odbcDebugFlag=131&uDebugFlag=131&qDebugFlag=131&tsdbDebugFlag=131&gitinfo=TAOS_CFG_VTYPE_STRING&gitinfoOfInternal=TAOS_CFG_VTYPE_STRING&buildinfo=TAOS_CFG_VTYPE_STRING&version=TAOS_CFG_VTYPE_STRING&maxBinaryDisplayWidth=30&tempDir=%2Ftmp%2F", net: "net", dbName: "wo"},
	}
	for i, tc := range tcs {
		name := fmt.Sprintf("%d - %s", i, tc.dsn)
		t.Run(name, func(t *testing.T) {
			cfg, err := parseDSN(tc.dsn)
			if err != nil {
				if errs := err.Error(); errs != tc.errs {
					t.Fatal(tc.errs, "\n", errs)
				}
				return
			}
			assert.Equal(t, tc.user, cfg.user)
			assert.Equal(t, tc.dbName, cfg.dbName)
			assert.Equal(t, tc.passwd, cfg.passwd)
			assert.Equal(t, tc.net, cfg.net)
			assert.Equal(t, tc.addr, cfg.addr)
			assert.Equal(t, tc.configPath, cfg.configPath)
			assert.Equal(t, tc.port, cfg.port)
		})
	}
}
