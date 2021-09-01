package taosSql

import (
	"fmt"
	"testing"
)

func TestParseDsn(t *testing.T) {
	tcs := []struct {
		dsn    string
		errs   string
		user   string
		passwd string
		net    string
		addr   string
		port   int
		dbName string
	}{{},
		{dsn: "abcd", errs: "invalid DSN: missing the slash separating the database name"},
		{"user:passwd@net(fqdn:6030)/dbname", "", "user", "passwd", "net", "fqdn", 6030, "dbname"},
		{dsn: "user:passwd@net()/dbname", errs: "invalid DSN: network address not terminated (missing closing brace)"},
		{"user:passwd@net(:)/dbname", "", "user", "passwd", "net", "", 0, "dbname"},
		{"user:passwd@net(:0)/dbname", "", "user", "passwd", "net", "", 0, "dbname"},
		{"user:passwd@net(:0)/", "", "user", "passwd", "net", "", 0, ""},
		{"net(:0)/wo", "", "", "", "net", "", 0, "wo"},
		{"net(:0)/wo?firstEp=LAPTOP-NNKFTLTG.localdomain%3A6030&secondEp=LAPTOP-NNKFTLTG.localdomain%3A6030&fqdn=LAPTOP-NNKFTLTG.localdomain&serverPort=6030&configDir=%2Fetc%2Ftaos&logDir=%2Fvar%2Flog%2Ftaos&scriptDir=%2Fetc%2Ftaos&arbitrator=&numOfThreadsPerCore=1.000000&maxNumOfDistinctRes=10000000&rpcTimer=300&rpcForceTcp=0&rpcMaxTime=600&shellActivityTimer=3&compressMsgSize=-1&maxSQLLength=1048576&maxWildCardsLength=100&maxNumOfOrderedRes=100000&keepColumnName=0&timezone=Asia%2FShanghai+%28CST%2C+%2B0800%29&locale=C.UTF-8&charset=UTF-8&numOfLogLines=10000000&logKeepDays=0&asyncLog=1&debugFlag=0&rpcDebugFlag=131&tmrDebugFlag=131&cDebugFlag=131&jniDebugFlag=131&odbcDebugFlag=131&uDebugFlag=131&qDebugFlag=131&tsdbDebugFlag=131&gitinfo=TAOS_CFG_VTYPE_STRING&gitinfoOfInternal=TAOS_CFG_VTYPE_STRING&buildinfo=TAOS_CFG_VTYPE_STRING&version=TAOS_CFG_VTYPE_STRING&maxBinaryDisplayWidth=30&tempDir=%2Ftmp%2F", "", "", "", "net", "", 0, "wo"},
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

			if cfg.user != tc.user ||
				cfg.dbName != tc.dbName ||
				cfg.passwd != tc.passwd ||
				cfg.net != tc.net ||
				cfg.addr != tc.addr ||
				cfg.port != tc.port {
				t.Fatal(cfg)
			}
		})
	}
}
