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
