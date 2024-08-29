package taosRestful

import (
	"fmt"
	"testing"
)

// @author: xftan
// @date: 2022/2/8 12:52
// @description: test parse dsn
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
		token      string
		skipVerify bool
	}{{},
		{dsn: "abcd", errs: "invalid DSN: missing the slash separating the database name"},
		{dsn: "user:passwd@http(fqdn:6041)/dbname", user: "user", passwd: "passwd", net: "http", addr: "fqdn", port: 6041, dbName: "dbname"},
		{dsn: "user:passwd@http()/dbname", errs: "invalid DSN: network address not terminated (missing closing brace)"},
		{dsn: "user:passwd@http(:)/dbname", user: "user", passwd: "passwd", net: "http", dbName: "dbname"},
		{dsn: "user:passwd@http(:0)/dbname", user: "user", passwd: "passwd", net: "http", dbName: "dbname"},
		{dsn: "user:passwd@https(:0)/", user: "user", passwd: "passwd", net: "https"},
		{dsn: "user:passwd@https(:0)/?interpolateParams=false&test=1", user: "user", passwd: "passwd", net: "https"},
		{dsn: "user:passwd@https(:0)/?interpolateParams=false&token=token", user: "user", passwd: "passwd", net: "https", token: "token"},
		{dsn: "user:passwd@https(:0)/?interpolateParams=false&token=token&skipVerify=true", user: "user", passwd: "passwd", net: "https", token: "token", skipVerify: true},
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
				cfg.port != tc.port ||
				cfg.token != tc.token ||
				cfg.skipVerify != tc.skipVerify {
				t.Fatal(cfg)
			}
		})
	}
}
