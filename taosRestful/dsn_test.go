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
		{"user:passwd@http(fqdn:6041)/dbname", "", "user", "passwd", "http", "fqdn", 6041, "dbname"},
		{dsn: "user:passwd@http()/dbname", errs: "invalid DSN: network address not terminated (missing closing brace)"},
		{"user:passwd@http(:)/dbname", "", "user", "passwd", "http", "", 0, "dbname"},
		{"user:passwd@http(:0)/dbname", "", "user", "passwd", "http", "", 0, "dbname"},
		{"user:passwd@https(:0)/", "", "user", "passwd", "https", "", 0, ""},
		{"user:passwd@https(:0)/?interpolateParams=false&test=1", "", "user", "passwd", "https", "", 0, ""},
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
