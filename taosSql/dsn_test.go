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
