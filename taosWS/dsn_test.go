package taosWS

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// @author: xftan
// @date: 2023/10/13 11:26
// @description: test parse dsn
func TestParseDsn(t *testing.T) {
	tests := []struct {
		dsn  string
		errs string
		want *config
	}{
		{dsn: "abcd", errs: "invalid DSN: missing the slash separating the database name"},
		{dsn: "user:passwd@ws(fqdn:6041)/dbname", want: &config{user: "user", passwd: "passwd", net: "ws", addr: "fqdn", port: 6041, dbName: "dbname", interpolateParams: true}},
		{dsn: "user:passwd@ws()/dbname", errs: "invalid DSN: network address not terminated (missing closing brace)"},
		{dsn: "user:passwd@ws(:)/dbname", want: &config{user: "user", passwd: "passwd", net: "ws", dbName: "dbname", interpolateParams: true}},
		{dsn: "user:passwd@ws(:0)/dbname", want: &config{user: "user", passwd: "passwd", net: "ws", dbName: "dbname", interpolateParams: true}},
		{dsn: "user:passwd@wss(:0)/", want: &config{user: "user", passwd: "passwd", net: "wss", interpolateParams: true}},
		{dsn: "user:passwd@wss(:0)/?interpolateParams=false&test=1", want: &config{user: "user", passwd: "passwd", net: "wss", params: map[string]string{"test": "1"}}},
		{dsn: "user:passwd@wss(:0)/?interpolateParams=false&token=token", want: &config{user: "user", passwd: "passwd", net: "wss", token: "token"}},
		{dsn: "user:passwd@wss(:0)/?writeTimeout=8s&readTimeout=10m", want: &config{user: "user", passwd: "passwd", net: "wss", readTimeout: 10 * time.Minute, writeTimeout: 8 * time.Second, interpolateParams: true}},
		{dsn: "user:passwd@wss(:0)/?writeTimeout=8s&readTimeout=10m&enableCompression=true", want: &config{
			user:              "user",
			passwd:            "passwd",
			net:               "wss",
			readTimeout:       10 * time.Minute,
			writeTimeout:      8 * time.Second,
			interpolateParams: true,
			enableCompression: true,
		}},
	}
	for _, tc := range tests {
		t.Run(tc.dsn, func(t *testing.T) {
			cfg, err := parseDSN(tc.dsn)
			if err != nil {
				if errs := err.Error(); errs != tc.errs {
					t.Fatal(tc.errs, "\n", errs)
				}
				return
			}
			assert.Equal(t, tc.want, cfg)
		})
	}
}
