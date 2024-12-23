package taosRestful

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// @author: xftan
// @date: 2022/2/8 12:52
// @description: test parse dsn
func TestParseDsn(t *testing.T) {
	tcs := []struct {
		name       string
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
		{name: "invalid", dsn: "abcd", errs: "invalid DSN: missing the slash separating the database name"},
		{name: "normal", dsn: "user:passwd@http(fqdn:6041)/dbname", user: "user", passwd: "passwd", net: "http", addr: "fqdn", port: 6041, dbName: "dbname"},
		{name: "invalid addr", dsn: "user:passwd@http()/dbname", errs: "invalid DSN: network address not terminated (missing closing brace)"},
		{name: "default addr", dsn: "user:passwd@http(:)/dbname", user: "user", passwd: "passwd", net: "http", dbName: "dbname"},
		{name: "0port", dsn: "user:passwd@http(:0)/dbname", user: "user", passwd: "passwd", net: "http", dbName: "dbname"},
		{name: "no db", dsn: "user:passwd@https(:0)/", user: "user", passwd: "passwd", net: "https"},
		{name: "params", dsn: "user:passwd@https(:0)/?interpolateParams=false&test=1", user: "user", passwd: "passwd", net: "https"},
		{name: "token", dsn: "user:passwd@https(:0)/?interpolateParams=false&token=token", user: "user", passwd: "passwd", net: "https", token: "token"},
		{name: "skipVerify", dsn: "user:passwd@https(:0)/?interpolateParams=false&token=token&skipVerify=true", user: "user", passwd: "passwd", net: "https", token: "token", skipVerify: true},
		{
			name:       "skipVerify",
			dsn:        "user:passwd@https(:0)/?interpolateParams=false&token=token&skipVerify=true",
			user:       "user",
			passwd:     "passwd",
			net:        "https",
			token:      "token",
			skipVerify: true,
		},
		{
			name:   "special char",
			dsn:    "!%40%23%24%25%5E%26*()-_%2B%3D%5B%5D%7B%7D%3A%3B%3E%3C%3F%7C~%2C.:!%40%23%24%25%5E%26*()-_%2B%3D%5B%5D%7B%7D%3A%3B%3E%3C%3F%7C~%2C.@https(:)/dbname",
			user:   "!@#$%^&*()-_+=[]{}:;><?|~,.",
			passwd: "!@#$%^&*()-_+=[]{}:;><?|~,.",
			net:    "https",
			dbName: "dbname",
		},
		//encodeURIComponent('!q@w#a$1%3^&*()-_+=[]{}:;><?|~,.')
		{
			name:   "special char2",
			dsn:    "!q%40w%23a%241%253%5E%26*()-_%2B%3D%5B%5D%7B%7D%3A%3B%3E%3C%3F%7C~%2C.:!q%40w%23a%241%253%5E%26*()-_%2B%3D%5B%5D%7B%7D%3A%3B%3E%3C%3F%7C~%2C.@https(:)/dbname",
			user:   "!q@w#a$1%3^&*()-_+=[]{}:;><?|~,.",
			passwd: "!q@w#a$1%3^&*()-_+=[]{}:;><?|~,.",
			net:    "https",
			dbName: "dbname",
		},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := ParseDSN(tc.dsn)
			if err != nil {
				if errs := err.Error(); errs != tc.errs {
					t.Fatal(tc.errs, "\n", errs)
				}
				return
			}

			if cfg.User != tc.user ||
				cfg.DbName != tc.dbName ||
				cfg.Passwd != tc.passwd ||
				cfg.Net != tc.net ||
				cfg.Addr != tc.addr ||
				cfg.Port != tc.port ||
				cfg.Token != tc.token ||
				cfg.SkipVerify != tc.skipVerify {
				t.Fatal(cfg)
			}
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
