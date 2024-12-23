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
		name string
		dsn  string
		errs string
		want *Config
	}{
		{name: "invalid DSN", dsn: "abcd", errs: "invalid DSN: missing the slash separating the database name"},
		{name: "common DSN", dsn: "user:passwd@ws(fqdn:6041)/dbname", want: &Config{User: "user", Passwd: "passwd", Net: "ws", Addr: "fqdn", Port: 6041, DbName: "dbname", InterpolateParams: true}},
		{name: "missing closing brace", dsn: "user:passwd@ws()/dbname", errs: "invalid DSN: network address not terminated (missing closing brace)"},
		{name: "default address", dsn: "user:passwd@ws(:)/dbname", want: &Config{User: "user", Passwd: "passwd", Net: "ws", DbName: "dbname", InterpolateParams: true}},
		{name: "0 port", dsn: "user:passwd@ws(:0)/dbname", want: &Config{User: "user", Passwd: "passwd", Net: "ws", DbName: "dbname", InterpolateParams: true}},
		{name: "wss protocol", dsn: "user:passwd@wss(:0)/", want: &Config{User: "user", Passwd: "passwd", Net: "wss", InterpolateParams: true}},
		{name: "params", dsn: "user:passwd@wss(:0)/?interpolateParams=false&test=1", want: &Config{User: "user", Passwd: "passwd", Net: "wss", Params: map[string]string{"test": "1"}}},
		{name: "token", dsn: "user:passwd@wss(:0)/?interpolateParams=false&token=token", want: &Config{User: "user", Passwd: "passwd", Net: "wss", Token: "token"}},
		{name: "readTimeout", dsn: "user:passwd@wss(:0)/?writeTimeout=8s&readTimeout=10m", want: &Config{User: "user", Passwd: "passwd", Net: "wss", ReadTimeout: 10 * time.Minute, WriteTimeout: 8 * time.Second, InterpolateParams: true}},
		{name: "compression", dsn: "user:passwd@wss(:0)/?writeTimeout=8s&readTimeout=10m&enableCompression=true", want: &Config{
			User:              "user",
			Passwd:            "passwd",
			Net:               "wss",
			ReadTimeout:       10 * time.Minute,
			WriteTimeout:      8 * time.Second,
			InterpolateParams: true,
			EnableCompression: true,
		}},
		// encodeURIComponent('!@#$%^&*()-_+=[]{}:;><?|~,.')
		{
			name: "special characters",
			dsn:  "!%40%23%24%25%5E%26*()-_%2B%3D%5B%5D%7B%7D%3A%3B%3E%3C%3F%7C~%2C.:!%40%23%24%25%5E%26*()-_%2B%3D%5B%5D%7B%7D%3A%3B%3E%3C%3F%7C~%2C.@wss(:0)/dbname?writeTimeout=8s&readTimeout=10m&enableCompression=true",
			want: &Config{
				User:              "!@#$%^&*()-_+=[]{}:;><?|~,.",
				Passwd:            "!@#$%^&*()-_+=[]{}:;><?|~,.",
				Net:               "wss",
				DbName:            "dbname",
				ReadTimeout:       10 * time.Minute,
				WriteTimeout:      8 * time.Second,
				InterpolateParams: true,
				EnableCompression: true,
			},
		},
		//encodeURIComponent('!q@w#a$1%3^&*()-_+=[]{}:;><?|~,.')
		{
			name: "special characters2",
			dsn:  "!q%40w%23a%241%253%5E%26*()-_%2B%3D%5B%5D%7B%7D%3A%3B%3E%3C%3F%7C~%2C.:!q%40w%23a%241%253%5E%26*()-_%2B%3D%5B%5D%7B%7D%3A%3B%3E%3C%3F%7C~%2C.@wss(:0)/dbname?writeTimeout=8s&readTimeout=10m&enableCompression=true",
			want: &Config{
				User:              "!q@w#a$1%3^&*()-_+=[]{}:;><?|~,.",
				Passwd:            "!q@w#a$1%3^&*()-_+=[]{}:;><?|~,.",
				Net:               "wss",
				DbName:            "dbname",
				ReadTimeout:       10 * time.Minute,
				WriteTimeout:      8 * time.Second,
				InterpolateParams: true,
				EnableCompression: true,
			},
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
			assert.Equal(t, tc.want, cfg)
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
