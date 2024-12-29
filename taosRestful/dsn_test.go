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
		name string
		dsn  string
		errs string
		want *Config
	}{
		{
			name: "invalid",
			dsn:  "abcd",
			errs: "invalid DSN: missing the slash separating the database name",
		},
		{
			name: "normal",
			dsn:  "user:passwd@http(fqdn:6041)/dbname",
			want: &Config{
				User:               "user",
				Passwd:             "passwd",
				Net:                "http",
				Addr:               "fqdn",
				Port:               6041,
				DbName:             "dbname",
				Params:             nil,
				InterpolateParams:  true,
				DisableCompression: true,
				ReadBufferSize:     4096,
				Token:              "",
				SkipVerify:         false,
			},
		},
		{
			name: "invalid addr",
			dsn:  "user:passwd@http()/dbname",
			errs: "invalid DSN: network address not terminated (missing closing brace)",
		},
		{
			name: "default addr",
			dsn:  "user:passwd@http(:)/dbname",
			want: &Config{
				User:               "user",
				Passwd:             "passwd",
				Net:                "http",
				Addr:               "",
				Port:               0,
				DbName:             "dbname",
				Params:             nil,
				InterpolateParams:  true,
				DisableCompression: true,
				ReadBufferSize:     4096,
				Token:              "",
				SkipVerify:         false,
			},
		},
		{
			name: "0port",
			dsn:  "user:passwd@http(:0)/dbname",
			want: &Config{
				User:               "user",
				Passwd:             "passwd",
				Net:                "http",
				Addr:               "",
				Port:               0,
				DbName:             "dbname",
				Params:             nil,
				InterpolateParams:  true,
				DisableCompression: true,
				ReadBufferSize:     4096,
				Token:              "",
				SkipVerify:         false,
			},
		},
		{
			name: "no db",
			dsn:  "user:passwd@https(:0)/",
			want: &Config{
				User:               "user",
				Passwd:             "passwd",
				Net:                "https",
				Addr:               "",
				Port:               0,
				DbName:             "",
				Params:             nil,
				InterpolateParams:  true,
				DisableCompression: true,
				ReadBufferSize:     4096,
				Token:              "",
				SkipVerify:         false,
			},
		},
		{
			name: "params",
			dsn:  "user:passwd@https(:0)/?interpolateParams=false&test=1",
			want: &Config{
				User:   "user",
				Passwd: "passwd",
				Net:    "https",
				Addr:   "",
				Port:   0,
				DbName: "",
				Params: map[string]string{
					"test": "1",
				},
				InterpolateParams:  false,
				DisableCompression: true,
				ReadBufferSize:     4096,
				Token:              "",
				SkipVerify:         false,
			},
		},
		{
			name: "token",
			dsn:  "user:passwd@https(:0)/?interpolateParams=false&token=token",
			want: &Config{
				User:               "user",
				Passwd:             "passwd",
				Net:                "https",
				Addr:               "",
				Port:               0,
				DbName:             "",
				Params:             nil,
				InterpolateParams:  false,
				DisableCompression: true,
				ReadBufferSize:     4096,
				Token:              "token",
				SkipVerify:         false,
			},
		},
		{
			name: "skipVerify",
			dsn:  "user:passwd@https(:0)/?interpolateParams=false&token=token&skipVerify=true",
			want: &Config{
				User:               "user",
				Passwd:             "passwd",
				Net:                "https",
				Addr:               "",
				Port:               0,
				DbName:             "",
				Params:             nil,
				InterpolateParams:  false,
				DisableCompression: true,
				ReadBufferSize:     4096,
				Token:              "token",
				SkipVerify:         true,
			},
		},
		{
			name: "skipVerify",
			dsn:  "user:passwd@https(:0)/?interpolateParams=false&token=token&skipVerify=true",
			want: &Config{
				User:               "user",
				Passwd:             "passwd",
				Net:                "https",
				Addr:               "",
				Port:               0,
				DbName:             "",
				Params:             nil,
				InterpolateParams:  false,
				DisableCompression: true,
				ReadBufferSize:     4096,
				Token:              "token",
				SkipVerify:         true,
			},
		},
		{
			name: "readBufferSize",
			dsn:  "user:passwd@https(:0)/?interpolateParams=false&token=token&skipVerify=true&readBufferSize=8192",
			want: &Config{
				User:               "user",
				Passwd:             "passwd",
				Net:                "https",
				Addr:               "",
				Port:               0,
				DbName:             "",
				Params:             nil,
				InterpolateParams:  false,
				DisableCompression: true,
				ReadBufferSize:     8192,
				Token:              "token",
				SkipVerify:         true,
			},
		},
		{
			name: "disableCompression",
			dsn:  "user:passwd@https(:0)/?interpolateParams=false&token=token&skipVerify=true&readBufferSize=8192&disableCompression=false",
			want: &Config{
				User:               "user",
				Passwd:             "passwd",
				Net:                "https",
				Addr:               "",
				Port:               0,
				DbName:             "",
				Params:             nil,
				InterpolateParams:  false,
				DisableCompression: false,
				ReadBufferSize:     8192,
				Token:              "token",
				SkipVerify:         true,
			},
		},
		{
			name: "special char",
			dsn:  "!%40%23%24%25%5E%26*()-_%2B%3D%5B%5D%7B%7D%3A%3B%3E%3C%3F%7C~%2C.:!%40%23%24%25%5E%26*()-_%2B%3D%5B%5D%7B%7D%3A%3B%3E%3C%3F%7C~%2C.@https(:)/dbname",
			want: &Config{
				User:               "!@#$%^&*()-_+=[]{}:;><?|~,.",
				Passwd:             "!@#$%^&*()-_+=[]{}:;><?|~,.",
				Net:                "https",
				Addr:               "",
				Port:               0,
				DbName:             "dbname",
				Params:             nil,
				InterpolateParams:  true,
				DisableCompression: true,
				ReadBufferSize:     4096,
				Token:              "",
				SkipVerify:         false,
			},
		},
		//encodeURIComponent('!q@w#a$1%3^&*()-_+=[]{}:;><?|~,.')
		{
			name: "special char2",
			dsn:  "!q%40w%23a%241%253%5E%26*()-_%2B%3D%5B%5D%7B%7D%3A%3B%3E%3C%3F%7C~%2C.:!q%40w%23a%241%253%5E%26*()-_%2B%3D%5B%5D%7B%7D%3A%3B%3E%3C%3F%7C~%2C.@https(:)/dbname",
			want: &Config{
				User:               "!q@w#a$1%3^&*()-_+=[]{}:;><?|~,.",
				Passwd:             "!q@w#a$1%3^&*()-_+=[]{}:;><?|~,.",
				Net:                "https",
				Addr:               "",
				Port:               0,
				DbName:             "dbname",
				Params:             nil,
				InterpolateParams:  true,
				DisableCompression: true,
				ReadBufferSize:     4096,
				Token:              "",
				SkipVerify:         false,
			},
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
