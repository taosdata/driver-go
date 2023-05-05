package schemaless

import (
	"encoding/json"
	"time"
)

const (
	connAction   = "conn"
	insertAction = "insert"
)

type Config struct {
	url          string
	chanLength   uint
	user         string
	password     string
	db           string
	readTimeout  time.Duration
	writeTimeout time.Duration
	errorHandler func(error)
}

func NewConfig(url string, chanLength uint, opts ...func(*Config)) *Config {
	c := Config{url: url, chanLength: chanLength}
	for _, opt := range opts {
		opt(&c)
	}

	return &c
}

func SetUser(user string) func(*Config) {
	return func(c *Config) {
		c.user = user
	}
}

func SetPassword(password string) func(*Config) {
	return func(c *Config) {
		c.password = password
	}
}

func SetDb(db string) func(*Config) {
	return func(c *Config) {
		c.db = db
	}
}

func SetReadTimeout(readTimeout time.Duration) func(*Config) {
	return func(c *Config) {
		c.readTimeout = readTimeout
	}
}

func SetWriteTimeout(writeTimeout time.Duration) func(*Config) {
	return func(c *Config) {
		c.writeTimeout = writeTimeout
	}
}

func SetErrorHandler(errorHandler func(error)) func(*Config) {
	return func(c *Config) {
		c.errorHandler = errorHandler
	}
}

type wsConnectReq struct {
	ReqID    uint64 `json:"req_id"`
	User     string `json:"user"`
	Password string `json:"password"`
	DB       string `json:"db"`
}

type wsConnectResp struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Action  string `json:"action"`
	ReqID   uint64 `json:"req_id"`
	Timing  int64  `json:"timing"`
}

type schemalessReq struct {
	ReqID     uint64 `json:"req_id"`
	DB        string `json:"db"`
	Protocol  int    `json:"protocol"`
	Precision string `json:"precision"`
	TTL       int    `json:"ttl"`
	Data      string `json:"data"`
}

type schemalessResp struct {
	ReqID  uint64 `json:"req_id"`
	Action string `json:"action"`
	Timing int64  `json:"timing"`
}

type wsAction struct {
	Action string          `json:"action"`
	Args   json.RawMessage `json:"args"`
}
