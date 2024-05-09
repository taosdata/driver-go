package schemaless

import (
	"time"
)

const (
	connAction   = "conn"
	insertAction = "insert"
)

type Config struct {
	url                 string
	chanLength          uint
	user                string
	password            string
	db                  string
	readTimeout         time.Duration
	writeTimeout        time.Duration
	errorHandler        func(error)
	enableCompression   bool
	autoReconnect       bool
	reconnectIntervalMs int
	reconnectRetryCount int
}

func NewConfig(url string, chanLength uint, opts ...func(*Config)) *Config {
	c := Config{url: url, chanLength: chanLength, reconnectRetryCount: 3, reconnectIntervalMs: 2000}
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

func SetEnableCompression(enableCompression bool) func(*Config) {
	return func(c *Config) {
		c.enableCompression = enableCompression
	}
}

func SetAutoReconnect(reconnect bool) func(*Config) {
	return func(c *Config) {
		c.autoReconnect = reconnect
	}
}

func SetReconnectIntervalMs(reconnectIntervalMs int) func(*Config) {
	return func(c *Config) {
		c.reconnectIntervalMs = reconnectIntervalMs
	}
}

func SetReconnectRetryCount(reconnectRetryCount int) func(*Config) {
	return func(c *Config) {
		c.reconnectRetryCount = reconnectRetryCount
	}
}
