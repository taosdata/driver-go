package tmq

import (
	"errors"
	"time"
)

type Config struct {
	Url            string
	ChanLength     uint
	MessageTimeout time.Duration
	WriteWait      time.Duration
	ErrorHandler   func(consumer *Consumer, err error)
	CloseHandler   func()
	User           string
	Password       string
	GroupID        string
	ClientID       string
	OffsetRest     string
}

// NewConfig create new config for tmq over websocket
func NewConfig(url string, chanLength uint) *Config {
	return &Config{
		Url:        url,
		ChanLength: chanLength,
	}
}

// SetConnectUser set connect user
func (c *Config) SetConnectUser(user string) error {
	c.User = user
	return nil
}

// SetConnectPass set connect password
func (c *Config) SetConnectPass(pass string) error {
	c.Password = pass
	return nil
}

// SetGroupID set group id
func (c *Config) SetGroupID(groupID string) error {
	c.GroupID = groupID
	return nil
}

// SetClientID set client id
func (c *Config) SetClientID(clientID string) error {
	c.ClientID = clientID
	return nil
}

// SetAutoOffsetReset set auto_offset_reset
func (c *Config) SetAutoOffsetReset(offsetReset string) error {
	c.OffsetRest = offsetReset
	return nil
}

// SetMessageTimeout set get message timeout
func (c *Config) SetMessageTimeout(timeout time.Duration) error {
	if timeout < time.Second {
		return errors.New("message timeout cannot be less than 1 second")
	}
	c.MessageTimeout = timeout
	return nil
}

// SetWriteWait set write deadline wait time
func (c *Config) SetWriteWait(writeWait time.Duration) error {
	if writeWait < 0 {
		return errors.New("write wait cannot be less than 0")
	}
	c.WriteWait = writeWait
	return nil
}

// SetErrorHandler set error handler. ErrorHandler function called when a read error occurs
func (c *Config) SetErrorHandler(f func(consumer *Consumer, err error)) {
	c.ErrorHandler = f
}

// SetCloseHandler set close handler. CloseHandler function called when the connection closed
func (c *Config) SetCloseHandler(f func()) {
	c.CloseHandler = f
}
