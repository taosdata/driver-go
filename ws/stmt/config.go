package stmt

import (
	"errors"
	"time"
)

type Config struct {
	Url            string
	ChanLength     uint
	MessageTimeout time.Duration
	WriteWait      time.Duration
	ErrorHandler   func(connector *Connector, err error)
	CloseHandler   func()
	User           string
	Password       string
	DB             string
}

func NewConfig(url string, chanLength uint) *Config {
	return &Config{
		Url:        url,
		ChanLength: chanLength,
	}
}
func (c *Config) SetConnectUser(user string) error {
	c.User = user
	return nil
}

func (c *Config) SetConnectPass(pass string) error {
	c.Password = pass
	return nil
}
func (c *Config) SetConnectDB(db string) error {
	c.DB = db
	return nil
}

func (c *Config) SetMessageTimeout(timeout time.Duration) error {
	if timeout < time.Second {
		return errors.New("message timeout cannot be less than 1 second")
	}
	c.MessageTimeout = timeout
	return nil
}

func (c *Config) SetWriteWait(writeWait time.Duration) error {
	if writeWait < 0 {
		return errors.New("write wait cannot be less than 0")
	}
	c.WriteWait = writeWait
	return nil
}

func (c *Config) SetErrorHandler(f func(connector *Connector, err error)) {
	c.ErrorHandler = f
}

func (c *Config) SetCloseHandler(f func()) {
	c.CloseHandler = f
}
