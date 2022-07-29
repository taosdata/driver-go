package tmq

import (
	"strconv"
	"unsafe"

	"github.com/taosdata/driver-go/v3/errors"
	"github.com/taosdata/driver-go/v3/wrapper"
)

type Config struct {
	cConfig          unsafe.Pointer
	autoCommit       bool
	cb               CommitHandleFunc
	needGetTableName bool
}

type CommitHandleFunc func(*wrapper.TMQCommitCallbackResult)

// NewConfig New TMQ config
func NewConfig() *Config {
	return &Config{cConfig: wrapper.TMQConfNew()}
}

// SetGroupID TMQ set `group.id`
func (c *Config) SetGroupID(groupID string) error {
	return c.SetConfig("group.id", groupID)
}

// SetAutoOffsetReset TMQ set `auto.offset.reset`
func (c *Config) SetAutoOffsetReset(auto string) error {
	return c.SetConfig("auto.offset.reset", auto)
}

// SetConnectIP TMQ set `td.connect.ip`
func (c *Config) SetConnectIP(ip string) error {
	return c.SetConfig("td.connect.ip", ip)
}

// SetConnectUser TMQ set `td.connect.user`
func (c *Config) SetConnectUser(user string) error {
	return c.SetConfig("td.connect.user", user)
}

// SetConnectPass TMQ set `td.connect.pass`
func (c *Config) SetConnectPass(pass string) error {
	return c.SetConfig("td.connect.pass", pass)
}

// SetConnectPort TMQ set `td.connect.port`
func (c *Config) SetConnectPort(port string) error {
	return c.SetConfig("td.connect.port", port)
}

// SetMsgWithTableName TMQ set `msg.with.table.name`
func (c *Config) SetMsgWithTableName(b bool) error {
	c.needGetTableName = b
	return c.SetConfig("msg.with.table.name", strconv.FormatBool(b))
}

func (c *Config) SetConfig(key string, value string) error {
	errCode := wrapper.TMQConfSet(c.cConfig, key, value)
	if errCode != errors.SUCCESS {
		errStr := wrapper.TMQErr2Str(errCode)
		return errors.NewError(int(errCode), errStr)
	}
	return nil
}

// EnableAutoCommit TMQ set `enable.auto.commit` to `true` and set auto commit callback
func (c *Config) EnableAutoCommit(f CommitHandleFunc) error {
	err := c.SetConfig("enable.auto.commit", "true")
	if err != nil {
		return err
	}
	c.cb = f
	c.autoCommit = true
	return nil
}

// DisableAutoCommit TMQ set `enable.auto.commit` to `false`
func (c *Config) DisableAutoCommit() error {
	err := c.SetConfig("enable.auto.commit", "false")
	if err != nil {
		return err
	}
	c.cb = nil
	c.autoCommit = false
	return nil
}

// EnableHeartBeat TMQ set `enable.heartbeat.background` to `true`
func (c *Config) EnableHeartBeat() error {
	return c.SetConfig("enable.heartbeat.background", "true")
}

// Destroy Release TMQ config
func (c *Config) Destroy() {
	wrapper.TMQConfDestroy(c.cConfig)
}
