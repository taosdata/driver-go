package tmq

import (
	"strconv"
	"unsafe"

	"github.com/taosdata/driver-go/v2/errors"
	"github.com/taosdata/driver-go/v2/wrapper"
)

type Config struct {
	cConfig          unsafe.Pointer
	autoCommit       bool
	cb               CommitHandleFunc
	needGetTableName bool
}

type CommitHandleFunc func(*wrapper.TMQCommitCallbackResult)

func NewConfig() *Config {
	return &Config{cConfig: wrapper.TMQConfNew()}
}

func (c *Config) SetGroupID(groupID string) error {
	return c.SetConfig("group.id", groupID)
}

func (c *Config) SetAutoOffsetReset(auto bool) error {
	return c.SetConfig("auto.offset.reset", strconv.FormatBool(auto))
}

func (c *Config) SetConnectIP(ip string) error {
	return c.SetConfig("td.connect.ip", ip)
}

func (c *Config) SetConnectUser(user string) error {
	return c.SetConfig("td.connect.user", user)
}

func (c *Config) SetConnectPass(pass string) error {
	return c.SetConfig("td.connect.pass", pass)
}

func (c *Config) SetConnectPort(port string) error {
	return c.SetConfig("td.connect.port", port)
}

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

func (c *Config) EnableAutoCommit(f CommitHandleFunc) error {
	err := c.SetConfig("enable.auto.commit", "true")
	if err != nil {
		return err
	}
	c.cb = f
	c.autoCommit = true
	return nil
}

func (c *Config) DisableAutoCommit() error {
	err := c.SetConfig("enable.auto.commit", "false")
	if err != nil {
		return err
	}
	c.cb = nil
	c.autoCommit = false
	return nil
}

func (c *Config) Destroy() {
	wrapper.TMQConfDestroy(c.cConfig)
}
