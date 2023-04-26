package tmq

import (
	"errors"
	"fmt"
	"time"

	"github.com/taosdata/driver-go/v3/common/tmq"
)

type config struct {
	Url                  string
	ChanLength           uint
	MessageTimeout       time.Duration
	WriteWait            time.Duration
	User                 string
	Password             string
	GroupID              string
	ClientID             string
	OffsetRest           string
	AutoCommit           string
	AutoCommitIntervalMS string
	SnapshotEnable       string
	WithTableName        string
}

func newConfig(url string, chanLength uint) *config {
	return &config{
		Url:        url,
		ChanLength: chanLength,
	}
}

func (c *config) setConnectUser(user tmq.ConfigValue) error {
	var ok bool
	c.User, ok = user.(string)
	if !ok {
		return fmt.Errorf("td.connect.user requires string got %T", user)
	}
	return nil
}

func (c *config) setConnectPass(pass tmq.ConfigValue) error {
	var ok bool
	c.Password, ok = pass.(string)
	if !ok {
		return fmt.Errorf("td.connect.pass requires string got %T", pass)
	}
	return nil
}

func (c *config) setGroupID(groupID tmq.ConfigValue) error {
	var ok bool
	c.GroupID, ok = groupID.(string)
	if !ok {
		return fmt.Errorf("group.id requires string got %T", groupID)
	}
	return nil
}

func (c *config) setClientID(clientID tmq.ConfigValue) error {
	var ok bool
	c.ClientID, ok = clientID.(string)
	if !ok {
		return fmt.Errorf("client.id requires string got %T", clientID)
	}
	return nil
}

func (c *config) setAutoOffsetReset(offsetReset tmq.ConfigValue) error {
	var ok bool
	c.OffsetRest, ok = offsetReset.(string)
	if !ok {
		return fmt.Errorf("auto.offset.reset requires string got %T", offsetReset)
	}
	return nil
}

func (c *config) setMessageTimeout(timeout tmq.ConfigValue) error {
	var ok bool
	c.MessageTimeout, ok = timeout.(time.Duration)
	if !ok {
		return fmt.Errorf("ws.message.timeout requires time.Duration got %T", timeout)
	}
	if c.MessageTimeout < time.Second {
		return errors.New("ws.message.timeout cannot be less than 1 second")
	}
	return nil
}

func (c *config) setWriteWait(writeWait tmq.ConfigValue) error {
	var ok bool
	c.WriteWait, ok = writeWait.(time.Duration)
	if !ok {
		return fmt.Errorf("ws.message.writeWait requires time.Duration got %T", writeWait)
	}
	if c.WriteWait < time.Second {
		return errors.New("ws.message.writeWait cannot be less than 1 second")
	}
	if c.WriteWait < 0 {
		return errors.New("ws.message.writeWait cannot be less than 0")
	}
	return nil
}

func (c *config) setAutoCommit(enable tmq.ConfigValue) error {
	var ok bool
	c.AutoCommit, ok = enable.(string)
	if !ok {
		return fmt.Errorf("enable.auto.commit requires string got %T", enable)
	}
	return nil
}

func (c *config) setAutoCommitIntervalMS(autoCommitIntervalMS tmq.ConfigValue) error {
	var ok bool
	c.AutoCommitIntervalMS, ok = autoCommitIntervalMS.(string)
	if !ok {
		return fmt.Errorf("auto.commit.interval.ms requires string got %T", autoCommitIntervalMS)
	}
	return nil
}

func (c *config) setSnapshotEnable(enableSnapshot tmq.ConfigValue) error {
	var ok bool
	c.SnapshotEnable, ok = enableSnapshot.(string)
	if !ok {
		return fmt.Errorf("experimental.snapshot.enable requires string got %T", enableSnapshot)
	}
	return nil
}

func (c *config) setWithTableName(withTableName tmq.ConfigValue) error {
	var ok bool
	c.SnapshotEnable, ok = withTableName.(string)
	if !ok {
		return fmt.Errorf("msg.with.table.name requires string got %T", withTableName)
	}
	return nil
}
