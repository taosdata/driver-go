package tmq

import (
	"errors"
	"time"
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
	EnableCompression    bool
	AutoReconnect        bool
	ReconnectIntervalMs  int
	ReconnectRetryCount  int
}

func newConfig(url string, chanLength uint) *config {
	return &config{
		Url:        url,
		ChanLength: chanLength,
	}
}

func (c *config) setConnectUser(user string) {
	c.User = user
}

func (c *config) setConnectPass(pass string) {
	c.Password = pass
}

func (c *config) setGroupID(groupID string) {
	c.GroupID = groupID
}

func (c *config) setClientID(clientID string) {
	c.ClientID = clientID
}

func (c *config) setAutoOffsetReset(offsetReset string) {
	c.OffsetRest = offsetReset
}

func (c *config) setMessageTimeout(timeout time.Duration) error {
	if timeout < time.Second {
		return errors.New("ws.message.timeout cannot be less than 1 second")
	}
	c.MessageTimeout = timeout
	return nil
}

func (c *config) setWriteWait(writeWait time.Duration) error {
	if writeWait < time.Second {
		return errors.New("ws.message.writeWait cannot be less than 1 second")
	}
	c.WriteWait = writeWait
	return nil
}

func (c *config) setAutoCommit(enable string) {
	c.AutoCommit = enable
}

func (c *config) setAutoCommitIntervalMS(autoCommitIntervalMS string) {
	c.AutoCommitIntervalMS = autoCommitIntervalMS
}

func (c *config) setSnapshotEnable(enableSnapshot string) {
	c.SnapshotEnable = enableSnapshot
}

func (c *config) setWithTableName(withTableName string) {
	c.WithTableName = withTableName
}

func (c *config) setEnableCompression(enableCompression bool) {
	c.EnableCompression = enableCompression
}

func (c *config) setAutoReconnect(autoReconnect bool) {
	c.AutoReconnect = autoReconnect
}

func (c *config) setReconnectIntervalMs(reconnectIntervalMs int) {
	c.ReconnectIntervalMs = reconnectIntervalMs
}

func (c *config) setReconnectRetryCount(reconnectRetryCount int) {
	c.ReconnectRetryCount = reconnectRetryCount
}
