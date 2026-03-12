package taosWS

import (
	"context"
	"database/sql/driver"

	"github.com/taosdata/driver-go/v3/common"
)

type connector struct {
	cfg *Config
}

// Connect implements driver.Connector interface.
// Connect returns a connection to the database.
func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	cfg := *c.cfg
	// Connect to Server
	if len(cfg.User) == 0 {
		cfg.User = common.DefaultUser
	}
	if len(cfg.Passwd) == 0 {
		cfg.Passwd = common.DefaultPassword
	}
	if cfg.Port == 0 {
		cfg.Port = common.DefaultHttpPort
	}
	if len(cfg.Net) == 0 {
		cfg.Net = "ws"
	}
	if len(cfg.Addr) == 0 {
		cfg.Addr = "127.0.0.1"
	}
	if cfg.ReadTimeout == 0 {
		cfg.ReadTimeout = common.DefaultMessageTimeout
	}
	if cfg.WriteTimeout == 0 {
		cfg.WriteTimeout = common.DefaultWriteWait
	}
	tc, err := newTaosConn(&cfg)
	return tc, err
}

// Driver implements driver.Connector interface.
// Driver returns &TDengineDriver{}.
func (c *connector) Driver() driver.Driver {
	return &TDengineDriver{}
}
