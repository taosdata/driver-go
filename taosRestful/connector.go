package taosRestful

import (
	"context"
	"database/sql/driver"

	"github.com/taosdata/driver-go/v3/common"
)

type connector struct {
	cfg *config
}

// Connect implements driver.Connector interface.
// Connect returns a connection to the database.
func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	// Connect to Server
	if len(c.cfg.user) == 0 {
		c.cfg.user = common.DefaultUser
	}
	if len(c.cfg.passwd) == 0 {
		c.cfg.passwd = common.DefaultPassword
	}
	if c.cfg.port == 0 {
		c.cfg.port = common.DefaultHttpPort
	}
	if len(c.cfg.net) == 0 {
		c.cfg.net = "http"
	}
	if len(c.cfg.addr) == 0 {
		c.cfg.addr = "127.0.0.1"
	}
	tc, err := newTaosConn(c.cfg)
	return tc, err
}

// Driver implements driver.Connector interface.
// Driver returns &TDengineDriver{}.
func (c *connector) Driver() driver.Driver {
	return &TDengineDriver{}
}
