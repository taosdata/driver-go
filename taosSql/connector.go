package taosSql

import (
	"context"
	"database/sql/driver"
	"github.com/taosdata/driver-go/v2/common"
	"github.com/taosdata/driver-go/v2/errors"
	"github.com/taosdata/driver-go/v2/wrapper"
	"sync"
)

type connector struct {
	cfg *config
}

var once = sync.Once{}

// Connect implements driver.Connector interface.
// Connect returns a connection to the database.
func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	var err error
	tc := &taosConn{
		cfg: c.cfg,
	}
	if c.cfg.net == "cfg" && len(c.cfg.configPath) > 0 {
		once.Do(func() {
			code := wrapper.TaosOptions(common.TSDB_OPTION_CONFIGDIR, c.cfg.configPath)
			err = errors.GetError(code)
		})
	}
	if err != nil {
		return nil, err
	}
	// Connect to Server
	if len(tc.cfg.user) == 0 {
		tc.cfg.user = common.DefaultUser
	}
	if len(tc.cfg.passwd) == 0 {
		tc.cfg.passwd = common.DefaultPassword
	}
	err = wrapper.TaosSetConfig(tc.cfg.params)
	if err != nil {
		return nil, err
	}
	tc.taos, err = wrapper.TaosConnect(tc.cfg.addr, tc.cfg.user, tc.cfg.passwd, tc.cfg.dbName, tc.cfg.port)
	if err != nil {
		return nil, err
	}

	return tc, nil
}

// Driver implements driver.Connector interface.
// Driver returns &tdengineDriver{}.
func (c *connector) Driver() driver.Driver {
	return &tdengineDriver{}
}
