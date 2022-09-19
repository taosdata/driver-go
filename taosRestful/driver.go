package taosRestful

import (
	"context"
	"database/sql"
	"database/sql/driver"
)

// TdengineDriver is exported to make the driver directly accessible.
// In general the driver is used via the database/sql package.
type TdengineDriver struct{}

// Open new Connection.
// the DSN string is formatted
func (d TdengineDriver) Open(dsn string) (driver.Conn, error) {
	cfg, err := parseDSN(dsn)
	if err != nil {
		return nil, err
	}
	c := &connector{
		cfg: cfg,
	}
	return c.Connect(context.Background())
}

func init() {
	sql.Register("taosRestful", &TdengineDriver{})
}
