package taosSql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"runtime"
	"sync"

	"github.com/taosdata/driver-go/v2/wrapper/handler"
	"github.com/taosdata/driver-go/v2/wrapper/thread"
)

var locker *thread.Locker
var onceInitLock = sync.Once{}
var asyncHandlerPool *handler.HandlerPool
var onceInitHandlerPool = sync.Once{}

// tdengineDriver is exported to make the driver directly accessible.
// In general the driver is used via the database/sql package.
type tdengineDriver struct{}

// Open new Connection.
// the DSN string is formatted
func (d tdengineDriver) Open(dsn string) (driver.Conn, error) {
	cfg, err := parseDSN(dsn)
	if err != nil {
		return nil, err
	}
	c := &connector{
		cfg: cfg,
	}
	onceInitLock.Do(func() {
		threads := cfg.cgoThread
		if threads <= 0 {
			threads = runtime.NumCPU()
		}
		locker = thread.NewLocker(threads)
	})
	onceInitHandlerPool.Do(func() {
		poolSize := cfg.cgoAsyncHandlerPoolSize
		if poolSize <= 0 {
			poolSize = 10000
		}
		asyncHandlerPool = handler.NewHandlerPool(poolSize)
	})
	return c.Connect(context.Background())
}

func init() {
	sql.Register("taosSql", &tdengineDriver{})
}
