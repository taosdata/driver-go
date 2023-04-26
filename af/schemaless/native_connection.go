package schemaless

import (
	"context"
	"unsafe"

	"github.com/taosdata/driver-go/v3/af/locker"
	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/errors"
	"github.com/taosdata/driver-go/v3/wrapper"
)

func newNativeConnection(user, password, host string, port int, db string) (connection, error) {
	if len(user) == 0 {
		user = common.DefaultUser
	}
	if len(password) == 0 {
		password = common.DefaultPassword
	}
	locker.Lock()
	defer locker.Unlock()

	taos, err := wrapper.TaosConnect(host, user, password, db, port)
	if err != nil {
		return nil, err
	}
	return &nativeConnection{taos: taos}, nil
}

type nativeConnection struct {
	taos unsafe.Pointer
}

func (n *nativeConnection) close(ctx context.Context) error {
	locker.Lock()
	defer locker.Unlock()

	if n.taos == nil {
		return nil
	}

	wrapper.TaosClose(n.taos)
	n.taos = nil
	return nil
}

func (n *nativeConnection) insert(_ context.Context, lines string, protocol int, precision string, ttl int, reqID int64) error {
	_, result := wrapper.TaosSchemalessInsertRawTTLWithReqID(n.taos, lines, protocol, precision, ttl, reqID)
	defer func() {
		locker.Lock()
		wrapper.TaosFreeResult(result)
		locker.Unlock()
	}()

	if code := wrapper.TaosError(result); code != 0 {
		errStr := wrapper.TaosErrorStr(result)
		return errors.NewError(code, errStr)
	}
	return nil
}

var _ connection = (*nativeConnection)(nil)
