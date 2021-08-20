package af

import (
	"database/sql/driver"
	"github.com/taosdata/driver-go/v2/errors"
	"github.com/taosdata/driver-go/v2/wrapper"
	"unsafe"
)

type Subscriber interface {
	Consume() (driver.Rows, error)
	Unsubscribe(keepProgress bool)
}

type taosSubscriber struct {
	sub unsafe.Pointer
}

func (s *taosSubscriber) Consume() (driver.Rows, error) {
	result := wrapper.TaosConsume(s.sub)
	code := wrapper.TaosError(result)
	if code != 0 {
		err := &errors.TaosError{Code: int32(code) & 0xffff, ErrStr: wrapper.TaosErrorStr(result)}
		return nil, err
	}
	count := wrapper.TaosNumFields(result)
	rh, err := wrapper.ReadColumn(result, count)
	if err != nil {
		return nil, err
	}
	return &subscribeRows{result: result, rowsHeader: rh}, nil
}

func (s *taosSubscriber) Unsubscribe(keepProgress bool) {
	wrapper.TaosUnsubscribe(s.sub, keepProgress)
}
