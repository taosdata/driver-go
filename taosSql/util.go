package taosSql

import (
	"database/sql/driver"
	"github.com/taosdata/driver-go/v2/errors"
)

func namedValueToValue(named []driver.NamedValue) ([]driver.Value, error) {
	args := make([]driver.Value, len(named))
	for n, param := range named {
		if len(param.Name) > 0 {
			return nil, &errors.TaosError{Code: 0xffff, ErrStr: "taosSql: driver does not support the use of Named Parameters"}
		}
		args[n] = param.Value
	}
	return args, nil
}
