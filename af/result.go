package af

import (
	"database/sql/driver"
	"github.com/taosdata/driver-go/v2/errors"
	"github.com/taosdata/driver-go/v2/wrapper"
	"io"
	"unsafe"
)

type subscribeRows struct {
	result unsafe.Pointer
	rh     *wrapper.RowsHeader
	keep   bool
}

func (rs *subscribeRows) Next(values []driver.Value) (err error) {
	if len(values) != len(rs.rh.ColTypes) {
		err = &errors.TaosError{Code: 0xffff, ErrStr: "values and fields length not match"}
		return
	}
	row := wrapper.TaosFetchRow(rs.result)
	if row == nil {
		return io.EOF
	}
	precision := wrapper.TaosResultPrecision(rs.result)
	for i := range rs.rh.ColTypes {
		values[i] = wrapper.FetchRow(row, i, rs.rh.ColTypes[i], precision)
	}
	return nil
}

func (rs *subscribeRows) Columns() []string {
	if rs.rh != nil {
		return rs.rh.ColNames
	}
	count := wrapper.TaosNumFields(rs.result)
	var err error
	rs.rh, err = wrapper.ReadColumn(rs.result, count)
	if err != nil {
		return nil
	}
	return rs.rh.ColNames
}

func (rs *subscribeRows) Close() (err error) {
	return
}
