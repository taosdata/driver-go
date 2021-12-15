package af

import (
	"database/sql/driver"
	"io"
	"unsafe"

	"github.com/taosdata/driver-go/v2/errors"
	"github.com/taosdata/driver-go/v2/wrapper"
)

type subscribeRows struct {
	result     unsafe.Pointer
	rowsHeader *wrapper.RowsHeader
	keep       bool
}

func (rs *subscribeRows) Next(values []driver.Value) (err error) {
	if len(values) != len(rs.rowsHeader.ColTypes) {
		err = &errors.TaosError{Code: 0xffff, ErrStr: "values and fields length not match"}
		return
	}
	row := wrapper.TaosFetchRow(rs.result)
	if row == nil {
		return io.EOF
	}
	precision := wrapper.TaosResultPrecision(rs.result)
	lengths := wrapper.FetchLengths(rs.result, len(rs.rowsHeader.ColTypes))
	for i := range rs.rowsHeader.ColTypes {
		values[i] = wrapper.FetchRow(row, i, rs.rowsHeader.ColTypes[i], lengths[i], precision)
	}
	return nil
}

func (rs *subscribeRows) Columns() []string {
	if rs.rowsHeader != nil {
		return rs.rowsHeader.ColNames
	}
	count := wrapper.TaosNumFields(rs.result)
	var err error
	rs.rowsHeader, err = wrapper.ReadColumn(rs.result, count)
	if err != nil {
		return nil
	}
	return rs.rowsHeader.ColNames
}

func (rs *subscribeRows) Close() (err error) {
	return
}
