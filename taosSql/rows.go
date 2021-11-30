package taosSql

import (
	"database/sql/driver"
	"io"
	"reflect"
	"unsafe"

	"github.com/taosdata/driver-go/v2/errors"
	"github.com/taosdata/driver-go/v2/wrapper"
)

type rows struct {
	rowsHeader  *wrapper.RowsHeader
	done        bool
	block       unsafe.Pointer
	blockOffset int
	blockSize   int
	lengthList  []int
	result      unsafe.Pointer
}

func (rs *rows) Columns() []string {
	return rs.rowsHeader.ColNames
}

func (rs *rows) ColumnTypeDatabaseTypeName(i int) string {
	return rs.rowsHeader.TypeDatabaseName(i)
}

func (rs *rows) ColumnTypeLength(i int) (length int64, ok bool) {
	return int64(rs.rowsHeader.ColLength[i]), true
}

func (rs *rows) ColumnTypeScanType(i int) reflect.Type {
	return rs.rowsHeader.ScanType(i)
}

func (rs *rows) Close() error {
	rs.freeResult()
	rs.block = nil
	return nil
}

func (rs *rows) Next(dest []driver.Value) error {
	if rs.done {
		return io.EOF
	}

	if rs.result == nil {
		return &errors.TaosError{Code: 0xffff, ErrStr: "result is nil!"}
	}

	if rs.block == nil {
		rs.taosFetchBlock()
	}
	if rs.blockSize == 0 {
		rs.block = nil
		rs.freeResult()
		return io.EOF
	}

	if rs.blockOffset >= rs.blockSize {
		rs.taosFetchBlock()
	}
	if rs.blockSize == 0 {
		rs.block = nil
		rs.freeResult()
		return io.EOF
	}
	wrapper.ReadRow(dest, rs.result, rs.block, rs.blockOffset, rs.lengthList, rs.rowsHeader.ColTypes)
	rs.blockOffset++
	return nil
}

func (rs *rows) taosFetchBlock() {
	rs.blockSize, rs.block = wrapper.TaosFetchBlock(rs.result)
	if len(rs.lengthList) == 0 {
		rs.lengthList = wrapper.FetchLengths(rs.result, len(rs.rowsHeader.ColLength))
	}
	rs.blockOffset = 0
}

func (rs *rows) freeResult() {
	if rs.result != nil {
		wrapper.TaosFreeResult(rs.result)
		rs.result = nil
	}
}
