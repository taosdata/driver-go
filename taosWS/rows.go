package taosWS

import (
	"bytes"
	"database/sql/driver"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"unsafe"

	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/common/parser"
	taosErrors "github.com/taosdata/driver-go/v3/errors"
)

type rows struct {
	buf              *bytes.Buffer
	blockPtr         unsafe.Pointer
	blockOffset      int
	blockSize        int
	resultID         uint64
	block            []byte
	conn             *taosConn
	fieldsCount      int
	fieldsNames      []string
	fieldsTypes      []uint8
	fieldsLengths    []int64
	fieldsPrecisions []int64
	fieldsScales     []int64
	precision        int
	isStmt           bool
}

func (rs *rows) ColumnTypePrecisionScale(index int) (precision, scale int64, ok bool) {
	if rs.fieldsTypes[index] == common.TSDB_DATA_TYPE_DECIMAL || rs.fieldsTypes[index] == common.TSDB_DATA_TYPE_DECIMAL64 {
		return rs.fieldsPrecisions[index], rs.fieldsScales[index], true
	}
	return 0, 0, false
}

func (rs *rows) Columns() []string {
	return rs.fieldsNames
}

func (rs *rows) ColumnTypeDatabaseTypeName(i int) string {
	return common.GetTypeName(int(rs.fieldsTypes[i]))
}

func (rs *rows) ColumnTypeLength(i int) (length int64, ok bool) {
	return rs.fieldsLengths[i], ok
}

func (rs *rows) ColumnTypeScanType(i int) reflect.Type {
	t, exist := common.ColumnTypeMap[int(rs.fieldsTypes[i])]
	if !exist {
		return common.UnknownType
	}
	return t
}

func (rs *rows) Close() error {
	rs.blockPtr = nil
	rs.block = nil
	return rs.freeResult()
}

func (rs *rows) Next(dest []driver.Value) error {
	if rs.blockPtr == nil {
		err := rs.taosFetchBlock()
		if err != nil {
			return err
		}
	}
	if rs.blockSize == 0 {
		rs.blockPtr = nil
		rs.block = nil
		return io.EOF
	}
	if rs.blockOffset >= rs.blockSize {
		err := rs.taosFetchBlock()
		if err != nil {
			return err
		}
	}
	if rs.blockSize == 0 {
		rs.blockPtr = nil
		rs.block = nil
		return io.EOF
	}
	err := parser.ReadRow(dest, rs.blockPtr, rs.blockSize, rs.blockOffset, rs.fieldsTypes, rs.precision, rs.fieldsScales)
	if err != nil {
		return err
	}
	rs.blockOffset += 1
	return nil
}

func (rs *rows) taosFetchBlock() error {
	reqID := uint64(common.GetReqID())
	rs.buf.Reset()
	WriteUint64(rs.buf, reqID)       // req id
	WriteUint64(rs.buf, rs.resultID) // message id
	WriteUint64(rs.buf, FetchRawBlockMessage)
	WriteUint16(rs.buf, 1) // version
	err := rs.conn.writeBinary(rs.buf.Bytes())
	if err != nil {
		return err
	}
	respBytes, err := rs.conn.readBytes()
	if err != nil {
		return err
	}
	if len(respBytes) < 51 {
		return taosErrors.NewError(0xffff, "invalid fetch raw block response")
	}
	version := binary.LittleEndian.Uint16(respBytes[16:])
	if version != 1 {
		return taosErrors.NewError(0xffff, fmt.Sprintf("unsupported fetch raw block version: %d", version))
	}
	code := binary.LittleEndian.Uint32(respBytes[34:])
	msgLen := int(binary.LittleEndian.Uint32(respBytes[38:]))
	if len(respBytes) < 51+msgLen {
		return taosErrors.NewError(0xffff, "invalid fetch raw block response")
	}
	errMsg := string(respBytes[42 : 42+msgLen])
	if code != 0 {
		return taosErrors.NewError(int(code), errMsg)
	}
	completed := respBytes[50+msgLen] == 1
	if completed {
		rs.blockSize = 0
		return nil
	}
	if len(respBytes) < 55+msgLen {
		return taosErrors.NewError(0xffff, "invalid fetch raw block response")
	}
	blockLength := binary.LittleEndian.Uint32(respBytes[51+msgLen:])
	if len(respBytes) < 55+msgLen+int(blockLength) {
		return taosErrors.NewError(0xffff, "invalid fetch raw block response")
	}
	rawBlock := respBytes[55+msgLen : 55+msgLen+int(blockLength)]
	rs.block = rawBlock
	rs.blockPtr = unsafe.Pointer(&rs.block[0])
	rs.blockSize = int(parser.RawBlockGetNumOfRows(rs.blockPtr))
	rs.blockOffset = 0
	return nil
}

func (rs *rows) freeResult() error {
	tc := rs.conn
	reqID := uint64(common.GetReqID())
	req := &WSFreeResultReq{
		ReqID: reqID,
		ID:    rs.resultID,
	}
	args, err := json.Marshal(req)
	if err != nil {
		return err
	}
	action := &WSAction{
		Action: WSFreeResult,
		Args:   args,
	}
	rs.buf.Reset()
	err = jsonI.NewEncoder(rs.buf).Encode(action)
	if err != nil {
		return err
	}
	return tc.writeText(rs.buf.Bytes())
}
