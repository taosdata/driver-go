package taosWS

import (
	"bytes"
	"database/sql/driver"
	"encoding/json"
	"io"
	"reflect"
	"unsafe"

	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/common/parser"
	taosErrors "github.com/taosdata/driver-go/v3/errors"
)

type rows struct {
	buf           *bytes.Buffer
	blockPtr      unsafe.Pointer
	blockOffset   int
	blockSize     int
	resultID      uint64
	block         []byte
	conn          *taosConn
	fieldsCount   int
	fieldsNames   []string
	fieldsTypes   []uint8
	fieldsLengths []int64
	precision     int
}

func (rs *rows) Columns() []string {
	return rs.fieldsNames
}

func (rs *rows) ColumnTypeDatabaseTypeName(i int) string {
	return common.TypeNameMap[int(rs.fieldsTypes[i])]
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
	parser.ReadRow(dest, rs.blockPtr, rs.blockSize, rs.blockOffset, rs.fieldsTypes, rs.precision)
	rs.blockOffset += 1
	return nil
}

func (rs *rows) taosFetchBlock() error {
	reqID := rs.conn.generateReqID()
	req := &WSFetchReq{
		ReqID: reqID,
		ID:    rs.resultID,
	}
	args, err := json.Marshal(req)
	if err != nil {
		return err
	}
	action := &WSAction{
		Action: WSFetch,
		Args:   args,
	}
	rs.buf.Reset()

	err = jsonI.NewEncoder(rs.buf).Encode(action)
	if err != nil {
		return err
	}
	err = rs.conn.writeText(rs.buf.Bytes())
	if err != nil {
		return err
	}
	var resp WSFetchResp
	err = rs.conn.readTo(&resp)
	if err != nil {
		return err
	}
	if resp.Code != 0 {
		return taosErrors.NewError(resp.Code, resp.Message)
	}
	if resp.Completed {
		rs.blockSize = 0
		return nil
	} else {
		rs.blockSize = resp.Rows
		return rs.fetchBlock()
	}
}

func (rs *rows) fetchBlock() error {
	reqID := rs.conn.generateReqID()
	req := &WSFetchBlockReq{
		ReqID: reqID,
		ID:    rs.resultID,
	}
	args, err := json.Marshal(req)
	if err != nil {
		return err
	}
	action := &WSAction{
		Action: WSFetchBlock,
		Args:   args,
	}
	rs.buf.Reset()
	err = jsonI.NewEncoder(rs.buf).Encode(action)
	if err != nil {
		return err
	}
	err = rs.conn.writeText(rs.buf.Bytes())
	if err != nil {
		return err
	}
	respBytes, err := rs.conn.readBytes()
	if err != nil {
		return err
	}
	rs.block = respBytes
	rs.blockPtr = unsafe.Pointer(*(*uintptr)(unsafe.Pointer(&rs.block)) + uintptr(16))
	rs.blockOffset = 0
	return nil
}

func (rs *rows) freeResult() error {
	tc := rs.conn
	reqID := tc.generateReqID()
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
	return nil
}
