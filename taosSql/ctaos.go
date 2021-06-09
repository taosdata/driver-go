/*
 * Copyright (c) 2021 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package taosSql

/*
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <taos.h>
#include <taoserror.h>
*/
import "C"

import (
	"database/sql/driver"
	"errors"
	"io"
	"log"
	"time"
	"unsafe"
)

// int taos_result_precision(TAOS_RES *res)
// Get the time precision of result
// call after fetchRow
func (res *taosRes) resultPrecision() int32 {
	if !res.precision.Valid {
		res.precision.Inner = int32(C.taos_result_precision(res.ref))
		res.precision.Valid = true
	}
	return res.precision.Inner
}

// TAOS_ROW taos_fetch_row(TAOS_RES *res)
func (res *taosRes) fetchRow() (row unsafe.Pointer) {
	row = unsafe.Pointer(C.taos_fetch_row(res.ref))
	return
}

// int taos_num_fields(TAOS_RES *res)
// number of the fields in the result set.
func (res *taosRes) numFields() int {
	if res.fields == nil {
		return int(C.taos_num_fields(res.ref))
	}
	return len(res.fields)
}

// int taos_field_count(TAOS_RES *res);
func (res *taosRes) fieldCount() int {
	if res.fields == nil {
		return int(C.taos_field_count(res.ref))
	}
	return len(res.fields)
}

// int taos_affected_rows(TAOS_RES *res)
func (res *taosRes) affectedRows() int64 {
	return int64(C.taos_affected_rows(res.ref))
}

// TAOS_FIELD *taos_fetch_fields(TAOS_RES *res)
func (res *taosRes) fetchFields() []taosField {
	if res.fields != nil {
		return res.fields
	}
	ref := unsafe.Pointer(C.taos_fetch_fields(res.ref))
	cfields := (*[1 << 7]C.struct_taosField)(ref)
	num := res.numFields()
	fields := make([]taosField, num)
	for i := 0; i < num; i++ {
		cfield := cfields[i]
		fields[i] = taosField{
			Name:  C.GoString((*C.char)(unsafe.Pointer(&cfield.name))),
			Type:  uint8(cfield._type),
			Bytes: int16(cfield.bytes),
		}
	}
	return fields
}

// void taos_stop_query(TAOS_RES *res)
func (res *taosRes) stopQuery() {
	C.taos_stop_query(res.ref)
}

// bool taos_is_null(TAOS_RES *res, int32_t row, int32_t col)
func (res *taosRes) isNull(row int, col int) bool {
	return bool(C.taos_is_null(res.ref, C.int(row), C.int(col)))
}

// void taos_free_result(TAOS_RES *res)
func (res *taosRes) freeResult() {
	C.taos_free_result(res.ref)
	res.ref = nil
}

// char *taos_errstr(TAOS_RES *tres)
func (res *taosRes) errstr() string {
	return C.GoString(C.taos_errstr(res.ref))
}

// int taos_errno(TAOS_RES *tres);
func (res *taosRes) errno() int {
	return int(C.taos_errno(res.ref))
}

// call after fetchRow
func (res *taosRes) fetchLengths(num int) (lengths []int32) {
	p := C.taos_fetch_lengths(res.ref)
	if p == nil {
		return
	}
	up := uintptr(unsafe.Pointer(p))
	size := unsafe.Sizeof(int32(0))
	for i := 0; i < num; i++ {
		l := *(*int32)(unsafe.Pointer(up + uintptr(i)*size))
		lengths = append(lengths, l)
	}
	return
}

// *taos_connect(const char *host, const char *user, const char *pass, const char *db, int port)
func taosConnect(host, user, passwd, dbname string, port int) (db *taosDB) {
	chost := C.CString(host)
	if host == "" {
		chost = nil
	}
	cuser := C.CString(user)
	if user == "" {
		cuser = nil
	}
	cpasswd := C.CString(passwd)
	if passwd == "" {
		cpasswd = nil
	}
	cdbname := C.CString(dbname)
	if dbname == "" {
		cdbname = nil
	}
	defer C.free(unsafe.Pointer(chost))
	defer C.free(unsafe.Pointer(cuser))
	defer C.free(unsafe.Pointer(cpasswd))
	defer C.free(unsafe.Pointer(cdbname))
	ref := unsafe.Pointer(C.taos_connect(chost, cuser, cpasswd, cdbname, C.ushort(port)))
	if ref == nil {
		return nil
	}
	db = &taosDB{ref: ref}
	return
}

// TAOS_RES* taos_query(TAOS *taos, const char *sql)
func (db *taosDB) query(sql string) (res *taosRes) {
	csql := C.CString(sql)
	defer C.free(unsafe.Pointer(csql))
	ref := C.taos_query(db.ref, csql)
	if ref != nil {
		res = &taosRes{ref: ref}
	}
	return
}

func (db *taosDB) close() {
	C.taos_close(db.ref)
}

// int taos_select_db(TAOS *taos, const char *db)
func (db *taosDB) selectDB(dbname string) int {
	cdbname := C.CString(dbname)
	defer C.free(unsafe.Pointer(cdbname))
	return int(C.taos_select_db(db.ref, cdbname))
}

func (db *taosDB) subscribe(restart bool, topic string, sql string, interval time.Duration) (sub *taosTopic) {
	ctopic := C.CString(topic)
	csql := C.CString(sql)
	defer C.free(unsafe.Pointer(ctopic))
	defer C.free(unsafe.Pointer(csql))
	flag := 0
	if restart {
		flag = 1
	}
	p := C.taos_subscribe(db.ref, (C.int)(flag), ctopic, csql, nil, nil, (C.int)(int(interval/time.Millisecond)))
	if p != nil {
		sub = &taosTopic{ref: p}
	}
	return
}

func (sub *taosTopic) consume() (res *taosRes) {
	ref := C.taos_consume(unsafe.Pointer(sub.ref))
	if ref != nil {
		res = &taosRes{ref: ref, keep: true}
	}
	return
}

func (sub *taosTopic) unsubscribe(keepProgress int) {
	C.taos_unsubscribe(unsafe.Pointer(sub.ref), (C.int)(keepProgress))
}

func (db *taosDB) stmtInit() (stmt *taosStmt) {
	ref := C.taos_stmt_init(db.ref)
	if ref != nil {
		stmt = &taosStmt{ref: ref}
	}
	return
}

func (stmt *taosStmt) prepare(sql string) int32 {
	csql := C.CString(sql)
	clen := C.ulong(len(sql))
	defer C.free(unsafe.Pointer(csql))
	return int32(C.taos_stmt_prepare(stmt.ref, csql, clen))
}

func (stmt *taosStmt) bindParam(params []driver.Value) int32 {
	if len(params) == 0 {
		return int32(C.taos_stmt_bind_param(stmt.ref, nil))
	}
	binds := make([]C.TAOS_BIND, len(params))
	for i, param := range params {
		bind := C.TAOS_BIND{}
		switch param.(type) {
		case bool:
			bind.buffer_type = C.TSDB_DATA_TYPE_BOOL
			value := param.(bool)
			p := C.malloc(1)
			if value {
				*(*C.int8_t)(p) = C.int8_t(1)
			} else {
				*(*C.int8_t)(p) = C.int8_t(0)
			}
			defer C.free(p)
			bind.buffer = p
		case int8:
			bind.buffer_type = C.TSDB_DATA_TYPE_TINYINT
			value := param.(int8)
			p := C.malloc(1)
			*(*C.int8_t)(p) = C.int8_t(value)
			defer C.free(p)
			bind.buffer = p
		case int16:
			bind.buffer_type = C.TSDB_DATA_TYPE_SMALLINT
			value := param.(int16)
			p := C.malloc(2)
			*(*C.int16_t)(p) = C.int16_t(value)
			defer C.free(p)
			bind.buffer = p
		case int:
			bind.buffer_type = C.TSDB_DATA_TYPE_INT
			value := param.(int)
			p := C.malloc(4)
			*(*C.int32_t)(p) = C.int32_t(value)
			defer C.free(p)
			bind.buffer = p
			bind.is_null = nil
		case int32:
			value := param.(int32)
			bind.buffer_type = C.TSDB_DATA_TYPE_INT
			p := C.malloc(4)
			*(*C.int32_t)(p) = C.int32_t(value)
			defer C.free(p)
			bind.buffer = p
		case int64:
			bind.buffer_type = C.TSDB_DATA_TYPE_BIGINT
			value := param.(int64)
			p := C.malloc(8)
			*(*C.int64_t)(p) = C.int64_t(value)
			defer C.free(p)
			bind.buffer = p
		case uint8:
			bind.buffer_type = C.TSDB_DATA_TYPE_UTINYINT
			buf := param.(uint8)
			cbuf := C.malloc(1)
			*(*C.char)(cbuf) = C.char(buf)
			defer C.free(cbuf)
			bind.buffer = cbuf
		case uint16:
			bind.buffer_type = C.TSDB_DATA_TYPE_USMALLINT
			value := param.(uint16)
			p := C.malloc(2)
			*(*C.int16_t)(p) = C.int16_t(value)
			defer C.free(p)
			bind.buffer = p
		case uint32:
			bind.buffer_type = C.TSDB_DATA_TYPE_UINT
			value := param.(uint32)
			p := C.malloc(4)
			*(*C.uint32_t)(p) = C.uint32_t(value)
			defer C.free(p)
			bind.buffer = p
		case uint64:
			bind.buffer_type = C.TSDB_DATA_TYPE_UBIGINT
			value := param.(uint64)
			p := C.malloc(8)
			*(*C.uint64_t)(p) = C.uint64_t(value)
			defer C.free(p)
			bind.buffer = p
		case float32:
			bind.buffer_type = C.TSDB_DATA_TYPE_FLOAT
			value := param.(float32)
			p := C.malloc(4)
			*(*C.float)(p) = C.float(value)
			defer C.free(p)
			bind.buffer = p
		case float64:
			bind.buffer_type = C.TSDB_DATA_TYPE_DOUBLE
			value := param.(float64)
			p := C.malloc(8)
			*(*C.double)(p) = C.double(value)
			defer C.free(p)
			bind.buffer = p
		case []byte:
			bind.buffer_type = C.TSDB_DATA_TYPE_BINARY
			buf := param.([]byte)
			cbuf := C.CString(string(buf))
			defer C.free(unsafe.Pointer(cbuf))
			bind.buffer = unsafe.Pointer(cbuf)
			clen := int32(len(buf))
			p := C.malloc(C.size_t(unsafe.Sizeof(clen)))
			bind.length = (*C.ulong)(p)
			*(bind.length) = C.ulong(clen)
			defer C.free(p)
		case string:
			bind.buffer_type = C.TSDB_DATA_TYPE_NCHAR
			value := param.(string)
			p := unsafe.Pointer(C.CString(string(value)))
			defer C.free(p)
			bind.buffer = unsafe.Pointer(p)
			clen := int32(len(value))
			bind.length = (*C.ulong)(C.malloc(C.size_t(unsafe.Sizeof(clen))))
			*(bind.length) = C.ulong(clen)
			defer C.free(unsafe.Pointer(bind.length))
		case time.Time:
			bind.buffer_type = C.TSDB_DATA_TYPE_TIMESTAMP
			ts := param.(time.Time)
			p := C.malloc(8)
			defer C.free(p)
			*(*C.int64_t)(p) = C.int64_t(ts.UnixNano() / 1e3)
			bind.buffer = p
		default:
			return -1
		}
		binds[i] = bind
	}
	return int32(C.taos_stmt_bind_param(stmt.ref, (*C.TAOS_BIND)(&binds[0])))
}

func (stmt *taosStmt) isInsert() int32 {
	p := C.malloc(C.size_t(4))
	isInsert := (*C.int)(p)
	defer C.free(p)
	C.taos_stmt_is_insert(stmt.ref, isInsert)
	return int32(*isInsert)
}

func (stmt *taosStmt) addBatch() int32 {
	return int32(C.taos_stmt_add_batch(stmt.ref))
}

func (stmt *taosStmt) execute() int32 {
	return int32(C.taos_stmt_execute(stmt.ref))
}

func (stmt *taosStmt) useResult() (res *taosRes) {
	ref := C.taos_stmt_use_result(stmt.ref)
	if ref != nil {
		res = &taosRes{ref: ref}
	}
	return
}

func (res *taosRes) Next(values []driver.Value) (err error) {
	fields := res.fetchFields()
	if len(values) != len(fields) {
		err = errors.New("values and fields length not match")
		return
	}

	row := res.fetchRow()
	if row == nil {
		return io.EOF
	}
	step := unsafe.Sizeof(int64(0))
	for i := range fields {
		p := (unsafe.Pointer)(uintptr(*((*int)(unsafe.Pointer(uintptr(row) + uintptr(i)*step)))))
		if p == nil {
			continue
		}
		field := fields[i]
		switch field.Type {
		case C.TSDB_DATA_TYPE_BOOL:
			if v := *((*byte)(p)); v != 0 {
				values[i] = true
			} else {
				values[i] = false
			}
		case C.TSDB_DATA_TYPE_TINYINT:
			values[i] = *((*int8)(p))
		case C.TSDB_DATA_TYPE_SMALLINT:
			values[i] = *((*int16)(p))
		case C.TSDB_DATA_TYPE_INT:
			values[i] = *((*int32)(p))
		case C.TSDB_DATA_TYPE_BIGINT:
			values[i] = *((*int64)(p))
		case C.TSDB_DATA_TYPE_UTINYINT:
			values[i] = *((*uint8)(p))
		case C.TSDB_DATA_TYPE_USMALLINT:
			values[i] = *((*uint16)(p))
		case C.TSDB_DATA_TYPE_UINT:
			values[i] = *((*uint32)(p))
		case C.TSDB_DATA_TYPE_UBIGINT:
			values[i] = *((*uint64)(p))
		case C.TSDB_DATA_TYPE_FLOAT:
			values[i] = *((*float32)(p))
		case C.TSDB_DATA_TYPE_DOUBLE:
			values[i] = *((*float64)(p))
		case C.TSDB_DATA_TYPE_BINARY, C.TSDB_DATA_TYPE_NCHAR:
			values[i] = C.GoString((*C.char)(p))
		case C.TSDB_DATA_TYPE_TIMESTAMP:
			ts := *((*int64)(p))
			precision := int(res.resultPrecision())
			values[i] = timestampConvertToTime(ts, precision)
		default:
			log.Println(field)
			values[i] = nil
		}
	}
	return nil
}

func (stmt *taosStmt) close() int32 {
	return int32(C.taos_stmt_close(stmt.ref))
}

func getErrno() int32 {
	return int32(*(*C.int32_t)(C.taosGetErrno()))
}

func tstrerror(code int32) (msg string) {
	return C.GoString(C.tstrerror(C.int32_t(code)))
}
