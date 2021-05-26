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
*/
import "C"

import (
	"time"
	"unsafe"
)

const (
	dataTypeBool      = uint8(C.TSDB_DATA_TYPE_BOOL)
	dataTypeTinyint   = uint8(C.TSDB_DATA_TYPE_TINYINT)
	dataTypeSmallint  = uint8(C.TSDB_DATA_TYPE_SMALLINT)
	dataTypeInt       = uint8(C.TSDB_DATA_TYPE_INT)
	dataTypeBigint    = uint8(C.TSDB_DATA_TYPE_BIGINT)
	dataTypeUtinyint  = uint8(C.TSDB_DATA_TYPE_UTINYINT)
	dataTypeUsmallint = uint8(C.TSDB_DATA_TYPE_USMALLINT)
	dataTypeUint      = uint8(C.TSDB_DATA_TYPE_UINT)
	dataTypeUbigint   = uint8(C.TSDB_DATA_TYPE_UBIGINT)
	dataTypeFloat     = uint8(C.TSDB_DATA_TYPE_FLOAT)
	dataTypeDouble    = uint8(C.TSDB_DATA_TYPE_DOUBLE)
	dataTypeBinary    = uint8(C.TSDB_DATA_TYPE_BINARY)
	dataTypeNchar     = uint8(C.TSDB_DATA_TYPE_NCHAR)
	dataTypeTimestamp = uint8(C.TSDB_DATA_TYPE_TIMESTAMP)
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
		field := taosField{}
		l := len(cfield.name)
		b := make([]byte, l)
		for i, c := range cfield.name {
			if c == 0 {
				l = i
				break
			}
			b[i] = byte(c)
		}
		field.Name = string(b[0:l])
		field.Type = uint8(cfield._type)
		field.Bytes = int16(cfield.bytes)
		fields[i] = field
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
		res = &taosRes{ref: ref}
	}
	return
}

func (sub *taosTopic) unsubscribe(keepProgress int) {
	C.taos_unsubscribe(unsafe.Pointer(sub.ref), (C.int)(keepProgress))
}
