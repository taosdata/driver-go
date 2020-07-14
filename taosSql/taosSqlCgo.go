/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
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
#cgo CFLAGS : -I/usr/include
#cgo LDFLAGS: -L/usr/lib -ltaos
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <taos.h>
*/
import "C"

import (
	"errors"
	"unsafe"
)

type UserChar struct {
	Str *C.char
	Len int
}

func (mc *taosConn) taosConnect(ip, user, pass, db string, port int) (taos unsafe.Pointer, err error) {
	cip := (*UserChar)(unsafe.Pointer(&ip))
	cuser := (*UserChar)(unsafe.Pointer(&user))
	cpass := (*UserChar)(unsafe.Pointer(&pass))
	cdb := (*UserChar)(unsafe.Pointer(&db))
	taosObj := C.taos_connect_c(cip.Str, C.uint8_t(cip.Len), cuser.Str, C.uint8_t(cuser.Len),
		cpass.Str, C.uint8_t(cpass.Len), cdb.Str, C.uint8_t(cdb.Len), (C.ushort)(port))
	if taosObj == nil {
		return nil, errors.New("taos_connect() fail!")
	}

	return (unsafe.Pointer)(taosObj), nil
}

func (mc *taosConn) taosQuery(sqlstr string) (int, error) {
	csqlstr := (*UserChar)(unsafe.Pointer(&sqlstr))
	if mc.result != nil {
		C.taos_free_result(mc.result)
		mc.result = nil
	}
	mc.result = unsafe.Pointer(C.taos_query_c(mc.taos, csqlstr.Str, C.uint32_t(csqlstr.Len)))
	code := C.taos_errno(mc.result)
	if 0 != code {

		errStr := C.GoString(C.taos_errstr(mc.result))
		mc.taos_error()
		return 0, errors.New(errStr)

	}

	// read result and save into mc struct
	num_fields := int(C.taos_field_count(mc.result))
	if 0 == num_fields { // there are no select and show kinds of commands
		mc.affectedRows = int(C.taos_affected_rows(mc.result))
		mc.insertId = 0
	}

	return num_fields, nil
}

func (mc *taosConn) taos_close() {
	C.taos_close(mc.taos)
	mc.taos = nil
}

func (mc *taosConn) taos_error() {
	// free local resouce: allocated memory/metric-meta refcnt
	//var pRes unsafe.Pointer
	C.taos_free_result(mc.result)
	mc.result = nil
}
