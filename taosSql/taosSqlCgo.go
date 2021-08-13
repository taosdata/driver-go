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
#cgo CFLAGS: -IC:/TDengine/include -I/usr/include
#cgo linux LDFLAGS: -L/usr/lib -ltaos
#cgo windows LDFLAGS: -LC:/TDengine/driver -ltaos
#cgo darwin LDFLAGS: -L/usr/local/taos/driver -ltaos
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <taos.h>
*/
import "C"

import (
	"unsafe"

	"github.com/taosdata/driver-go/errors"
)

type UserChar struct {
	Str *C.char
	Len int
}

func (mc *taosConn) taosConnect(ip, user, pass, db string, port int) (taos unsafe.Pointer, err error) {
	cUser := C.CString(user)
	cPass := C.CString(pass)
	cdb := C.CString(db)
	defer C.free(unsafe.Pointer(cUser))
	defer C.free(unsafe.Pointer(cPass))
	defer C.free(unsafe.Pointer(cdb))
	var taosObj unsafe.Pointer
	if len(ip) == 0 {
		taosObj = C.taos_connect(nil, cUser, cPass, cdb, (C.ushort)(0))
	} else {
		cip := C.CString(ip)
		defer C.free(unsafe.Pointer(cip))
		taosObj = C.taos_connect(cip, cUser, cPass, cdb, (C.ushort)(port))
	}

	if taosObj == nil {
		return nil, &errors.TaosError{
			Code:   errors.TSC_INVALID_CONNECTION,
			ErrStr: "invalid connection",
		}
	}

	return taosObj, nil
}

func (mc *taosConn) taosQuery(sqlStr string) (int, error) {
	//cSqlStr := (*UserChar)(unsafe.Pointer(&sqlStr))

	cSqlStr := C.CString(sqlStr)
	defer C.free(unsafe.Pointer(cSqlStr))
	if mc.result != nil {
		mc.freeResult()
	}
	mc.result = unsafe.Pointer(C.taos_query(mc.taos, cSqlStr))
	//mc.result = unsafe.Pointer(C.taos_query_c(mc.taos, cSqlStr.Str, C.uint32_t(cSqlStr.Len)))
	code := C.taos_errno(mc.result)
	if code != 0 {
		errStr := C.GoString(C.taos_errstr(mc.result))
		mc.freeResult()

		return 0, &errors.TaosError{
			Code:   int32(code) & 0xffff,
			ErrStr: errStr,
		}
	}

	// read result and save into mc struct
	numFields := int(C.taos_field_count(mc.result))
	if numFields == 0 {
		// there are no select and show kinds of commands
		mc.affectedRows = int(C.taos_affected_rows(mc.result))
		mc.insertId = 0
	}

	return numFields, nil
}

func (mc *taosConn) taosFetchBlock() (uint, error) {
	var block C.TAOS_ROW
	mc.block = unsafe.Pointer(&block)
	mc.blockOffset = 0
	mc.blockSize = uint(C.taos_fetch_block(mc.result, (*C.TAOS_ROW)(mc.block)))
	return mc.blockSize, nil
}

func (mc *taosConn) taosClose() {
	C.taos_close(mc.taos)
	mc.taos = nil
}

//func (mc *taosConn) taosError() {
//	// free local resouce: allocated memory/metric-meta refcnt
//	//var pRes unsafe.Pointer
//	C.taos_free_result(mc.result)
//	mc.result = nil
//}

func (mc *taosConn) freeResult() {
	// free result
	if mc.result != nil {
		C.taos_free_result(mc.result)
		mc.result = nil
	}
}

func (mc *taosConn) setConfiguration() {
	if GetTaosClientConf() != "" {
		c := C.CString(GetTaosClientConf())
		defer C.free(unsafe.Pointer(c))
		C.taos_setConfiguration(c)
	}
}
