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
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <taos.h>
*/
import "C"

import (
	"bytes"
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"io"
	"time"
	"unsafe"
)

/******************************************************************************
*                              Result                                         *
******************************************************************************/
// Read Packets as Field Packets until EOF-Packet or an Error appears
func (mc *taosConn) readColumns(count int) ([]taosSqlField, error) {

	columns := make([]taosSqlField, count)

	if mc.result == nil {
		return nil, &TaosError{Code: 0xffff, ErrStr: "invalid result"}
	}

	pFields := (*C.struct_taosField)(C.taos_fetch_fields(mc.result))

	// TODO: Optimized rewriting !!!!
	fields := (*[1 << 11]C.struct_taosField)(unsafe.Pointer(pFields))

	for i := 0; i < count; i++ {
		//columns[i].tableName = ms.taos.
		//fmt.Println(reflect.TypeOf(fields[i].name))
		var charray []byte
		for j := range fields[i].name {
			//fmt.Println("fields[i].name[j]: ", fields[i].name[j])
			if fields[i].name[j] != 0 {
				charray = append(charray, byte(fields[i].name[j]))
			} else {
				break
			}
		}
		columns[i].name = string(charray)
		columns[i].length = (uint32)(fields[i].bytes)
		columns[i].fieldType = fieldType(fields[i]._type)
		columns[i].flags = 0
		// columns[i].decimals  = 0
		//columns[i].charSet    = 0
	}
	return columns, nil
}

func readFloat32(b []byte) float32 {
	var pi float32
	buf := bytes.NewReader(b)
	err := binary.Read(buf, binary.LittleEndian, &pi)
	if err != nil {
		fmt.Println("binary.Read failed:", err)
	}
	return pi
}

func readFloat64(b []byte) float64 {
	var pi float64
	buf := bytes.NewReader(b)
	err := binary.Read(buf, binary.LittleEndian, &pi)
	if err != nil {
		fmt.Println("binary.Read failed:", err)
	}
	return pi
}

func (rows *taosSqlRows) readRow(dest []driver.Value) error {
	mc := rows.mc

	if rows.rs.done || mc == nil {
		return io.EOF
	}

	if mc.result == nil {
		return &TaosError{Code: 0xffff, ErrStr: "result is nil!"}
	}

	var row C.TAOS_ROW
	if mc.block == nil {
		mc.blockScanned = 0
		mc.taosFetchBlock()
	}
	if mc.blockSize == 0 {
		mc.block = nil
		mc.result = nil
		C.taos_free_result(mc.result)
		return io.EOF
	}

	if mc.blockOffset >= mc.blockSize {
		mc.taosFetchBlock()
	}
	if mc.blockSize == 0 {
		mc.block = nil
		mc.result = nil
		C.taos_free_result(mc.result)
		return io.EOF
	}

	row = *(*C.TAOS_ROW)(unsafe.Pointer(uintptr(mc.block)))

	if row == nil {
		rows.rs.done = true
		C.taos_free_result(mc.result)
		mc.result = nil
		rows.mc = nil
		return io.EOF
	}

	// length := C.taos_fetch_lengths(mc.result)
	for i := range dest {
		pCol := *(*uintptr)(unsafe.Pointer(uintptr(unsafe.Pointer(row)) + uintptr(i)*unsafe.Sizeof(row)))
		currentRow := unsafe.Pointer(pCol + uintptr(mc.blockOffset)*uintptr(rows.rs.columns[i].length))

		if currentRow == nil {
			dest[i] = nil
			continue
		}

		switch rows.rs.columns[i].fieldType {
		case C.TSDB_DATA_TYPE_BOOL:
			if (*((*byte)(currentRow))) != 0 {
				dest[i] = true
			} else {
				dest[i] = false
			}

		case C.TSDB_DATA_TYPE_TINYINT:
			dest[i] = (int8)(*((*int8)(currentRow)))

		case C.TSDB_DATA_TYPE_SMALLINT:
			dest[i] = (int16)(*((*int16)(currentRow)))

		case C.TSDB_DATA_TYPE_INT:
			dest[i] = (int32)(*((*int32)(currentRow))) // notes int32 of go <----> int of C

		case C.TSDB_DATA_TYPE_BIGINT:
			dest[i] = (int64)(*((*int64)(currentRow)))

		case C.TSDB_DATA_TYPE_UTINYINT:
			dest[i] = (uint8)(*((*uint8)(currentRow)))

		case C.TSDB_DATA_TYPE_USMALLINT:
			dest[i] = (uint16)(*((*uint16)(currentRow)))

		case C.TSDB_DATA_TYPE_UINT:
			dest[i] = (uint32)(*((*uint32)(currentRow))) // notes uint32 of go <----> unsigned int of C

		case C.TSDB_DATA_TYPE_UBIGINT:
			dest[i] = (uint64)(*((*uint64)(currentRow)))

		case C.TSDB_DATA_TYPE_FLOAT:
			dest[i] = *((*float32)(currentRow))

		case C.TSDB_DATA_TYPE_DOUBLE:
			dest[i] = *((*float64)(currentRow))

		case C.TSDB_DATA_TYPE_BINARY, C.TSDB_DATA_TYPE_NCHAR:
			currentRow = unsafe.Pointer(pCol + uintptr(mc.blockOffset)*uintptr(rows.rs.columns[i].length+2))
			clen := (int16)(*((*int16)(currentRow)))
			currentRow = unsafe.Pointer(uintptr(currentRow) + 2)
			binaryVal := make([]byte, clen)

			for index := int16(0); index < clen; index++ {
				binaryVal[index] = *((*byte)(unsafe.Pointer(uintptr(currentRow) + uintptr(index))))
			}
			dest[i] = string(binaryVal[:])

		case C.TSDB_DATA_TYPE_TIMESTAMP:
			if mc.cfg.parseTime {
				timestamp := (int64)(*((*int64)(currentRow)))
				dest[i] = timestampConvertToTime(timestamp, int(C.taos_result_precision(mc.result)))
			} else {
				dest[i] = (int64)(*((*int64)(currentRow)))
			}

		default:
			fmt.Println("default fieldType: set dest[] to nil")
			dest[i] = nil
		}
	}
	// if mc.blockOffset == 0 {
	// 	fmt.Println("block ", mc.block, " with size ", mc.blockSize, dest)
	// }
	mc.blockOffset++
	mc.blockScanned++
	return nil
}

// Read result as Field format until all rows or an Error appears
// call this func in conn mode
func (rows *textRows) readRow(dest []driver.Value) error {
	return rows.taosSqlRows.readRow(dest)
}

// call this func in stmt mode
func (rows *binaryRows) readRow(dest []driver.Value) error {
	return rows.taosSqlRows.readRow(dest)
}

func timestampConvertToTime(timestamp int64, precision int) time.Time {
	switch precision {
	case 0: // milli-second
		s := timestamp / 1e3
		ns := timestamp % 1e3 * 1e6
		return time.Unix(s, ns)
	case 1: // micro-second
		s := timestamp / 1e6
		ns := timestamp % 1e6 * 1e3
		return time.Unix(s, ns)
	case 2: // nano-second
		s := timestamp / 1e9
		ns := timestamp % 1e9
		return time.Unix(s, ns)
	default:
		panic("unknown precision")
	}
}
