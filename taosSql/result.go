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
	"database/sql/driver"
	"errors"
        "bytes"
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
		return nil, errors.New("invalid result")
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
		return errors.New("result is nil!")
	}

	//var row *unsafe.Pointer
	row := C.taos_fetch_row(mc.result)
	if row == nil {
		rows.rs.done = true
		C.taos_free_result(mc.result)
		mc.result = nil
		rows.mc = nil
		return io.EOF
	}

	length := C.taos_fetch_lengths(mc.result);

	//fmt.Printf("sizeof(int): %d, sizeof(int32): %d\n\n", unsafe.Sizeof(int(0)), unsafe.Sizeof(int32(0)))
	// because sizeof(void*)  == sizeof(int*) == 8
	// notes: sizeof(int) == 8 in go, but sizeof(int) == 4 in C.
	for i := range dest {
		currentRow := (unsafe.Pointer)(uintptr(*((*int)(unsafe.Pointer(uintptr(unsafe.Pointer(row)) + uintptr(i)*unsafe.Sizeof(int(0)))))))

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
			break

		case C.TSDB_DATA_TYPE_TINYINT:
			dest[i] = (int8)(*((*int8)(currentRow)))
			break

		case C.TSDB_DATA_TYPE_SMALLINT:
			dest[i] = (int16)(*((*int16)(currentRow)))
			break

		case C.TSDB_DATA_TYPE_INT:
			dest[i] = (int32)(*((*int32)(currentRow))) // notes int32 of go <----> int of C
			break

		case C.TSDB_DATA_TYPE_BIGINT:
			dest[i] = (int64)(*((*int64)(currentRow)))
			break

		case C.TSDB_DATA_TYPE_FLOAT:
			//dest[i] = (*((*float32)(currentRow)))
                        fvLen := *((*int32)(unsafe.Pointer(uintptr(unsafe.Pointer(length)) + uintptr(i)*unsafe.Sizeof(int32(0)))))
			var fi int32
			fVal := make([]byte, fvLen)
			for fi = 0; fi < fvLen; fi++ {
		          fVal[fi] = *((*byte)(unsafe.Pointer(uintptr(currentRow) + uintptr(fi))))
			}
			
			dest[i]=readFloat32(fVal)
			break

		case C.TSDB_DATA_TYPE_DOUBLE:
			//dest[i] = (*((*float64)(currentRow)))
                        dvLen := *((*int32)(unsafe.Pointer(uintptr(unsafe.Pointer(length)) + uintptr(i)*unsafe.Sizeof(int32(0)))))
                        var di int32
                        dVal := make([]byte, dvLen)
                        for di = 0; di < dvLen; di++ {
                          dVal[di] = *((*byte)(unsafe.Pointer(uintptr(currentRow) + uintptr(di))))
                        }

                        dest[i]=readFloat64(dVal)
			break

		case C.TSDB_DATA_TYPE_BINARY, C.TSDB_DATA_TYPE_NCHAR:
			charLen := *((*int32)(unsafe.Pointer(uintptr(unsafe.Pointer(length)) + uintptr(i)*unsafe.Sizeof(int32(0)))))
			var index int32
			binaryVal := make([]byte, charLen)
			for index = 0; index < charLen; index++ {
				binaryVal[index] = *((*byte)(unsafe.Pointer(uintptr(currentRow) + uintptr(index))))
			}
			dest[i] = string(binaryVal[:])
			break

		case C.TSDB_DATA_TYPE_TIMESTAMP:
			if mc.cfg.parseTime == true {
				timestamp := (int64)(*((*int64)(currentRow)))
				dest[i] = timestampConvertToString(timestamp, int(C.taos_result_precision(mc.result)))
			} else {
				dest[i] = (int64)(*((*int64)(currentRow)))
			}
			break

		default:
			fmt.Println("default fieldType: set dest[] to nil")
			dest[i] = nil
			break
		}
	}

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

func timestampConvertToString(timestamp int64, precision int) string {
	switch precision {
	case 0: // milli-second
		s := timestamp / 1e3
		ns := timestamp % 1e3 * 1e6
		return time.Unix(s, ns).Format("2006-01-02 15:04:05.000")
	case 1: // micro-second
		s := timestamp / 1e6
		ns := timestamp % 1e6 * 1e3
		return time.Unix(s, ns).Format("2006-01-02 15:04:05.000000")
	case 2: // nano-second
		s := timestamp / 1e9
		ns := timestamp % 1e9
		return time.Unix(s, ns).Format("2006-01-02 15:04:05.000000000")
	default:
		panic("unknown precision")
	}
}
