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

import (
	"database/sql/driver"
	"errors"
	"io"
	"log"
	"unsafe"
)

type taosRes struct {
	ref       unsafe.Pointer
	fields    []taosField
	precision NullInt32
}

func (res *taosRes) Columns() (cols []string) {
	fields := res.fetchFields()
	for _, field := range fields {
		cols = append(cols, field.Name)
	}
	return
}

func (res *taosRes) Close() error {
	res.freeResult()
	return nil
}

func (res *taosRes) Next(values []driver.Value) (err error) {
	fields := res.fetchFields()
	if len(values) != len(fields) {
		err = errors.New("values and fields length not match")
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
		case dataTypeBool:
			if v := *((*byte)(p)); v != 0 {
				values[i] = true
			} else {
				values[i] = false
			}
		case dataTypeTinyint:
			values[i] = *((*int8)(p))
		case dataTypeSmallint:
			values[i] = *((*int16)(p))
		case dataTypeInt:
			values[i] = *((*int32)(p))
		case dataTypeBigint:
			values[i] = *((*int64)(p))
		case dataTypeUtinyint:
			values[i] = *((*uint8)(p))
		case dataTypeUsmallint:
			values[i] = *((*uint16)(p))
		case dataTypeUint:
			values[i] = *((*uint32)(p))
		case dataTypeUbigint:
			values[i] = *((*uint64)(p))
		case dataTypeFloat:
			values[i] = *((*float32)(p))
		case dataTypeDouble:
			values[i] = *((*float64)(p))
		case dataTypeBinary, dataTypeNchar:
			b := make([]byte, field.Bytes)
			var j int16
			for j = 0; j < field.Bytes; j++ {
				c := *((*byte)(unsafe.Pointer(uintptr(p) + uintptr(j))))
				if c == 0 {
					break
				}
				b[j] = c
			}
			values[i] = string(b[0:j])
		case dataTypeTimestamp:
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

type taosField struct {
	Name  string
	Type  uint8
	Bytes int16
}
