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
	"unsafe"
)

type taosRes struct {
	ref       unsafe.Pointer
	fields    []taosField
	precision NullInt32
	keep      bool
}

func (res *taosRes) Columns() (cols []string) {
	fields := res.fetchFields()
	for _, field := range fields {
		cols = append(cols, field.Name)
	}
	return
}

func (res *taosRes) Close() (err error) {
	if res.keep {
		return
	}
	res.freeResult()
	return
}

type taosField struct {
	Name  string
	Type  uint8
	Bytes int16
}
