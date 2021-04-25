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

import "C"
import (
	"database/sql/driver"
	"errors"
	"fmt"
)

type Int8 int8
type Int16 int16
type Int32 int32
type Int64 int64
type UInt8 int8
type UInt16 int16
type UInt32 int32
type UInt64 int64

type NullInt64 struct {
	Inner int64
	Valid bool // Valid is true if Inner is not NULL
}

// Scan implements the Scanner interface.
func (n *NullInt64) Scan(value interface{}) error {
	if value == nil {
		n.Inner, n.Valid = 0, false
		return nil
	}
	n.Valid = true
	v, ok := value.(int64)
	if !ok {
		return errors.New("taosSql parse int64 errer")
	}
	n.Inner = v
	return nil
}

// Value implements the driver Valuer interface.
func (n NullInt64) Value() (driver.Value, error) {
	if !n.Valid {
		return nil, nil
	}
	return n.Inner, nil
}

type NullInt32 struct {
	Inner int32
	Valid bool // Valid is true if Inner is not NULL
}

// Scan implements the Scanner interface.
func (n *NullInt32) Scan(value interface{}) error {
	if value == nil {
		n.Inner, n.Valid = 0, false
		return nil
	}
	n.Valid = true
	v, ok := value.(int32)
	if !ok {
		return errors.New("taosSql parse int32 errer")
	}
	n.Inner = v
	return nil
}

// Value implements the driver Valuer interface.
func (n NullInt32) Value() (driver.Value, error) {
	if !n.Valid {
		return nil, nil
	}
	return n.Inner, nil
}

type NullInt16 struct {
	Inner int16
	Valid bool // Valid is true if Inner is not NULL
}

// Scan implements the Scanner interface.
func (n *NullInt16) Scan(value interface{}) error {
	if value == nil {
		n.Inner, n.Valid = 0, false
		return nil
	}
	n.Valid = true
	v, ok := value.(int16)
	if !ok {
		return errors.New("taosSql parse int16 errer")
	}
	n.Inner = v
	return nil
}

// Value implements the driver Valuer interface.
func (n NullInt16) Value() (driver.Value, error) {
	if !n.Valid {
		return nil, nil
	}
	return n.Inner, nil
}

type NullInt8 struct {
	Inner int8
	Valid bool // Valid is true if Inner is not NULL
}

// Scan implements the Scanner interface.
func (n *NullInt8) Scan(value interface{}) error {
	if value == nil {
		n.Inner, n.Valid = 0, false
		return nil
	}
	n.Valid = true
	v, ok := value.(int8)
	if !ok {
		return errors.New("taosSql parse int8 errer")
	}
	n.Inner = v
	return nil
}

// Value implements the driver Valuer interface.
func (n NullInt8) Value() (driver.Value, error) {
	if !n.Valid {
		return nil, nil
	}
	return n.Inner, nil
}

type NullUInt64 struct {
	Inner uint64
	Valid bool // Valid is true if Inner is not NULL
}

// Scan implements the Scanner interface.
func (n *NullUInt64) Scan(value interface{}) error {
	if value == nil {
		n.Inner, n.Valid = 0, false
		return nil
	}
	n.Valid = true
	v, ok := value.(uint64)
	if !ok {
		return errors.New("taosSql parse uint64 errer")
	}
	n.Inner = v
	return nil
}

// Value implements the driver Valuer interface.
func (n NullUInt64) Value() (driver.Value, error) {
	if !n.Valid {
		return nil, nil
	}
	return n.Inner, nil
}

type NullUInt32 struct {
	Inner uint32
	Valid bool // Valid is true if Inner is not NULL
}

// Scan implements the Scanner interface.
func (n *NullUInt32) Scan(value interface{}) error {
	if value == nil {
		n.Inner, n.Valid = 0, false
		return nil
	}
	n.Valid = true
	v, ok := value.(uint32)
	if !ok {
		return errors.New("taosSql parse uint32 errer")
	}
	n.Inner = v
	return nil
}

// Value implements the driver Valuer interface.
func (n NullUInt32) Value() (driver.Value, error) {
	if !n.Valid {
		return nil, nil
	}
	return n.Inner, nil
}

type NullUInt16 struct {
	Inner uint16
	Valid bool // Valid is true if Inner is not NULL
}

// Scan implements the Scanner interface.
func (n *NullUInt16) Scan(value interface{}) error {
	if value == nil {
		n.Inner, n.Valid = 0, false
		return nil
	}
	n.Valid = true
	v, ok := value.(uint16)
	if !ok {
		return errors.New("taosSql parse uint16 errer")
	}
	n.Inner = v
	return nil
}

// Value implements the driver Valuer interface.
func (n NullUInt16) Value() (driver.Value, error) {
	if !n.Valid {
		return nil, nil
	}
	return n.Inner, nil
}

type NullUInt8 struct {
	Inner uint8
	Valid bool // Valid is true if Inner is not NULL
}

// Scan implements the Scanner interface.
func (n *NullUInt8) Scan(value interface{}) error {
	if value == nil {
		n.Inner, n.Valid = 0, false
		return nil
	}
	n.Valid = true
	v, ok := value.(uint8)
	if !ok {
		return errors.New("taosSql parse uint8 errer")
	}
	n.Inner = v
	return nil
}

// Value implements the driver Valuer interface.
func (n NullUInt8) Value() (driver.Value, error) {
	if !n.Valid {
		return nil, nil
	}
	return n.Inner, nil
}

func (v NullUInt8) String() string {
	if v.Valid {
		return fmt.Sprintf("%v", v.Inner)
	}
	return "NULL"
}
func (v NullUInt16) String() string {
	if v.Valid {
		return fmt.Sprintf("%v", v.Inner)
	}
	return "NULL"
}

func (v NullUInt32) String() string {
	if v.Valid {
		return fmt.Sprintf("%v", v.Inner)
	}
	return "NULL"
}

func (v NullUInt64) String() string {
	if v.Valid {
		return fmt.Sprintf("%v", v.Inner)
	}
	return "NULL"
}
func (v NullInt8) String() string {
	if v.Valid {
		return fmt.Sprintf("%v", v.Inner)
	}
	return "NULL"
}
func (v NullInt16) String() string {
	if v.Valid {
		return fmt.Sprintf("%v", v.Inner)
	}
	return "NULL"
}

func (v NullInt32) String() string {
	if v.Valid {
		return fmt.Sprintf("%v", v.Inner)
	}
	return "NULL"
}

func (v NullInt64) String() string {
	if v.Valid {
		return fmt.Sprintf("%v", v.Inner)
	}
	return "NULL"
}
