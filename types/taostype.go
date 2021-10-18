package types

import (
	"reflect"
	"time"
)

type TaosBool bool
type TaosTinyint int8
type TaosSmallint int16
type TaosInt int32
type TaosBigint int64
type TaosUTinyint uint8
type TaosUSmallint uint16
type TaosUInt uint32
type TaosUBigint uint64
type TaosFloat float32
type TaosDouble float64
type TaosBinary []byte
type TaosNchar string
type TaosTimestamp struct {
	T         time.Time
	Precision int
}

var (
	TaosBoolType      = reflect.TypeOf(TaosBool(false))
	TaosTinyintType   = reflect.TypeOf(TaosTinyint(0))
	TaosSmallintType  = reflect.TypeOf(TaosSmallint(0))
	TaosIntType       = reflect.TypeOf(TaosInt(0))
	TaosBigintType    = reflect.TypeOf(TaosBigint(0))
	TaosUTinyintType  = reflect.TypeOf(TaosUTinyint(0))
	TaosUSmallintType = reflect.TypeOf(TaosUSmallint(0))
	TaosUIntType      = reflect.TypeOf(TaosUInt(0))
	TaosUBigintType   = reflect.TypeOf(TaosUBigint(0))
	TaosFloatType     = reflect.TypeOf(TaosFloat(0))
	TaosDoubleType    = reflect.TypeOf(TaosDouble(0))
	TaosBinaryType    = reflect.TypeOf(TaosBinary(nil))
	TaosNcharType     = reflect.TypeOf(TaosNchar(""))
	TaosTimestampType = reflect.TypeOf(TaosTimestamp{})
)

type ColumnType struct {
	Type   reflect.Type
	MaxLen int
}
