package wrapper

/*
#include <taos.h>
*/
import "C"
import (
	"bytes"
	"reflect"
	"unsafe"

	"github.com/taosdata/driver-go/v2/errors"
	"github.com/taosdata/driver-go/v2/types"
)

type RowsHeader struct {
	ColNames  []string
	ColTypes  []uint8
	ColLength []uint16
}

func ReadColumn(result unsafe.Pointer, count int) (*RowsHeader, error) {
	if result == nil {
		return nil, &errors.TaosError{Code: 0xffff, ErrStr: "invalid result"}
	}
	rowsHeader := &RowsHeader{
		ColNames:  make([]string, count),
		ColTypes:  make([]uint8, count),
		ColLength: make([]uint16, count),
	}
	pFields := TaosFetchFields(result)
	for i := 0; i < count; i++ {
		field := *(*C.struct_taosField)(unsafe.Pointer(uintptr(pFields) + uintptr(C.sizeof_struct_taosField*C.int(i))))
		buf := bytes.NewBufferString("")
		for _, c := range field.name {
			if c == 0 {
				break
			}
			buf.WriteByte(byte(c))
		}
		rowsHeader.ColNames[i] = buf.String()
		rowsHeader.ColTypes[i] = (uint8)(field._type)
		rowsHeader.ColLength[i] = (uint16)(field.bytes)
	}
	return rowsHeader, nil
}

func (rh *RowsHeader) TypeDatabaseName(i int) string {
	switch rh.ColTypes[i] {
	case C.TSDB_DATA_TYPE_BOOL:
		return "BOOL"

	case C.TSDB_DATA_TYPE_TINYINT:
		return "TINYINT"

	case C.TSDB_DATA_TYPE_SMALLINT:
		return "SMALLINT"

	case C.TSDB_DATA_TYPE_INT:
		return "INT"

	case C.TSDB_DATA_TYPE_BIGINT:
		return "BIGINT"

	case C.TSDB_DATA_TYPE_UTINYINT:
		return "TINYINT UNSIGNED"

	case C.TSDB_DATA_TYPE_USMALLINT:
		return "SMALLINT UNSIGNED"

	case C.TSDB_DATA_TYPE_UINT:
		return "INT UNSIGNED"

	case C.TSDB_DATA_TYPE_UBIGINT:
		return "BIGINT UNSIGNED"

	case C.TSDB_DATA_TYPE_FLOAT:
		return "FLOAT"

	case C.TSDB_DATA_TYPE_DOUBLE:
		return "DOUBLE"

	case C.TSDB_DATA_TYPE_BINARY:
		return "BINARY"

	case C.TSDB_DATA_TYPE_NCHAR:
		return "NCHAR"

	case C.TSDB_DATA_TYPE_TIMESTAMP:
		return "TIMESTAMP"

	case C.TSDB_DATA_TYPE_JSON:
		return "JSON"

	default:
		return ""
	}
}

var (
	nullInt8    = reflect.TypeOf(types.NullInt8{})
	nullInt16   = reflect.TypeOf(types.NullInt16{})
	nullInt32   = reflect.TypeOf(types.NullInt32{})
	nullInt64   = reflect.TypeOf(types.NullInt64{})
	nullUInt8   = reflect.TypeOf(types.NullUInt8{})
	nullUInt16  = reflect.TypeOf(types.NullUInt16{})
	nullUInt32  = reflect.TypeOf(types.NullUInt32{})
	nullUInt64  = reflect.TypeOf(types.NullUInt64{})
	nullFloat32 = reflect.TypeOf(types.NullFloat32{})
	nullFloat64 = reflect.TypeOf(types.NullFloat64{})
	nullTime    = reflect.TypeOf(types.NullTime{})
	nullBool    = reflect.TypeOf(types.NullBool{})
	nullString  = reflect.TypeOf(types.NullString{})
	nullJson    = reflect.TypeOf(types.NullJson{})
	unknown     = reflect.TypeOf(new(interface{})).Elem()
)

func (rh *RowsHeader) ScanType(i int) reflect.Type {
	//fmt.Println("######## (mf *taosSqlField) scanType() mf.fieldType:", mf.fieldType)
	switch rh.ColTypes[i] {
	case C.TSDB_DATA_TYPE_BOOL:
		return nullBool

	case C.TSDB_DATA_TYPE_TINYINT:
		return nullInt8

	case C.TSDB_DATA_TYPE_SMALLINT:
		return nullInt16

	case C.TSDB_DATA_TYPE_INT:
		return nullInt32

	case C.TSDB_DATA_TYPE_BIGINT:
		return nullInt64

	case C.TSDB_DATA_TYPE_UTINYINT:
		return nullUInt8

	case C.TSDB_DATA_TYPE_USMALLINT:
		return nullUInt16

	case C.TSDB_DATA_TYPE_UINT:
		return nullUInt32

	case C.TSDB_DATA_TYPE_UBIGINT:
		return nullUInt64

	case C.TSDB_DATA_TYPE_FLOAT:
		return nullFloat32

	case C.TSDB_DATA_TYPE_DOUBLE:
		return nullFloat64

	case C.TSDB_DATA_TYPE_BINARY:
		return nullString

	case C.TSDB_DATA_TYPE_NCHAR:
		return nullString

	case C.TSDB_DATA_TYPE_TIMESTAMP:
		return nullTime

	case C.TSDB_DATA_TYPE_JSON:
		return nullJson

	default:
		return unknown
	}
}

func FetchLengths(res unsafe.Pointer, count int) []int {
	lengths := TaosFetchLengths(res)
	result := make([]int, count)
	for i := 0; i < count; i++ {
		result[i] = int(*(*C.int)(unsafe.Pointer(uintptr(lengths) + uintptr(C.sizeof_int*C.int(i)))))
	}
	return result
}
