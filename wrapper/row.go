package wrapper

/*
#include <taos.h>
*/
import "C"
import (
	"database/sql/driver"
	"unsafe"

	"github.com/taosdata/driver-go/v2/common"
)

const (
	PointerSize = unsafe.Sizeof(uintptr(1))
)

type FormatTimeFunc func(ts int64, precision int) driver.Value

func FetchRow(row unsafe.Pointer, offset int, colType uint8, length int, arg ...interface{}) driver.Value {
	p := unsafe.Pointer(*(*uintptr)(unsafe.Pointer(uintptr(row) + uintptr(offset)*PointerSize)))
	if p == nil {
		return nil
	}
	switch colType {
	case C.TSDB_DATA_TYPE_BOOL:
		if v := *((*byte)(p)); v != 0 {
			return true
		} else {
			return false
		}
	case C.TSDB_DATA_TYPE_TINYINT:
		return *((*int8)(p))
	case C.TSDB_DATA_TYPE_SMALLINT:
		return *((*int16)(p))
	case C.TSDB_DATA_TYPE_INT:
		return *((*int32)(p))
	case C.TSDB_DATA_TYPE_BIGINT:
		return *((*int64)(p))
	case C.TSDB_DATA_TYPE_UTINYINT:
		return *((*uint8)(p))
	case C.TSDB_DATA_TYPE_USMALLINT:
		return *((*uint16)(p))
	case C.TSDB_DATA_TYPE_UINT:
		return *((*uint32)(p))
	case C.TSDB_DATA_TYPE_UBIGINT:
		return *((*uint64)(p))
	case C.TSDB_DATA_TYPE_FLOAT:
		return *((*float32)(p))
	case C.TSDB_DATA_TYPE_DOUBLE:
		return *((*float64)(p))
	case C.TSDB_DATA_TYPE_BINARY, C.TSDB_DATA_TYPE_NCHAR:
		data := make([]byte, length)
		for i := 0; i < length; i++ {
			data[i] = *((*byte)(unsafe.Pointer(uintptr(p) + uintptr(i))))
		}
		return string(data)
	case C.TSDB_DATA_TYPE_TIMESTAMP:
		if len(arg) == 1 {
			return common.TimestampConvertToTime(*((*int64)(p)), arg[0].(int))
		} else if len(arg) == 2 {
			return arg[1].(FormatTimeFunc)(*((*int64)(p)), arg[0].(int))
		} else {
			panic("convertTime error")
		}
	case C.TSDB_DATA_TYPE_JSON:
		data := make([]byte, length)
		for i := 0; i < length; i++ {
			data[i] = *((*byte)(unsafe.Pointer(uintptr(p) + uintptr(i))))
		}
		return data
	default:
		return nil
	}
}
