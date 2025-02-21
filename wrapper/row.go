package wrapper

/*
#include <taos.h>
*/
import "C"
import (
	"database/sql/driver"
	"unsafe"

	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/common/pointer"
)

const (
	PointerSize = unsafe.Sizeof(uintptr(1))
)

type FormatTimeFunc func(ts int64, precision int) driver.Value

func FetchRow(row unsafe.Pointer, offset int, colType uint8, length int, arg ...interface{}) driver.Value {
	base := *(**C.void)(pointer.AddUintptr(row, uintptr(offset)*PointerSize))
	p := unsafe.Pointer(base)
	if p == nil {
		return nil
	}
	switch colType {
	case C.TSDB_DATA_TYPE_BOOL:
		if v := *((*byte)(p)); v != 0 {
			return true
		}
		return false
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
			data[i] = *((*byte)(pointer.AddUintptr(p, uintptr(i))))
		}
		return string(data)
	case C.TSDB_DATA_TYPE_TIMESTAMP:
		if len(arg) == 1 {
			return common.TimestampConvertToTime(*((*int64)(p)), arg[0].(int))
		} else if len(arg) == 2 {
			return arg[1].(FormatTimeFunc)(*((*int64)(p)), arg[0].(int))
		}
		panic("convertTime error")
	case C.TSDB_DATA_TYPE_JSON, C.TSDB_DATA_TYPE_VARBINARY, C.TSDB_DATA_TYPE_GEOMETRY:
		data := make([]byte, length)
		for i := 0; i < length; i++ {
			data[i] = *((*byte)(pointer.AddUintptr(p, uintptr(i))))
		}
		return data
	default:
		return nil
	}
}
