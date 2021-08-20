package wrapper

/*
#include <taos.h>
*/
import "C"
import (
	"database/sql/driver"
	"github.com/taosdata/driver-go/v2/common"
	"math"
	"unsafe"
)

const (
	CBoolNull             = 0x02
	CTinyintNull          = -128
	CTinyintUnsignedNull  = 255
	CSmallintNull         = -32768
	CSmallintUnsignedNull = 65535
	CIntNull              = -2147483648
	CIntUnsignedNull      = 4294967295
	CBigintNull           = -9223372036854775808
	CBigintUnsignedNull   = 18446744073709551615
	CBinaryNull           = byte(0xff)
	CNcharNull            = byte(0xff)
	CTimestampNull        = CBigintNull
	PointerSize           = unsafe.Sizeof(uintptr(1))
	Step                  = unsafe.Sizeof(int64(0))
)

var changeFuncMap = map[uint8]changeFunc{
	uint8(C.TSDB_DATA_TYPE_BOOL):      changeBool,
	uint8(C.TSDB_DATA_TYPE_TINYINT):   changeTinyint,
	uint8(C.TSDB_DATA_TYPE_SMALLINT):  changeSmallint,
	uint8(C.TSDB_DATA_TYPE_INT):       changeInt,
	uint8(C.TSDB_DATA_TYPE_BIGINT):    changeBigint,
	uint8(C.TSDB_DATA_TYPE_UTINYINT):  changeUTinyint,
	uint8(C.TSDB_DATA_TYPE_USMALLINT): changeUSmallint,
	uint8(C.TSDB_DATA_TYPE_UINT):      changeUInt,
	uint8(C.TSDB_DATA_TYPE_UBIGINT):   changeUBigint,
	uint8(C.TSDB_DATA_TYPE_FLOAT):     changeFloat,
	uint8(C.TSDB_DATA_TYPE_DOUBLE):    changeDouble,
	uint8(C.TSDB_DATA_TYPE_BINARY):    changeBinary,
	uint8(C.TSDB_DATA_TYPE_NCHAR):     changeNchar,
	uint8(C.TSDB_DATA_TYPE_TIMESTAMP): changeTime,
}

func changeBool(colPointer uintptr, row int, length uint16, arg ...interface{}) driver.Value {
	currentRow := unsafe.Pointer(colPointer + uintptr(row)*uintptr(length))
	if (*((*byte)(currentRow))) == CBoolNull {
		return nil
	} else if (*((*byte)(currentRow))) != 0 {
		return true
	} else {
		return false
	}
}

func changeTinyint(colPointer uintptr, row int, length uint16, arg ...interface{}) driver.Value {
	currentRow := unsafe.Pointer(colPointer + uintptr(row)*uintptr(length))
	if (int8)(*((*int8)(currentRow))) == CTinyintNull {
		return nil
	} else {
		return *((*int8)(currentRow))
	}
}

func changeSmallint(colPointer uintptr, row int, length uint16, arg ...interface{}) driver.Value {
	currentRow := unsafe.Pointer(colPointer + uintptr(row)*uintptr(length))
	if (int16)(*((*int16)(currentRow))) == CSmallintNull {
		return nil
	} else {
		return *((*int16)(currentRow))
	}
}

func changeInt(colPointer uintptr, row int, length uint16, arg ...interface{}) driver.Value {
	currentRow := unsafe.Pointer(colPointer + uintptr(row)*uintptr(length))
	if (int32)(*((*int32)(currentRow))) == CIntNull {
		return nil
	} else {
		return *((*int32)(currentRow))
	}
}

func changeBigint(colPointer uintptr, row int, length uint16, arg ...interface{}) driver.Value {
	currentRow := unsafe.Pointer(colPointer + uintptr(row)*uintptr(length))
	if (int64)(*((*int64)(currentRow))) == CBigintNull {
		return nil
	} else {
		return *((*int64)(currentRow))
	}
}

func changeUTinyint(colPointer uintptr, row int, length uint16, arg ...interface{}) driver.Value {
	currentRow := unsafe.Pointer(colPointer + uintptr(row)*uintptr(length))
	if (uint8)(*((*uint8)(currentRow))) == CTinyintUnsignedNull {
		return nil
	} else {
		return *((*uint8)(currentRow))
	}
}

func changeUSmallint(colPointer uintptr, row int, length uint16, arg ...interface{}) driver.Value {
	currentRow := unsafe.Pointer(colPointer + uintptr(row)*uintptr(length))
	if (uint16)(*((*uint16)(currentRow))) == CSmallintUnsignedNull {
		return nil
	} else {
		return *((*uint16)(currentRow))
	}
}

func changeUInt(colPointer uintptr, row int, length uint16, arg ...interface{}) driver.Value {
	currentRow := unsafe.Pointer(colPointer + uintptr(row)*uintptr(length))
	if (uint32)(*((*uint32)(currentRow))) == CIntUnsignedNull {
		return nil
	} else {
		return *((*uint32)(currentRow))
	}
}

func changeUBigint(colPointer uintptr, row int, length uint16, arg ...interface{}) driver.Value {
	currentRow := unsafe.Pointer(colPointer + uintptr(row)*uintptr(length))
	if (uint64)(*((*uint64)(currentRow))) == CBigintUnsignedNull {
		return nil
	} else {
		return *((*uint64)(currentRow))
	}
}

func changeFloat(colPointer uintptr, row int, length uint16, arg ...interface{}) driver.Value {
	currentRow := unsafe.Pointer(colPointer + uintptr(row)*uintptr(length))
	if math.IsNaN(float64(*((*float32)(currentRow)))) {
		return nil
	} else {
		return *((*float32)(currentRow))
	}
}

func changeDouble(colPointer uintptr, row int, length uint16, arg ...interface{}) driver.Value {
	currentRow := unsafe.Pointer(colPointer + uintptr(row)*uintptr(length))
	if math.IsNaN(*((*float64)(currentRow))) {
		return nil
	} else {
		return *((*float64)(currentRow))
	}
}

func changeBinary(colPointer uintptr, row int, length uint16, arg ...interface{}) driver.Value {
	currentRow := unsafe.Pointer(colPointer + uintptr(row)*uintptr(length+2))
	clen := *((*int16)(currentRow))
	currentRow = unsafe.Pointer(uintptr(currentRow) + 2)

	binaryVal := make([]byte, clen)

	for index := int16(0); index < clen; index++ {
		binaryVal[index] = *((*byte)(unsafe.Pointer(uintptr(currentRow) + uintptr(index))))
	}
	if clen == 1 && binaryVal[0] == CBinaryNull {
		return nil
	} else {
		return string(binaryVal[:])
	}
}

func changeNchar(colPointer uintptr, row int, length uint16, arg ...interface{}) driver.Value {
	currentRow := unsafe.Pointer(colPointer + uintptr(row)*uintptr(length*4+2))
	clen := *((*int16)(currentRow))
	currentRow = unsafe.Pointer(uintptr(currentRow) + 2)

	binaryVal := make([]byte, clen)

	for index := int16(0); index < clen; index++ {
		binaryVal[index] = *((*byte)(unsafe.Pointer(uintptr(currentRow) + uintptr(index))))
	}
	if clen == 4 && binaryVal[0] == CNcharNull && binaryVal[1] == CNcharNull && binaryVal[2] == CNcharNull && binaryVal[3] == CNcharNull {
		return nil
	} else {
		return string(binaryVal[:])
	}
}

func changeTime(colPointer uintptr, row int, length uint16, arg ...interface{}) driver.Value {
	currentRow := unsafe.Pointer(colPointer + uintptr(row)*uintptr(length))
	if (int64)(*((*int64)(currentRow))) == CTimestampNull {
		return nil
	} else {
		return common.TimestampConvertToTime(*((*int64)(currentRow)), arg[0].(int))
	}
}

type changeFunc func(colPointer uintptr, row int, length uint16, arg ...interface{}) driver.Value

func ReadRow(dest []driver.Value, result, block unsafe.Pointer, row int, colLength []uint16, colTypes []uint8) {
	//block (pointer)->  | *col1 | *col2 | *col3 | ...*coln |
	//                       ↓
	//                   | row1 | row2 | row3 | row4 | ....
	//
	precision := TaosResultPrecision(result)
	for column := range dest {
		colPointer := *(*uintptr)(unsafe.Pointer(uintptr(unsafe.Pointer(*(*C.TAOS_ROW)(block))) + uintptr(column)*PointerSize))
		currentRow := unsafe.Pointer(colPointer + uintptr(row)*uintptr(colLength[column]))
		if currentRow == nil {
			dest[column] = nil
			continue
		}
		function := changeFuncMap[colTypes[column]]
		dest[column] = function(colPointer, row, colLength[column], precision)
	}
}

func ReadBlock(result, block unsafe.Pointer, blockSize int, colLength []uint16, colTypes []uint8) [][]driver.Value {
	r := make([][]driver.Value, blockSize)
	colCount := len(colTypes)
	precision := TaosResultPrecision(result)
	for column := 0; column < colCount; column++ {
		// column
		colPointer := *(*uintptr)(unsafe.Pointer(uintptr(unsafe.Pointer(*(*C.TAOS_ROW)(block))) + uintptr(column)*PointerSize))
		function := changeFuncMap[colTypes[column]]
		for row := 0; row < blockSize; row++ {
			//row
			if column == 0 {
				r[row] = make([]driver.Value, colCount)
			}
			r[row][column] = function(colPointer, row, colLength[column], precision)
		}
	}
	return r
}

func FetchRow(row unsafe.Pointer, offset int, colType uint8, precision int) driver.Value {
	p := (unsafe.Pointer)(uintptr(*((*int)(unsafe.Pointer(uintptr(row) + uintptr(offset)*Step)))))
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
		return C.GoString((*C.char)(p))
	case C.TSDB_DATA_TYPE_TIMESTAMP:
		ts := *((*int64)(p))
		return common.TimestampConvertToTime(ts, precision)
	default:
		return nil
	}
}
