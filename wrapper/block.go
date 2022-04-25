package wrapper

/*
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <taos.h>
*/
import "C"
import (
	"database/sql/driver"
	"unsafe"

	"github.com/taosdata/driver-go/v2/common"
)

// TaosFetchRawBlock  int         taos_fetch_raw_block(TAOS_RES *res, int* numOfRows, void** pData);
func TaosFetchRawBlock(result unsafe.Pointer) (int, int, unsafe.Pointer) {
	var cSize int
	size := unsafe.Pointer(&cSize)
	var block unsafe.Pointer
	errCode := int(C.taos_fetch_raw_block(result, (*C.int)(size), &block))
	return cSize, errCode, block
}

func IsVarDataType(colType uint8) bool {
	return colType == common.TSDB_DATA_TYPE_BINARY || colType == common.TSDB_DATA_TYPE_NCHAR
}

func BitmapLen(n int) int {
	return ((n) + ((1 << 3) - 1)) >> 3
}

func BitPos(n int) int {
	return n & ((1 << 3) - 1)
}

func CharOffset(n int) int {
	return n >> 3
}

func BMIsNull(c byte, n int) bool {
	return c&(1<<(7-BitPos(n))) == (1 << (7 - BitPos(n)))
}

type rawConvertFunc func(pStart uintptr, row int, arg ...interface{}) driver.Value

type rawConvertVarDataFunc func(pHeader, pStart uintptr, row int) driver.Value

var rawConvertFuncMap = map[uint8]rawConvertFunc{
	uint8(common.TSDB_DATA_TYPE_BOOL):      rawConvertBool,
	uint8(common.TSDB_DATA_TYPE_TINYINT):   rawConvertTinyint,
	uint8(common.TSDB_DATA_TYPE_SMALLINT):  rawConvertSmallint,
	uint8(common.TSDB_DATA_TYPE_INT):       rawConvertInt,
	uint8(common.TSDB_DATA_TYPE_BIGINT):    rawConvertBigint,
	uint8(common.TSDB_DATA_TYPE_UTINYINT):  rawConvertUTinyint,
	uint8(common.TSDB_DATA_TYPE_USMALLINT): rawConvertUSmallint,
	uint8(common.TSDB_DATA_TYPE_UINT):      rawConvertUInt,
	uint8(common.TSDB_DATA_TYPE_UBIGINT):   rawConvertUBigint,
	uint8(common.TSDB_DATA_TYPE_FLOAT):     rawConvertFloat,
	uint8(common.TSDB_DATA_TYPE_DOUBLE):    rawConvertDouble,
	uint8(common.TSDB_DATA_TYPE_TIMESTAMP): rawConvertTime,
}

var rawConvertVarDataMap = map[uint8]rawConvertVarDataFunc{
	uint8(common.TSDB_DATA_TYPE_BINARY): rawConvertBinary,
	uint8(common.TSDB_DATA_TYPE_NCHAR):  rawConvertNchar,
	uint8(common.TSDB_DATA_TYPE_JSON):   rawConvertJson,
}

func ItemIsNull(pHeader uintptr, row int) bool {
	offset := CharOffset(row)
	c := *((*byte)(unsafe.Pointer(pHeader + uintptr(offset))))
	if BMIsNull(c, row) {
		return true
	}
	return false
}

func rawConvertBool(pStart uintptr, row int, _ ...interface{}) driver.Value {
	if (*((*byte)(unsafe.Pointer(pStart + uintptr(row)*1)))) != 0 {
		return true
	} else {
		return false
	}
}

func rawConvertTinyint(pStart uintptr, row int, _ ...interface{}) driver.Value {
	return *((*int8)(unsafe.Pointer(pStart + uintptr(row)*1)))
}

func rawConvertSmallint(pStart uintptr, row int, _ ...interface{}) driver.Value {
	return *((*int16)(unsafe.Pointer(pStart + uintptr(row)*2)))
}

func rawConvertInt(pStart uintptr, row int, _ ...interface{}) driver.Value {
	return *((*int32)(unsafe.Pointer(pStart + uintptr(row)*4)))
}

func rawConvertBigint(pStart uintptr, row int, _ ...interface{}) driver.Value {
	return *((*int64)(unsafe.Pointer(pStart + uintptr(row)*8)))
}

func rawConvertUTinyint(pStart uintptr, row int, _ ...interface{}) driver.Value {
	return *((*uint8)(unsafe.Pointer(pStart + uintptr(row)*1)))
}

func rawConvertUSmallint(pStart uintptr, row int, _ ...interface{}) driver.Value {
	return *((*uint16)(unsafe.Pointer(pStart + uintptr(row)*2)))
}

func rawConvertUInt(pStart uintptr, row int, _ ...interface{}) driver.Value {
	return *((*uint32)(unsafe.Pointer(pStart + uintptr(row)*4)))
}

func rawConvertUBigint(pStart uintptr, row int, _ ...interface{}) driver.Value {
	return *((*uint64)(unsafe.Pointer(pStart + uintptr(row)*8)))
}

func rawConvertFloat(pStart uintptr, row int, _ ...interface{}) driver.Value {
	return *((*float32)(unsafe.Pointer(pStart + uintptr(row)*4)))
}

func rawConvertDouble(pStart uintptr, row int, _ ...interface{}) driver.Value {
	return *((*float64)(unsafe.Pointer(pStart + uintptr(row)*8)))
}

func rawConvertTime(pStart uintptr, row int, arg ...interface{}) driver.Value {
	if len(arg) == 1 {
		return common.TimestampConvertToTime(*((*int64)(unsafe.Pointer(pStart + uintptr(row)*8))), arg[0].(int))
	} else if len(arg) == 2 {
		return arg[1].(FormatTimeFunc)(*((*int64)(unsafe.Pointer(pStart + uintptr(row)*8))), arg[0].(int))
	} else {
		panic("convertTime error")
	}
}

func rawConvertBinary(pHeader, pStart uintptr, row int) driver.Value {
	offset := *((*int32)(unsafe.Pointer(pHeader + uintptr(row*4))))
	if offset == -1 {
		return nil
	}
	currentRow := unsafe.Pointer(pStart + uintptr(offset))
	clen := *((*int16)(currentRow))
	currentRow = unsafe.Pointer(uintptr(currentRow) + 2)

	binaryVal := make([]byte, clen)

	for index := int16(0); index < clen; index++ {
		binaryVal[index] = *((*byte)(unsafe.Pointer(uintptr(currentRow) + uintptr(index))))
	}
	return string(binaryVal[:])
}

func rawConvertNchar(pHeader, pStart uintptr, row int) driver.Value {
	offset := *((*int32)(unsafe.Pointer(pHeader + uintptr(row*4))))
	if offset == -1 {
		return nil
	}
	currentRow := unsafe.Pointer(pStart + uintptr(offset))
	clen := *((*int16)(currentRow)) / 4
	currentRow = unsafe.Pointer(uintptr(currentRow) + 2)

	binaryVal := make([]rune, clen)

	for index := int16(0); index < clen; index++ {
		binaryVal[index] = *((*rune)(unsafe.Pointer(uintptr(currentRow) + uintptr(index*4))))
	}
	return string(binaryVal)
}

// just like nchar
func rawConvertJson(pHeader, pStart uintptr, row int) driver.Value {
	offset := *((*int32)(unsafe.Pointer(pHeader + uintptr(row*4))))
	if offset == -1 {
		return nil
	}
	currentRow := unsafe.Pointer(pStart + uintptr(offset))
	clen := *((*int16)(currentRow)) / 4
	currentRow = unsafe.Pointer(uintptr(currentRow) + 2)

	binaryVal := make([]byte, 0, clen)

	for index := int16(0); index < clen; index++ {
		binaryVal = AppendRune(binaryVal, *((*rune)(unsafe.Pointer(uintptr(currentRow) + uintptr(index*4)))))
	}

	return binaryVal
}

// ReadBlock in-place
func ReadBlock(block unsafe.Pointer, blockSize int, colTypes []uint8, precision int) [][]driver.Value {
	r := make([][]driver.Value, blockSize)
	colCount := len(colTypes)
	payloadOffset := uintptr(4 * colCount)
	nullBitMapOffset := uintptr(BitmapLen(blockSize))
	pHeader := uintptr(block) + payloadOffset + 12 // length i32, group u64
	pStart := pHeader
	for column := 0; column < colCount; column++ {
		colLength := *((*int32)(unsafe.Pointer(uintptr(block) + 12 + uintptr(column)*4)))
		if IsVarDataType(colTypes[column]) {
			convertF := rawConvertVarDataMap[colTypes[column]]
			pStart = pHeader + uintptr(4*blockSize)
			for row := 0; row < blockSize; row++ {
				if column == 0 {
					r[row] = make([]driver.Value, colCount)
				}
				r[row][column] = convertF(pHeader, pStart, row)
			}
		} else {
			convertF := rawConvertFuncMap[colTypes[column]]
			pStart = pHeader + nullBitMapOffset
			for row := 0; row < blockSize; row++ {
				if column == 0 {
					r[row] = make([]driver.Value, colCount)
				}
				if ItemIsNull(pHeader, row) {
					r[row][column] = nil
				} else {
					r[row][column] = convertF(pStart, row, precision)
				}
			}
		}
		pHeader = pStart + uintptr(colLength)
	}
	return r
}

func ReadRow(dest []driver.Value, result, block unsafe.Pointer, blockSize int, row int, colTypes []uint8) {
	precision := TaosResultPrecision(result)
	colCount := len(colTypes)
	payloadOffset := uintptr(4 * colCount)
	nullBitMapOffset := uintptr(BitmapLen(blockSize))
	pHeader := uintptr(block) + payloadOffset + 12 // length i32, group u64
	pStart := pHeader
	for column := 0; column < colCount; column++ {
		colLength := *((*int32)(unsafe.Pointer(uintptr(block) + 12 + uintptr(column)*4)))
		if IsVarDataType(colTypes[column]) {
			convertF := rawConvertVarDataMap[colTypes[column]]
			pStart = pHeader + uintptr(4*blockSize)
			dest[column] = convertF(pHeader, pStart, row)
		} else {
			convertF := rawConvertFuncMap[colTypes[column]]
			pStart = pHeader + nullBitMapOffset
			if ItemIsNull(pHeader, row) {
				dest[column] = nil
			} else {
				dest[column] = convertF(pStart, row, precision)
			}
		}
		pHeader = pStart + uintptr(colLength)
	}
}

func ReadBlockWithTimeFormat(block unsafe.Pointer, blockSize int, colTypes []uint8, precision int, formatFunc FormatTimeFunc) [][]driver.Value {
	r := make([][]driver.Value, blockSize)
	colCount := len(colTypes)
	payloadOffset := uintptr(4 * colCount)
	nullBitMapOffset := uintptr(BitmapLen(blockSize))
	pHeader := uintptr(block) + payloadOffset + 12 // length i32, group u64
	pStart := pHeader
	for column := 0; column < colCount; column++ {
		colLength := *((*int32)(unsafe.Pointer(uintptr(block) + 12 + uintptr(column)*4)))
		if IsVarDataType(colTypes[column]) {
			convertF := rawConvertVarDataMap[colTypes[column]]
			pStart = pHeader + uintptr(4*blockSize)
			for row := 0; row < blockSize; row++ {
				if column == 0 {
					r[row] = make([]driver.Value, colCount)
				}
				r[row][column] = convertF(pHeader, pStart, row)
			}
		} else {
			convertF := rawConvertFuncMap[colTypes[column]]
			pStart = pHeader + nullBitMapOffset
			for row := 0; row < blockSize; row++ {
				if column == 0 {
					r[row] = make([]driver.Value, colCount)
				}
				if ItemIsNull(pHeader, row) {
					r[row][column] = nil
				} else {
					r[row][column] = convertF(pStart, row, precision, formatFunc)
				}
			}
		}
		pHeader = pStart + uintptr(colLength)
	}
	return r
}
