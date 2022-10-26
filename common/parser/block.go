package parser

import (
	"database/sql/driver"
	"math"
	"unsafe"

	"github.com/taosdata/driver-go/v3/common"
)

const (
	Int8Size    = common.Int8Size
	Int16Size   = common.Int16Size
	Int32Size   = common.Int32Size
	Int64Size   = common.Int64Size
	UInt8Size   = common.UInt8Size
	UInt16Size  = common.UInt16Size
	UInt32Size  = common.UInt32Size
	UInt64Size  = common.UInt64Size
	Float32Size = common.Float32Size
	Float64Size = common.Float64Size
)

const (
	ColInfoSize            = Int8Size + Int32Size
	RawBlockVersionOffset  = 0
	RawBlockLengthOffset   = RawBlockVersionOffset + Int32Size
	NumOfRowsOffset        = RawBlockLengthOffset + Int32Size
	NumOfColsOffset        = NumOfRowsOffset + Int32Size
	HasColumnSegmentOffset = NumOfColsOffset + Int32Size
	GroupIDOffset          = HasColumnSegmentOffset + Int32Size
	ColInfoOffset          = GroupIDOffset + UInt64Size
)

func RawBlockGetVersion(rawBlock unsafe.Pointer) int32 {
	return *((*int32)(unsafe.Pointer(uintptr(rawBlock) + RawBlockVersionOffset)))
}

func RawBlockGetLength(rawBlock unsafe.Pointer) int32 {
	return *((*int32)(unsafe.Pointer(uintptr(rawBlock) + RawBlockLengthOffset)))
}

func RawBlockGetNumOfRows(rawBlock unsafe.Pointer) int32 {
	return *((*int32)(unsafe.Pointer(uintptr(rawBlock) + NumOfRowsOffset)))
}

func RawBlockGetNumOfCols(rawBlock unsafe.Pointer) int32 {
	return *((*int32)(unsafe.Pointer(uintptr(rawBlock) + NumOfColsOffset)))
}

func RawBlockGetHasColumnSegment(rawBlock unsafe.Pointer) int32 {
	return *((*int32)(unsafe.Pointer(uintptr(rawBlock) + HasColumnSegmentOffset)))
}

func RawBlockGetGroupID(rawBlock unsafe.Pointer) uint64 {
	return *((*uint64)(unsafe.Pointer(uintptr(rawBlock) + GroupIDOffset)))
}

type RawBlockColInfo struct {
	ColType int8
	Bytes   int32
}

func RawBlockGetColInfo(rawBlock unsafe.Pointer, infos []RawBlockColInfo) {
	for i := 0; i < len(infos); i++ {
		offset := uintptr(rawBlock) + ColInfoOffset + ColInfoSize*uintptr(i)
		infos[i].ColType = *((*int8)(unsafe.Pointer(offset)))
		infos[i].Bytes = *((*int32)(unsafe.Pointer(offset + Int8Size)))
	}
}

func RawBlockGetColumnLengthOffset(colCount int) uintptr {
	return ColInfoOffset + uintptr(colCount)*ColInfoSize
}

func RawBlockGetColDataOffset(colCount int) uintptr {
	return ColInfoOffset + uintptr(colCount)*ColInfoSize + uintptr(colCount)*Int32Size
}

type FormatTimeFunc func(ts int64, precision int) driver.Value

func IsVarDataType(colType uint8) bool {
	return colType == common.TSDB_DATA_TYPE_BINARY || colType == common.TSDB_DATA_TYPE_NCHAR || colType == common.TSDB_DATA_TYPE_JSON
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
	return BMIsNull(c, row)
}

func rawConvertBool(pStart uintptr, row int, _ ...interface{}) driver.Value {
	if (*((*byte)(unsafe.Pointer(pStart + uintptr(row)*1)))) != 0 {
		return true
	} else {
		return false
	}
}

func rawConvertTinyint(pStart uintptr, row int, _ ...interface{}) driver.Value {
	return *((*int8)(unsafe.Pointer(pStart + uintptr(row)*Int8Size)))
}

func rawConvertSmallint(pStart uintptr, row int, _ ...interface{}) driver.Value {
	return *((*int16)(unsafe.Pointer(pStart + uintptr(row)*Int16Size)))
}

func rawConvertInt(pStart uintptr, row int, _ ...interface{}) driver.Value {
	return *((*int32)(unsafe.Pointer(pStart + uintptr(row)*Int32Size)))
}

func rawConvertBigint(pStart uintptr, row int, _ ...interface{}) driver.Value {
	return *((*int64)(unsafe.Pointer(pStart + uintptr(row)*Int64Size)))
}

func rawConvertUTinyint(pStart uintptr, row int, _ ...interface{}) driver.Value {
	return *((*uint8)(unsafe.Pointer(pStart + uintptr(row)*UInt8Size)))
}

func rawConvertUSmallint(pStart uintptr, row int, _ ...interface{}) driver.Value {
	return *((*uint16)(unsafe.Pointer(pStart + uintptr(row)*UInt16Size)))
}

func rawConvertUInt(pStart uintptr, row int, _ ...interface{}) driver.Value {
	return *((*uint32)(unsafe.Pointer(pStart + uintptr(row)*UInt32Size)))
}

func rawConvertUBigint(pStart uintptr, row int, _ ...interface{}) driver.Value {
	return *((*uint64)(unsafe.Pointer(pStart + uintptr(row)*UInt64Size)))
}

func rawConvertFloat(pStart uintptr, row int, _ ...interface{}) driver.Value {
	return math.Float32frombits(*((*uint32)(unsafe.Pointer(pStart + uintptr(row)*Float32Size))))
}

func rawConvertDouble(pStart uintptr, row int, _ ...interface{}) driver.Value {
	return math.Float64frombits(*((*uint64)(unsafe.Pointer(pStart + uintptr(row)*Float64Size))))
}

func rawConvertTime(pStart uintptr, row int, arg ...interface{}) driver.Value {
	if len(arg) == 1 {
		return common.TimestampConvertToTime(*((*int64)(unsafe.Pointer(pStart + uintptr(row)*Int64Size))), arg[0].(int))
	} else if len(arg) == 2 {
		return arg[1].(FormatTimeFunc)(*((*int64)(unsafe.Pointer(pStart + uintptr(row)*Int64Size))), arg[0].(int))
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

func rawConvertJson(pHeader, pStart uintptr, row int) driver.Value {
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
	return binaryVal[:]
}

// ReadBlock in-place
func ReadBlock(block unsafe.Pointer, blockSize int, colTypes []uint8, precision int) [][]driver.Value {
	r := make([][]driver.Value, blockSize)
	colCount := len(colTypes)
	nullBitMapOffset := uintptr(BitmapLen(blockSize))
	lengthOffset := RawBlockGetColumnLengthOffset(colCount)
	pHeader := uintptr(block) + RawBlockGetColDataOffset(colCount)
	var pStart uintptr
	for column := 0; column < colCount; column++ {
		colLength := *((*int32)(unsafe.Pointer(uintptr(block) + lengthOffset + uintptr(column)*Int32Size)))
		if IsVarDataType(colTypes[column]) {
			convertF := rawConvertVarDataMap[colTypes[column]]
			pStart = pHeader + Int32Size*uintptr(blockSize)
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

func ReadRow(dest []driver.Value, block unsafe.Pointer, blockSize int, row int, colTypes []uint8, precision int) {
	colCount := len(colTypes)
	nullBitMapOffset := uintptr(BitmapLen(blockSize))
	lengthOffset := RawBlockGetColumnLengthOffset(colCount)
	pHeader := uintptr(block) + RawBlockGetColDataOffset(colCount)
	var pStart uintptr
	for column := 0; column < colCount; column++ {
		colLength := *((*int32)(unsafe.Pointer(uintptr(block) + lengthOffset + uintptr(column)*Int32Size)))
		if IsVarDataType(colTypes[column]) {
			convertF := rawConvertVarDataMap[colTypes[column]]
			pStart = pHeader + Int32Size*uintptr(blockSize)
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
	nullBitMapOffset := uintptr(BitmapLen(blockSize))
	lengthOffset := RawBlockGetColumnLengthOffset(colCount)
	pHeader := uintptr(block) + RawBlockGetColDataOffset(colCount)
	var pStart uintptr
	for column := 0; column < colCount; column++ {
		colLength := *((*int32)(unsafe.Pointer(uintptr(block) + lengthOffset + uintptr(column)*Int32Size)))
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

func ItemRawBlock(colType uint8, pHeader, pStart uintptr, row int, precision int, timeFormat FormatTimeFunc) driver.Value {
	if IsVarDataType(colType) {
		switch colType {
		case uint8(common.TSDB_DATA_TYPE_BINARY):
			return rawConvertBinary(pHeader, pStart, row)
		case uint8(common.TSDB_DATA_TYPE_NCHAR):
			return rawConvertNchar(pHeader, pStart, row)
		case uint8(common.TSDB_DATA_TYPE_JSON):
			return rawConvertJson(pHeader, pStart, row)
		}
	} else {
		if ItemIsNull(pHeader, row) {
			return nil
		} else {
			switch colType {
			case uint8(common.TSDB_DATA_TYPE_BOOL):
				return rawConvertBool(pStart, row)
			case uint8(common.TSDB_DATA_TYPE_TINYINT):
				return rawConvertTinyint(pStart, row)
			case uint8(common.TSDB_DATA_TYPE_SMALLINT):
				return rawConvertSmallint(pStart, row)
			case uint8(common.TSDB_DATA_TYPE_INT):
				return rawConvertInt(pStart, row)
			case uint8(common.TSDB_DATA_TYPE_BIGINT):
				return rawConvertBigint(pStart, row)
			case uint8(common.TSDB_DATA_TYPE_UTINYINT):
				return rawConvertUTinyint(pStart, row)
			case uint8(common.TSDB_DATA_TYPE_USMALLINT):
				return rawConvertUSmallint(pStart, row)
			case uint8(common.TSDB_DATA_TYPE_UINT):
				return rawConvertUInt(pStart, row)
			case uint8(common.TSDB_DATA_TYPE_UBIGINT):
				return rawConvertUBigint(pStart, row)
			case uint8(common.TSDB_DATA_TYPE_FLOAT):
				return rawConvertFloat(pStart, row)
			case uint8(common.TSDB_DATA_TYPE_DOUBLE):
				return rawConvertDouble(pStart, row)
			case uint8(common.TSDB_DATA_TYPE_TIMESTAMP):
				return rawConvertTime(pStart, row, precision, timeFormat)
			}
		}
	}
	return nil
}
