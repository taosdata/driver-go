package parser

import (
	"database/sql/driver"
	"math"
	"unsafe"

	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/common/pointer"
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
	return *((*int32)(pointer.AddUintptr(rawBlock, RawBlockVersionOffset)))
}

func RawBlockGetLength(rawBlock unsafe.Pointer) int32 {
	return *((*int32)(pointer.AddUintptr(rawBlock, RawBlockLengthOffset)))
}

func RawBlockGetNumOfRows(rawBlock unsafe.Pointer) int32 {
	return *((*int32)(pointer.AddUintptr(rawBlock, NumOfRowsOffset)))
}

func RawBlockGetNumOfCols(rawBlock unsafe.Pointer) int32 {
	return *((*int32)(pointer.AddUintptr(rawBlock, NumOfColsOffset)))
}

func RawBlockGetHasColumnSegment(rawBlock unsafe.Pointer) int32 {
	return *((*int32)(pointer.AddUintptr(rawBlock, HasColumnSegmentOffset)))
}

func RawBlockGetGroupID(rawBlock unsafe.Pointer) uint64 {
	return *((*uint64)(pointer.AddUintptr(rawBlock, GroupIDOffset)))
}

type RawBlockColInfo struct {
	ColType int8
	Bytes   int32
}

func RawBlockGetColInfo(rawBlock unsafe.Pointer, infos []RawBlockColInfo) {
	for i := 0; i < len(infos); i++ {
		offset := ColInfoOffset + ColInfoSize*uintptr(i)
		infos[i].ColType = *((*int8)(pointer.AddUintptr(rawBlock, offset)))
		infos[i].Bytes = *((*int32)(pointer.AddUintptr(rawBlock, offset+Int8Size)))
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
	return colType == common.TSDB_DATA_TYPE_BINARY ||
		colType == common.TSDB_DATA_TYPE_NCHAR ||
		colType == common.TSDB_DATA_TYPE_JSON ||
		colType == common.TSDB_DATA_TYPE_VARBINARY ||
		colType == common.TSDB_DATA_TYPE_GEOMETRY
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

type rawConvertFunc func(pStart unsafe.Pointer, row int, arg ...interface{}) driver.Value

type rawConvertVarDataFunc func(pHeader, pStart unsafe.Pointer, row int) driver.Value

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
	uint8(common.TSDB_DATA_TYPE_BINARY):    rawConvertBinary,
	uint8(common.TSDB_DATA_TYPE_NCHAR):     rawConvertNchar,
	uint8(common.TSDB_DATA_TYPE_JSON):      rawConvertJson,
	uint8(common.TSDB_DATA_TYPE_VARBINARY): rawConvertVarBinary,
	uint8(common.TSDB_DATA_TYPE_GEOMETRY):  rawConvertGeometry,
}

func ItemIsNull(pHeader unsafe.Pointer, row int) bool {
	offset := CharOffset(row)
	c := *((*byte)(pointer.AddUintptr(pHeader, uintptr(offset))))
	return BMIsNull(c, row)
}

func rawConvertBool(pStart unsafe.Pointer, row int, _ ...interface{}) driver.Value {
	if (*((*byte)(pointer.AddUintptr(pStart, uintptr(row)*1)))) != 0 {
		return true
	} else {
		return false
	}
}

func rawConvertTinyint(pStart unsafe.Pointer, row int, _ ...interface{}) driver.Value {
	return *((*int8)(pointer.AddUintptr(pStart, uintptr(row)*Int8Size)))
}

func rawConvertSmallint(pStart unsafe.Pointer, row int, _ ...interface{}) driver.Value {
	return *((*int16)(pointer.AddUintptr(pStart, uintptr(row)*Int16Size)))
}

func rawConvertInt(pStart unsafe.Pointer, row int, _ ...interface{}) driver.Value {
	return *((*int32)(pointer.AddUintptr(pStart, uintptr(row)*Int32Size)))
}

func rawConvertBigint(pStart unsafe.Pointer, row int, _ ...interface{}) driver.Value {
	return *((*int64)(pointer.AddUintptr(pStart, uintptr(row)*Int64Size)))
}

func rawConvertUTinyint(pStart unsafe.Pointer, row int, _ ...interface{}) driver.Value {
	return *((*uint8)(pointer.AddUintptr(pStart, uintptr(row)*UInt8Size)))
}

func rawConvertUSmallint(pStart unsafe.Pointer, row int, _ ...interface{}) driver.Value {
	return *((*uint16)(pointer.AddUintptr(pStart, uintptr(row)*UInt16Size)))
}

func rawConvertUInt(pStart unsafe.Pointer, row int, _ ...interface{}) driver.Value {
	return *((*uint32)(pointer.AddUintptr(pStart, uintptr(row)*UInt32Size)))
}

func rawConvertUBigint(pStart unsafe.Pointer, row int, _ ...interface{}) driver.Value {
	return *((*uint64)(pointer.AddUintptr(pStart, uintptr(row)*UInt64Size)))
}

func rawConvertFloat(pStart unsafe.Pointer, row int, _ ...interface{}) driver.Value {
	return math.Float32frombits(*((*uint32)(pointer.AddUintptr(pStart, uintptr(row)*Float32Size))))
}

func rawConvertDouble(pStart unsafe.Pointer, row int, _ ...interface{}) driver.Value {
	return math.Float64frombits(*((*uint64)(pointer.AddUintptr(pStart, uintptr(row)*Float64Size))))
}

func rawConvertTime(pStart unsafe.Pointer, row int, arg ...interface{}) driver.Value {
	if len(arg) == 1 {
		return common.TimestampConvertToTime(*((*int64)(pointer.AddUintptr(pStart, uintptr(row)*Int64Size))), arg[0].(int))
	} else if len(arg) == 2 {
		return arg[1].(FormatTimeFunc)(*((*int64)(pointer.AddUintptr(pStart, uintptr(row)*Int64Size))), arg[0].(int))
	} else {
		panic("convertTime error")
	}
}

func rawConvertVarBinary(pHeader, pStart unsafe.Pointer, row int) driver.Value {
	offset := *((*int32)(pointer.AddUintptr(pHeader, uintptr(row*4))))
	if offset == -1 {
		return nil
	}
	currentRow := pointer.AddUintptr(pStart, uintptr(offset))
	clen := *((*uint16)(currentRow))
	currentRow = unsafe.Pointer(uintptr(currentRow) + 2)

	binaryVal := make([]byte, clen)

	for index := uint16(0); index < clen; index++ {
		binaryVal[index] = *((*byte)(unsafe.Pointer(uintptr(currentRow) + uintptr(index))))
	}
	return binaryVal[:]
}

func rawConvertGeometry(pHeader, pStart unsafe.Pointer, row int) driver.Value {
	return rawConvertVarBinary(pHeader, pStart, row)
}

func rawConvertBinary(pHeader, pStart unsafe.Pointer, row int) driver.Value {
	offset := *((*int32)(pointer.AddUintptr(pHeader, uintptr(row*4))))
	if offset == -1 {
		return nil
	}
	currentRow := pointer.AddUintptr(pStart, uintptr(offset))
	clen := *((*uint16)(currentRow))
	currentRow = unsafe.Pointer(uintptr(currentRow) + 2)

	binaryVal := make([]byte, clen)

	for index := uint16(0); index < clen; index++ {
		binaryVal[index] = *((*byte)(unsafe.Pointer(uintptr(currentRow) + uintptr(index))))
	}
	return string(binaryVal[:])
}

func rawConvertNchar(pHeader, pStart unsafe.Pointer, row int) driver.Value {
	offset := *((*int32)(pointer.AddUintptr(pHeader, uintptr(row*4))))
	if offset == -1 {
		return nil
	}
	currentRow := pointer.AddUintptr(pStart, uintptr(offset))
	clen := *((*uint16)(currentRow)) / 4
	currentRow = unsafe.Pointer(uintptr(currentRow) + 2)

	binaryVal := make([]rune, clen)

	for index := uint16(0); index < clen; index++ {
		binaryVal[index] = *((*rune)(unsafe.Pointer(uintptr(currentRow) + uintptr(index*4))))
	}
	return string(binaryVal)
}

func rawConvertJson(pHeader, pStart unsafe.Pointer, row int) driver.Value {
	offset := *((*int32)(pointer.AddUintptr(pHeader, uintptr(row*4))))
	if offset == -1 {
		return nil
	}
	currentRow := pointer.AddUintptr(pStart, uintptr(offset))
	clen := *((*uint16)(currentRow))
	currentRow = pointer.AddUintptr(currentRow, 2)

	binaryVal := make([]byte, clen)

	for index := uint16(0); index < clen; index++ {
		binaryVal[index] = *((*byte)(pointer.AddUintptr(currentRow, uintptr(index))))
	}
	return binaryVal[:]
}

// ReadBlock in-place
func ReadBlock(block unsafe.Pointer, blockSize int, colTypes []uint8, precision int) [][]driver.Value {
	r := make([][]driver.Value, blockSize)
	colCount := len(colTypes)
	nullBitMapOffset := uintptr(BitmapLen(blockSize))
	lengthOffset := RawBlockGetColumnLengthOffset(colCount)
	pHeader := pointer.AddUintptr(block, RawBlockGetColDataOffset(colCount))
	var pStart unsafe.Pointer
	for column := 0; column < colCount; column++ {
		colLength := *((*int32)(pointer.AddUintptr(block, lengthOffset+uintptr(column)*Int32Size)))
		if IsVarDataType(colTypes[column]) {
			convertF := rawConvertVarDataMap[colTypes[column]]
			pStart = pointer.AddUintptr(pHeader, Int32Size*uintptr(blockSize))
			for row := 0; row < blockSize; row++ {
				if column == 0 {
					r[row] = make([]driver.Value, colCount)
				}
				r[row][column] = convertF(pHeader, pStart, row)
			}
		} else {
			convertF := rawConvertFuncMap[colTypes[column]]
			pStart = pointer.AddUintptr(pHeader, nullBitMapOffset)
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
		pHeader = pointer.AddUintptr(pStart, uintptr(colLength))
	}
	return r
}

func ReadRow(dest []driver.Value, block unsafe.Pointer, blockSize int, row int, colTypes []uint8, precision int) {
	colCount := len(colTypes)
	nullBitMapOffset := uintptr(BitmapLen(blockSize))
	lengthOffset := RawBlockGetColumnLengthOffset(colCount)
	pHeader := pointer.AddUintptr(block, RawBlockGetColDataOffset(colCount))
	var pStart unsafe.Pointer
	for column := 0; column < colCount; column++ {
		colLength := *((*int32)(pointer.AddUintptr(block, lengthOffset+uintptr(column)*Int32Size)))
		if IsVarDataType(colTypes[column]) {
			convertF := rawConvertVarDataMap[colTypes[column]]
			pStart = pointer.AddUintptr(pHeader, Int32Size*uintptr(blockSize))
			dest[column] = convertF(pHeader, pStart, row)
		} else {
			convertF := rawConvertFuncMap[colTypes[column]]
			pStart = pointer.AddUintptr(pHeader, nullBitMapOffset)
			if ItemIsNull(pHeader, row) {
				dest[column] = nil
			} else {
				dest[column] = convertF(pStart, row, precision)
			}
		}
		pHeader = pointer.AddUintptr(pStart, uintptr(colLength))
	}
}

func ReadBlockWithTimeFormat(block unsafe.Pointer, blockSize int, colTypes []uint8, precision int, formatFunc FormatTimeFunc) [][]driver.Value {
	r := make([][]driver.Value, blockSize)
	colCount := len(colTypes)
	nullBitMapOffset := uintptr(BitmapLen(blockSize))
	lengthOffset := RawBlockGetColumnLengthOffset(colCount)
	pHeader := pointer.AddUintptr(block, RawBlockGetColDataOffset(colCount))
	var pStart unsafe.Pointer
	for column := 0; column < colCount; column++ {
		colLength := *((*int32)(pointer.AddUintptr(block, lengthOffset+uintptr(column)*Int32Size)))
		if IsVarDataType(colTypes[column]) {
			convertF := rawConvertVarDataMap[colTypes[column]]
			pStart = pointer.AddUintptr(pHeader, uintptr(4*blockSize))
			for row := 0; row < blockSize; row++ {
				if column == 0 {
					r[row] = make([]driver.Value, colCount)
				}
				r[row][column] = convertF(pHeader, pStart, row)
			}
		} else {
			convertF := rawConvertFuncMap[colTypes[column]]
			pStart = pointer.AddUintptr(pHeader, nullBitMapOffset)
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
		pHeader = pointer.AddUintptr(pStart, uintptr(colLength))
	}
	return r
}

func ItemRawBlock(colType uint8, pHeader, pStart unsafe.Pointer, row int, precision int, timeFormat FormatTimeFunc) driver.Value {
	if IsVarDataType(colType) {
		return rawConvertVarDataMap[colType](pHeader, pStart, row)
	} else {
		if ItemIsNull(pHeader, row) {
			return nil
		} else {
			return rawConvertFuncMap[colType](pStart, row, precision, timeFormat)
		}
	}
}
