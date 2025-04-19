package stmt

import (
	"bytes"
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"math"
	"time"
	"unsafe"

	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/common/mem"
)

const (
	TotalLengthPosition      = 0
	CountPosition            = TotalLengthPosition + 4
	TagCountPosition         = CountPosition + 4
	ColCountPosition         = TagCountPosition + 4
	TableNamesOffsetPosition = ColCountPosition + 4
	TagsOffsetPosition       = TableNamesOffsetPosition + 4
	ColsOffsetPosition       = TagsOffsetPosition + 4
	DataPosition             = ColsOffsetPosition + 4
)

const (
	BindDataTotalLengthOffset = 0
	BindDataTypeOffset        = BindDataTotalLengthOffset + 4
	BindDataNumOffset         = BindDataTypeOffset + 4
	BindDataIsNullOffset      = BindDataNumOffset + 4
)

func MarshalStmt2Binary(bindData []*TaosStmt2BindData, isInsert bool, fields []*Stmt2AllField) ([]byte, error) {
	var colType []*Stmt2AllField
	var tagType []*Stmt2AllField
	for i := 0; i < len(fields); i++ {
		if fields[i].BindType == TAOS_FIELD_COL {
			colType = append(colType, fields[i])
		} else if fields[i].BindType == TAOS_FIELD_TAG {
			tagType = append(tagType, fields[i])
		}
	}
	// count
	count := len(bindData)
	if count == 0 {
		return nil, fmt.Errorf("empty data")
	}
	needTableNames := false
	needTags := false
	needCols := false
	tagCount := len(tagType)
	colCount := len(colType)
	if isInsert {
		for i := 0; i < count; i++ {
			data := bindData[i]
			if data.TableName != "" {
				needTableNames = true
			}
			if len(data.Tags) != tagCount {
				return nil, fmt.Errorf("tag count not match, data count:%d, type count:%d", len(data.Tags), tagCount)
			}
			if len(data.Cols) != colCount {
				return nil, fmt.Errorf("col count not match, data count:%d, type count:%d", len(data.Cols), colCount)
			}
		}
	} else {
		if tagCount != 0 {
			return nil, fmt.Errorf("query not need tag types")
		}
		if colCount != 0 {
			return nil, fmt.Errorf("query not need col types")
		}
		if count != 1 {
			return nil, fmt.Errorf("query only need one data")
		}

		data := bindData[0]
		if data.TableName != "" {
			return nil, fmt.Errorf("query not need table name")
		}
		if len(data.Tags) != 0 {
			return nil, fmt.Errorf("query not need tag")
		}
		if len(data.Cols) == 0 {
			return nil, fmt.Errorf("query need col")
		}
		colCount = len(data.Cols)
		for j := 0; j < colCount; j++ {
			if len(data.Cols[j]) != 1 {
				return nil, fmt.Errorf("query col data must be one row, col:%d, count:%d", j, len(data.Cols[j]))
			}
		}
	}

	header := make([]byte, DataPosition)
	// count
	binary.LittleEndian.PutUint32(header[CountPosition:], uint32(count))
	// tag count
	if tagCount != 0 {
		needTags = true
		binary.LittleEndian.PutUint32(header[TagCountPosition:], uint32(tagCount))
	}
	// col count
	if colCount != 0 {
		needCols = true
		binary.LittleEndian.PutUint32(header[ColCountPosition:], uint32(colCount))
	}
	if !needTableNames && !needTags && !needCols {
		return nil, fmt.Errorf("no data")
	}
	tmpBuf := &bytes.Buffer{}
	tableNameBuf := &bytes.Buffer{}
	var tableNameLength []uint16
	if needTableNames {
		tableNameLength = make([]uint16, count)
	}
	tagBuf := &bytes.Buffer{}
	var tagDataLength []uint32
	if needTags {
		tagDataLength = make([]uint32, count)
	}
	colBuf := &bytes.Buffer{}
	var colDataLength []uint32
	if needCols {
		colDataLength = make([]uint32, count)
	}
	for index, data := range bindData {
		// table name
		if needTableNames {
			if data.TableName != "" {
				if len(data.TableName) > math.MaxUint16-1 {
					return nil, fmt.Errorf("table name too long, index:%d, length:%d", index, len(data.TableName))
				}
				tableNameBuf.WriteString(data.TableName)
			}
			tableNameBuf.WriteByte(0)
			tableNameLength[index] = uint16(len(data.TableName) + 1)
		}

		// tag
		if needTags {
			length := 0
			for i := 0; i < len(data.Tags); i++ {
				tag := data.Tags[i]
				tagDataBuffer, err := generateBindColData([]driver.Value{tag}, tagType[i], tmpBuf)
				if err != nil {
					return nil, err
				}
				length += len(tagDataBuffer)
				tagBuf.Write(tagDataBuffer)
			}
			tagDataLength[index] = uint32(length)
		}
		// col
		if needCols {
			length := 0
			for i := 0; i < len(data.Cols); i++ {
				col := data.Cols[i]
				var colDataBuffer []byte
				var err error
				if isInsert {
					colDataBuffer, err = generateBindColData(col, colType[i], tmpBuf)
				} else {
					colDataBuffer, err = generateBindQueryData(col[0])
				}
				if err != nil {
					return nil, err
				}
				length += len(colDataBuffer)
				colBuf.Write(colDataBuffer)
			}
			colDataLength[index] = uint32(length)
		}
	}
	tableTotalLength := tableNameBuf.Len()
	tagTotalLength := tagBuf.Len()
	colTotalLength := colBuf.Len()
	tagOffset := DataPosition + tableTotalLength + len(tableNameLength)*2
	colOffset := tagOffset + tagTotalLength + len(tagDataLength)*4
	totalLength := colOffset + colTotalLength + len(colDataLength)*4
	if needTableNames {
		binary.LittleEndian.PutUint32(header[TableNamesOffsetPosition:], uint32(DataPosition))
	}
	if needTags {
		binary.LittleEndian.PutUint32(header[TagsOffsetPosition:], uint32(tagOffset))
	}
	if needCols {
		binary.LittleEndian.PutUint32(header[ColsOffsetPosition:], uint32(colOffset))
	}
	binary.LittleEndian.PutUint32(header[TotalLengthPosition:], uint32(totalLength))
	buffer := make([]byte, totalLength)
	copy(buffer, header)
	if needTableNames {
		offset := DataPosition
		for _, length := range tableNameLength {
			binary.LittleEndian.PutUint16(buffer[offset:], length)
			offset += 2
		}
		copy(buffer[offset:], tableNameBuf.Bytes())
	}
	if needTags {
		offset := tagOffset
		for _, length := range tagDataLength {
			binary.LittleEndian.PutUint32(buffer[offset:], length)
			offset += 4
		}
		copy(buffer[offset:], tagBuf.Bytes())
	}
	if needCols {
		offset := colOffset
		for _, length := range colDataLength {
			binary.LittleEndian.PutUint32(buffer[offset:], length)
			offset += 4
		}
		copy(buffer[offset:], colBuf.Bytes())
	}
	return buffer, nil
}

func getBindDataHeaderLength(num int, needLength bool) int {
	//TotalLength 4 + Type 4 + Num 4  + haveLength 1 + BufferLength * 4 = 17
	// + IsNull num
	// + Length num * 4
	length := 17 + num
	if needLength {
		length += num * 4
	}
	return length
}

func generateBindColData(data []driver.Value, colType *Stmt2AllField, tmpBuffer *bytes.Buffer) ([]byte, error) {
	num := len(data)
	tmpBuffer.Reset()
	needLength := needLength(colType.FieldType)
	headerLength := getBindDataHeaderLength(num, needLength)
	tmpHeader := make([]byte, headerLength)
	// type
	binary.LittleEndian.PutUint32(tmpHeader[BindDataTypeOffset:], uint32(colType.FieldType))
	// num
	binary.LittleEndian.PutUint32(tmpHeader[BindDataNumOffset:], uint32(num))
	// is null
	isNull := tmpHeader[BindDataIsNullOffset : BindDataIsNullOffset+num]
	// has length
	if needLength {
		tmpHeader[BindDataIsNullOffset+num] = 1
	}
	bufferLengthOffset := BindDataIsNullOffset + num + 1
	isAllNull := checkAllNull(data)
	if isAllNull {
		for i := 0; i < num; i++ {
			isNull[i] = 1
		}
	} else {
		switch colType.FieldType {
		case common.TSDB_DATA_TYPE_BOOL:
			for i := 0; i < num; i++ {
				if data[i] == nil {
					isNull[i] = 1
					tmpBuffer.WriteByte(0)
				} else {
					v, ok := data[i].(bool)
					if !ok {
						return nil, fmt.Errorf("data type not match, expect bool, but get %T, value:%v", data[i], data[i])
					}
					if v {
						tmpBuffer.WriteByte(1)
					} else {
						tmpBuffer.WriteByte(0)
					}
				}
			}
		case common.TSDB_DATA_TYPE_TINYINT:
			for i := 0; i < num; i++ {
				if data[i] == nil {
					isNull[i] = 1
					tmpBuffer.WriteByte(0)
				} else {
					v, ok := data[i].(int8)
					if !ok {
						return nil, fmt.Errorf("data type not match, expect int8, but get %T, value:%v", data[i], data[i])
					}
					tmpBuffer.WriteByte(byte(v))
				}
			}

		case common.TSDB_DATA_TYPE_SMALLINT:
			for i := 0; i < num; i++ {
				if data[i] == nil {
					isNull[i] = 1
					writeUint16(tmpBuffer, uint16(0))
				} else {
					v, ok := data[i].(int16)
					if !ok {
						return nil, fmt.Errorf("data type not match, expect int16, but get %T, value:%v", data[i], data[i])
					}
					writeUint16(tmpBuffer, uint16(v))
				}
			}

		case common.TSDB_DATA_TYPE_INT:
			for i := 0; i < num; i++ {
				if data[i] == nil {
					isNull[i] = 1
					writeUint32(tmpBuffer, uint32(0))
				} else {
					v, ok := data[i].(int32)
					if !ok {
						return nil, fmt.Errorf("data type not match, expect int32, but get %T, value:%v", data[i], data[i])
					}
					writeUint32(tmpBuffer, uint32(v))
				}
			}
		case common.TSDB_DATA_TYPE_BIGINT:
			for i := 0; i < num; i++ {
				if data[i] == nil {
					isNull[i] = 1
					writeUint64(tmpBuffer, 0)
				} else {
					v, ok := data[i].(int64)
					if !ok {
						return nil, fmt.Errorf("data type not match, expect int64, but get %T, value:%v", data[i], data[i])
					}
					writeUint64(tmpBuffer, uint64(v))
				}
			}
		case common.TSDB_DATA_TYPE_FLOAT:
			for i := 0; i < num; i++ {
				if data[i] == nil {
					isNull[i] = 1
					writeUint32(tmpBuffer, 0)
				} else {
					v, ok := data[i].(float32)
					if !ok {
						return nil, fmt.Errorf("data type not match, expect float32, but get %T, value:%v", data[i], data[i])
					}
					writeUint32(tmpBuffer, math.Float32bits(v))
				}
			}
		case common.TSDB_DATA_TYPE_DOUBLE:
			for i := 0; i < num; i++ {
				if data[i] == nil {
					isNull[i] = 1
					writeUint64(tmpBuffer, 0)
				} else {
					v, ok := data[i].(float64)
					if !ok {
						return nil, fmt.Errorf("data type not match, expect float64, but get %T, value:%v", data[i], data[i])
					}
					writeUint64(tmpBuffer, math.Float64bits(v))
				}
			}
		case common.TSDB_DATA_TYPE_TIMESTAMP:
			precision := int(colType.Precision)
			for i := 0; i < num; i++ {
				if data[i] == nil {
					isNull[i] = 1
					writeUint64(tmpBuffer, 0)
				} else {
					switch v := data[i].(type) {
					case int64:
						writeUint64(tmpBuffer, uint64(v))
					case time.Time:
						ts := common.TimeToTimestamp(v, precision)
						writeUint64(tmpBuffer, uint64(ts))
					default:
						return nil, fmt.Errorf("data type not match, expect int64 or time.Time, but get %T, value:%v", data[i], data[i])
					}
				}
			}
		case common.TSDB_DATA_TYPE_BINARY, common.TSDB_DATA_TYPE_NCHAR, common.TSDB_DATA_TYPE_VARBINARY, common.TSDB_DATA_TYPE_GEOMETRY, common.TSDB_DATA_TYPE_JSON:
			for i := 0; i < num; i++ {
				if data[i] == nil {
					isNull[i] = 1
				} else {
					switch v := data[i].(type) {
					case string:
						tmpBuffer.WriteString(v)
						binary.LittleEndian.PutUint32(tmpHeader[bufferLengthOffset+i*4:], uint32(len(v)))
					case []byte:
						tmpBuffer.Write(v)
						binary.LittleEndian.PutUint32(tmpHeader[bufferLengthOffset+i*4:], uint32(len(v)))
					default:
						return nil, fmt.Errorf("data type not match, expect string or []byte, but get %T, value:%v", data[i], data[i])
					}
				}
			}
		case common.TSDB_DATA_TYPE_UTINYINT:
			for i := 0; i < num; i++ {
				if data[i] == nil {
					isNull[i] = 1
					tmpBuffer.WriteByte(0)
				} else {
					v, ok := data[i].(uint8)
					if !ok {
						return nil, fmt.Errorf("data type not match, expect uint8, but get %T, value:%v", data[i], data[i])
					}
					tmpBuffer.WriteByte(v)
				}
			}
		case common.TSDB_DATA_TYPE_USMALLINT:
			for i := 0; i < num; i++ {
				if data[i] == nil {
					isNull[i] = 1
					writeUint16(tmpBuffer, 0)
				} else {
					v, ok := data[i].(uint16)
					if !ok {
						return nil, fmt.Errorf("data type not match, expect uint16, but get %T, value:%v", data[i], data[i])
					}
					writeUint16(tmpBuffer, v)
				}
			}
		case common.TSDB_DATA_TYPE_UINT:
			for i := 0; i < num; i++ {
				if data[i] == nil {
					isNull[i] = 1
					writeUint32(tmpBuffer, 0)
				} else {
					v, ok := data[i].(uint32)
					if !ok {
						return nil, fmt.Errorf("data type not match, expect uint32, but get %T, value:%v", data[i], data[i])
					}
					writeUint32(tmpBuffer, v)
				}
			}
		case common.TSDB_DATA_TYPE_UBIGINT:
			for i := 0; i < num; i++ {
				if data[i] == nil {
					isNull[i] = 1
					writeUint64(tmpBuffer, 0)
				} else {
					v, ok := data[i].(uint64)
					if !ok {
						return nil, fmt.Errorf("data type not match, expect uint64, but get %T, value:%v", data[i], data[i])
					}
					writeUint64(tmpBuffer, v)
				}
			}
		default:
			return nil, fmt.Errorf("unsupported type: %d", colType.FieldType)
		}
	}
	buffer := tmpBuffer.Bytes()
	// bufferLength
	binary.LittleEndian.PutUint32(tmpHeader[headerLength-4:], uint32(len(buffer)))
	totalLength := len(buffer) + headerLength
	binary.LittleEndian.PutUint32(tmpHeader[BindDataTotalLengthOffset:], uint32(totalLength))
	dataBuffer := make([]byte, totalLength)
	copy(dataBuffer, tmpHeader)
	copy(dataBuffer[headerLength:], buffer)
	return dataBuffer, nil
}

func checkAllNull(data []driver.Value) bool {
	for i := 0; i < len(data); i++ {
		if data[i] != nil {
			return false
		}
	}
	return true
}

func generateBindQueryData(data driver.Value) ([]byte, error) {
	var colType uint32
	var haveLength = false
	var length = 0
	var buf []byte
	switch v := data.(type) {
	case string:
		colType = common.TSDB_DATA_TYPE_BINARY
		haveLength = true
		length = len(v)
		buf = make([]byte, length)
		copy(buf, v)
	case []byte:
		colType = common.TSDB_DATA_TYPE_BINARY
		haveLength = true
		length = len(v)
		buf = make([]byte, length)
		copy(buf, v)
	case int8:
		colType = common.TSDB_DATA_TYPE_TINYINT
		buf = make([]byte, 1)
		buf[0] = byte(v)
	case int16:
		colType = common.TSDB_DATA_TYPE_SMALLINT
		buf = make([]byte, 2)
		binary.LittleEndian.PutUint16(buf, uint16(v))
	case int32:
		colType = common.TSDB_DATA_TYPE_INT
		buf = make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, uint32(v))
	case int64:
		colType = common.TSDB_DATA_TYPE_BIGINT
		buf = make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, uint64(v))
	case uint8:
		colType = common.TSDB_DATA_TYPE_UTINYINT
		buf = make([]byte, 1)
		buf[0] = byte(v)
	case uint16:
		colType = common.TSDB_DATA_TYPE_USMALLINT
		buf = make([]byte, 2)
		binary.LittleEndian.PutUint16(buf, v)
	case uint32:
		colType = common.TSDB_DATA_TYPE_UINT
		buf = make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, v)
	case uint64:
		colType = common.TSDB_DATA_TYPE_UBIGINT
		buf = make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, v)
	case float32:
		colType = common.TSDB_DATA_TYPE_FLOAT
		buf = make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, math.Float32bits(v))
	case float64:
		colType = common.TSDB_DATA_TYPE_DOUBLE
		buf = make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, math.Float64bits(v))
	case bool:
		colType = common.TSDB_DATA_TYPE_BOOL
		buf = make([]byte, 1)
		if v {
			buf[0] = 1
		} else {
			buf[0] = 0
		}
	case time.Time:
		buf = make([]byte, 0, 35)
		colType = common.TSDB_DATA_TYPE_BINARY
		haveLength = true
		buf = v.AppendFormat(buf, time.RFC3339Nano)
		length = len(buf)
	default:
		return nil, fmt.Errorf("unsupported type: %T", data)
	}
	headerLength := getBindDataHeaderLength(1, haveLength)
	totalLength := len(buf) + headerLength
	dataBuf := make([]byte, totalLength)
	// type
	binary.LittleEndian.PutUint32(dataBuf[BindDataTypeOffset:], colType)
	// num
	binary.LittleEndian.PutUint32(dataBuf[BindDataNumOffset:], 1)
	// is null
	dataBuf[BindDataIsNullOffset] = 0
	// has length
	if haveLength {
		dataBuf[BindDataIsNullOffset+1] = 1
		binary.LittleEndian.PutUint32(dataBuf[BindDataIsNullOffset+2:], uint32(length))

	}
	// bufferLength
	binary.LittleEndian.PutUint32(dataBuf[headerLength-4:], uint32(len(buf)))
	copy(dataBuf[headerLength:], buf)
	binary.LittleEndian.PutUint32(dataBuf[BindDataTotalLengthOffset:], uint32(totalLength))
	return dataBuf, nil
}

func writeUint64(buffer *bytes.Buffer, v uint64) {
	buffer.WriteByte(byte(v))
	buffer.WriteByte(byte(v >> 8))
	buffer.WriteByte(byte(v >> 16))
	buffer.WriteByte(byte(v >> 24))
	buffer.WriteByte(byte(v >> 32))
	buffer.WriteByte(byte(v >> 40))
	buffer.WriteByte(byte(v >> 48))
	buffer.WriteByte(byte(v >> 56))
}

func writeUint32(buffer *bytes.Buffer, v uint32) {
	buffer.WriteByte(byte(v))
	buffer.WriteByte(byte(v >> 8))
	buffer.WriteByte(byte(v >> 16))
	buffer.WriteByte(byte(v >> 24))
}

func writeUint16(buffer *bytes.Buffer, v uint16) {
	buffer.WriteByte(byte(v))
	buffer.WriteByte(byte(v >> 8))
}

func needLength(colType int8) bool {
	switch colType {
	case common.TSDB_DATA_TYPE_BINARY,
		common.TSDB_DATA_TYPE_NCHAR,
		common.TSDB_DATA_TYPE_JSON,
		common.TSDB_DATA_TYPE_VARBINARY,
		common.TSDB_DATA_TYPE_GEOMETRY:
		return true
	}
	return false
}

type Stmt2AllField struct {
	Name      string `json:"name"`
	FieldType int8   `json:"field_type"`
	Precision uint8  `json:"precision"`
	Scale     uint8  `json:"scale"`
	Bytes     int32  `json:"bytes"`
	BindType  int8   `json:"bind_type"`
}

func MarshalStmt2Binary2(bindData []*TaosStmt2BindData, isInsert bool, fields []*Stmt2AllField) ([]byte, error) {
	count := len(bindData)
	if count == 0 {
		return nil, fmt.Errorf("empty data")
	}
	var colType []*Stmt2AllField
	var tagType []*Stmt2AllField
	var fieldsHasTableName bool
	for i := 0; i < len(fields); i++ {
		switch fields[i].BindType {
		case TAOS_FIELD_COL:
			colType = append(colType, fields[i])
		case TAOS_FIELD_TAG:
			tagType = append(tagType, fields[i])
		case TAOS_FIELD_TBNAME:
			fieldsHasTableName = true
		default:
			return nil, fmt.Errorf("unsupported bind type: %d", fields[i].BindType)
		}
	}
	tagCount := len(tagType)
	colCount := len(colType)
	needTableNames := false
	needTags := tagCount > 0
	needCols := colCount > 0
	totalTableNameBufferLength := 0
	var tableNameBufferLength []uint16
	if isInsert {
		for i := 0; i < count; i++ {
			data := bindData[i]
			if data.TableName != "" {
				if !fieldsHasTableName {
					return nil, fmt.Errorf("got table name, but no table name field")
				}
				needTableNames = true
				totalTableNameBufferLength += len(data.TableName) + 1
				//if len(tableNameBufferLength) == 0 {
				//	tableNameBufferLength = make([]uint16, count)
				//}
				tableNameBufferLength[i] = uint16(len(data.TableName) + 1)
			}
			if len(data.Tags) != tagCount {
				return nil, fmt.Errorf("tag count not match, data count:%d, type count:%d", len(data.Tags), tagCount)
			}
			if len(data.Cols) != colCount {
				return nil, fmt.Errorf("col count not match, data count:%d, type count:%d", len(data.Cols), colCount)
			}
		}
	} else {
		if tagCount != 0 {
			return nil, fmt.Errorf("query not need tag types")
		}
		if colCount != 0 {
			return nil, fmt.Errorf("query not need col types")
		}
		if count != 1 {
			return nil, fmt.Errorf("query only need one data")
		}

		data := bindData[0]
		if data.TableName != "" {
			return nil, fmt.Errorf("query not need table name")
		}
		if len(data.Tags) != 0 {
			return nil, fmt.Errorf("query not need tag")
		}
		if len(data.Cols) == 0 {
			return nil, fmt.Errorf("query need col")
		}
		colCount = len(data.Cols)
		for j := 0; j < colCount; j++ {
			if len(data.Cols[j]) != 1 {
				return nil, fmt.Errorf("query col data must be one row, col:%d, count:%d", j, len(data.Cols[j]))
			}
		}
	}
	if !needTableNames && !needTags && !needCols {
		return nil, fmt.Errorf("no data")
	}
	totalTag := count * tagCount
	totalCol := count * colCount
	totalTagBufferLength := 0
	var tagBufferLength []uint32
	var tagDataLength [][]uint32
	var tableTagLength []uint32
	var everyTagBindBinarylen []uint32
	if needTags {
		tagBufferLength = make([]uint32, totalTag)
		tagDataLength = make([][]uint32, totalTag)
		tableTagLength = make([]uint32, count)
		everyTagBindBinarylen = make([]uint32, totalTag)
	}
	totalColBufferLength := 0
	var colBufferLength []uint32
	var colDataLength [][]uint32
	var tableColLength []uint32
	var everyColBindBinaryLen []uint32
	if needCols {
		colBufferLength = make([]uint32, totalCol)
		colDataLength = make([][]uint32, totalCol)
		tableColLength = make([]uint32, count)
		everyColBindBinaryLen = make([]uint32, totalCol)
	}
	tmpTagData := make([]driver.Value, 1)
	for i := 0; i < count; i++ {
		data := bindData[i]
		if needTags {
			baseIndex := i * tagCount
			for j := 0; j < tagCount; j++ {
				index := baseIndex + j
				if needLength(tagType[j].FieldType) {
					tmpTagData[0] = data.Tags[j]
					tagDataTotalLen, tagBufferLen, dataLengths, err := getVarDataTypeBufferLen(tmpTagData, tagType[j])
					if err != nil {
						return nil, err
					}
					tagBufferLength[index] = tagBufferLen
					tagDataLength[index] = dataLengths
					totalTagBufferLength += tagDataTotalLen
					tableTagLength[i] += uint32(tagDataTotalLen)
					everyTagBindBinarylen[index] = uint32(tagDataTotalLen)
				} else {
					tagDataTotalLen, tagBufferLen, err := getNumTypeBufferLen(1, tagType[j])
					if err != nil {
						return nil, err
					}
					tagBufferLength[index] = tagBufferLen
					totalTagBufferLength += tagDataTotalLen
					tableTagLength[i] += uint32(tagDataTotalLen)
					everyTagBindBinarylen[index] = uint32(tagDataTotalLen)
				}
			}
		}
		if needCols {
			for j := 0; j < colCount; j++ {
				if needLength(colType[j].FieldType) {
					colDataTotalLen, colBufferLen, dataLengths, err := getVarDataTypeBufferLen(data.Cols[j], colType[j])
					if err != nil {
						return nil, err
					}
					colDataLength[i*colCount+j] = dataLengths
					colBufferLength[i*colCount+j] = colBufferLen
					totalColBufferLength += colDataTotalLen
					tableColLength[i] += uint32(colDataTotalLen)
					everyColBindBinaryLen[i*colCount+j] = uint32(colDataTotalLen)
				} else {
					colDataTotalLen, colBufferLen, err := getNumTypeBufferLen(len(data.Cols[j]), colType[j])
					if err != nil {
						return nil, err
					}
					colBufferLength[i*colCount+j] = colBufferLen
					totalColBufferLength += colDataTotalLen
					tableColLength[i] += uint32(colDataTotalLen)
					everyColBindBinaryLen[i*colCount+j] = uint32(colDataTotalLen)
				}
			}
		}
	}

	tableNamesOffset := DataPosition
	tagOffset := tableNamesOffset + totalTableNameBufferLength + len(tableNameBufferLength)*2
	colOffset := tagOffset + totalTagBufferLength + len(tableTagLength)*4
	totalBufferLength := colOffset + totalColBufferLength + len(tableColLength)*4
	buffer := make([]byte, totalBufferLength)
	binary.LittleEndian.PutUint32(buffer[TotalLengthPosition:], uint32(totalBufferLength))
	// count
	binary.LittleEndian.PutUint32(buffer[CountPosition:], uint32(count))

	if tagCount != 0 {
		binary.LittleEndian.PutUint32(buffer[TagCountPosition:], uint32(tagCount))
	}
	if colCount != 0 {
		binary.LittleEndian.PutUint32(buffer[ColCountPosition:], uint32(colCount))
	}
	if needTableNames {
		binary.LittleEndian.PutUint32(buffer[TableNamesOffsetPosition:], uint32(tableNamesOffset))
		mem.Copy(unsafe.Pointer(&tableNameBufferLength[0]), buffer, tableNamesOffset, count*2)
	}
	if needTags {
		binary.LittleEndian.PutUint32(buffer[TagsOffsetPosition:], uint32(tagOffset))
		mem.Copy(unsafe.Pointer(&tableTagLength[0]), buffer, tagOffset, count*4)
	}
	if needCols {
		binary.LittleEndian.PutUint32(buffer[ColsOffsetPosition:], uint32(colOffset))
		mem.Copy(unsafe.Pointer(&tableColLength[0]), buffer, colOffset, count*4)
	}
	if len(tableNameBufferLength) != 0 {
		mem.Copy(unsafe.Pointer(&tableNameBufferLength[0]), buffer, tableNamesOffset, len(tableNameBufferLength)*2)
	}
	tableNameP := tableNamesOffset + len(tableNameBufferLength)*2
	tagP := tagOffset + len(tableTagLength)*4
	colP := colOffset + len(tableColLength)*4
	for i := 0; i < len(bindData); i++ {
		if bindData[i].TableName != "" {
			copy(buffer[tableNameP:], bindData[i].TableName)
			tableNameP += len(bindData[i].TableName) + 1
		}
		if bindData[i].Tags != nil {
			for j := 0; j < tagCount; j++ {
				tmpTagData[0] = bindData[i].Tags[j]
				err := generateBindColData2(buffer[tagP:], tmpTagData, tagType[j], everyTagBindBinarylen[i*tagCount+j], tagDataLength[i*tagCount+j], tagBufferLength[i*tagCount+j])
				if err != nil {
					return nil, err
				}
			}
		}
		if bindData[i].Cols != nil {
			for j := 0; j < colCount; j++ {
				err := generateBindColData2(buffer[colP:], bindData[i].Cols[j], colType[j], everyColBindBinaryLen[i*colCount+j], colDataLength[i*colCount+j], colBufferLength[i*colCount+j])
				if err != nil {
					return nil, err
				}
				colP += int(everyColBindBinaryLen[i*colCount+j])
			}
		}
	}
	return buffer, nil
}

func getNumTypeBufferLen(rows int, colType *Stmt2AllField) (int, uint32, error) {
	//TotalLength 4 + Type 4 + Num 4  + haveLength 1 + BufferLength * 4 = 17
	// + IsNull num
	headerLength := 17 + rows
	switch colType.FieldType {
	case common.TSDB_DATA_TYPE_BOOL, common.TSDB_DATA_TYPE_TINYINT, common.TSDB_DATA_TYPE_UTINYINT:
		return headerLength + rows, uint32(rows), nil
	case common.TSDB_DATA_TYPE_SMALLINT, common.TSDB_DATA_TYPE_USMALLINT:
		bufferLen := rows * 2
		return headerLength + bufferLen, uint32(bufferLen), nil
	case common.TSDB_DATA_TYPE_INT, common.TSDB_DATA_TYPE_UINT, common.TSDB_DATA_TYPE_FLOAT:
		bufferLen := rows * 4
		return headerLength + bufferLen, uint32(bufferLen), nil
	case common.TSDB_DATA_TYPE_BIGINT, common.TSDB_DATA_TYPE_UBIGINT, common.TSDB_DATA_TYPE_DOUBLE, common.TSDB_DATA_TYPE_TIMESTAMP:
		bufferLen := rows * 8
		return headerLength + bufferLen, uint32(bufferLen), nil
	default:
		return 0, 0, fmt.Errorf("unsupported field type: %d", colType.FieldType)
	}
}

func getVarDataTypeBufferLen(data []driver.Value, colType *Stmt2AllField) (int, uint32, []uint32, error) {
	//TotalLength 4 + Type 4 + Num 4  + haveLength 1 + BufferLength * 4 = 17
	// + IsNull num
	// + Length num * 4
	rows := len(data)
	lengthList := make([]uint32, rows)
	bufLength := uint32(0)
	headerLength := 17 + rows*5
	switch colType.FieldType {
	case common.TSDB_DATA_TYPE_BINARY, common.TSDB_DATA_TYPE_NCHAR, common.TSDB_DATA_TYPE_VARBINARY, common.TSDB_DATA_TYPE_GEOMETRY, common.TSDB_DATA_TYPE_JSON:
		for i := 0; i < rows; i++ {
			switch v := data[i].(type) {
			case string:
				length := uint32(len(v))
				lengthList[i] = length
				bufLength += length
			case []byte:
				length := uint32(len(v))
				lengthList[i] = length
				bufLength += length
			default:
				return 0, 0, nil, fmt.Errorf("unsupported data type: %T, expect string or []byte", data[i])
			}
		}
		return headerLength + int(bufLength), bufLength, lengthList, nil
	default:
		return 0, 0, nil, fmt.Errorf("unsupported field type: %d", colType.FieldType)
	}
}

type stringHeader struct {
	data unsafe.Pointer
	len  int
}

func generateBindColData2(buffer []byte, data []driver.Value, colType *Stmt2AllField, totalLen uint32, length []uint32, bufferLen uint32) error {
	rows := len(data)
	_ = buffer[BindDataIsNullOffset+rows+1+4]
	//TotalLength  uint32  // 4, 当前 TagData 的全部长度,包括 TotalLength 字段长度
	//Type         int32   // 4, 数据类型
	//Num          int32   // 4, 多少行数据
	//IsNull       []byte  // Num * 1 每个 tag 是否为 null, Num 个元素
	//haveLength   byte    // 1, 是否有长度，0 为没有，1 为有，当数据类型为变长时必须有长度（binary, nchar, json, varbinary, varchar）
	//Length       []int32 // Num * 4 每个 tag 的长度, Num 个元素，当 hasLength 为 0 时，无该字段
	//BufferLength uint32  // 4, Buffer 的长度
	//Buffer       []byte  // 绑定数据
	binary.LittleEndian.PutUint32(buffer[BindDataTotalLengthOffset:], totalLen)
	binary.LittleEndian.PutUint32(buffer[BindDataTypeOffset:], uint32(colType.FieldType))
	binary.LittleEndian.PutUint32(buffer[BindDataNumOffset:], uint32(rows))
	if needLength(colType.FieldType) {
		// have length
		haveLengthOffset := BindDataIsNullOffset + rows
		lengthOffset := haveLengthOffset + 1
		bufferLengthOffset := lengthOffset + rows*4
		bufferOffset := bufferLengthOffset + 4
		buffer[haveLengthOffset] = 1
		// length
		mem.Copy(unsafe.Pointer(&length[0]), buffer, lengthOffset, rows*4)
		// buffer length
		binary.LittleEndian.PutUint32(buffer[bufferLengthOffset:bufferOffset], bufferLen)
		for i := 0; i < rows; i++ {
			// buffer
			if data[i] == nil {
				buffer[BindDataIsNullOffset+i] = 1
				continue
			}
			switch v := data[i].(type) {
			case string:
				x := (*stringHeader)(unsafe.Pointer(&v)).data
				mem.Copy(x, buffer, bufferOffset, len(v))
				bufferOffset += len(v)
			case []byte:
				mem.Copy(unsafe.Pointer(&v[0]), buffer, bufferOffset, len(v))
				bufferOffset += len(v)
			}
		}
	} else {
		// buffer length
		binary.LittleEndian.PutUint32(buffer[BindDataIsNullOffset+rows+1:], bufferLen)
		bufferOffset := BindDataIsNullOffset + rows + 1 + 4
		switch colType.FieldType {
		case common.TSDB_DATA_TYPE_BOOL:
			for i := 0; i < rows; i++ {
				if data[i] == nil {
					buffer[BindDataIsNullOffset+i] = 1
					continue
				}
				v, ok := data[i].(bool)
				if !ok {
					return fmt.Errorf("data type not match, expect bool, but get %T, value:%v", data[i], data[i])
				}
				if v {
					buffer[bufferOffset+i] = 1
				}
			}
		case common.TSDB_DATA_TYPE_TINYINT:
			for i := 0; i < rows; i++ {
				if data[i] == nil {
					buffer[BindDataIsNullOffset+i] = 1
					continue
				}
				v, ok := data[i].(int8)
				if !ok {
					return fmt.Errorf("data type not match, expect int8, but get %T, value:%v", data[i], data[i])
				}
				buffer[bufferOffset+i] = byte(v)
			}
		case common.TSDB_DATA_TYPE_SMALLINT:
			for i := 0; i < rows; i++ {
				if data[i] == nil {
					buffer[BindDataIsNullOffset+i] = 1
					continue
				}
				v, ok := data[i].(int16)
				if !ok {
					return fmt.Errorf("data type not match, expect int16, but get %T, value:%v", data[i], data[i])
				}
				binary.LittleEndian.PutUint16(buffer[bufferOffset+i*2:], uint16(v))
			}
		case common.TSDB_DATA_TYPE_INT:
			for i := 0; i < rows; i++ {
				if data[i] == nil {
					buffer[BindDataIsNullOffset+i] = 1
					continue
				}
				v, ok := data[i].(int32)
				if !ok {
					return fmt.Errorf("data type not match, expect int32, but get %T, value:%v", data[i], data[i])
				}
				binary.LittleEndian.PutUint32(buffer[bufferOffset+i*4:], uint32(v))
			}
		case common.TSDB_DATA_TYPE_BIGINT:
			for i := 0; i < rows; i++ {
				if data[i] == nil {
					buffer[BindDataIsNullOffset+i] = 1
					continue
				}
				v, ok := data[i].(int64)
				if !ok {
					return fmt.Errorf("data type not match, expect int64, but get %T, value:%v", data[i], data[i])
				}
				binary.LittleEndian.PutUint64(buffer[bufferOffset+i*8:], uint64(v))
			}
		case common.TSDB_DATA_TYPE_FLOAT:
			for i := 0; i < rows; i++ {
				if data[i] == nil {
					buffer[BindDataIsNullOffset+i] = 1
					continue
				}
				v, ok := data[i].(float32)
				if !ok {
					return fmt.Errorf("data type not match, expect float32, but get %T, value:%v", data[i], data[i])
				}
				binary.LittleEndian.PutUint32(buffer[bufferOffset+i*4:], math.Float32bits(v))
			}
		case common.TSDB_DATA_TYPE_DOUBLE:
			for i := 0; i < rows; i++ {
				if data[i] == nil {
					buffer[BindDataIsNullOffset+i] = 1
					continue
				}
				v, ok := data[i].(float64)
				if !ok {
					return fmt.Errorf("data type not match, expect float64, but get %T, value:%v", data[i], data[i])
				}
				binary.LittleEndian.PutUint64(buffer[bufferOffset+i*8:], math.Float64bits(v))
			}
		case common.TSDB_DATA_TYPE_TIMESTAMP:
			precision := int(colType.Precision)
			for i := 0; i < rows; i++ {
				if data[i] == nil {
					buffer[BindDataIsNullOffset+i] = 1
					continue
				}
				switch v := data[i].(type) {
				case int64:
					binary.LittleEndian.PutUint64(buffer[bufferOffset+i*8:], uint64(v))
				case time.Time:
					ts := common.TimeToTimestamp(v, precision)
					binary.LittleEndian.PutUint64(buffer[bufferOffset+i*8:], uint64(ts))
				default:
					return fmt.Errorf("data type not match, expect int64 or time.Time, but get %T, value:%v", data[i], data[i])
				}
			}
		case common.TSDB_DATA_TYPE_UTINYINT:
			for i := 0; i < rows; i++ {
				if data[i] == nil {
					buffer[BindDataIsNullOffset+i] = 1
					continue
				}
				v, ok := data[i].(uint8)
				if !ok {
					return fmt.Errorf("data type not match, expect uint8, but get %T, value:%v", data[i], data[i])
				}
				buffer[bufferOffset+i] = byte(v)
			}
		case common.TSDB_DATA_TYPE_USMALLINT:
			for i := 0; i < rows; i++ {
				if data[i] == nil {
					buffer[BindDataIsNullOffset+i] = 1
					continue
				}
				v, ok := data[i].(uint16)
				if !ok {
					return fmt.Errorf("data type not match, expect uint16, but get %T, value:%v", data[i], data[i])
				}
				binary.LittleEndian.PutUint16(buffer[bufferOffset+i*2:], v)
			}
		case common.TSDB_DATA_TYPE_UINT:
			for i := 0; i < rows; i++ {
				if data[i] == nil {
					buffer[BindDataIsNullOffset+i] = 1
					continue
				}
				v, ok := data[i].(uint32)
				if !ok {
					return fmt.Errorf("data type not match, expect uint32, but get %T, value:%v", data[i], data[i])
				}
				binary.LittleEndian.PutUint32(buffer[bufferOffset+i*4:], v)
			}
		case common.TSDB_DATA_TYPE_UBIGINT:
			for i := 0; i < rows; i++ {
				if data[i] == nil {
					buffer[BindDataIsNullOffset+i] = 1
					continue
				}
				v, ok := data[i].(uint64)
				if !ok {
					return fmt.Errorf("data type not match, expect uint64, but get %T, value:%v", data[i], data[i])
				}
				binary.LittleEndian.PutUint64(buffer[bufferOffset+i*8:], v)
			}
		default:
			return fmt.Errorf("unsupported field type: %d", colType.FieldType)
		}
	}
	return nil
}

type TaosStmt2BindDatax struct {
	TableName string
	Tags      *RecordBuilder
	Cols      *RecordBuilder
}

func MarshalStmt2Binary3(bindData []*TaosStmt2BindDatax, isInsert bool, fields []*Stmt2AllField) ([]byte, error) {
	count := len(bindData)
	if count == 0 {
		return nil, fmt.Errorf("empty data")
	}
	var colType []*Stmt2AllField
	var tagType []*Stmt2AllField
	//var fieldsHasTableName bool
	for i := 0; i < len(fields); i++ {
		switch fields[i].BindType {
		case TAOS_FIELD_COL:
			colType = append(colType, fields[i])
		case TAOS_FIELD_TAG:
			tagType = append(tagType, fields[i])
		case TAOS_FIELD_TBNAME:
			//fieldsHasTableName = true
		default:

			return nil, fmt.Errorf("unsupported bind type: %d", fields[i].BindType)
		}
	}
	tagCount := len(tagType)
	colCount := len(colType)
	needTableNames := false
	needTags := tagCount > 0
	needCols := colCount > 0
	totalTableNameBufferLength := 0
	var tableNameBufferLength []uint16
	//if isInsert {
	//	for i := 0; i < count; i++ {
	//		data := bindData[i]
	//		if data.TableName != "" {
	//			if !fieldsHasTableName {
	//				return nil, fmt.Errorf("got table name, but no table name field")
	//			}
	//			needTableNames = true
	//			totalTableNameBufferLength += len(data.TableName) + 1
	//			tableNameBufferLength[i] = uint16(len(data.TableName) + 1)
	//		}
	//	}
	//} else {
	//}
	//if !needTableNames && !needTags && !needCols {
	//	return nil, fmt.Errorf("no data")
	//}
	totalTagBufferLength := 0
	var tableTagLength []uint32
	if needTags {
		tableTagLength = make([]uint32, count)
	}
	totalColBufferLength := 0
	var tableColLength []uint32
	if needCols {
		tableColLength = make([]uint32, count)
	}
	for i := 0; i < len(bindData); i++ {
		if bindData[i].Tags != nil {
			builders := bindData[i].Tags.Fields()
			for j := 0; j < len(builders); j++ {
				bindLen := builders[j].BindBytesLength()
				totalTagBufferLength += bindLen
				tableTagLength[i] += uint32(bindLen)
			}
		}
		if bindData[i].Cols != nil {
			builders := bindData[i].Cols.Fields()
			for j := 0; j < len(builders); j++ {
				bindLen := builders[j].BindBytesLength()
				totalColBufferLength += bindLen
				tableColLength[i] += uint32(bindLen)
			}
		}
	}
	tableNamesOffset := DataPosition
	tagOffset := tableNamesOffset + totalTableNameBufferLength
	if needTableNames {
		tagOffset += count * 2
	}
	colOffset := tagOffset + totalTagBufferLength
	if needTags {
		colOffset += count * 4
	}
	totalBufferLength := colOffset + totalColBufferLength
	if needCols {
		totalBufferLength += count * 4
	}
	buffer := make([]byte, totalBufferLength)
	p0 := unsafe.Pointer(&buffer[0])
	*(*uint32)(unsafe.Pointer(uintptr(p0) + TotalLengthPosition)) = uint32(totalBufferLength)
	// count
	*(*uint32)(unsafe.Pointer(uintptr(p0) + CountPosition)) = uint32(count)
	if tagCount != 0 {
		*(*uint32)(unsafe.Pointer(uintptr(p0) + TagCountPosition)) = uint32(tagCount)
	}
	if colCount != 0 {
		*(*uint32)(unsafe.Pointer(uintptr(p0) + ColCountPosition)) = uint32(colCount)
	}
	if needTableNames {
		*(*uint32)(unsafe.Pointer(uintptr(p0) + TableNamesOffsetPosition)) = uint32(tableNamesOffset)
		mem.CopyUncheck(unsafe.Pointer(&tableNameBufferLength[0]), unsafe.Pointer(uintptr(p0)+uintptr(tableNamesOffset)), count*2)
	}
	if needTags {
		*(*uint32)(unsafe.Pointer(uintptr(p0) + TagsOffsetPosition)) = uint32(tagOffset)
		mem.CopyUncheck(unsafe.Pointer(&tableTagLength[0]), unsafe.Pointer(uintptr(p0)+uintptr(tagOffset)), count*4)
	}
	if needCols {
		*(*uint32)(unsafe.Pointer(uintptr(p0) + ColsOffsetPosition)) = uint32(colOffset)
		mem.CopyUncheck(unsafe.Pointer(&tableColLength[0]), unsafe.Pointer(uintptr(p0)+uintptr(colOffset)), count*4)
	}
	if len(tableNameBufferLength) != 0 {
		mem.CopyUncheck(unsafe.Pointer(&tableNameBufferLength[0]), unsafe.Pointer(uintptr(p0)+uintptr(tableNamesOffset)), len(tableNameBufferLength)*2)
	}
	tableNameP := tableNamesOffset + len(tableNameBufferLength)*2
	colP := colOffset + len(tableColLength)*4
	for i := 0; i < len(bindData); i++ {
		if bindData[i].TableName != "" {
			copy(buffer[tableNameP:], bindData[i].TableName)
			tableNameP += len(bindData[i].TableName) + 1
		}
		if bindData[i].Cols != nil {
			builders := bindData[i].Cols.Fields()
			for j := 0; j < len(builders); j++ {
				err := generateBindColData3(buffer[colP:], builders[j], colType[j])
				if err != nil {
					return nil, err
				}
				colP += builders[j].BindBytesLength()
			}
		}
	}
	return buffer, nil
}

func generateBindColData3(buffer []byte, data Builder, colType *Stmt2AllField) error {
	rows := data.Len()
	p0 := unsafe.Pointer(&buffer[0])
	//_ = buffer[BindDataIsNullOffset+rows+1+4]
	//TotalLength  uint32  // 4, 当前 TagData 的全部长度,包括 TotalLength 字段长度
	//Type         int32   // 4, 数据类型
	//Num          int32   // 4, 多少行数据
	//IsNull       []byte  // Num * 1 每个 tag 是否为 null, Num 个元素
	//haveLength   byte    // 1, 是否有长度，0 为没有，1 为有，当数据类型为变长时必须有长度（binary, nchar, json, varbinary, varchar）
	//Length       []int32 // Num * 4 每个 tag 的长度, Num 个元素，当 hasLength 为 0 时，无该字段
	//BufferLength uint32  // 4, Buffer 的长度
	//Buffer       []byte  // 绑定数据
	*(*uint32)(unsafe.Pointer(uintptr(p0) + BindDataTotalLengthOffset)) = uint32(data.BindBytesLength())
	*(*int32)(unsafe.Pointer(uintptr(p0) + BindDataTypeOffset)) = int32(colType.FieldType)
	*(*uint32)(unsafe.Pointer(uintptr(p0) + BindDataNumOffset)) = uint32(rows)
	if data.VariableLengthType() {
		haveLengthOffset := BindDataIsNullOffset + rows
		data.CopyNullBytes(buffer[BindDataIsNullOffset : BindDataIsNullOffset+rows : BindDataIsNullOffset+rows])
		buffer[haveLengthOffset] = 1
		bufferLengths := data.BufferLengths()
		lengthOffset := haveLengthOffset + 1
		mem.CopyUncheck(unsafe.Pointer(&bufferLengths[0]), unsafe.Pointer(uintptr(p0)+uintptr(lengthOffset)), rows*4)
		bufferLenOffset := lengthOffset + rows*4
		bufferLen := data.ValueBytesLength()
		*(*uint32)(unsafe.Pointer(uintptr(p0) + uintptr(bufferLenOffset))) = uint32(bufferLen)
		bufferOffset := bufferLenOffset + 4
		data.CopyValueBytes(buffer[bufferOffset : bufferOffset+bufferLen : bufferOffset+bufferLen])
	} else {
		// buffer length
		bufferLenOffset := BindDataIsNullOffset + rows + 1
		bufferLen := data.ValueBytesLength()
		*(*uint32)(unsafe.Pointer(uintptr(p0) + uintptr(bufferLenOffset))) = uint32(bufferLen)
		bufferOffset := BindDataIsNullOffset + rows + 1 + 4
		data.CopyNullBytes(buffer[BindDataIsNullOffset : BindDataIsNullOffset+rows : BindDataIsNullOffset+rows])
		data.CopyValueBytes(buffer[bufferOffset : bufferOffset+bufferLen : bufferOffset+bufferLen])
	}
	return nil
}
