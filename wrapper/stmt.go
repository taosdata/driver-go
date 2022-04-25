package wrapper

/*
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <taos.h>
*/
import "C"
import (
	"errors"
	"unsafe"

	"github.com/taosdata/driver-go/v2/common"
	taosTypes "github.com/taosdata/driver-go/v2/types"
)

// TaosStmtInit TAOS_STMT *taos_stmt_init(TAOS *taos);
func TaosStmtInit(taosConnect unsafe.Pointer) unsafe.Pointer {
	return C.taos_stmt_init(taosConnect)
}

// TaosStmtPrepare int        taos_stmt_prepare(TAOS_STMT *stmt, const char *sql, unsigned long length);
func TaosStmtPrepare(stmt unsafe.Pointer, sql string) int {
	cSql := C.CString(sql)
	cLen := C.ulong(len(sql))
	defer C.free(unsafe.Pointer(cSql))
	return int(C.taos_stmt_prepare(stmt, cSql, cLen))
}

//typedef struct TAOS_BIND_v2 {
//int       buffer_type;
//void     *buffer;
//int32_t   buffer_length;
//int32_t  *length;
//char     *is_null;
//int       num;
//} TAOS_BIND_v2;

// TaosStmtSetTBNameTags int        taos_stmt_set_tbname_tags(TAOS_STMT* stmt, const char* name, TAOS_BIND_v2* tags);
func TaosStmtSetTBNameTags(stmt unsafe.Pointer, name string, tags []interface{}) int {
	cStr := C.CString(name)
	defer C.free(unsafe.Pointer(cStr))
	if len(tags) == 0 {
		return int(C.taos_stmt_set_tbname_tags(stmt, cStr, nil))
	}
	binds, needFreePointer, err := generateTaosBindList(tags)
	defer func() {
		for _, pointer := range needFreePointer {
			C.free(pointer)
		}
	}()
	if err != nil {
		return -1
	}
	result := int(C.taos_stmt_set_tbname_tags(stmt, cStr, (*C.TAOS_BIND_v2)(&binds[0])))
	return result
}

// TaosStmtSetTBName int        taos_stmt_set_tbname(TAOS_STMT* stmt, const char* name);
func TaosStmtSetTBName(stmt unsafe.Pointer, name string) int {
	cStr := C.CString(name)
	defer C.free(unsafe.Pointer(cStr))
	return int(C.taos_stmt_set_tbname(stmt, cStr))
}

// TaosStmtIsInsert int        taos_stmt_is_insert(TAOS_STMT *stmt, int *insert);
func TaosStmtIsInsert(stmt unsafe.Pointer) (is bool, errorCode int) {
	p := C.malloc(C.size_t(4))
	isInsert := (*C.int)(p)
	defer C.free(p)
	errorCode = int(C.taos_stmt_is_insert(stmt, isInsert))
	return int(*isInsert) == 1, errorCode
}

// TaosStmtNumParams int        taos_stmt_num_params(TAOS_STMT *stmt, int *nums);
func TaosStmtNumParams(stmt unsafe.Pointer) (count int, errorCode int) {
	p := C.malloc(C.size_t(4))
	num := (*C.int)(p)
	defer C.free(p)
	errorCode = int(C.taos_stmt_num_params(stmt, num))
	return int(*num), errorCode
}

// TaosStmtBindParam int        taos_stmt_bind_param(TAOS_STMT *stmt, TAOS_BIND_v2 *bind);
func TaosStmtBindParam(stmt unsafe.Pointer, params []interface{}) int {
	if len(params) == 0 {
		return int(C.taos_stmt_bind_param(stmt, nil))
	}
	binds, needFreePointer, err := generateTaosBindList(params)
	defer func() {
		for _, pointer := range needFreePointer {
			if pointer != nil {
				C.free(pointer)
			}
		}
	}()
	if err != nil {
		return -1
	}
	result := int(C.taos_stmt_bind_param(stmt, (*C.TAOS_BIND_v2)(unsafe.Pointer(&binds[0]))))
	return result
}

func generateTaosBindList(params []interface{}) ([]C.TAOS_BIND_v2, []unsafe.Pointer, error) {
	binds := make([]C.TAOS_BIND_v2, len(params))
	var needFreePointer []unsafe.Pointer
	for i, param := range params {
		bind := C.TAOS_BIND_v2{}
		bind.num = C.int(1)
		if param == nil {
			bind.buffer_type = C.TSDB_DATA_TYPE_BOOL
			p := C.malloc(1)
			*(*C.char)(p) = C.char(1)
			needFreePointer = append(needFreePointer, p)
			bind.is_null = (*C.char)(p)
		} else {
			switch param.(type) {
			case taosTypes.TaosBool:
				bind.buffer_type = C.TSDB_DATA_TYPE_BOOL
				value := param.(taosTypes.TaosBool)
				p := C.malloc(1)
				if value {
					*(*C.int8_t)(p) = C.int8_t(1)
				} else {
					*(*C.int8_t)(p) = C.int8_t(0)
				}
				needFreePointer = append(needFreePointer, p)
				bind.buffer = p
				bind.buffer_length = C.int32_t(1)
			case taosTypes.TaosTinyint:
				bind.buffer_type = C.TSDB_DATA_TYPE_TINYINT
				value := param.(taosTypes.TaosTinyint)
				p := C.malloc(1)
				*(*C.int8_t)(p) = C.int8_t(value)
				needFreePointer = append(needFreePointer, p)
				bind.buffer = p
				bind.buffer_length = C.int32_t(1)
			case taosTypes.TaosSmallint:
				bind.buffer_type = C.TSDB_DATA_TYPE_SMALLINT
				value := param.(taosTypes.TaosSmallint)
				p := C.malloc(2)
				*(*C.int16_t)(p) = C.int16_t(value)
				needFreePointer = append(needFreePointer, p)
				bind.buffer = p
				bind.buffer_length = C.int32_t(2)
			case taosTypes.TaosInt:
				value := param.(taosTypes.TaosInt)
				bind.buffer_type = C.TSDB_DATA_TYPE_INT
				p := C.malloc(4)
				*(*C.int32_t)(p) = C.int32_t(value)
				needFreePointer = append(needFreePointer, p)
				bind.buffer = p
				bind.buffer_length = C.int32_t(4)
			case taosTypes.TaosBigint:
				bind.buffer_type = C.TSDB_DATA_TYPE_BIGINT
				value := param.(taosTypes.TaosBigint)
				p := C.malloc(8)
				*(*C.int64_t)(p) = C.int64_t(value)
				needFreePointer = append(needFreePointer, p)
				bind.buffer = p
				bind.buffer_length = C.int32_t(8)
			case taosTypes.TaosUTinyint:
				bind.buffer_type = C.TSDB_DATA_TYPE_UTINYINT
				buf := param.(taosTypes.TaosUTinyint)
				cbuf := C.malloc(1)
				*(*C.uint8_t)(cbuf) = C.uint8_t(buf)
				needFreePointer = append(needFreePointer, cbuf)
				bind.buffer = cbuf
				bind.buffer_length = C.int32_t(1)
			case taosTypes.TaosUSmallint:
				bind.buffer_type = C.TSDB_DATA_TYPE_USMALLINT
				value := param.(taosTypes.TaosUSmallint)
				p := C.malloc(2)
				*(*C.uint16_t)(p) = C.uint16_t(value)
				needFreePointer = append(needFreePointer, p)
				bind.buffer = p
				bind.buffer_length = C.int32_t(2)
			case taosTypes.TaosUInt:
				bind.buffer_type = C.TSDB_DATA_TYPE_UINT
				value := param.(taosTypes.TaosUInt)
				p := C.malloc(4)
				*(*C.uint32_t)(p) = C.uint32_t(value)
				needFreePointer = append(needFreePointer, p)
				bind.buffer = p
				bind.buffer_length = C.int32_t(4)
			case taosTypes.TaosUBigint:
				bind.buffer_type = C.TSDB_DATA_TYPE_UBIGINT
				value := param.(taosTypes.TaosUBigint)
				p := C.malloc(8)
				*(*C.uint64_t)(p) = C.uint64_t(value)
				needFreePointer = append(needFreePointer, p)
				bind.buffer = p
				bind.buffer_length = C.int32_t(8)
			case taosTypes.TaosFloat:
				bind.buffer_type = C.TSDB_DATA_TYPE_FLOAT
				value := param.(taosTypes.TaosFloat)
				p := C.malloc(4)
				*(*C.float)(p) = C.float(value)
				needFreePointer = append(needFreePointer, p)
				bind.buffer = p
				bind.buffer_length = C.int32_t(4)
			case taosTypes.TaosDouble:
				bind.buffer_type = C.TSDB_DATA_TYPE_DOUBLE
				value := param.(taosTypes.TaosDouble)
				p := C.malloc(8)
				*(*C.double)(p) = C.double(value)
				needFreePointer = append(needFreePointer, p)
				bind.buffer = p
				bind.buffer_length = C.int32_t(8)
			case taosTypes.TaosBinary:
				bind.buffer_type = C.TSDB_DATA_TYPE_BINARY
				buf := param.(taosTypes.TaosBinary)
				cbuf := C.CString(string(buf))
				needFreePointer = append(needFreePointer, unsafe.Pointer(cbuf))
				bind.buffer = unsafe.Pointer(cbuf)
				clen := int32(len(buf))
				p := C.malloc(C.size_t(unsafe.Sizeof(clen)))
				bind.length = (*C.int32_t)(p)
				*(bind.length) = C.int32_t(clen)
				needFreePointer = append(needFreePointer, p)
				bind.buffer_length = C.int32_t(clen)
			case taosTypes.TaosNchar:
				bind.buffer_type = C.TSDB_DATA_TYPE_NCHAR
				value := param.(taosTypes.TaosNchar)
				p := unsafe.Pointer(C.CString(string(value)))
				needFreePointer = append(needFreePointer, p)
				bind.buffer = unsafe.Pointer(p)
				clen := int32(len(value))
				bind.length = (*C.int32_t)(C.malloc(C.size_t(unsafe.Sizeof(clen))))
				*(bind.length) = C.int32_t(clen)
				needFreePointer = append(needFreePointer, unsafe.Pointer(bind.length))
				bind.buffer_length = C.int32_t(clen)
			case taosTypes.TaosTimestamp:
				bind.buffer_type = C.TSDB_DATA_TYPE_TIMESTAMP
				v := param.(taosTypes.TaosTimestamp)
				ts := common.TimeToTimestamp(v.T, v.Precision)
				p := C.malloc(8)
				needFreePointer = append(needFreePointer, p)
				*(*C.int64_t)(p) = C.int64_t(ts)
				bind.buffer = p
				bind.buffer_length = C.int32_t(8)
			default:
				return nil, nil, errors.New("unsupported type")
			}
		}
		binds[i] = bind
	}
	return binds, needFreePointer, nil
}

// TaosStmtAddBatch int        taos_stmt_add_batch(TAOS_STMT *stmt);
func TaosStmtAddBatch(stmt unsafe.Pointer) int {
	return int(C.taos_stmt_add_batch(stmt))
}

// TaosStmtExecute int        taos_stmt_execute(TAOS_STMT *stmt);
func TaosStmtExecute(stmt unsafe.Pointer) int {
	return int(C.taos_stmt_execute(stmt))
}

// TaosStmtUseResult TAOS_RES * taos_stmt_use_result(TAOS_STMT *stmt);
func TaosStmtUseResult(stmt unsafe.Pointer) unsafe.Pointer {
	return C.taos_stmt_use_result(stmt)
}

// TaosStmtClose int        taos_stmt_close(TAOS_STMT *stmt);
func TaosStmtClose(stmt unsafe.Pointer) int {
	return int(C.taos_stmt_close(stmt))
}

//TaosStmtSetSubTBName int        taos_stmt_set_sub_tbname(TAOS_STMT* stmt, const char* name);
func TaosStmtSetSubTBName(stmt unsafe.Pointer, name string) int {
	cStr := C.CString(name)
	defer C.free(unsafe.Pointer(cStr))
	return int(C.taos_stmt_set_tbname(stmt, cStr))
}

// TaosStmtBindParamBatch int        taos_stmt_bind_param_batch(TAOS_STMT* stmt, TAOS_BIND_v2* bind);
func TaosStmtBindParamBatch(stmt unsafe.Pointer, multiBind [][]interface{}, bindType []*taosTypes.ColumnType) int {
	var binds = make([]C.TAOS_BIND_v2, len(multiBind))
	var needFreePointer []unsafe.Pointer
	defer func() {
		for _, pointer := range needFreePointer {
			C.free(pointer)
		}
	}()
	for columnIndex, columnData := range multiBind {
		bind := C.TAOS_BIND_v2{}
		//malloc
		rowLen := len(multiBind[0])
		bind.num = C.int(rowLen)
		nullList := unsafe.Pointer(C.malloc(C.size_t(C.uint(rowLen))))
		needFreePointer = append(needFreePointer, nullList)
		lengthList := unsafe.Pointer(C.malloc(C.size_t(C.uint(rowLen * 4))))
		needFreePointer = append(needFreePointer, lengthList)
		var p unsafe.Pointer
		columnType := bindType[columnIndex]
		switch columnType.Type {
		case taosTypes.TaosBoolType:
			//1
			p = unsafe.Pointer(C.malloc(C.size_t(C.uint(rowLen))))
			bind.buffer_type = C.TSDB_DATA_TYPE_BOOL
			bind.buffer_length = C.int32_t(1)
			for i, rowData := range columnData {
				currentNull := unsafe.Pointer(uintptr(nullList) + uintptr(i))
				if rowData == nil {
					*(*C.int)(currentNull) = C.int(1)
				} else {
					*(*C.int)(currentNull) = C.int(0)
					value := rowData.(taosTypes.TaosBool)
					current := unsafe.Pointer(uintptr(p) + uintptr(i))
					if value {
						*(*C.int8_t)(current) = C.int8_t(1)
					} else {
						*(*C.int8_t)(current) = C.int8_t(0)
					}
				}
			}
		case taosTypes.TaosTinyintType:
			//1
			p = unsafe.Pointer(C.malloc(C.size_t(C.uint(rowLen))))
			bind.buffer_type = C.TSDB_DATA_TYPE_TINYINT
			bind.buffer_length = C.int32_t(1)
			for i, rowData := range columnData {
				currentNull := unsafe.Pointer(uintptr(nullList) + uintptr(i))
				if rowData == nil {
					*(*C.int)(currentNull) = C.int(1)
				} else {
					*(*C.int)(currentNull) = C.int(0)
					value := rowData.(taosTypes.TaosTinyint)
					current := unsafe.Pointer(uintptr(p) + uintptr(i))
					*(*C.int8_t)(current) = C.int8_t(value)
				}
			}
		case taosTypes.TaosSmallintType:
			//2
			p = unsafe.Pointer(C.malloc(C.size_t(C.uint(2 * rowLen))))
			bind.buffer_type = C.TSDB_DATA_TYPE_SMALLINT
			bind.buffer_length = C.int32_t(2)
			for i, rowData := range columnData {
				currentNull := unsafe.Pointer(uintptr(nullList) + uintptr(i))
				if rowData == nil {
					*(*C.int)(currentNull) = C.int(1)
				} else {
					*(*C.int)(currentNull) = C.int(0)
					value := rowData.(taosTypes.TaosSmallint)
					current := unsafe.Pointer(uintptr(p) + uintptr(2*i))
					*(*C.int16_t)(current) = C.int16_t(value)
				}
			}
		case taosTypes.TaosIntType:
			//4
			p = unsafe.Pointer(C.malloc(C.size_t(C.uint(4 * rowLen))))
			bind.buffer_type = C.TSDB_DATA_TYPE_INT
			bind.buffer_length = C.int32_t(4)
			for i, rowData := range columnData {
				currentNull := unsafe.Pointer(uintptr(nullList) + uintptr(i))
				if rowData == nil {
					*(*C.int)(currentNull) = C.int(1)
				} else {
					*(*C.int)(currentNull) = C.int(0)
					value := rowData.(taosTypes.TaosInt)
					current := unsafe.Pointer(uintptr(p) + uintptr(4*i))
					*(*C.int32_t)(current) = C.int32_t(value)
				}
			}
		case taosTypes.TaosBigintType:
			//8
			p = unsafe.Pointer(C.malloc(C.size_t(C.uint(8 * rowLen))))
			bind.buffer_type = C.TSDB_DATA_TYPE_BIGINT
			bind.buffer_length = C.int32_t(8)
			for i, rowData := range columnData {
				currentNull := unsafe.Pointer(uintptr(nullList) + uintptr(i))
				if rowData == nil {
					*(*C.int)(currentNull) = C.int(1)
				} else {
					*(*C.int)(currentNull) = C.int(0)
					value := rowData.(taosTypes.TaosBigint)
					current := unsafe.Pointer(uintptr(p) + uintptr(8*i))
					*(*C.int64_t)(current) = C.int64_t(value)
				}
			}
		case taosTypes.TaosUTinyintType:
			//1
			p = unsafe.Pointer(C.malloc(C.size_t(C.uint(rowLen))))
			bind.buffer_type = C.TSDB_DATA_TYPE_UTINYINT
			bind.buffer_length = C.int32_t(1)
			for i, rowData := range columnData {
				currentNull := unsafe.Pointer(uintptr(nullList) + uintptr(i))
				if rowData == nil {
					*(*C.int)(currentNull) = C.int(1)
				} else {
					*(*C.int)(currentNull) = C.int(0)
					value := rowData.(taosTypes.TaosUTinyint)
					current := unsafe.Pointer(uintptr(p) + uintptr(i))
					*(*C.uint8_t)(current) = C.uint8_t(value)
				}
			}
		case taosTypes.TaosUSmallintType:
			//2
			p = unsafe.Pointer(C.malloc(C.size_t(C.uint(2 * rowLen))))
			bind.buffer_type = C.TSDB_DATA_TYPE_USMALLINT
			bind.buffer_length = C.int32_t(2)
			for i, rowData := range columnData {
				currentNull := unsafe.Pointer(uintptr(nullList) + uintptr(i))
				if rowData == nil {
					*(*C.int)(currentNull) = C.int(1)
				} else {
					*(*C.int)(currentNull) = C.int(0)
					value := rowData.(taosTypes.TaosUSmallint)
					current := unsafe.Pointer(uintptr(p) + uintptr(2*i))
					*(*C.uint16_t)(current) = C.uint16_t(value)
				}
			}
		case taosTypes.TaosUIntType:
			//4
			p = unsafe.Pointer(C.malloc(C.size_t(C.uint(4 * rowLen))))
			bind.buffer_type = C.TSDB_DATA_TYPE_UINT
			bind.buffer_length = C.int32_t(4)
			for i, rowData := range columnData {
				currentNull := unsafe.Pointer(uintptr(nullList) + uintptr(i))
				if rowData == nil {
					*(*C.int)(currentNull) = C.int(1)
				} else {
					*(*C.int)(currentNull) = C.int(0)
					value := rowData.(taosTypes.TaosUInt)
					current := unsafe.Pointer(uintptr(p) + uintptr(4*i))
					*(*C.uint32_t)(current) = C.uint32_t(value)
				}
			}
		case taosTypes.TaosUBigintType:
			//8
			p = unsafe.Pointer(C.malloc(C.size_t(C.uint(8 * rowLen))))
			bind.buffer_type = C.TSDB_DATA_TYPE_UBIGINT
			bind.buffer_length = C.int32_t(8)
			for i, rowData := range columnData {
				currentNull := unsafe.Pointer(uintptr(nullList) + uintptr(i))
				if rowData == nil {
					*(*C.int)(currentNull) = C.int(1)
				} else {
					*(*C.int)(currentNull) = C.int(0)
					value := rowData.(taosTypes.TaosUBigint)
					current := unsafe.Pointer(uintptr(p) + uintptr(8*i))
					*(*C.uint64_t)(current) = C.uint64_t(value)
				}
			}
		case taosTypes.TaosFloatType:
			//4
			p = unsafe.Pointer(C.malloc(C.size_t(C.uint(4 * rowLen))))
			bind.buffer_type = C.TSDB_DATA_TYPE_FLOAT
			bind.buffer_length = C.int32_t(4)
			for i, rowData := range columnData {
				currentNull := unsafe.Pointer(uintptr(nullList) + uintptr(i))
				if rowData == nil {
					*(*C.int)(currentNull) = C.int(1)
				} else {
					*(*C.int)(currentNull) = C.int(0)
					value := rowData.(taosTypes.TaosFloat)
					current := unsafe.Pointer(uintptr(p) + uintptr(4*i))
					*(*C.float)(current) = C.float(value)
				}
			}
		case taosTypes.TaosDoubleType:
			//8
			p = unsafe.Pointer(C.malloc(C.size_t(C.uint(8 * rowLen))))
			bind.buffer_type = C.TSDB_DATA_TYPE_DOUBLE
			bind.buffer_length = C.int32_t(8)
			for i, rowData := range columnData {
				currentNull := unsafe.Pointer(uintptr(nullList) + uintptr(i))
				if rowData == nil {
					*(*C.int)(currentNull) = C.int(1)
				} else {
					*(*C.int)(currentNull) = C.int(0)
					value := rowData.(taosTypes.TaosDouble)
					current := unsafe.Pointer(uintptr(p) + uintptr(8*i))
					*(*C.double)(current) = C.double(value)
				}
			}
		case taosTypes.TaosBinaryType:
			p = unsafe.Pointer(C.malloc(C.size_t(C.uint(columnType.MaxLen * rowLen))))
			bind.buffer_type = C.TSDB_DATA_TYPE_BINARY
			bind.buffer_length = C.int32_t(columnType.MaxLen)
			for i, rowData := range columnData {
				currentNull := unsafe.Pointer(uintptr(nullList) + uintptr(i))
				if rowData == nil {
					*(*C.int)(currentNull) = C.int(1)
				} else {
					*(*C.int)(currentNull) = C.int(0)
					value := rowData.(taosTypes.TaosBinary)
					for j := 0; j < len(value); j++ {
						*(*C.char)(unsafe.Pointer(uintptr(p) + uintptr(columnType.MaxLen*i+j))) = (C.char)(value[j])
					}
					l := unsafe.Pointer(uintptr(lengthList) + uintptr(4*i))
					*(*C.int32_t)(l) = C.int32_t(len(value))
				}
			}
		case taosTypes.TaosNcharType:
			p = unsafe.Pointer(C.malloc(C.size_t(C.uint(columnType.MaxLen * rowLen))))
			bind.buffer_type = C.TSDB_DATA_TYPE_NCHAR
			bind.buffer_length = C.int32_t(columnType.MaxLen)
			for i, rowData := range columnData {
				currentNull := unsafe.Pointer(uintptr(nullList) + uintptr(i))
				if rowData == nil {
					*(*C.int)(currentNull) = C.int(1)
				} else {
					*(*C.int)(currentNull) = C.int(0)
					value := rowData.(taosTypes.TaosNchar)
					for j := 0; j < len(value); j++ {
						*(*C.char)(unsafe.Pointer(uintptr(p) + uintptr(columnType.MaxLen*i+j))) = (C.char)(value[j])
					}
					l := unsafe.Pointer(uintptr(lengthList) + uintptr(4*i))
					*(*C.int32_t)(l) = C.int32_t(len(value))
				}
			}
		case taosTypes.TaosTimestampType:
			//8
			p = unsafe.Pointer(C.malloc(C.size_t(C.uint(8 * rowLen))))
			bind.buffer_type = C.TSDB_DATA_TYPE_TIMESTAMP
			bind.buffer_length = C.int32_t(8)
			for i, rowData := range columnData {
				currentNull := unsafe.Pointer(uintptr(nullList) + uintptr(i))
				if rowData == nil {
					*(*C.int)(currentNull) = C.int(1)
				} else {
					*(*C.int)(currentNull) = C.int(0)
					value := rowData.(taosTypes.TaosTimestamp)
					ts := common.TimeToTimestamp(value.T, value.Precision)
					current := unsafe.Pointer(uintptr(p) + uintptr(8*i))
					*(*C.int64_t)(current) = C.int64_t(ts)
				}
			}
		}
		needFreePointer = append(needFreePointer, p)
		bind.buffer = p
		bind.length = (*C.int32_t)(lengthList)
		bind.is_null = (*C.char)(nullList)
		binds[columnIndex] = bind
	}
	return int(C.taos_stmt_bind_param_batch(stmt, (*C.TAOS_BIND_v2)(&binds[0])))
}

// TaosStmtErrStr char       *taos_stmt_errstr(TAOS_STMT *stmt);
func TaosStmtErrStr(stmt unsafe.Pointer) string {
	return C.GoString(C.taos_stmt_errstr(stmt))
}

// TaosStmtAffectedRows int         taos_stmt_affected_rows(TAOS_STMT *stmt);
func TaosStmtAffectedRows(stmt unsafe.Pointer) int {
	return int(C.taos_stmt_affected_rows(stmt))
}

// TaosStmtAffectedRowsOnce  int         taos_stmt_affected_rows_once(TAOS_STMT *stmt);
func TaosStmtAffectedRowsOnce(stmt unsafe.Pointer) int {
	//return int(C.taos_stmt_affected_rows_once(stmt))
	//todo
	return 1
}
