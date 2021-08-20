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
	"github.com/taosdata/driver-go/v2/common"
	taosTypes "github.com/taosdata/driver-go/v2/types"
	"unsafe"
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

// TaosStmtSetTBNameTags int        taos_stmt_set_tbname_tags(TAOS_STMT* stmt, const char* name, TAOS_BIND* tags);
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
	result := int(C.taos_stmt_set_tbname_tags(stmt, cStr, (*C.TAOS_BIND)(&binds[0])))
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

// TaosStmtBindParam int        taos_stmt_bind_param(TAOS_STMT *stmt, TAOS_BIND *bind);
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
	result := int(C.taos_stmt_bind_param(stmt, (*C.TAOS_BIND)(unsafe.Pointer(&binds[0]))))
	return result
}

func generateTaosBindList(params []interface{}) ([]C.TAOS_BIND, []unsafe.Pointer, error) {
	binds := make([]C.TAOS_BIND, len(params))
	var needFreePointer []unsafe.Pointer
	for i, param := range params {
		bind := C.TAOS_BIND{}
		if param == nil {
			bind.buffer_type = C.TSDB_DATA_TYPE_NULL
			p := C.malloc(1)
			*(*C.int)(p) = C.int(1)
			needFreePointer = append(needFreePointer, p)
			bind.is_null = (*C.int)(p)
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
			case taosTypes.TaosTinyint:
				bind.buffer_type = C.TSDB_DATA_TYPE_TINYINT
				value := param.(taosTypes.TaosTinyint)
				p := C.malloc(1)
				*(*C.int8_t)(p) = C.int8_t(value)
				needFreePointer = append(needFreePointer, p)
				bind.buffer = p
			case taosTypes.TaosSmallint:
				bind.buffer_type = C.TSDB_DATA_TYPE_SMALLINT
				value := param.(taosTypes.TaosSmallint)
				p := C.malloc(2)
				*(*C.int16_t)(p) = C.int16_t(value)
				needFreePointer = append(needFreePointer, p)
				bind.buffer = p
			case taosTypes.TaosInt:
				value := param.(taosTypes.TaosInt)
				bind.buffer_type = C.TSDB_DATA_TYPE_INT
				p := C.malloc(4)
				*(*C.int32_t)(p) = C.int32_t(value)
				needFreePointer = append(needFreePointer, p)
				bind.buffer = p
			case taosTypes.TaosBigint:
				bind.buffer_type = C.TSDB_DATA_TYPE_BIGINT
				value := param.(taosTypes.TaosBigint)
				p := C.malloc(8)
				*(*C.int64_t)(p) = C.int64_t(value)
				needFreePointer = append(needFreePointer, p)
				bind.buffer = p
			case taosTypes.TaosUTinyint:
				bind.buffer_type = C.TSDB_DATA_TYPE_UTINYINT
				buf := param.(taosTypes.TaosUTinyint)
				cbuf := C.malloc(1)
				*(*C.char)(cbuf) = C.char(buf)
				needFreePointer = append(needFreePointer, cbuf)
				bind.buffer = cbuf
			case taosTypes.TaosUSmallint:
				bind.buffer_type = C.TSDB_DATA_TYPE_USMALLINT
				value := param.(taosTypes.TaosUSmallint)
				p := C.malloc(2)
				*(*C.int16_t)(p) = C.int16_t(value)
				needFreePointer = append(needFreePointer, p)
				bind.buffer = p
			case taosTypes.TaosUInt:
				bind.buffer_type = C.TSDB_DATA_TYPE_UINT
				value := param.(taosTypes.TaosUInt)
				p := C.malloc(4)
				*(*C.uint32_t)(p) = C.uint32_t(value)
				needFreePointer = append(needFreePointer, p)
				bind.buffer = p
			case taosTypes.TaosUBigint:
				bind.buffer_type = C.TSDB_DATA_TYPE_UBIGINT
				value := param.(taosTypes.TaosUBigint)
				p := C.malloc(8)
				*(*C.uint64_t)(p) = C.uint64_t(value)
				needFreePointer = append(needFreePointer, p)
				bind.buffer = p
			case taosTypes.TaosFloat:
				bind.buffer_type = C.TSDB_DATA_TYPE_FLOAT
				value := param.(taosTypes.TaosFloat)
				p := C.malloc(4)
				*(*C.float)(p) = C.float(value)
				needFreePointer = append(needFreePointer, p)
				bind.buffer = p
			case taosTypes.TaosDouble:
				bind.buffer_type = C.TSDB_DATA_TYPE_DOUBLE
				value := param.(taosTypes.TaosDouble)
				p := C.malloc(8)
				*(*C.double)(p) = C.double(value)
				needFreePointer = append(needFreePointer, p)
				bind.buffer = p
			case taosTypes.TaosBinary:
				bind.buffer_type = C.TSDB_DATA_TYPE_BINARY
				buf := param.(taosTypes.TaosBinary)
				cbuf := C.CString(string(buf))
				needFreePointer = append(needFreePointer, unsafe.Pointer(cbuf))
				bind.buffer = unsafe.Pointer(cbuf)
				clen := int32(len(buf))
				p := C.malloc(C.size_t(unsafe.Sizeof(clen)))
				bind.length = (*C.uintptr_t)(p)
				*(bind.length) = C.uintptr_t(clen)
				needFreePointer = append(needFreePointer, p)
			case taosTypes.TaosNchar:
				bind.buffer_type = C.TSDB_DATA_TYPE_NCHAR
				value := param.(taosTypes.TaosNchar)
				p := unsafe.Pointer(C.CString(string(value)))
				needFreePointer = append(needFreePointer, p)
				bind.buffer = unsafe.Pointer(p)
				clen := int32(len(value))
				bind.length = (*C.uintptr_t)(C.malloc(C.size_t(unsafe.Sizeof(clen))))
				*(bind.length) = C.uintptr_t(clen)
				needFreePointer = append(needFreePointer, unsafe.Pointer(bind.length))
			case taosTypes.TaosTimestamp:
				bind.buffer_type = C.TSDB_DATA_TYPE_TIMESTAMP
				v := param.(taosTypes.TaosTimestamp)
				ts := common.TimeToTimestamp(v.T, v.Precision)
				p := C.malloc(8)
				needFreePointer = append(needFreePointer, p)
				*(*C.int64_t)(p) = C.int64_t(ts)
				bind.buffer = p
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
