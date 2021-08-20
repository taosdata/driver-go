package wrapper

/*
#cgo CFLAGS: -IC:/TDengine/include -I/usr/include
#cgo linux LDFLAGS: -L/usr/lib -ltaos
#cgo windows LDFLAGS: -LC:/TDengine/driver -ltaos
#cgo darwin LDFLAGS: -L/usr/local/taos/driver -ltaos
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <taos.h>
*/
import "C"
import (
	"github.com/taosdata/driver-go/v2/errors"
	"unsafe"
)

// TaosFreeResult void taos_free_result(TAOS_RES *res);
func TaosFreeResult(res unsafe.Pointer) {
	C.taos_free_result(res)
}

// TaosConnect TAOS *taos_connect(const char *ip, const char *user, const char *pass, const char *db, uint16_t port);
func TaosConnect(host, user, pass, db string, port int) (taos unsafe.Pointer, err error) {
	cUser := C.CString(user)
	cPass := C.CString(pass)
	cdb := C.CString(db)
	defer C.free(unsafe.Pointer(cUser))
	defer C.free(unsafe.Pointer(cPass))
	defer C.free(unsafe.Pointer(cdb))
	var taosObj unsafe.Pointer
	if len(host) == 0 {
		taosObj = C.taos_connect(nil, cUser, cPass, cdb, (C.ushort)(0))
	} else {
		cHost := C.CString(host)
		defer C.free(unsafe.Pointer(cHost))
		taosObj = C.taos_connect(cHost, cUser, cPass, cdb, (C.ushort)(port))
	}

	if taosObj == nil {
		return nil, &errors.TaosError{
			Code:   errors.TSC_INVALID_CONNECTION,
			ErrStr: "invalid connection",
		}
	}

	return taosObj, nil
}

// TaosClose void  taos_close(TAOS *taos);
func TaosClose(taosConnect unsafe.Pointer) {
	C.taos_close(taosConnect)
}

// TaosQuery TAOS_RES *taos_query(TAOS *taos, const char *sql);
func TaosQuery(taosConnect unsafe.Pointer, sql string) unsafe.Pointer {
	cSql := C.CString(sql)
	defer C.free(unsafe.Pointer(cSql))
	return unsafe.Pointer(C.taos_query(taosConnect, cSql))
}

// TaosError int taos_errno(TAOS_RES *tres);
func TaosError(result unsafe.Pointer) int {
	return int(C.taos_errno(result))
}

// TaosErrorStr char *taos_errstr(TAOS_RES *tres);
func TaosErrorStr(result unsafe.Pointer) string {
	return C.GoString(C.taos_errstr(result))
}

// TaosFieldCount int taos_field_count(TAOS_RES *res);
func TaosFieldCount(result unsafe.Pointer) int {
	return int(C.taos_field_count(result))
}

// TaosAffectedRows int taos_affected_rows(TAOS_RES *res);
func TaosAffectedRows(result unsafe.Pointer) int {
	return int(C.taos_affected_rows(result))
}

// TaosFetchFields TAOS_FIELD *taos_fetch_fields(TAOS_RES *res);
func TaosFetchFields(result unsafe.Pointer) unsafe.Pointer {
	return unsafe.Pointer(C.taos_fetch_fields(result))
}

// TaosFetchBlock int taos_fetch_block(TAOS_RES *res, TAOS_ROW *rows);
func TaosFetchBlock(result unsafe.Pointer) (int, unsafe.Pointer) {
	var block C.TAOS_ROW
	b := unsafe.Pointer(&block)
	blockSize := int(C.taos_fetch_block(result, (*C.TAOS_ROW)(b)))
	return blockSize, b
}

// TaosResultPrecision int taos_result_precision(TAOS_RES *res);
func TaosResultPrecision(result unsafe.Pointer) int {
	return int(C.taos_result_precision(result))
}

// TaosNumFields int taos_num_fields(TAOS_RES *res);
func TaosNumFields(result unsafe.Pointer) int {
	return int(C.taos_num_fields(result))
}

// TaosFetchRow TAOS_ROW taos_fetch_row(TAOS_RES *res);
func TaosFetchRow(result unsafe.Pointer) unsafe.Pointer {
	return unsafe.Pointer(C.taos_fetch_row(result))
}
