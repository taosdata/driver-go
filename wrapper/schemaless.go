package wrapper

/*
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <taos.h>
*/
import "C"
import "unsafe"

const (
	InfluxDBLineProtocol       = 1
	OpenTSDBTelnetLineProtocol = 2
	OpenTSDBJsonFormatProtocol = 3
)
const (
	TSDB_SML_TIMESTAMP_NOT_CONFIGURED = iota
	TSDB_SML_TIMESTAMP_HOURS
	TSDB_SML_TIMESTAMP_MINUTES
	TSDB_SML_TIMESTAMP_SECONDS
	TSDB_SML_TIMESTAMP_MILLI_SECONDS
	TSDB_SML_TIMESTAMP_MICRO_SECONDS
	TSDB_SML_TIMESTAMP_NANO_SECONDS
)

// TaosSchemalessInsert TAOS_RES *taos_schemaless_insert(TAOS* taos, char* lines[], int numLines, int protocol, int precision);
// Deprecated
func TaosSchemalessInsert(taosConnect unsafe.Pointer, lines []string, protocol int, precision string) unsafe.Pointer {
	numLines, cLines, needFree := taosSchemalessInsertParams(lines)
	defer func() {
		for _, p := range needFree {
			C.free(p)
		}
	}()
	return C.taos_schemaless_insert(taosConnect, (**C.char)(&cLines[0]), (C.int)(numLines), (C.int)(protocol),
		(C.int)(exchange(precision)))
}

// TaosSchemalessInsertWithReqId TAOS_RES *taos_schemaless_insert_with_reqid(TAOS *taos, char *lines[], int numLines, int protocol, int precision, int64_t reqid);
// Deprecated
func TaosSchemalessInsertWithReqId(taosConn unsafe.Pointer, lines []string, protocol int, precision string, reqId int64) unsafe.Pointer {
	numLines, cLines, needFree := taosSchemalessInsertParams(lines)
	defer func() {
		for _, p := range needFree {
			C.free(p)
		}
	}()

	return C.taos_schemaless_insert_with_reqid(taosConn, (**C.char)(&cLines[0]), (C.int)(numLines),
		(C.int)(protocol), (C.int)(exchange(precision)), (C.int64_t)(reqId))
}

func taosSchemalessInsertParams(lines []string) (numLines int, cLines []*C.char, needFree []unsafe.Pointer) {
	numLines = len(lines)
	cLines = make([]*C.char, numLines)
	needFree = make([]unsafe.Pointer, numLines)
	for i, line := range lines {
		cLine := C.CString(line)
		needFree[i] = unsafe.Pointer(cLine)
		cLines[i] = cLine
	}
	return
}

// TaosSchemalessInsertRaw DLL_EXPORT TAOS_RES *taos_schemaless_insert_raw(TAOS* taos, char* lines, int len, int32_t *totalRows, int protocol, int precision);
func TaosSchemalessInsertRaw(taosConnect unsafe.Pointer, lines string, protocol int, precision string) (int32, unsafe.Pointer) {
	cLine := C.CString(lines)
	defer C.free(unsafe.Pointer(cLine))
	var totalRows int32
	pTotalRows := unsafe.Pointer(&totalRows)

	res := C.taos_schemaless_insert_raw(taosConnect, cLine, (C.int)(len(lines)), (*C.int32_t)(pTotalRows), (C.int)(protocol), (C.int)(exchange(precision)))
	return totalRows, res
}

// TaosSchemalessInsertRawWithReqId TAOS_RES *taos_schemaless_insert_raw_with_reqid(TAOS *taos, char *lines, int len, int32_t *totalRows, int protocol, int precision, int64_t reqid);
func TaosSchemalessInsertRawWithReqId(taosConn unsafe.Pointer, lines string, protocol int, precision string, reqId int64) (int32, unsafe.Pointer) {
	cLine := C.CString(lines)
	defer C.free(unsafe.Pointer(cLine))
	var totalRows int32
	pTotalRows := unsafe.Pointer(&totalRows)

	res := C.taos_schemaless_insert_raw_with_reqid(taosConn, cLine, (C.int)(len(lines)), (*C.int32_t)(pTotalRows),
		(C.int)(protocol), (C.int)(exchange(precision)), (C.int64_t)(reqId))
	return totalRows, res
}

func exchange(ts string) int {
	switch ts {
	case "":
		return TSDB_SML_TIMESTAMP_NOT_CONFIGURED
	case "h":
		return TSDB_SML_TIMESTAMP_HOURS
	case "m":
		return TSDB_SML_TIMESTAMP_MINUTES
	case "s":
		return TSDB_SML_TIMESTAMP_SECONDS
	case "ms":
		return TSDB_SML_TIMESTAMP_MILLI_SECONDS
	case "u", "μ":
		return TSDB_SML_TIMESTAMP_MICRO_SECONDS
	case "ns":
		return TSDB_SML_TIMESTAMP_NANO_SECONDS
	}
	return TSDB_SML_TIMESTAMP_NOT_CONFIGURED
}
