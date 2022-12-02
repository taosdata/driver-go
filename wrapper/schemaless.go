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
	return taosSchemalessInsert(taosConnect, lines, protocol, precision, nil, nil)
}

// TaosSchemalessInsertTTL TAOS_RES *taos_schemaless_insert_ttl(TAOS *taos, char *lines[], int numLines, int protocol, int precision, int32_t ttl)
// Deprecated
func TaosSchemalessInsertTTL(taosConnect unsafe.Pointer, lines []string, protocol int, precision string, ttl int) unsafe.Pointer {
	return taosSchemalessInsert(taosConnect, lines, protocol, precision, &ttl, nil)
}

// TaosSchemalessInsertWithReqID TAOS_RES *taos_schemaless_insert_with_reqid(TAOS *taos, char *lines[], int numLines, int protocol, int precision, int64_t reqid);
// Deprecated
func TaosSchemalessInsertWithReqID(taosConn unsafe.Pointer, lines []string, protocol int, precision string, reqID int64) unsafe.Pointer {
	return taosSchemalessInsert(taosConn, lines, protocol, precision, nil, &reqID)
}

// TaosSchemalessInsertTTLWithReqID TAOS_RES *taos_schemaless_insert_ttl_with_reqid(TAOS *taos, char *lines[], int numLines, int protocol, int precision, int32_t ttl, int64_t reqid)
// Deprecated
func TaosSchemalessInsertTTLWithReqID(taosConn unsafe.Pointer, lines []string, protocol int, precision string, ttl int, reqID int64) unsafe.Pointer {
	return taosSchemalessInsert(taosConn, lines, protocol, precision, &ttl, &reqID)
}

func taosSchemalessInsert(conn unsafe.Pointer, lines []string, protocol int, precision string, ttl *int, reqID *int64) (result unsafe.Pointer) {
	numLines, cLines, needFree := taosSchemalessInsertParams(lines)
	defer func() {
		for _, p := range needFree {
			C.free(p)
		}
	}()

	if ttl == nil && reqID == nil {
		// TAOS_RES *taos_schemaless_insert(TAOS* taos, char* lines[], int numLines, int protocol, int precision)
		result = C.taos_schemaless_insert(conn, (**C.char)(&cLines[0]), (C.int)(numLines), (C.int)(protocol),
			(C.int)(exchange(precision)))
		return
	}
	if ttl == nil && reqID != nil {
		// TAOS_RES *taos_schemaless_insert_with_reqid(TAOS *taos, char *lines[], int numLines, int protocol, int precision, int64_t reqid)
		result = C.taos_schemaless_insert_with_reqid(conn, (**C.char)(&cLines[0]), (C.int)(numLines), (C.int)(protocol),
			(C.int)(exchange(precision)), (C.int64_t)(*reqID))
		return
	}
	if ttl != nil && reqID == nil {
		// TAOS_RES *taos_schemaless_insert_ttl(TAOS *taos, char *lines[], int numLines, int protocol, int precision, int32_t ttl)
		result = C.taos_schemaless_insert_ttl(conn, (**C.char)(&cLines[0]), (C.int)(numLines), (C.int)(protocol),
			(C.int)(exchange(precision)), (C.int32_t)(*ttl))
		return
	}

	// TAOS_RES *taos_schemaless_insert_ttl_with_reqid(TAOS *taos, char *lines[], int numLines, int protocol, int precision, int32_t ttl, int64_t reqid)
	result = C.taos_schemaless_insert_ttl_with_reqid(conn, (**C.char)(&cLines[0]), (C.int)(numLines), (C.int)(protocol),
		(C.int)(exchange(precision)), (C.int32_t)(*ttl), (C.int64_t)(*reqID))
	return
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

// TaosSchemalessInsertRaw TAOS_RES *taos_schemaless_insert_raw(TAOS* taos, char* lines, int len, int32_t *totalRows, int protocol, int precision);
func TaosSchemalessInsertRaw(taosConnect unsafe.Pointer, lines string, protocol int, precision string) (int32, unsafe.Pointer) {
	return taosSchemalessInsertRaw(taosConnect, lines, protocol, precision, nil, nil)
}

// TaosSchemalessInsertRawTTL TAOS_RES *taos_schemaless_insert_raw_ttl(TAOS *taos, char *lines, int len, int32_t *totalRows, int protocol, int precision, int32_t ttl);
func TaosSchemalessInsertRawTTL(taosConnect unsafe.Pointer, lines string, protocol int, precision string, ttl int) (int32, unsafe.Pointer) {
	return taosSchemalessInsertRaw(taosConnect, lines, protocol, precision, &ttl, nil)
}

// TaosSchemalessInsertRawWithReqID TAOS_RES *taos_schemaless_insert_raw_with_reqid(TAOS *taos, char *lines, int len, int32_t *totalRows, int protocol, int precision, int64_t reqid);
func TaosSchemalessInsertRawWithReqID(taosConn unsafe.Pointer, lines string, protocol int, precision string, reqID int64) (int32, unsafe.Pointer) {
	return taosSchemalessInsertRaw(taosConn, lines, protocol, precision, nil, &reqID)
}

// TaosSchemalessInsertRawTTLWithReqID TAOS_RES *taos_schemaless_insert_raw_ttl_with_reqid(TAOS *taos, char *lines, int len, int32_t *totalRows, int protocol, int precision, int32_t ttl, int64_t reqid)
func TaosSchemalessInsertRawTTLWithReqID(taosConn unsafe.Pointer, lines string, protocol int, precision string, ttl int, reqID int64) (int32, unsafe.Pointer) {
	return taosSchemalessInsertRaw(taosConn, lines, protocol, precision, &ttl, &reqID)
}

func taosSchemalessInsertRaw(conn unsafe.Pointer, lines string, protocol int, precision string, ttl *int, reqID *int64) (rows int32, result unsafe.Pointer) {
	cLine := C.CString(lines)
	defer C.free(unsafe.Pointer(cLine))
	pTotalRows := unsafe.Pointer(&rows)

	if ttl == nil && reqID == nil {
		// TAOS_RES *taos_schemaless_insert_raw(TAOS* taos, char* lines, int len, int32_t *totalRows, int protocol, int precision)
		result = C.taos_schemaless_insert_raw(conn, cLine, (C.int)(len(lines)), (*C.int32_t)(pTotalRows),
			(C.int)(protocol), (C.int)(exchange(precision)))
		return
	}
	if ttl == nil && reqID != nil {
		// TAOS_RES *taos_schemaless_insert_raw_with_reqid(TAOS *taos, char *lines, int len, int32_t *totalRows, int protocol, int precision, int64_t reqid)
		result = C.taos_schemaless_insert_raw_with_reqid(conn, cLine, (C.int)(len(lines)), (*C.int32_t)(pTotalRows),
			(C.int)(protocol), (C.int)(exchange(precision)), (C.int64_t)(*reqID))
		return
	}
	if ttl != nil && reqID == nil {
		// TAOS_RES *taos_schemaless_insert_raw_ttl(TAOS *taos, char *lines, int len, int32_t *totalRows, int protocol, int precision, int32_t ttl)
		result = C.taos_schemaless_insert_raw_ttl(conn, cLine, (C.int)(len(lines)), (*C.int32_t)(pTotalRows),
			(C.int)(protocol), (C.int)(exchange(precision)), (C.int32_t)(*ttl))
		return
	}
	// TAOS_RES *taos_schemaless_insert_raw_ttl_with_reqid(TAOS *taos, char *lines, int len, int32_t *totalRows, int protocol, int precision, int32_t ttl, int64_t reqid)
	result = C.taos_schemaless_insert_raw_ttl_with_reqid(conn, cLine, (C.int)(len(lines)), (*C.int32_t)(pTotalRows),
		(C.int)(protocol), (C.int)(exchange(precision)), (C.int32_t)(*ttl), (C.int64_t)(*reqID))

	return
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
