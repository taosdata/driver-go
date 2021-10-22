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
func TaosSchemalessInsert(taosConnect unsafe.Pointer, lines []string, protocol int, precision string) unsafe.Pointer {
	numLines := len(lines)
	var cLines = make([]*C.char, numLines)
	needFreeList := make([]unsafe.Pointer, numLines)
	defer func() {
		for _, p := range needFreeList {
			C.free(p)
		}
	}()
	for i, line := range lines {
		cLine := C.CString(line)
		needFreeList[i] = unsafe.Pointer(cLine)
		cLines[i] = cLine
	}
	if len(precision) == 0 {
		return C.taos_schemaless_insert(taosConnect, (**C.char)(&cLines[0]), (C.int)(numLines), (C.int)(protocol), (C.int)(TSDB_SML_TIMESTAMP_NOT_CONFIGURED))
	} else {
		return C.taos_schemaless_insert(taosConnect, (**C.char)(&cLines[0]), (C.int)(numLines), (C.int)(protocol), (C.int)(exchange(precision)))
	}
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
