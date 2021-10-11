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
	InfluxDBLineProtocol       = 0
	OpenTSDBTelnetLineProtocol = 1
	OpenTSDBJsonFormatProtocol = 2
)

// TaosSchemalessInsert DLL_EXPORT int taos_schemaless_insert(TAOS* taos, char* lines[], int numLines, int protocol, char* precision);
func TaosSchemalessInsert(taosConnect unsafe.Pointer, lines []string, protocol int, precision string) int {
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
		return int(C.taos_schemaless_insert(taosConnect, (**C.char)(&cLines[0]), (C.int)(numLines), (C.int)(protocol), nil))
	} else {
		cPrecision := C.CString(precision)
		defer C.free(unsafe.Pointer(cPrecision))
		return int(C.taos_schemaless_insert(taosConnect, (**C.char)(&cLines[0]), (C.int)(numLines), (C.int)(protocol), cPrecision))
	}
}
