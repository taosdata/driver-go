package wrapper

/*
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <taos.h>
*/
import "C"
import "unsafe"

// TaosInsertLines DLL_EXPORT int taos_insert_lines(TAOS* taos, char* lines[], int numLines);
func TaosInsertLines(taosConnect unsafe.Pointer, lines []string) int {
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
	return int(C.taos_insert_lines(taosConnect, (**C.char)(&cLines[0]), (C.int)(numLines)))
}

// TaosInsertTelnetLines DLL_EXPORT int taos_insert_telnet_lines(TAOS* taos, char* lines[], int numLines);
func TaosInsertTelnetLines(taosConnect unsafe.Pointer, lines []string) int {
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
	return int(C.taos_insert_telnet_lines(taosConnect, (**C.char)(&cLines[0]), (C.int)(numLines)))
}

// TaosInsertJsonPayload DLL_EXPORT int taos_insert_json_payload(TAOS* taos, char* payload);
func TaosInsertJsonPayload(taosConnect unsafe.Pointer, payload string) int {
	cPayload := C.CString(payload)
	defer C.free(unsafe.Pointer(cPayload))
	return int(C.taos_insert_json_payload(taosConnect, cPayload))
}
