package wrapper

/*
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <taos.h>
*/
import "C"
import (
	"encoding/json"
	"unsafe"
)

// TaosSetConfig int   taos_set_config(const char *config);
func TaosSetConfig(params map[string]string) int {
	config, _ := json.Marshal(params)
	cConfig := C.CString(string(config))
	defer C.free(unsafe.Pointer(cConfig))
	return int(C.taos_set_config(cConfig))
}
