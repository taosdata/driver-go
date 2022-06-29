package wrapper

/*
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <taos.h>
*/
import "C"

// TaosSetConfig int   taos_set_config(const char *config);
func TaosSetConfig(params map[string]string) error {
	return nil
	// danger!! taos_set_config must set params. if set nil or empty map will get error
	//if len(params) == 0 {
	//	return nil
	//}
	//config, _ := json.Marshal(params)
	//cConfig := C.CString(string(config))
	//defer C.free(unsafe.Pointer(cConfig))
	//result := (C.struct_setConfRet)(C.taos_set_config(cConfig))
	//if int(result.retCode) == -5 || int(result.retCode) == 0 {
	//	return nil
	//}
	//buf := &strings.Builder{}
	//for _, c := range result.retMsg {
	//	if c == 0 {
	//		break
	//	}
	//	buf.WriteByte(byte(c))
	//}
	//return &errors.TaosError{Code: int32(result.retCode) & 0xffff, ErrStr: buf.String()}
}
