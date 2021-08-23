package wrapper

import "C"
import "encoding/json"

// TaosSetConfig int   taos_set_config(const char *config);
func TaosSetConfig(params map[string]string) int {
	config, _ := json.Marshal(params)
	cConfig := C.CString(string(config))
	defer C.free(cConfig)
	return int(C.taos_set_config(cConfig))
}
