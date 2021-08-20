package wrapper

/*
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <taos.h>
*/
import "C"
import (
	"time"
	"unsafe"
)

// TaosSubscribe TAOS_SUB *taos_subscribe(TAOS* taos, int restart, const char* topic, const char *sql, TAOS_SUBSCRIBE_CALLBACK fp, void *param, int interval);
func TaosSubscribe(taosConnect unsafe.Pointer, topic string, sql string, restart bool, interval time.Duration) unsafe.Pointer {
	cTopic := C.CString(topic)
	cSql := C.CString(sql)
	defer C.free(unsafe.Pointer(cTopic))
	defer C.free(unsafe.Pointer(cSql))
	flag := 0
	if restart {
		flag = 1
	}
	return unsafe.Pointer(C.taos_subscribe(taosConnect, (C.int)(flag), cTopic, cSql, nil, nil, (C.int)(int(interval/time.Millisecond))))
}

// TaosUnsubscribe void      taos_unsubscribe(TAOS_SUB *tsub, int keepProgress);
func TaosUnsubscribe(sub unsafe.Pointer, keepProgress bool) {
	keep := 0
	if keepProgress {
		keep = 1
	}
	C.taos_unsubscribe(sub, (C.int)(keep))
}

// TaosConsume TAOS_RES *taos_consume(TAOS_SUB *tsub);
func TaosConsume(sub unsafe.Pointer) unsafe.Pointer {
	return unsafe.Pointer(C.taos_consume(sub))
}
