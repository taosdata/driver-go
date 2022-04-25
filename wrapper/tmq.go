package wrapper

/*
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <taos.h>
extern void TMQCommitCB(tmq_t *, tmq_resp_err_t, tmq_topic_vgroup_list_t *, void *param);
*/
import "C"
import (
	"unsafe"

	"github.com/taosdata/driver-go/v2/errors"
)

//DLL_EXPORT tmq_conf_t    *tmq_conf_new();
func TMQConfNew() unsafe.Pointer {
	return unsafe.Pointer(C.tmq_conf_new())
}

//DLL_EXPORT tmq_conf_res_t tmq_conf_set(tmq_conf_t *conf, const char *key, const char *value);
func TMQConfSet(conf unsafe.Pointer, key string, value string) int32 {
	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))
	cValue := C.CString(value)
	defer C.free(unsafe.Pointer(cValue))
	return int32(C.tmq_conf_set((*C.struct_tmq_conf_t)(conf), cKey, cValue))
}

//DLL_EXPORT void           tmq_conf_destroy(tmq_conf_t *conf);
func TMQConfDestroy(conf unsafe.Pointer) {
	C.tmq_conf_destroy((*C.struct_tmq_conf_t)(conf))
}

//typedef void(tmq_commit_cb(tmq_t *, tmq_resp_err_t, tmq_topic_vgroup_list_t *, void *param));
//DLL_EXPORT void           tmq_conf_set_offset_commit_cb(tmq_conf_t *conf, tmq_commit_cb *cb);
func TMQConfSetOffsetCommitCB(conf unsafe.Pointer) {
	//todo
	C.tmq_conf_set_offset_commit_cb((*C.struct_tmq_conf_t)(conf), (*C.tmq_commit_cb)(C.TMQCommitCB))
}

//DLL_EXPORT tmq_list_t *tmq_list_new();
func TMQListNew() unsafe.Pointer {
	return unsafe.Pointer(C.tmq_list_new())
}

//DLL_EXPORT int32_t     tmq_list_append(tmq_list_t *, const char *);
func TMQListAppend(list unsafe.Pointer, str string) int32 {
	cStr := C.CString(str)
	defer C.free(unsafe.Pointer(cStr))
	return int32(C.tmq_list_append((*C.tmq_list_t)(list), cStr))
}

//DLL_EXPORT void        tmq_list_destroy(tmq_list_t *);
func TMQListDestroy(list unsafe.Pointer) {
	C.tmq_list_destroy((*C.tmq_list_t)(list))
}

// will replace last one
//DLL_EXPORT tmq_t *tmq_consumer_new1(tmq_conf_t *conf, char *errstr, int32_t errstrLen);
func TMQConsumerNew(conf unsafe.Pointer) (unsafe.Pointer, error) {
	p := (*C.char)(C.calloc(C.size_t(C.uint(1024)), C.size_t(C.uint(1024))))
	defer C.free(unsafe.Pointer(p))
	tmq := unsafe.Pointer(C.tmq_consumer_new((*C.struct_tmq_conf_t)(conf), p, C.int32_t(1024)))
	errStr := C.GoString(p)
	if len(errStr) > 0 {
		return tmq, errors.NewError(-1, errStr)
	}
	if tmq == nil {
		panic("new consumer return nil")
	}
	return tmq, nil
}

//DLL_EXPORT const char *tmq_err2str(tmq_resp_err_t);
func TMQErr2Str(code int32) string {
	return C.GoString(C.tmq_err2str((C.tmq_resp_err_t)(code)))
}

//DLL_EXPORT tmq_resp_err_t tmq_subscribe(tmq_t *tmq, tmq_list_t *topic_list);
func TMQSubscribe(consumer unsafe.Pointer, topicList unsafe.Pointer) int32 {
	return int32(C.tmq_subscribe((*C.tmq_t)(consumer), (*C.tmq_list_t)(topicList)))
}

//DLL_EXPORT tmq_resp_err_t tmq_unsubscribe(tmq_t *tmq);
func TMQUnsubscribe(consumer unsafe.Pointer) int32 {
	return int32(C.tmq_unsubscribe((*C.tmq_t)(consumer)))
}

//DLL_EXPORT tmq_resp_err_t tmq_subscription(tmq_t *tmq, tmq_list_t **topics);
func TMQSubscription(consumer unsafe.Pointer) int32 {
	//todo
	return 0
}

//DLL_EXPORT TAOS_RES *tmq_consumer_poll(tmq_t *tmq, int64_t blocking_time);
func TMQConsumerPoll(consumer unsafe.Pointer, blockingTime int64) unsafe.Pointer {
	return unsafe.Pointer(C.tmq_consumer_poll((*C.tmq_t)(consumer), (C.int64_t)(blockingTime)))
}

//DLL_EXPORT tmq_resp_err_t tmq_consumer_close(tmq_t *tmq);
func TMQConsumerClose(consumer unsafe.Pointer) int32 {
	return int32(C.tmq_consumer_close((*C.tmq_t)(consumer)))
}

//DLL_EXPORT tmq_resp_err_t tmq_commit(tmq_t *tmq, const tmq_topic_vgroup_list_t *offsets, int32_t async);
func TMQCommit(consumer unsafe.Pointer, offsets unsafe.Pointer, async bool) int32 {
	if async {
		return int32(C.tmq_commit((*C.tmq_t)(consumer), (*C.tmq_topic_vgroup_list_t)(offsets), (C.int32_t)(1)))
	} else {
		return int32(C.tmq_commit((*C.tmq_t)(consumer), (*C.tmq_topic_vgroup_list_t)(offsets), (C.int32_t)(0)))
	}
}

////DLL_EXPORT tmq_resp_err_t tmq_seek(tmq_t *tmq, const tmq_topic_vgroup_t *offset);
//func TMQSeek(tmq unsafe.Pointer, offset unsafe.Pointer) int32 {
//	return int32(C.tmq_seek((*C.tmq_t)(tmq), (*C.tmq_topic_vgroup_t)(offset)))
//}

//
////DLL_EXPORT TAOS_ROW    tmq_get_row(tmq_message_t *message);
//func TMQGetRow(message unsafe.Pointer) unsafe.Pointer {
//	return unsafe.Pointer(C.tmq_get_row(message))
//}
//
////DLL_EXPORT char       *tmq_get_topic_name(tmq_message_t *message);
//func TMQGetTopicName(message unsafe.Pointer) string {
//	return C.GoString(C.tmq_get_topic_name(message))
//}
//
////DLL_EXPORT int32_t     tmq_get_vgroup_id(tmq_message_t *message);
//func TMQGetVgroupID(message unsafe.Pointer) int32 {
//	return int32(C.tmq_get_vgroup_id(message))
//}
//
////DLL_EXPORT int64_t     tmq_get_request_offset(tmq_message_t *message);
//func TMQGetRequestOffset(message unsafe.Pointer) int64 {
//	return int64(C.tmq_get_request_offset(message))
//}
//
////DLL_EXPORT int64_t     tmq_get_response_offset(tmq_message_t *message);
//func TMQGetResponseOffset(message unsafe.Pointer) int64 {
//	return int64(C.tmq_get_response_offset(message))
//}
//
////DLL_EXPORT TAOS_FIELD *tmq_get_fields(tmq_t *tmq, const char *topic);
//func TMQGetFields(tmq unsafe.Pointer, topic string) unsafe.Pointer {
//	cTopic := C.CString(topic)
//	defer C.free(unsafe.Pointer(cTopic))
//	return unsafe.Pointer(C.tmq_get_fields(tmq, cTopic))
//}
//
////DLL_EXPORT int32_t     tmq_field_count(tmq_t *tmq, const char *topic);
//func TMQFieldCount(tmq unsafe.Pointer, topic string) int32 {
//	cTopic := C.CString(topic)
//	defer C.free(unsafe.Pointer(cTopic))
//	return int32(C.tmq_field_count(tmq, cTopic))
//}

////DLL_EXPORT void        tmq_message_destroy(tmq_message_t *tmq_message);
//func TMQMessageDestroy(message unsafe.Pointer) {
//	C.tmq_message_destroy((*C.tmq_message_t)(message))
//}

//DLL_EXPORT TAOS_RES *tmq_create_stream(TAOS *taos, const char *streamName, const char *tbName, const char *sql);
func TMQCreateStream(taos unsafe.Pointer, streamName string, tbName string, sql string) unsafe.Pointer {
	cStreamName := C.CString(streamName)
	defer C.free(unsafe.Pointer(cStreamName))
	cTbName := C.CString(tbName)
	defer C.free(unsafe.Pointer(cTbName))
	cSql := C.CString(sql)
	defer C.free(unsafe.Pointer(cSql))
	return unsafe.Pointer(C.tmq_create_stream(taos, cStreamName, cTbName, cSql))
}

////void    tmqShowMsg(tmq_message_t *tmq_message);
//func TMQShowMsg(message unsafe.Pointer) {
//	C.tmqShowMsg((*C.tmq_message_t)(message))
//}

////int32_t tmqGetSkipLogNum(tmq_message_t *tmq_message);
//func TMQGetSkipLogNum(message unsafe.Pointer) int32 {
//	return int32(C.tmqGetSkipLogNum((*C.tmq_message_t)(message)))
//}
