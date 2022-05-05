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
	"sync"
	"unsafe"

	"github.com/taosdata/driver-go/v2/errors"
	"github.com/taosdata/driver-go/v2/wrapper/cgo"
)

var tmqCommitCallbackResultPool = sync.Pool{}

type TMQCommitCallbackResult struct {
	ErrCode  int32
	Consumer unsafe.Pointer
	Offset   unsafe.Pointer
}

func GetTMQCommitCallbackResult(errCode int32, consumer unsafe.Pointer, offset unsafe.Pointer) *TMQCommitCallbackResult {
	t, ok := tmqCommitCallbackResultPool.Get().(*TMQCommitCallbackResult)
	if ok {
		t.ErrCode = errCode
		t.Consumer = consumer
		t.Offset = offset
		return t
	} else {
		return &TMQCommitCallbackResult{ErrCode: errCode, Consumer: consumer, Offset: offset}
	}
}

func PutTMQCommitCallbackResult(result *TMQCommitCallbackResult) {
	tmqCommitCallbackResultPool.Put(result)
}

// TMQConfNew  tmq_conf_t    *tmq_conf_new();
func TMQConfNew() unsafe.Pointer {
	return unsafe.Pointer(C.tmq_conf_new())
}

// TMQConfSet  tmq_conf_res_t tmq_conf_set(tmq_conf_t *conf, const char *key, const char *value);
func TMQConfSet(conf unsafe.Pointer, key string, value string) int32 {
	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))
	cValue := C.CString(value)
	defer C.free(unsafe.Pointer(cValue))
	return int32(C.tmq_conf_set((*C.struct_tmq_conf_t)(conf), cKey, cValue))
}

// TMQConfDestroy void           tmq_conf_destroy(tmq_conf_t *conf);
func TMQConfDestroy(conf unsafe.Pointer) {
	C.tmq_conf_destroy((*C.struct_tmq_conf_t)(conf))
}

//typedef void(tmq_commit_cb(tmq_t *, tmq_resp_err_t, tmq_topic_vgroup_list_t *, void *param));

// TMQConfSetOffsetCommitCB  void           tmq_conf_set_offset_commit_cb(tmq_conf_t *conf, tmq_commit_cb *cb);
func TMQConfSetOffsetCommitCB(conf unsafe.Pointer, h cgo.Handle) {
	C.tmq_conf_set_offset_commit_cb((*C.struct_tmq_conf_t)(conf), (*C.tmq_commit_cb)(C.TMQCommitCB), unsafe.Pointer(h))
}

// TMQListNew tmq_list_t *tmq_list_new();
func TMQListNew() unsafe.Pointer {
	return unsafe.Pointer(C.tmq_list_new())
}

// TMQListAppend int32_t     tmq_list_append(tmq_list_t *, const char *);
func TMQListAppend(list unsafe.Pointer, str string) int32 {
	cStr := C.CString(str)
	defer C.free(unsafe.Pointer(cStr))
	return int32(C.tmq_list_append((*C.tmq_list_t)(list), cStr))
}

// TMQListDestroy void        tmq_list_destroy(tmq_list_t *);
func TMQListDestroy(list unsafe.Pointer) {
	C.tmq_list_destroy((*C.tmq_list_t)(list))
}

// TMQListGetSize int32_t     tmq_list_get_size(const tmq_list_t *);
func TMQListGetSize(list unsafe.Pointer) int32 {
	return int32(C.tmq_list_get_size((*C.tmq_list_t)(list)))
}

// TMQListToCArray char      **tmq_list_to_c_array(const tmq_list_t *);
func TMQListToCArray(list unsafe.Pointer, size int) []string {
	head := uintptr(unsafe.Pointer(C.tmq_list_to_c_array((*C.tmq_list_t)(list))))
	result := make([]string, size)
	for i := 0; i < size; i++ {
		result[i] = C.GoString(*(**C.char)(unsafe.Pointer(head + PointerSize*uintptr(i))))
	}
	return result
}

// TMQConsumerNew tmq_t *tmq_consumer_new1(tmq_conf_t *conf, char *errstr, int32_t errstrLen);
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

// TMQErr2Str const char *tmq_err2str(tmq_resp_err_t);
func TMQErr2Str(code int32) string {
	return C.GoString(C.tmq_err2str((C.tmq_resp_err_t)(code)))
}

// TMQSubscribe tmq_resp_err_t tmq_subscribe(tmq_t *tmq, tmq_list_t *topic_list);
func TMQSubscribe(consumer unsafe.Pointer, topicList unsafe.Pointer) int32 {
	return int32(C.tmq_subscribe((*C.tmq_t)(consumer), (*C.tmq_list_t)(topicList)))
}

// TMQUnsubscribe tmq_resp_err_t tmq_unsubscribe(tmq_t *tmq);
func TMQUnsubscribe(consumer unsafe.Pointer) int32 {
	return int32(C.tmq_unsubscribe((*C.tmq_t)(consumer)))
}

// TMQSubscription tmq_resp_err_t tmq_subscription(tmq_t *tmq, tmq_list_t **topics);
func TMQSubscription(consumer unsafe.Pointer) (int32, unsafe.Pointer) {
	list := C.tmq_list_new()
	code := int32(C.tmq_subscription(
		(*C.tmq_t)(consumer),
		(**C.tmq_list_t)(&list),
	))
	return code, unsafe.Pointer(list)
}

// TMQConsumerPoll TAOS_RES *tmq_consumer_poll(tmq_t *tmq, int64_t blocking_time);
func TMQConsumerPoll(consumer unsafe.Pointer, blockingTime int64) unsafe.Pointer {
	return unsafe.Pointer(C.tmq_consumer_poll((*C.tmq_t)(consumer), (C.int64_t)(blockingTime)))
}

// TMQConsumerClose tmq_resp_err_t tmq_consumer_close(tmq_t *tmq);
func TMQConsumerClose(consumer unsafe.Pointer) int32 {
	return int32(C.tmq_consumer_close((*C.tmq_t)(consumer)))
}

// TMQCommit tmq_resp_err_t tmq_commit(tmq_t *tmq, const tmq_topic_vgroup_list_t *offsets, int32_t async);
func TMQCommit(consumer unsafe.Pointer, offsets unsafe.Pointer, async bool) int32 {
	if async {
		return int32(C.tmq_commit((*C.tmq_t)(consumer), (*C.tmq_topic_vgroup_list_t)(offsets), (C.int32_t)(1)))
	} else {
		return int32(C.tmq_commit((*C.tmq_t)(consumer), (*C.tmq_topic_vgroup_list_t)(offsets), (C.int32_t)(0)))
	}
}

// TMQGetTopicName char       *tmq_get_topic_name(tmq_message_t *message);
func TMQGetTopicName(message unsafe.Pointer) string {
	return C.GoString(C.tmq_get_topic_name(message))
}

// TMQGetVgroupID int32_t     tmq_get_vgroup_id(tmq_message_t *message);
func TMQGetVgroupID(message unsafe.Pointer) int32 {
	return int32(C.tmq_get_vgroup_id(message))
}
