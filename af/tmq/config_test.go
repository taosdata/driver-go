package tmq

import (
	"testing"

	"github.com/taosdata/driver-go/v2/wrapper"
)

func TestConfig(t *testing.T) {
	conf := NewConfig()
	conf.Destroy()
}

func TestList(t *testing.T) {
	topicList := wrapper.TMQListNew()
	wrapper.TMQListAppend(topicList, "123")
	wrapper.TMQListDestroy(topicList)
}
