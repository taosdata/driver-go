package tmq

import (
	"testing"

	"github.com/taosdata/driver-go/v3/wrapper"
)

func TestConfig(t *testing.T) {
	conf := newConfig()
	conf.destroy()
}

func TestList(t *testing.T) {
	topicList := wrapper.TMQListNew()
	wrapper.TMQListAppend(topicList, "123")
	wrapper.TMQListDestroy(topicList)
}
