package tmq

import (
	"database/sql/driver"
	"time"
	"unsafe"

	"github.com/taosdata/driver-go/v2/errors"
	"github.com/taosdata/driver-go/v2/wrapper"
	"github.com/taosdata/driver-go/v2/wrapper/cgo"
)

type Consumer struct {
	conf      *Config
	cConsumer unsafe.Pointer
	c         chan *wrapper.TMQCommitCallbackResult
	handle    cgo.Handle
	exit      chan struct{}
	cb        func(result *wrapper.TMQCommitCallbackResult)
}

func NewConsumer(conf *Config) (*Consumer, error) {
	//avoid blocking due to auto commit
	c := make(chan *wrapper.TMQCommitCallbackResult, 2)
	h := cgo.NewHandle(c)
	wrapper.TMQConfSetOffsetCommitCB(conf.cConfig, h)
	cConsumer, err := wrapper.TMQConsumerNew(conf.cConfig)
	if err != nil {
		return nil, err
	}
	consumer := &Consumer{
		conf:      conf,
		cConsumer: cConsumer,
		c:         c,
		handle:    h,
		exit:      make(chan struct{}),
	}
	if conf.cb != nil {
		consumer.cb = conf.cb
	} else {
		consumer.cb = IgnoreCommitCallback
	}
	return consumer, nil
}

func IgnoreCommitCallback(_ *wrapper.TMQCommitCallbackResult) {
	return
}

func (c *Consumer) handlerCommitCallback() {
	go func() {
		for {
			select {
			case <-c.exit:
				return
			case d := <-c.c:
				c.cb(d)
				wrapper.PutTMQCommitCallbackResult(d)
			}
		}
	}()
}

func (c *Consumer) Subscribe(topics []string) error {
	topicList := wrapper.TMQListNew()
	defer wrapper.TMQListDestroy(topicList)
	for _, topic := range topics {
		errCode := wrapper.TMQListAppend(topicList, topic)
		if errCode != 0 {
			errStr := wrapper.TMQErr2Str(errCode)
			return errors.NewError(int(errCode), errStr)
		}
	}
	errCode := wrapper.TMQSubscribe(c.cConsumer, topicList)
	if errCode != 0 {
		errStr := wrapper.TMQErr2Str(errCode)
		return errors.NewError(int(errCode), errStr)
	}
	return nil
}

func (c *Consumer) Unsubscribe() error {
	errCode := wrapper.TMQUnsubscribe(c.cConsumer)
	if errCode != errors.SUCCESS {
		errStr := wrapper.TMQErr2Str(errCode)
		return errors.NewError(int(errCode), errStr)
	}
	return nil
}

func (c *Consumer) Poll(timeout time.Duration) ([]*Result, error) {
	message := wrapper.TMQConsumerPoll(c.cConsumer, timeout.Milliseconds())
	if message == nil {
		return nil, &errors.TaosError{Code: 0xffff, ErrStr: "invalid result"}
	}
	defer wrapper.TaosFreeResult(message)
	var result []*Result
	for {
		blockSize, errCode, block := wrapper.TaosFetchRawBlock(message)
		if errCode != int(errors.SUCCESS) {
			errStr := wrapper.TaosErrorStr(message)
			err := errors.NewError(errCode, errStr)
			return nil, err
		}
		if blockSize == 0 {
			break
		}
		r := &Result{}
		if c.conf.needGetTableName {
			r.TableName = wrapper.TMQGetTableName(message)
		}
		fileCount := wrapper.TaosNumFields(message)
		rh, err := wrapper.ReadColumn(message, fileCount)
		if err != nil {
			return nil, err
		}
		precision := wrapper.TaosResultPrecision(message)
		r.Data = append(r.Data, wrapper.ReadBlock(block, blockSize, rh.ColTypes, precision)...)
		result = append(result, r)
	}
	return result, nil
}

type Result struct {
	TableName string
	Data      [][]driver.Value
}

func (c *Consumer) Commit() error {
	errCode := wrapper.TMQCommit(c.cConsumer, nil, true)
	if errCode != errors.SUCCESS {
		errStr := wrapper.TMQErr2Str(errCode)
		return errors.NewError(int(errCode), errStr)
	}
	return nil
}

func (c *Consumer) Close() error {
	defer c.handle.Delete()
	errCode := wrapper.TMQConsumerClose(c.cConsumer)
	if errCode != 0 {
		errStr := wrapper.TMQErr2Str(errCode)
		return errors.NewError(int(errCode), errStr)
	}
	return nil
}
