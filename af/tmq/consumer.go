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
	timer     *time.Timer
}

func NewConsumer(conf *Config) (*Consumer, error) {
	c := make(chan *wrapper.TMQCommitCallbackResult, 1)
	h := cgo.NewHandle(c)
	wrapper.TMQConfSetOffsetCommitCB(conf.cConfig, h)
	cConsumer, err := wrapper.TMQConsumerNew(conf.cConfig)
	if err != nil {
		return nil, err
	}
	t := time.NewTimer(time.Minute)
	t.Stop()
	consumer := &Consumer{
		conf:      conf,
		cConsumer: cConsumer,
		c:         c,
		timer:     t,
		handle:    h,
	}
	return consumer, nil
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

func (c *Consumer) Poll(timeout time.Duration) (*Result, error) {
	message := wrapper.TMQConsumerPoll(c.cConsumer, timeout.Milliseconds())
	if message == nil {
		return nil, &errors.TaosError{Code: 0xffff, ErrStr: "invalid result"}
	}
	defer wrapper.TaosFreeResult(message)
	fileCount := wrapper.TaosNumFields(message)
	rh, err := wrapper.ReadColumn(message, fileCount)
	if err != nil {
		return nil, err
	}
	precision := wrapper.TaosResultPrecision(message)
	result := &Result{}
	for {
		blockSize, errCode, block := wrapper.TaosFetchRawBlock(message)
		if errCode != int(errors.SUCCESS) {
			errStr := wrapper.TaosErrorStr(message)
			err = errors.NewError(errCode, errStr)
			return nil, err
		}
		if blockSize == 0 {
			break
		}
		result.data = append(result.data, wrapper.ReadBlock(block, blockSize, rh.ColTypes, precision)...)
	}
	return result, err
}

type Result struct {
	data [][]driver.Value
}

func (c *Consumer) Commit(timeout time.Duration) error {
	errCode := wrapper.TMQCommit(c.cConsumer, nil, true)
	if errCode != errors.SUCCESS {
		errStr := wrapper.TMQErr2Str(errCode)
		return errors.NewError(int(errCode), errStr)
	}
	c.timer.Reset(timeout)
	select {
	case d := <-c.c:
		if d.ErrCode != errors.SUCCESS {
			errStr := wrapper.TMQErr2Str(d.ErrCode)
			err := errors.NewError(int(d.ErrCode), errStr)
			return err
		}
		wrapper.PutTMQCommitCallbackResult(d)
		break
	case <-c.timer.C:
		c.timer.Stop()
		return &errors.TaosError{Code: 0xffff, ErrStr: "commit timeout"}
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
