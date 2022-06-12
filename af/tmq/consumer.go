package tmq

import (
	"context"
	"database/sql/driver"
	"time"
	"unsafe"

	"github.com/taosdata/driver-go/v2/errors"
	"github.com/taosdata/driver-go/v2/wrapper"
	"github.com/taosdata/driver-go/v2/wrapper/cgo"
)

var (
	ClosedError = errors.NewError(0xffff, "consumer closed")
)

type Consumer struct {
	conf                 *Config
	cConsumer            unsafe.Pointer
	autoCommitChan       chan *wrapper.TMQCommitCallbackResult
	autoCommitHandle     cgo.Handle
	autoCommitHandleFunc CommitHandleFunc
	asyncCommitChan      chan *wrapper.TMQCommitCallbackResult
	asyncCommitHandle    cgo.Handle
	exit                 chan struct{}
}

func NewConsumer(conf *Config) (*Consumer, error) {
	cConsumer, err := wrapper.TMQConsumerNew(conf.cConfig)
	if err != nil {
		return nil, err
	}
	asyncChan := make(chan *wrapper.TMQCommitCallbackResult, 1)
	asyncHandle := cgo.NewHandle(asyncChan)
	consumer := &Consumer{
		conf:              conf,
		cConsumer:         cConsumer,
		exit:              make(chan struct{}),
		asyncCommitChan:   asyncChan,
		asyncCommitHandle: asyncHandle,
	}
	if conf.autoCommit {
		autoChan := make(chan *wrapper.TMQCommitCallbackResult, 1)
		autoHandle := cgo.NewHandle(autoChan)
		wrapper.TMQConfSetAutoCommitCB(conf.cConfig, autoHandle)
		consumer.autoCommitChan = autoChan
		consumer.autoCommitHandle = autoHandle
	}
	return consumer, nil
}

func (c *Consumer) handlerCommitCallback() {
	go func() {
		for {
			select {
			case <-c.exit:
				c.autoCommitHandle.Delete()
				close(c.asyncCommitChan)
				return
			case d := <-c.autoCommitChan:
				c.autoCommitHandleFunc(d)
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

type Result struct {
	DBName string
	Topic  string
	Data   []*Data
}
type Data struct {
	TableName string
	Data      [][]driver.Value
}

func (c *Consumer) Poll(timeout time.Duration) (*Result, error) {
	message := wrapper.TMQConsumerPoll(c.cConsumer, timeout.Milliseconds())
	if message == nil {
		return nil, &errors.TaosError{Code: 0xffff, ErrStr: "invalid result"}
	}
	defer wrapper.TaosFreeResult(message)
	topic := wrapper.TMQGetTopicName(message)
	db := wrapper.TMQGetDBName(message)
	result := &Result{
		DBName: db,
		Topic:  topic,
	}
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
		r := &Data{}
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
		result.Data = append(result.Data, r)
	}
	return result, nil
}

func (c *Consumer) Commit(ctx context.Context, offset unsafe.Pointer) (unsafe.Pointer, error) {
	wrapper.TMQCommitAsync(c.cConsumer, offset, c.asyncCommitHandle)
	for {
		select {
		case <-c.exit:
			c.asyncCommitHandle.Delete()
			close(c.asyncCommitChan)
			return nil, ClosedError
		case <-ctx.Done():
			return offset, ctx.Err()
		case d := <-c.asyncCommitChan:
			callbackOffset := d.Offset
			return callbackOffset, d.GetError()
		}
	}
}

func (c *Consumer) Close() error {
	defer c.autoCommitHandle.Delete()
	errCode := wrapper.TMQConsumerClose(c.cConsumer)
	if errCode != 0 {
		errStr := wrapper.TMQErr2Str(errCode)
		return errors.NewError(int(errCode), errStr)
	}
	close(c.exit)
	return nil
}
