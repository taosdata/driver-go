package tmq

import (
	"context"
	"database/sql/driver"
	"time"
	"unsafe"

	jsoniter "github.com/json-iterator/go"
	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/errors"
	"github.com/taosdata/driver-go/v3/wrapper"
	"github.com/taosdata/driver-go/v3/wrapper/cgo"
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

// NewConsumer Create new TMQ consumer with TMQ config
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
		consumer.handlerCommitCallback()
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

// Subscribe TMQ consumer subscribe topics
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

// Unsubscribe TMQ unsubscribe
func (c *Consumer) Unsubscribe() error {
	errCode := wrapper.TMQUnsubscribe(c.cConsumer)
	if errCode != errors.SUCCESS {
		errStr := wrapper.TMQErr2Str(errCode)
		return errors.NewError(int(errCode), errStr)
	}
	return nil
}

type Result struct {
	Type    int32
	DBName  string
	Topic   string
	Message unsafe.Pointer
	Meta    *common.Meta
	Data    []*Data
}
type Data struct {
	TableName string
	Data      [][]driver.Value
}

//Poll consumer poll message with timeout
func (c *Consumer) Poll(timeout time.Duration) (*Result, error) {
	message := wrapper.TMQConsumerPoll(c.cConsumer, timeout.Milliseconds())
	if message == nil {
		return nil, nil
	}
	topic := wrapper.TMQGetTopicName(message)
	db := wrapper.TMQGetDBName(message)
	resultType := wrapper.TMQGetResType(message)
	result := &Result{
		Type:    resultType,
		DBName:  db,
		Topic:   topic,
		Message: message,
	}
	switch resultType {
	case common.TMQ_RES_TABLE_META:
		var meta common.Meta
		p := wrapper.TMQGetJsonMeta(message)
		if p != nil {
			data := wrapper.ParseJsonMeta(p)
			wrapper.TMQFreeJsonMeta(p)
			err := jsoniter.Unmarshal(data, &meta)
			if err != nil {
				return nil, err
			}
			result.Meta = &meta
		}
		return result, nil
	case common.TMQ_RES_DATA:
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
	default:
		return nil, errors.NewError(0xfffff, "invalid tmq message type")
	}
}

// FreeMessage Release message after commit
func (c *Consumer) FreeMessage(message unsafe.Pointer) {
	wrapper.TaosFreeResult(message)
}

//Commit commit message
func (c *Consumer) Commit(ctx context.Context, message unsafe.Pointer) error {
	wrapper.TMQCommitAsync(c.cConsumer, message, c.asyncCommitHandle)
	for {
		select {
		case <-c.exit:
			c.asyncCommitHandle.Delete()
			close(c.asyncCommitChan)
			return ClosedError
		case <-ctx.Done():
			return ctx.Err()
		case d := <-c.asyncCommitChan:
			return d.GetError()
		}
	}
}

// Close release consumer
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
