package tmq

import (
	"errors"
	"unsafe"

	jsoniter "github.com/json-iterator/go"
	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/common/parser"
	"github.com/taosdata/driver-go/v3/common/tmq"
	taosError "github.com/taosdata/driver-go/v3/errors"
	"github.com/taosdata/driver-go/v3/wrapper"
)

type Consumer struct {
	cConsumer unsafe.Pointer
}

// NewConsumer Create new TMQ consumer with TMQ config
func NewConsumer(conf *tmq.ConfigMap) (*Consumer, error) {
	confStruct, err := configMapToConfig(conf)
	if err != nil {
		return nil, err
	}
	defer confStruct.destroy()
	cConsumer, err := wrapper.TMQConsumerNew(confStruct.cConfig)
	if err != nil {
		return nil, err
	}
	consumer := &Consumer{
		cConsumer: cConsumer,
	}
	return consumer, nil
}

func configMapToConfig(m *tmq.ConfigMap) (*config, error) {
	c := newConfig()
	confCopy := m.Clone()
	for k, v := range confCopy {
		vv, ok := v.(string)
		if !ok {
			c.destroy()
			return nil, errors.New("config value requires string")
		}
		err := c.setConfig(k, vv)
		if err != nil {
			c.destroy()
			return nil, err
		}
	}
	return c, nil
}

type RebalanceCb func(*Consumer, tmq.Event) error

func (c *Consumer) Subscribe(topic string, rebalanceCb RebalanceCb) error {
	return c.SubscribeTopics([]string{topic}, rebalanceCb)
}

func (c *Consumer) SubscribeTopics(topics []string, rebalanceCb RebalanceCb) error {
	topicList := wrapper.TMQListNew()
	defer wrapper.TMQListDestroy(topicList)
	for _, topic := range topics {
		errCode := wrapper.TMQListAppend(topicList, topic)
		if errCode != 0 {
			errStr := wrapper.TMQErr2Str(errCode)
			return taosError.NewError(int(errCode), errStr)
		}
	}
	errCode := wrapper.TMQSubscribe(c.cConsumer, topicList)
	if errCode != 0 {
		errStr := wrapper.TMQErr2Str(errCode)
		return taosError.NewError(int(errCode), errStr)
	}
	return nil
}

// Unsubscribe TMQ unsubscribe
func (c *Consumer) Unsubscribe() error {
	errCode := wrapper.TMQUnsubscribe(c.cConsumer)
	if errCode != taosError.SUCCESS {
		errStr := wrapper.TMQErr2Str(errCode)
		return taosError.NewError(int(errCode), errStr)
	}
	return nil
}

// Poll consumer poll message with timeout
func (c *Consumer) Poll(timeoutMs int) tmq.Event {
	message := wrapper.TMQConsumerPoll(c.cConsumer, int64(timeoutMs))
	if message == nil {
		return nil
	}
	topic := wrapper.TMQGetTopicName(message)
	db := wrapper.TMQGetDBName(message)
	resultType := wrapper.TMQGetResType(message)
	switch resultType {
	case common.TMQ_RES_DATA:
		result := &tmq.DataMessage{}
		result.SetDbName(db)
		result.SetTopic(topic)
		data, err := c.getData(message)
		if err != nil {
			return tmq.NewTMQErrorWithErr(err)
		}
		result.SetData(data)
		wrapper.TaosFreeResult(message)
		return result
	case common.TMQ_RES_TABLE_META:
		result := &tmq.MetaMessage{}
		result.SetDbName(db)
		result.SetTopic(topic)
		meta, err := c.getMeta(message)
		if err != nil {
			return tmq.NewTMQErrorWithErr(err)
		}
		result.SetMeta(meta)
		wrapper.TaosFreeResult(message)
		return result
	case common.TMQ_RES_METADATA:
		result := &tmq.MetaDataMessage{}
		result.SetDbName(db)
		result.SetTopic(topic)
		data, err := c.getData(message)
		if err != nil {
			return tmq.NewTMQErrorWithErr(err)
		}
		meta, err := c.getMeta(message)
		if err != nil {
			return tmq.NewTMQErrorWithErr(err)
		}
		result.SetMetaData(&tmq.MetaData{
			Meta: meta,
			Data: data,
		})
		wrapper.TaosFreeResult(message)
		return result
	default:
		return tmq.NewTMQError(0xfffff, "invalid tmq message type")
	}
}

func (c *Consumer) getMeta(message unsafe.Pointer) (*tmq.Meta, error) {
	var meta tmq.Meta
	p := wrapper.TMQGetJsonMeta(message)
	if p != nil {
		data := wrapper.ParseJsonMeta(p)
		wrapper.TMQFreeJsonMeta(p)
		err := jsoniter.Unmarshal(data, &meta)
		if err != nil {
			return nil, err
		}
		return &meta, nil
	}
	return &meta, nil
}

func (c *Consumer) getData(message unsafe.Pointer) ([]*tmq.Data, error) {
	var tmqData []*tmq.Data
	for {
		blockSize, errCode, block := wrapper.TaosFetchRawBlock(message)
		if errCode != int(taosError.SUCCESS) {
			errStr := wrapper.TaosErrorStr(message)
			err := taosError.NewError(errCode, errStr)
			return nil, err
		}
		if blockSize == 0 {
			break
		}
		tableName := wrapper.TMQGetTableName(message)
		fileCount := wrapper.TaosNumFields(message)
		rh, err := wrapper.ReadColumn(message, fileCount)
		if err != nil {
			return nil, err
		}
		precision := wrapper.TaosResultPrecision(message)
		tmqData = append(tmqData, &tmq.Data{
			TableName: tableName,
			Data:      parser.ReadBlock(block, blockSize, rh.ColTypes, precision),
		})
	}
	return tmqData, nil
}

func (c *Consumer) Commit() ([]tmq.TopicPartition, error) {
	return c.doCommit(nil)
}

func (c *Consumer) doCommit(message unsafe.Pointer) ([]tmq.TopicPartition, error) {
	errCode := wrapper.TMQCommitSync(c.cConsumer, message)
	if errCode != taosError.SUCCESS {
		errStr := wrapper.TMQErr2Str(errCode)
		return nil, taosError.NewError(int(errCode), errStr)
	}
	return nil, nil
}

// Close release consumer
func (c *Consumer) Close() error {
	errCode := wrapper.TMQConsumerClose(c.cConsumer)
	if errCode != 0 {
		errStr := wrapper.TMQErr2Str(errCode)
		return taosError.NewError(int(errCode), errStr)
	}
	return nil
}
