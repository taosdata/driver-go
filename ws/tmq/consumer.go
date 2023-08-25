package tmq

import (
	"container/list"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/gorilla/websocket"
	jsoniter "github.com/json-iterator/go"
	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/common/parser"
	"github.com/taosdata/driver-go/v3/common/tmq"
	taosErrors "github.com/taosdata/driver-go/v3/errors"
	"github.com/taosdata/driver-go/v3/ws/client"
)

type Consumer struct {
	client               *client.Client
	requestID            uint64
	err                  error
	latestMessageID      uint64
	listLock             sync.RWMutex
	sendChanList         *list.List
	messageTimeout       time.Duration
	url                  string
	user                 string
	password             string
	groupID              string
	clientID             string
	offsetRest           string
	autoCommit           string
	autoCommitIntervalMS string
	snapshotEnable       string
	withTableName        string
	closeOnce            sync.Once
	closeChan            chan struct{}
	topics               []string
}

type IndexedChan struct {
	index   uint64
	channel chan []byte
}

type WSError struct {
	err error
}

func (e *WSError) Error() string {
	return fmt.Sprintf("websocket close with error %s", e.err)
}

// NewConsumer create a tmq consumer
func NewConsumer(conf *tmq.ConfigMap) (*Consumer, error) {
	confCopy := conf.Clone()
	config, err := configMapToConfig(&confCopy)
	if err != nil {
		return nil, err
	}
	ws, _, err := common.DefaultDialer.Dial(config.Url, nil)
	if err != nil {
		return nil, err
	}
	wsClient := client.NewClient(ws, config.ChanLength)
	tmq := &Consumer{
		client:               wsClient,
		requestID:            0,
		sendChanList:         list.New(),
		messageTimeout:       config.MessageTimeout,
		url:                  config.Url,
		user:                 config.User,
		password:             config.Password,
		groupID:              config.GroupID,
		clientID:             config.ClientID,
		offsetRest:           config.OffsetRest,
		autoCommit:           config.AutoCommit,
		autoCommitIntervalMS: config.AutoCommitIntervalMS,
		snapshotEnable:       config.SnapshotEnable,
		withTableName:        config.WithTableName,
		closeChan:            make(chan struct{}),
	}
	if config.WriteWait > 0 {
		wsClient.WriteWait = config.WriteWait
	}
	wsClient.BinaryMessageHandler = tmq.handleBinaryMessage
	wsClient.TextMessageHandler = tmq.handleTextMessage
	wsClient.ErrorHandler = tmq.handleError
	go wsClient.WritePump()
	go wsClient.ReadPump()
	return tmq, nil
}

func configMapToConfig(m *tmq.ConfigMap) (*config, error) {
	url, err := m.Get("ws.url", "")
	if err != nil {
		return nil, err
	}
	if url == "" {
		return nil, errors.New("ws.url required")
	}
	chanLen, err := m.Get("ws.message.channelLen", uint(0))
	if err != nil {
		return nil, err
	}
	messageTimeout, err := m.Get("ws.message.timeout", common.DefaultMessageTimeout)
	if err != nil {
		return nil, err
	}
	writeWait, err := m.Get("ws.message.writeWait", common.DefaultWriteWait)
	if err != nil {
		return nil, err
	}
	user, err := m.Get("td.connect.user", "")
	if err != nil {
		return nil, err
	}
	pass, err := m.Get("td.connect.pass", "")
	if err != nil {
		return nil, err
	}
	groupID, err := m.Get("group.id", "")
	if err != nil {
		return nil, err
	}
	clientID, err := m.Get("client.id", "")
	if err != nil {
		return nil, err
	}
	offsetReset, err := m.Get("auto.offset.reset", "")
	if err != nil {
		return nil, err
	}
	enableAutoCommit, err := m.Get("enable.auto.commit", "")
	if err != nil {
		return nil, err
	}
	//auto.commit.interval.ms
	autoCommitIntervalMS, err := m.Get("auto.commit.interval.ms", "")
	if err != nil {
		return nil, err
	}
	enableSnapshot, err := m.Get("experimental.snapshot.enable", "")
	if err != nil {
		return nil, err
	}
	withTableName, err := m.Get("msg.with.table.name", "")
	if err != nil {
		return nil, err
	}
	config := newConfig(url.(string), chanLen.(uint))
	err = config.setMessageTimeout(messageTimeout.(time.Duration))
	if err != nil {
		return nil, err
	}
	err = config.setWriteWait(writeWait.(time.Duration))
	if err != nil {
		return nil, err
	}
	err = config.setConnectUser(user)
	if err != nil {
		return nil, err
	}
	err = config.setConnectPass(pass)
	if err != nil {
		return nil, err
	}
	err = config.setGroupID(groupID)
	if err != nil {
		return nil, err
	}
	err = config.setClientID(clientID)
	if err != nil {
		return nil, err
	}
	err = config.setAutoOffsetReset(offsetReset)
	if err != nil {
		return nil, err
	}
	err = config.setAutoCommit(enableAutoCommit)
	if err != nil {
		return nil, err
	}
	err = config.setAutoCommitIntervalMS(autoCommitIntervalMS)
	if err != nil {
		return nil, err
	}
	err = config.setSnapshotEnable(enableSnapshot)
	if err != nil {
		return nil, err
	}
	err = config.setWithTableName(withTableName)
	if err != nil {
		return nil, err
	}
	return config, nil
}

func (c *Consumer) handleTextMessage(message []byte) {
	iter := client.JsonI.BorrowIterator(message)
	var reqID uint64
	iter.ReadObjectCB(func(iter *jsoniter.Iterator, s string) bool {
		switch s {
		case "req_id":
			reqID = iter.ReadUint64()
			return false
		default:
			iter.Skip()
		}
		return iter.Error == nil
	})
	client.JsonI.ReturnIterator(iter)
	c.listLock.Lock()
	element := c.findOutChanByID(reqID)
	if element != nil {
		element.Value.(*IndexedChan).channel <- message
		c.sendChanList.Remove(element)
	}
	c.listLock.Unlock()
}

func (c *Consumer) handleBinaryMessage(message []byte) {
	reqID := binary.LittleEndian.Uint64(message[8:16])
	c.listLock.Lock()
	element := c.findOutChanByID(reqID)
	if element != nil {
		element.Value.(*IndexedChan).channel <- message
		c.sendChanList.Remove(element)
	}
	c.listLock.Unlock()
}

func (c *Consumer) handleError(err error) {
	c.err = &WSError{err: err}
	c.Close()
}

func (c *Consumer) generateReqID() uint64 {
	return atomic.AddUint64(&c.requestID, 1)
}

// Close consumer. This function can be called multiple times
func (c *Consumer) Close() error {
	c.closeOnce.Do(func() {
		close(c.closeChan)
		c.client.Close()
	})
	return nil
}

func (c *Consumer) addMessageOutChan(outChan *IndexedChan) *list.Element {
	c.listLock.Lock()
	element := c.sendChanList.PushBack(outChan)
	c.listLock.Unlock()
	return element
}

func (c *Consumer) findOutChanByID(index uint64) *list.Element {
	root := c.sendChanList.Front()
	if root == nil {
		return nil
	}
	rootIndex := root.Value.(*IndexedChan).index
	if rootIndex == index {
		return root
	}
	item := root.Next()
	for {
		if item == nil || item == root {
			return nil
		}
		if item.Value.(*IndexedChan).index == index {
			return item
		}
		item = item.Next()
	}
}

const (
	TMQSubscribe          = "subscribe"
	TMQPoll               = "poll"
	TMQFetch              = "fetch"
	TMQFetchBlock         = "fetch_block"
	TMQFetchJsonMeta      = "fetch_json_meta"
	TMQCommit             = "commit"
	TMQUnsubscribe        = "unsubscribe"
	TMQGetTopicAssignment = "assignment"
	TMQSeek               = "seek"
	TMQCommitOffset       = "commit_offset"
	TMQCommitted          = "committed"
	TMQPosition           = "position"
	TMQListTopics         = "list_topics"
)

var ClosedErr = errors.New("connection closed")

func (c *Consumer) sendText(reqID uint64, envelope *client.Envelope) ([]byte, error) {
	if !c.client.IsRunning() {
		c.client.PutEnvelope(envelope)
		return nil, ClosedErr
	}
	channel := &IndexedChan{
		index:   reqID,
		channel: make(chan []byte, 1),
	}
	element := c.addMessageOutChan(channel)
	envelope.Type = websocket.TextMessage
	c.client.Send(envelope)
	ctx, cancel := context.WithTimeout(context.Background(), c.messageTimeout)
	defer cancel()
	select {
	case <-c.closeChan:
		return nil, ClosedErr
	case resp := <-channel.channel:
		return resp, nil
	case <-ctx.Done():
		c.listLock.Lock()
		c.sendChanList.Remove(element)
		c.listLock.Unlock()
		return nil, fmt.Errorf("message timeout :%s", envelope.Msg.String())
	}
}

type RebalanceCb func(*Consumer, tmq.Event) error

func (c *Consumer) Subscribe(topic string, rebalanceCb RebalanceCb) error {
	return c.SubscribeTopics([]string{topic}, rebalanceCb)
}

func (c *Consumer) SubscribeTopics(topics []string, rebalanceCb RebalanceCb) error {
	if c.err != nil {
		return c.err
	}
	reqID := c.generateReqID()
	req := &SubscribeReq{
		ReqID:                reqID,
		User:                 c.user,
		Password:             c.password,
		GroupID:              c.groupID,
		ClientID:             c.clientID,
		OffsetRest:           c.offsetRest,
		Topics:               topics,
		AutoCommit:           c.autoCommit,
		AutoCommitIntervalMS: c.autoCommitIntervalMS,
		SnapshotEnable:       c.snapshotEnable,
		WithTableName:        c.withTableName,
	}
	args, err := client.JsonI.Marshal(req)
	if err != nil {
		return err
	}
	action := &client.WSAction{
		Action: TMQSubscribe,
		Args:   args,
	}
	envelope := c.client.GetEnvelope()
	err = client.JsonI.NewEncoder(envelope.Msg).Encode(action)
	if err != nil {
		c.client.PutEnvelope(envelope)
		return err
	}
	respBytes, err := c.sendText(reqID, envelope)
	if err != nil {
		return err
	}
	var resp SubscribeResp
	err = client.JsonI.Unmarshal(respBytes, &resp)
	if err != nil {
		return err
	}
	if resp.Code != 0 {
		return taosErrors.NewError(resp.Code, resp.Message)
	}
	c.topics = make([]string, len(topics))
	copy(c.topics, topics)
	return nil
}

// Poll messages
func (c *Consumer) Poll(timeoutMs int) tmq.Event {
	if c.err != nil {
		panic(c.err)
	}
	reqID := c.generateReqID()
	req := &PollReq{
		ReqID:        reqID,
		BlockingTime: int64(timeoutMs),
	}
	args, err := client.JsonI.Marshal(req)
	if err != nil {
		return tmq.NewTMQErrorWithErr(err)
	}
	action := &client.WSAction{
		Action: TMQPoll,
		Args:   args,
	}
	envelope := c.client.GetEnvelope()
	err = client.JsonI.NewEncoder(envelope.Msg).Encode(action)
	if err != nil {
		c.client.PutEnvelope(envelope)
		return tmq.NewTMQErrorWithErr(err)
	}
	respBytes, err := c.sendText(reqID, envelope)
	if err != nil {
		return tmq.NewTMQErrorWithErr(err)
	}
	var resp PollResp
	err = client.JsonI.Unmarshal(respBytes, &resp)
	if err != nil {
		return tmq.NewTMQErrorWithErr(err)
	}
	if resp.Code != 0 {
		panic(taosErrors.NewError(resp.Code, resp.Message))
	}
	c.latestMessageID = resp.MessageID
	if resp.HaveMessage {
		switch resp.MessageType {
		case common.TMQ_RES_DATA:
			result := &tmq.DataMessage{}
			result.SetDbName(resp.Database)
			result.SetTopic(resp.Topic)
			result.SetOffset(tmq.Offset(resp.Offset))
			data, err := c.fetch(resp.MessageID)
			if err != nil {
				return tmq.NewTMQErrorWithErr(err)
			}
			result.SetData(data)
			topic := resp.Topic
			result.TopicPartition = tmq.TopicPartition{
				Topic:     &topic,
				Partition: resp.VgroupID,
				Offset:    tmq.Offset(resp.Offset),
			}
			return result
		case common.TMQ_RES_TABLE_META:
			result := &tmq.MetaMessage{}
			result.SetDbName(resp.Database)
			result.SetTopic(resp.Topic)
			result.SetOffset(tmq.Offset(resp.Offset))
			meta, err := c.fetchJsonMeta(resp.MessageID)
			if err != nil {
				return tmq.NewTMQErrorWithErr(err)
			}
			topic := resp.Topic
			result.TopicPartition = tmq.TopicPartition{
				Topic:     &topic,
				Partition: resp.VgroupID,
				Offset:    tmq.Offset(resp.Offset),
			}
			result.SetMeta(meta)
			return result
		case common.TMQ_RES_METADATA:
			result := &tmq.MetaDataMessage{}
			result.SetDbName(resp.Database)
			result.SetTopic(resp.Topic)
			result.SetOffset(tmq.Offset(resp.Offset))
			meta, err := c.fetchJsonMeta(resp.MessageID)
			if err != nil {
				return tmq.NewTMQErrorWithErr(err)
			}
			data, err := c.fetch(resp.MessageID)
			if err != nil {
				return tmq.NewTMQErrorWithErr(err)
			}
			result.SetMetaData(&tmq.MetaData{
				Meta: meta,
				Data: data,
			})
			topic := resp.Topic
			result.TopicPartition = tmq.TopicPartition{
				Topic:     &topic,
				Partition: resp.VgroupID,
				Offset:    tmq.Offset(resp.Offset),
			}
			return result
		default:
			return tmq.NewTMQErrorWithErr(err)
		}
	} else {
		return nil
	}
}

func (c *Consumer) fetchJsonMeta(messageID uint64) (*tmq.Meta, error) {
	reqID := c.generateReqID()
	req := &FetchJsonMetaReq{
		ReqID:     reqID,
		MessageID: messageID,
	}
	args, err := client.JsonI.Marshal(req)
	if err != nil {
		return nil, err
	}
	action := &client.WSAction{
		Action: TMQFetchJsonMeta,
		Args:   args,
	}
	envelope := c.client.GetEnvelope()
	err = client.JsonI.NewEncoder(envelope.Msg).Encode(action)
	if err != nil {
		c.client.PutEnvelope(envelope)
		return nil, err
	}
	respBytes, err := c.sendText(reqID, envelope)
	if err != nil {
		return nil, err
	}
	var resp FetchJsonMetaResp
	err = client.JsonI.Unmarshal(respBytes, &resp)
	if err != nil {
		return nil, err
	}
	if resp.Code != 0 {
		return nil, taosErrors.NewError(resp.Code, resp.Message)
	}
	var meta tmq.Meta
	err = client.JsonI.Unmarshal(resp.Data, &meta)
	if err != nil {
		return nil, err
	}
	return &meta, nil
}

func (c *Consumer) fetch(messageID uint64) ([]*tmq.Data, error) {
	var tmqData []*tmq.Data
	for {
		reqID := c.generateReqID()
		req := &FetchReq{
			ReqID:     reqID,
			MessageID: messageID,
		}
		args, err := client.JsonI.Marshal(req)
		if err != nil {
			return nil, err
		}
		action := &client.WSAction{
			Action: TMQFetch,
			Args:   args,
		}
		envelope := c.client.GetEnvelope()
		err = client.JsonI.NewEncoder(envelope.Msg).Encode(action)
		if err != nil {
			c.client.PutEnvelope(envelope)
			return nil, err
		}
		respBytes, err := c.sendText(reqID, envelope)
		if err != nil {
			return nil, err
		}
		var resp FetchResp
		err = client.JsonI.Unmarshal(respBytes, &resp)
		if err != nil {
			return nil, err
		}
		if resp.Code != 0 {
			return nil, taosErrors.NewError(resp.Code, resp.Message)
		}
		if resp.Completed {
			break
		}
		// fetch block
		{
			req := &FetchBlockReq{
				ReqID:     reqID,
				MessageID: messageID,
			}
			args, err := client.JsonI.Marshal(req)
			if err != nil {
				return nil, err
			}
			action := &client.WSAction{
				Action: TMQFetchBlock,
				Args:   args,
			}
			envelope := c.client.GetEnvelope()
			err = client.JsonI.NewEncoder(envelope.Msg).Encode(action)
			if err != nil {
				c.client.PutEnvelope(envelope)
				return nil, err
			}
			respBytes, err := c.sendText(reqID, envelope)
			if err != nil {
				return nil, err
			}
			block := respBytes[24:]
			p := unsafe.Pointer(&block[0])
			data := parser.ReadBlock(p, resp.Rows, resp.FieldsTypes, resp.Precision)
			tmqData = append(tmqData, &tmq.Data{
				TableName: resp.TableName,
				Data:      data,
			})
		}
	}
	return tmqData, nil
}

func (c *Consumer) Commit() ([]tmq.TopicPartition, error) {
	return c.doCommit(c.latestMessageID)
}

func (c *Consumer) doCommit(messageID uint64) ([]tmq.TopicPartition, error) {
	if c.err != nil {
		return nil, c.err
	}
	reqID := c.generateReqID()
	req := &CommitReq{
		ReqID:     reqID,
		MessageID: messageID,
	}
	args, err := client.JsonI.Marshal(req)
	if err != nil {
		return nil, err
	}
	action := &client.WSAction{
		Action: TMQCommit,
		Args:   args,
	}
	envelope := c.client.GetEnvelope()
	err = client.JsonI.NewEncoder(envelope.Msg).Encode(action)
	if err != nil {
		c.client.PutEnvelope(envelope)
		return nil, err
	}
	respBytes, err := c.sendText(reqID, envelope)
	if err != nil {
		return nil, err
	}
	var resp CommitResp
	err = client.JsonI.Unmarshal(respBytes, &resp)
	if err != nil {
		return nil, err
	}
	if resp.Code != 0 {
		return nil, taosErrors.NewError(resp.Code, resp.Message)
	}
	partitions, err := c.Assignment()
	if err != nil {
		return nil, err
	}
	return c.Committed(partitions, 0)
}

func (c *Consumer) Unsubscribe() error {
	if c.err != nil {
		return c.err
	}
	reqID := c.generateReqID()
	req := &UnsubscribeReq{
		ReqID: reqID,
	}
	args, err := client.JsonI.Marshal(req)
	if err != nil {
		return err
	}
	action := &client.WSAction{
		Action: TMQUnsubscribe,
		Args:   args,
	}
	envelope := c.client.GetEnvelope()
	err = client.JsonI.NewEncoder(envelope.Msg).Encode(action)
	if err != nil {
		c.client.PutEnvelope(envelope)
		return err
	}
	respBytes, err := c.sendText(reqID, envelope)
	if err != nil {
		return err
	}
	var resp CommitResp
	err = client.JsonI.Unmarshal(respBytes, &resp)
	if err != nil {
		return err
	}
	if resp.Code != 0 {
		return taosErrors.NewError(resp.Code, resp.Message)
	}
	return nil
}

func (c *Consumer) Assignment() (partitions []tmq.TopicPartition, err error) {
	if c.err != nil {
		return nil, c.err
	}
	for _, topic := range c.topics {
		reqID := c.generateReqID()
		req := &AssignmentReq{
			ReqID: reqID,
			Topic: topic,
		}
		args, err := client.JsonI.Marshal(req)
		if err != nil {
			return nil, err
		}
		action := &client.WSAction{
			Action: TMQGetTopicAssignment,
			Args:   args,
		}
		envelope := c.client.GetEnvelope()
		err = client.JsonI.NewEncoder(envelope.Msg).Encode(action)
		if err != nil {
			c.client.PutEnvelope(envelope)
			return nil, err
		}
		respBytes, err := c.sendText(reqID, envelope)
		if err != nil {
			return nil, err
		}
		var resp AssignmentResp
		err = client.JsonI.Unmarshal(respBytes, &resp)
		if err != nil {
			return nil, err
		}
		if resp.Code != 0 {
			return nil, taosErrors.NewError(resp.Code, resp.Message)
		}
		topicName := topic
		for i := 0; i < len(resp.Assignment); i++ {
			offset := tmq.Offset(resp.Assignment[i].Offset)
			partitions = append(partitions, tmq.TopicPartition{
				Topic:     &topicName,
				Partition: resp.Assignment[i].VGroupID,
				Offset:    offset,
			})
		}
	}
	return partitions, nil
}

func (c *Consumer) Seek(partition tmq.TopicPartition, ignoredTimeoutMs int) error {
	if c.err != nil {
		return c.err
	}
	reqID := c.generateReqID()
	req := &OffsetSeekReq{
		ReqID:    reqID,
		Topic:    *partition.Topic,
		VgroupID: partition.Partition,
		Offset:   int64(partition.Offset),
	}
	args, err := client.JsonI.Marshal(req)
	if err != nil {
		return err
	}
	action := &client.WSAction{
		Action: TMQSeek,
		Args:   args,
	}
	envelope := c.client.GetEnvelope()
	err = client.JsonI.NewEncoder(envelope.Msg).Encode(action)
	if err != nil {
		c.client.PutEnvelope(envelope)
		return err
	}
	respBytes, err := c.sendText(reqID, envelope)
	if err != nil {
		return err
	}
	var resp OffsetSeekResp
	err = client.JsonI.Unmarshal(respBytes, &resp)
	if err != nil {
		return err
	}
	if resp.Code != 0 {
		return taosErrors.NewError(resp.Code, resp.Message)
	}
	return nil
}

func (c *Consumer) Committed(partitions []tmq.TopicPartition, timeoutMs int) (offsets []tmq.TopicPartition, err error) {
	offsets = make([]tmq.TopicPartition, len(partitions))
	reqID := c.generateReqID()
	req := &CommittedReq{
		ReqID:          reqID,
		TopicVgroupIDs: make([]TopicVgroupID, len(partitions)),
	}
	for i := 0; i < len(partitions); i++ {
		req.TopicVgroupIDs[i] = TopicVgroupID{
			Topic:    *partitions[i].Topic,
			VgroupID: partitions[i].Partition,
		}
	}
	args, err := client.JsonI.Marshal(req)
	if err != nil {
		return nil, err
	}
	action := &client.WSAction{
		Action: TMQCommitted,
		Args:   args,
	}
	envelope := c.client.GetEnvelope()
	err = client.JsonI.NewEncoder(envelope.Msg).Encode(action)
	if err != nil {
		c.client.PutEnvelope(envelope)
		return nil, err
	}
	respBytes, err := c.sendText(reqID, envelope)
	if err != nil {
		return nil, err
	}
	var resp CommittedResp
	err = client.JsonI.Unmarshal(respBytes, &resp)
	if err != nil {
		return nil, err
	}
	if resp.Code != 0 {
		return nil, taosErrors.NewError(resp.Code, resp.Message)
	}
	for i := 0; i < len(resp.Committed); i++ {
		offsets[i] = tmq.TopicPartition{
			Topic:     partitions[i].Topic,
			Partition: partitions[i].Partition,
			Offset:    tmq.Offset(resp.Committed[i]),
		}
	}
	return offsets, nil
}

func (c *Consumer) CommitOffsets(offsets []tmq.TopicPartition) ([]tmq.TopicPartition, error) {
	if c.err != nil {
		return nil, c.err
	}
	for i := 0; i < len(offsets); i++ {
		reqID := c.generateReqID()
		req := &CommitOffsetReq{
			ReqID:    reqID,
			Topic:    *offsets[i].Topic,
			VgroupID: offsets[i].Partition,
			Offset:   int64(offsets[i].Offset),
		}
		args, err := client.JsonI.Marshal(req)
		if err != nil {
			return nil, err
		}
		action := &client.WSAction{
			Action: TMQCommitOffset,
			Args:   args,
		}
		envelope := c.client.GetEnvelope()
		err = client.JsonI.NewEncoder(envelope.Msg).Encode(action)
		if err != nil {
			c.client.PutEnvelope(envelope)
			return nil, err
		}
		respBytes, err := c.sendText(reqID, envelope)
		if err != nil {
			return nil, err
		}
		var resp CommitOffsetResp
		err = client.JsonI.Unmarshal(respBytes, &resp)
		if err != nil {
			return nil, err
		}
		if resp.Code != 0 {
			return nil, taosErrors.NewError(resp.Code, resp.Message)
		}
	}
	return c.Committed(offsets, 0)
}

func (c *Consumer) Position(partitions []tmq.TopicPartition) (offsets []tmq.TopicPartition, err error) {
	offsets = make([]tmq.TopicPartition, len(partitions))
	reqID := c.generateReqID()
	req := &PositionReq{
		ReqID:          reqID,
		TopicVgroupIDs: make([]TopicVgroupID, len(partitions)),
	}
	for i := 0; i < len(partitions); i++ {
		req.TopicVgroupIDs[i] = TopicVgroupID{
			Topic:    *partitions[i].Topic,
			VgroupID: partitions[i].Partition,
		}
	}
	args, err := client.JsonI.Marshal(req)
	if err != nil {
		return nil, err
	}
	action := &client.WSAction{
		Action: TMQPosition,
		Args:   args,
	}
	envelope := c.client.GetEnvelope()
	err = client.JsonI.NewEncoder(envelope.Msg).Encode(action)
	if err != nil {
		c.client.PutEnvelope(envelope)
		return nil, err
	}
	respBytes, err := c.sendText(reqID, envelope)
	if err != nil {
		return nil, err
	}
	var resp PositionResp
	err = client.JsonI.Unmarshal(respBytes, &resp)
	if err != nil {
		return nil, err
	}
	if resp.Code != 0 {
		return nil, taosErrors.NewError(resp.Code, resp.Message)
	}
	for i := 0; i < len(resp.Position); i++ {
		offsets[i] = tmq.TopicPartition{
			Topic:     partitions[i].Topic,
			Partition: partitions[i].Partition,
			Offset:    tmq.Offset(resp.Position[i]),
		}
	}
	return offsets, nil
}
