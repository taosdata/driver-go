package tmq

import (
	"container/list"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
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
	client              *client.Client
	requestID           uint64
	err                 error
	dataParser          *parser.TMQRawDataParser
	listLock            sync.RWMutex
	sendChanList        *list.List
	messageTimeout      time.Duration
	autoCommit          bool
	autoCommitInterval  time.Duration
	nextAutoCommitTime  time.Time
	url                 string
	user                string
	password            string
	groupID             string
	clientID            string
	offsetRest          string
	snapshotEnable      string
	withTableName       string
	sessionTimeoutMS    string
	maxPollIntervalMS   string
	otherOptions        map[string]string
	closeOnce           sync.Once
	closeChan           chan struct{}
	topics              []string
	autoReconnect       bool
	reconnectIntervalMs int
	reconnectRetryCount int
	chanLength          uint
	writeWait           time.Duration
	dialer              *websocket.Dialer
}

type IndexedChan struct {
	index   uint64
	channel chan []byte
}

type WSError struct {
	err error
}

func (e *WSError) Error() string {
	return fmt.Sprintf("websocket close with error %v", e.err)
}

// NewConsumer create a tmq consumer
func NewConsumer(conf *tmq.ConfigMap) (*Consumer, error) {
	confCopy := conf.Clone()
	config, err := configMapToConfig(confCopy)
	if err != nil {
		return nil, err
	}
	autoCommit := true
	if config.AutoCommit == "false" {
		autoCommit = false
	}
	autoCommitInterval := time.Second * 5
	if config.AutoCommitIntervalMS != "" {
		interval, err := strconv.ParseUint(config.AutoCommitIntervalMS, 10, 64)
		if err != nil {
			return nil, err
		}
		autoCommitInterval = time.Millisecond * time.Duration(interval)
	}

	dialer := common.DefaultDialer
	dialer.EnableCompression = config.EnableCompression
	u, err := url.Parse(config.Url)
	if err != nil {
		return nil, err
	}
	u.Path = "/rest/tmq"
	ws, _, err := dialer.Dial(u.String(), nil)
	if err != nil {
		return nil, err
	}
	ws.EnableWriteCompression(config.EnableCompression)
	wsClient := client.NewClient(ws, config.ChanLength)

	consumer := &Consumer{
		client:              wsClient,
		requestID:           0,
		sendChanList:        list.New(),
		messageTimeout:      config.MessageTimeout,
		url:                 u.String(),
		user:                config.User,
		password:            config.Password,
		groupID:             config.GroupID,
		clientID:            config.ClientID,
		offsetRest:          config.OffsetRest,
		autoCommit:          autoCommit,
		autoCommitInterval:  autoCommitInterval,
		snapshotEnable:      config.SnapshotEnable,
		withTableName:       config.WithTableName,
		closeChan:           make(chan struct{}),
		dataParser:          parser.NewTMQRawDataParser(),
		autoReconnect:       config.AutoReconnect,
		reconnectIntervalMs: config.ReconnectIntervalMs,
		reconnectRetryCount: config.ReconnectRetryCount,
		chanLength:          config.ChanLength,
		writeWait:           config.WriteWait,
		otherOptions:        config.OtherOptions,
		dialer:              &dialer,
	}
	consumer.initClient(consumer.client)
	return consumer, nil
}

func (c *Consumer) initClient(client *client.Client) {
	if c.writeWait > 0 {
		client.WriteWait = c.writeWait
	}
	client.BinaryMessageHandler = c.handleBinaryMessage
	client.TextMessageHandler = c.handleTextMessage
	client.ErrorHandler = c.handleError
	go client.WritePump()
	go client.ReadPump()
}

func (c *Consumer) reconnect() error {
	reconnected := false
	for i := 0; i < c.reconnectRetryCount; i++ {
		time.Sleep(time.Duration(c.reconnectIntervalMs) * time.Millisecond)
		conn, _, err := c.dialer.Dial(c.url, nil)
		if err != nil {
			continue
		}
		conn.EnableWriteCompression(c.dialer.EnableCompression)
		cl := client.NewClient(conn, c.chanLength)
		c.initClient(cl)
		if c.client != nil {
			c.client.Close()
		}
		c.client = cl
		if len(c.topics) > 0 {
			err = c.doSubscribe(c.topics, false)
			if err != nil {
				c.client.Close()
				continue
			}
		}
		reconnected = true
		break
	}
	if !reconnected {
		return errors.New("reconnect failed")
	}
	return nil
}

var excludeConfig = map[string]struct{}{
	"ws.url":                       {},
	"ws.message.channelLen":        {},
	"ws.message.timeout":           {},
	"ws.message.writeWait":         {},
	"td.connect.user":              {},
	"td.connect.pass":              {},
	"group.id":                     {},
	"client.id":                    {},
	"auto.offset.reset":            {},
	"enable.auto.commit":           {},
	"auto.commit.interval.ms":      {},
	"experimental.snapshot.enable": {},
	"msg.with.table.name":          {},
	"ws.message.enableCompression": {},
	"ws.autoReconnect":             {},
	"ws.reconnectIntervalMs":       {},
	"ws.reconnectRetryCount":       {},
	"session.timeout.ms":           {},
	"max.poll.interval.ms":         {},
}

func configMapToConfig(m tmq.ConfigMap) (*config, error) {
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
	enableCompression, err := m.Get("ws.message.enableCompression", false)
	if err != nil {
		return nil, err
	}
	autoReconnect, err := m.Get("ws.autoReconnect", false)
	if err != nil {
		return nil, err
	}
	reconnectIntervalMs, err := m.Get("ws.reconnectIntervalMs", int(2000))
	if err != nil {
		return nil, err
	}
	reconnectRetryCount, err := m.Get("ws.reconnectRetryCount", int(3))
	if err != nil {
		return nil, err
	}
	sessionTimeoutMS, err := m.Get("session.timeout.ms", "")
	if err != nil {
		return nil, err
	}
	maxPollIntervalMS, err := m.Get("max.poll.interval.ms", "")
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
	config.setConnectUser(user.(string))
	config.setConnectPass(pass.(string))
	config.setGroupID(groupID.(string))
	config.setClientID(clientID.(string))
	config.setAutoOffsetReset(offsetReset.(string))
	config.setAutoCommit(enableAutoCommit.(string))
	config.setAutoCommitIntervalMS(autoCommitIntervalMS.(string))
	config.setSnapshotEnable(enableSnapshot.(string))
	config.setWithTableName(withTableName.(string))
	config.setEnableCompression(enableCompression.(bool))
	config.setAutoReconnect(autoReconnect.(bool))
	config.setReconnectIntervalMs(reconnectIntervalMs.(int))
	config.setReconnectRetryCount(reconnectRetryCount.(int))
	config.setSessionTimeoutMS(sessionTimeoutMS.(string))
	config.setMaxPollIntervalMS(maxPollIntervalMS.(string))
	for k, v := range m {
		if _, ok := excludeConfig[k]; ok {
			continue
		}
		if strV, ok := v.(string); ok {
			config.OtherOptions[k] = strV
		} else {
			return nil, fmt.Errorf("config %s value must be string", k)
		}
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
	if !c.autoReconnect {
		c.err = &WSError{err: err}
	}
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
	TMQFetchRaw           = "fetch_raw"
	TMQFetchJsonMeta      = "fetch_json_meta"
	TMQCommit             = "commit"
	TMQUnsubscribe        = "unsubscribe"
	TMQGetTopicAssignment = "assignment"
	TMQSeek               = "seek"
	TMQCommitOffset       = "commit_offset"
	TMQCommitted          = "committed"
	TMQPosition           = "position"
)

//revive:disable-next-line
var ClosedErr = errors.New("connection closed")

func (c *Consumer) sendText(reqID uint64, envelope *client.Envelope) ([]byte, error) {
	channel := &IndexedChan{
		index:   reqID,
		channel: make(chan []byte, 1),
	}
	element := c.addMessageOutChan(channel)
	envelope.Type = websocket.TextMessage
	err := c.client.Send(envelope)
	if err != nil {
		c.listLock.Lock()
		c.sendChanList.Remove(element)
		c.listLock.Unlock()
		return nil, err
	}
	err = <-envelope.ErrorChan
	if err != nil {
		c.listLock.Lock()
		c.sendChanList.Remove(element)
		c.listLock.Unlock()
		return nil, err
	}
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
	return c.doSubscribe(topics, c.autoReconnect)
}

func (c *Consumer) doSubscribe(topics []string, reconnect bool) error {
	if c.err != nil {
		return c.err
	}
	reqID := c.generateReqID()
	req := &SubscribeReq{
		ReqID:             reqID,
		User:              c.user,
		Password:          c.password,
		GroupID:           c.groupID,
		ClientID:          c.clientID,
		OffsetRest:        c.offsetRest,
		Topics:            topics,
		AutoCommit:        "false",
		SnapshotEnable:    c.snapshotEnable,
		WithTableName:     c.withTableName,
		SessionTimeoutMS:  c.sessionTimeoutMS,
		MaxPollIntervalMS: c.maxPollIntervalMS,
		Config:            c.otherOptions,
	}
	args, err := client.JsonI.Marshal(req)
	if err != nil {
		return err
	}
	action := &client.WSAction{
		Action: TMQSubscribe,
		Args:   args,
	}
	envelope := client.GlobalEnvelopePool.Get()
	defer client.GlobalEnvelopePool.Put(envelope)
	err = client.JsonI.NewEncoder(envelope.Msg).Encode(action)
	if err != nil {
		return err
	}
	respBytes, err := c.sendText(reqID, envelope)
	if err != nil {
		if !reconnect {
			return err
		}
		var opError *net.OpError
		if errors.Is(err, ClosedErr) || errors.Is(err, client.ClosedError) || errors.As(err, &opError) {
			err = c.reconnect()
			if err != nil {
				return err
			}
			respBytes, err = c.sendText(reqID, envelope)
			if err != nil {
				return err
			}
		} else {
			return err
		}
	}
	var resp SubscribeResp
	err = client.JsonI.Unmarshal(respBytes, &resp)
	err = client.HandleResponseError(err, resp.Code, resp.Message)
	if err != nil {
		return err
	}
	c.topics = make([]string, len(topics))
	copy(c.topics, topics)
	return nil
}

// Poll messages
func (c *Consumer) Poll(timeoutMs int) tmq.Event {
	if c.err != nil {
		return tmq.NewTMQErrorWithErr(c.err)
	}
	if c.autoCommit {
		if c.nextAutoCommitTime.IsZero() {
			c.nextAutoCommitTime = time.Now().Add(c.autoCommitInterval)
		} else {
			if time.Now().After(c.nextAutoCommitTime) {
				_ = c.doCommit()
				c.nextAutoCommitTime = time.Now().Add(c.autoCommitInterval)
			}
		}
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
	envelope := client.GlobalEnvelopePool.Get()
	defer client.GlobalEnvelopePool.Put(envelope)
	err = client.JsonI.NewEncoder(envelope.Msg).Encode(action)
	if err != nil {
		return tmq.NewTMQErrorWithErr(err)
	}
	respBytes, err := c.sendText(reqID, envelope)
	if err != nil {
		if !c.autoReconnect {
			return tmq.NewTMQErrorWithErr(err)
		}
		var opError *net.OpError
		if errors.Is(err, ClosedErr) || errors.Is(err, client.ClosedError) || errors.As(err, &opError) {
			err = c.reconnect()
			if err != nil {
				return tmq.NewTMQErrorWithErr(err)
			}
			respBytes, err = c.sendText(reqID, envelope)
			if err != nil {
				return tmq.NewTMQErrorWithErr(err)
			}
		} else {
			return tmq.NewTMQErrorWithErr(err)
		}
	}
	var resp PollResp
	err = client.JsonI.Unmarshal(respBytes, &resp)
	if err != nil {
		return tmq.NewTMQErrorWithErr(err)
	}
	if resp.Code != 0 {
		return tmq.NewTMQErrorWithErr(taosErrors.NewError(resp.Code, resp.Message))
	}
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
	envelope := client.GlobalEnvelopePool.Get()
	defer client.GlobalEnvelopePool.Put(envelope)
	err = client.JsonI.NewEncoder(envelope.Msg).Encode(action)
	if err != nil {
		return nil, err
	}
	respBytes, err := c.sendText(reqID, envelope)
	if err != nil {
		return nil, err
	}
	var resp FetchJsonMetaResp
	err = client.JsonI.Unmarshal(respBytes, &resp)
	err = client.HandleResponseError(err, resp.Code, resp.Message)
	if err != nil {
		return nil, err
	}
	var meta tmq.Meta
	err = client.JsonI.Unmarshal(resp.Data, &meta)
	if err != nil {
		return nil, err
	}
	return &meta, nil
}

func (c *Consumer) fetch(messageID uint64) ([]*tmq.Data, error) {
	reqID := c.generateReqID()
	req := &TMQFetchRawMetaReq{
		ReqID:     reqID,
		MessageID: messageID,
	}
	args, err := client.JsonI.Marshal(req)
	if err != nil {
		return nil, err
	}
	action := &client.WSAction{
		Action: TMQFetchRaw,
		Args:   args,
	}
	envelope := client.GlobalEnvelopePool.Get()
	defer client.GlobalEnvelopePool.Put(envelope)
	err = client.JsonI.NewEncoder(envelope.Msg).Encode(action)
	if err != nil {
		return nil, err
	}
	respBytes, err := c.sendText(reqID, envelope)
	if err != nil {
		return nil, err
	}
	blockInfo, err := c.dataParser.Parse(unsafe.Pointer(&respBytes[38]))
	if err != nil {
		return nil, err
	}
	tmqData := make([]*tmq.Data, len(blockInfo))
	for i := 0; i < len(blockInfo); i++ {
		data, err := parser.ReadBlockSimple(blockInfo[i].RawBlock, blockInfo[i].Precision)
		if err != nil {
			return nil, err
		}
		tmqData[i] = &tmq.Data{
			TableName: blockInfo[i].TableName,
			Data:      data,
		}
	}
	return tmqData, nil
}

func (c *Consumer) Commit() ([]tmq.TopicPartition, error) {
	err := c.doCommit()
	if err != nil {
		return nil, err
	}
	partitions, err := c.Assignment()
	if err != nil {
		return nil, err
	}
	return c.Committed(partitions, 0)
}

func (c *Consumer) doCommit() error {
	if c.err != nil {
		return c.err
	}
	reqID := c.generateReqID()
	req := &CommitReq{
		ReqID:     reqID,
		MessageID: 0,
	}
	args, err := client.JsonI.Marshal(req)
	if err != nil {
		return err
	}
	action := &client.WSAction{
		Action: TMQCommit,
		Args:   args,
	}
	envelope := client.GlobalEnvelopePool.Get()
	defer client.GlobalEnvelopePool.Put(envelope)
	err = client.JsonI.NewEncoder(envelope.Msg).Encode(action)
	if err != nil {
		return err
	}
	respBytes, err := c.sendText(reqID, envelope)
	if err != nil {
		return err
	}
	var resp CommitResp
	err = client.JsonI.Unmarshal(respBytes, &resp)
	return client.HandleResponseError(err, resp.Code, resp.Message)
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
	envelope := client.GlobalEnvelopePool.Get()
	defer client.GlobalEnvelopePool.Put(envelope)
	err = client.JsonI.NewEncoder(envelope.Msg).Encode(action)
	if err != nil {
		return err
	}
	respBytes, err := c.sendText(reqID, envelope)
	if err != nil {
		return err
	}
	var resp CommitResp
	err = client.JsonI.Unmarshal(respBytes, &resp)
	return client.HandleResponseError(err, resp.Code, resp.Message)
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
		envelope := client.GlobalEnvelopePool.Get()
		defer client.GlobalEnvelopePool.Put(envelope)
		err = client.JsonI.NewEncoder(envelope.Msg).Encode(action)
		if err != nil {
			return nil, err
		}
		respBytes, err := c.sendText(reqID, envelope)
		if err != nil {
			return nil, err
		}
		var resp AssignmentResp
		err = client.JsonI.Unmarshal(respBytes, &resp)
		err = client.HandleResponseError(err, resp.Code, resp.Message)
		if err != nil {
			return nil, err
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
	envelope := client.GlobalEnvelopePool.Get()
	defer client.GlobalEnvelopePool.Put(envelope)
	err = client.JsonI.NewEncoder(envelope.Msg).Encode(action)
	if err != nil {
		return err
	}
	respBytes, err := c.sendText(reqID, envelope)
	if err != nil {
		return err
	}
	var resp OffsetSeekResp
	err = client.JsonI.Unmarshal(respBytes, &resp)
	return client.HandleResponseError(err, resp.Code, resp.Message)
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
	envelope := client.GlobalEnvelopePool.Get()
	defer client.GlobalEnvelopePool.Put(envelope)
	err = client.JsonI.NewEncoder(envelope.Msg).Encode(action)
	if err != nil {
		return nil, err
	}
	respBytes, err := c.sendText(reqID, envelope)
	if err != nil {
		return nil, err
	}
	var resp CommittedResp
	err = client.JsonI.Unmarshal(respBytes, &resp)
	err = client.HandleResponseError(err, resp.Code, resp.Message)
	if err != nil {
		return nil, err
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
	envelope := client.GlobalEnvelopePool.Get()
	defer client.GlobalEnvelopePool.Put(envelope)
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
		envelope.Reset()
		err = client.JsonI.NewEncoder(envelope.Msg).Encode(action)
		if err != nil {
			return nil, err
		}
		respBytes, err := c.sendText(reqID, envelope)
		if err != nil {
			return nil, err
		}
		var resp CommitOffsetResp
		err = client.JsonI.Unmarshal(respBytes, &resp)
		err = client.HandleResponseError(err, resp.Code, resp.Message)
		if err != nil {
			return nil, err
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
	envelope := client.GlobalEnvelopePool.Get()
	defer client.GlobalEnvelopePool.Put(envelope)
	err = client.JsonI.NewEncoder(envelope.Msg).Encode(action)
	if err != nil {
		return nil, err
	}
	respBytes, err := c.sendText(reqID, envelope)
	if err != nil {
		return nil, err
	}
	var resp PositionResp
	err = client.JsonI.Unmarshal(respBytes, &resp)
	err = client.HandleResponseError(err, resp.Code, resp.Message)
	if err != nil {
		return nil, err
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
