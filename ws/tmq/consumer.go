package tmq

import (
	"container/list"
	"context"
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/gorilla/websocket"
	jsoniter "github.com/json-iterator/go"
	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/common/parser"
	taosErrors "github.com/taosdata/driver-go/v3/errors"
	"github.com/taosdata/driver-go/v3/ws/client"
)

type Consumer struct {
	client             *client.Client
	requestID          uint64
	listLock           sync.RWMutex
	sendChanList       *list.List
	messageTimeout     time.Duration
	url                string
	user               string
	password           string
	groupID            string
	clientID           string
	offsetRest         string
	closeOnce          sync.Once
	closeChan          chan struct{}
	customErrorHandler func(*Consumer, error)
	customCloseHandler func()
}

type IndexedChan struct {
	index   uint64
	channel chan []byte
}

// NewConsumer create a tmq consumer
func NewConsumer(config *Config) (*Consumer, error) {
	ws, _, err := common.DefaultDialer.Dial(config.Url, nil)
	if err != nil {
		return nil, err
	}
	if config.MessageTimeout <= 0 {
		config.MessageTimeout = common.DefaultMessageTimeout
	}
	wsClient := client.NewClient(ws, config.ChanLength)
	tmq := &Consumer{
		client:             wsClient,
		requestID:          0,
		listLock:           sync.RWMutex{},
		sendChanList:       list.New(),
		messageTimeout:     config.MessageTimeout,
		url:                config.Url,
		user:               config.User,
		password:           config.Password,
		groupID:            config.GroupID,
		clientID:           config.ClientID,
		offsetRest:         config.OffsetRest,
		closeOnce:          sync.Once{},
		closeChan:          make(chan struct{}),
		customErrorHandler: config.ErrorHandler,
		customCloseHandler: config.CloseHandler,
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
	if c.customErrorHandler != nil {
		c.customErrorHandler(c, err)
	}
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
		if c.customCloseHandler != nil {
			c.customCloseHandler()
		}
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
	TMQSubscribe     = "subscribe"
	TMQPoll          = "poll"
	TMQFetch         = "fetch"
	TMQFetchBlock    = "fetch_block"
	TMQFetchJsonMeta = "fetch_json_meta"
	TMQCommit        = "commit"
)

func (c *Consumer) sendText(reqID uint64, envelope *client.Envelope) ([]byte, error) {
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
		return nil, errors.New("connection closed")
	case resp := <-channel.channel:
		return resp, nil
	case <-ctx.Done():
		c.listLock.Lock()
		c.sendChanList.Remove(element)
		c.listLock.Unlock()
		return nil, fmt.Errorf("message timeout :%s", envelope.Msg.String())
	}
}

// Subscribe with topic list
func (c *Consumer) Subscribe(topic []string) error {
	reqID := c.generateReqID()
	req := &SubscribeReq{
		ReqID:      reqID,
		User:       c.user,
		Password:   c.password,
		GroupID:    c.groupID,
		ClientID:   c.clientID,
		OffsetRest: c.offsetRest,
		Topics:     topic,
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
	return nil
}

type Result struct {
	Type    int32
	DBName  string
	Topic   string
	Message uint64
	Meta    *common.Meta
	Data    []*Data
}

type Data struct {
	TableName string
	Data      [][]driver.Value
}

// Poll messages
func (c *Consumer) Poll(timeout time.Duration) (*Result, error) {
	reqID := c.generateReqID()
	req := &PollReq{
		ReqID:        reqID,
		BlockingTime: timeout.Milliseconds(),
	}
	args, err := client.JsonI.Marshal(req)
	if err != nil {
		return nil, err
	}
	action := &client.WSAction{
		Action: TMQPoll,
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
	var resp PollResp
	err = client.JsonI.Unmarshal(respBytes, &resp)
	if err != nil {
		return nil, err
	}
	if resp.Code != 0 {
		return nil, taosErrors.NewError(resp.Code, resp.Message)
	}
	if resp.HaveMessage {
		result := &Result{
			Type:    resp.MessageType,
			DBName:  resp.Database,
			Topic:   resp.Topic,
			Message: resp.MessageID,
		}
		switch resp.MessageType {
		case common.TMQ_RES_DATA:
			err = c.fetch(resp.MessageID, result)
			if err != nil {
				return nil, err
			}
			return result, nil
		case common.TMQ_RES_TABLE_META:
			meta, err := c.fetchJsonMeta(resp.MessageID)
			if err != nil {
				return nil, err
			}
			result.Meta = meta
			return result, nil
		case common.TMQ_RES_METADATA:
			meta, err := c.fetchJsonMeta(resp.MessageID)
			if err != nil {
				return nil, err
			}
			result.Meta = meta
			err = c.fetch(resp.MessageID, result)
			if err != nil {
				return nil, err
			}
			return result, nil
		default:
			return nil, errors.New("unknown message type:" + strconv.FormatUint(resp.MessageID, 10))
		}
	} else {
		return nil, nil
	}
}

func (c *Consumer) fetchJsonMeta(messageID uint64) (*common.Meta, error) {
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
	var meta common.Meta
	err = client.JsonI.Unmarshal(resp.Data, &meta)
	if err != nil {
		return nil, err
	}
	return &meta, nil
}

func (c *Consumer) fetch(messageID uint64, result *Result) error {
	for {
		reqID := c.generateReqID()
		req := &FetchReq{
			ReqID:     reqID,
			MessageID: messageID,
		}
		args, err := client.JsonI.Marshal(req)
		if err != nil {
			return err
		}
		action := &client.WSAction{
			Action: TMQFetch,
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
		var resp FetchResp
		err = client.JsonI.Unmarshal(respBytes, &resp)
		if err != nil {
			return err
		}
		if resp.Code != 0 {
			return taosErrors.NewError(resp.Code, resp.Message)
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
				return err
			}
			action := &client.WSAction{
				Action: TMQFetchBlock,
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
			block := respBytes[24:]
			data := parser.ReadBlock(unsafe.Pointer(*(*uintptr)(unsafe.Pointer(&block))), resp.Rows, resp.FieldsTypes, resp.Precision)
			result.Data = append(result.Data, &Data{
				TableName: resp.TableName,
				Data:      data,
			})
		}
	}
	return nil
}

// Commit message with messageID
func (c *Consumer) Commit(messageID uint64) error {
	reqID := c.generateReqID()
	req := &CommitReq{
		ReqID:     reqID,
		MessageID: messageID,
	}
	args, err := client.JsonI.Marshal(req)
	if err != nil {
		return err
	}
	action := &client.WSAction{
		Action: TMQCommit,
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
