package stmt

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	jsoniter "github.com/json-iterator/go"
	"github.com/taosdata/driver-go/v3/common"
	taosErrors "github.com/taosdata/driver-go/v3/errors"
	"github.com/taosdata/driver-go/v3/ws/client"
)

type Connector struct {
	client             *client.Client
	requestID          uint64
	listLock           sync.RWMutex
	sendChanList       *list.List
	writeTimeout       time.Duration
	readTimeout        time.Duration
	config             *Config
	closeOnce          sync.Once
	closeChan          chan struct{}
	customErrorHandler func(*Connector, error)
	customCloseHandler func()
}

var (
	ConnectTimeoutErr = errors.New("stmt connect timeout")
)

func NewConnector(config *Config) (*Connector, error) {
	var connector *Connector
	readTimeout := common.DefaultMessageTimeout
	writeTimeout := common.DefaultWriteWait
	if config.MessageTimeout > 0 {
		readTimeout = config.MessageTimeout
	}
	if config.WriteWait > 0 {
		writeTimeout = config.WriteWait
	}
	ws, _, err := common.DefaultDialer.Dial(config.Url, nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		if connector == nil {
			ws.Close()
		}
	}()
	if config.MessageTimeout <= 0 {
		config.MessageTimeout = common.DefaultMessageTimeout
	}
	req := &ConnectReq{
		ReqID:    0,
		User:     config.User,
		Password: config.Password,
		DB:       config.DB,
	}
	args, err := client.JsonI.Marshal(req)
	if err != nil {
		return nil, err
	}
	action := &client.WSAction{
		Action: STMTConnect,
		Args:   args,
	}
	connectAction, err := client.JsonI.Marshal(action)
	if err != nil {
		return nil, err
	}
	ws.SetWriteDeadline(time.Now().Add(writeTimeout))
	err = ws.WriteMessage(websocket.TextMessage, connectAction)
	if err != nil {
		return nil, err
	}
	done := make(chan struct{})
	ctx, cancel := context.WithTimeout(context.Background(), readTimeout)
	var respBytes []byte
	go func() {
		_, respBytes, err = ws.ReadMessage()
		close(done)
	}()
	select {
	case <-done:
		cancel()
	case <-ctx.Done():
		cancel()
		return nil, ConnectTimeoutErr
	}
	if err != nil {
		return nil, err
	}
	var resp ConnectResp
	err = client.JsonI.Unmarshal(respBytes, &resp)
	if err != nil {
		return nil, err
	}
	if resp.Code != 0 {
		return nil, taosErrors.NewError(resp.Code, resp.Message)
	}
	wsClient := client.NewClient(ws, config.ChanLength)
	wsClient.WriteWait = writeTimeout
	connector = &Connector{
		client:             wsClient,
		requestID:          0,
		listLock:           sync.RWMutex{},
		sendChanList:       list.New(),
		writeTimeout:       writeTimeout,
		readTimeout:        readTimeout,
		config:             config,
		closeOnce:          sync.Once{},
		closeChan:          make(chan struct{}),
		customErrorHandler: config.ErrorHandler,
		customCloseHandler: config.CloseHandler,
	}

	wsClient.TextMessageHandler = connector.handleTextMessage
	wsClient.ErrorHandler = connector.handleError
	go wsClient.WritePump()
	go wsClient.ReadPump()
	return connector, nil
}

func (c *Connector) handleTextMessage(message []byte) {
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

type IndexedChan struct {
	index   uint64
	channel chan []byte
}

func (c *Connector) sendText(reqID uint64, envelope *client.Envelope) ([]byte, error) {
	envelope.Type = websocket.TextMessage
	return c.send(reqID, envelope)
}
func (c *Connector) sendBinary(reqID uint64, envelope *client.Envelope) ([]byte, error) {
	envelope.Type = websocket.BinaryMessage
	return c.send(reqID, envelope)
}
func (c *Connector) send(reqID uint64, envelope *client.Envelope) ([]byte, error) {
	channel := &IndexedChan{
		index:   reqID,
		channel: make(chan []byte, 1),
	}
	element := c.addMessageOutChan(channel)
	c.client.Send(envelope)
	ctx, cancel := context.WithTimeout(context.Background(), c.readTimeout)
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

func (c *Connector) sendTextWithoutResp(envelope *client.Envelope) {
	envelope.Type = websocket.TextMessage
	c.client.Send(envelope)
}

func (c *Connector) findOutChanByID(index uint64) *list.Element {
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

func (c *Connector) addMessageOutChan(outChan *IndexedChan) *list.Element {
	c.listLock.Lock()
	element := c.sendChanList.PushBack(outChan)
	c.listLock.Unlock()
	return element
}

func (c *Connector) handleError(err error) {
	if c.customErrorHandler != nil {
		c.customErrorHandler(c, err)
	}
	c.Close()
}

func (c *Connector) generateReqID() uint64 {
	return atomic.AddUint64(&c.requestID, 1)
}

func (c *Connector) Init() (*Stmt, error) {
	reqID := c.generateReqID()
	req := &InitReq{
		ReqID: reqID,
	}
	args, err := client.JsonI.Marshal(req)
	if err != nil {
		return nil, err
	}
	action := &client.WSAction{
		Action: STMTInit,
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
	var resp InitResp
	err = client.JsonI.Unmarshal(respBytes, &resp)
	if err != nil {
		return nil, err
	}
	if resp.Code != 0 {
		return nil, taosErrors.NewError(resp.Code, resp.Message)
	}
	return &Stmt{
		id:        resp.StmtID,
		connector: c,
	}, nil
}

func (c *Connector) Close() error {
	c.closeOnce.Do(func() {
		close(c.closeChan)
		c.client.Close()
		if c.customCloseHandler != nil {
			c.customCloseHandler()
		}
	})
	return nil
}
