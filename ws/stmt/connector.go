package stmt

import (
	"container/list"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"net/url"
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
	client              *client.Client
	requestID           uint64
	listLock            sync.RWMutex
	sendChanList        *list.List
	writeTimeout        time.Duration
	readTimeout         time.Duration
	config              *Config
	closeOnce           sync.Once
	closeChan           chan struct{}
	customErrorHandler  func(*Connector, error)
	customCloseHandler  func()
	url                 string
	chanLength          uint
	dialer              *websocket.Dialer
	autoReconnect       bool
	reconnectIntervalMs int
	reconnectRetryCount int
	user                string
	password            string
	db                  string
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
	dialer := common.DefaultDialer
	dialer.EnableCompression = config.EnableCompression
	u, err := url.Parse(config.Url)
	if err != nil {
		return nil, err
	}
	u.Path = "/ws"
	ws, _, err := dialer.Dial(u.String(), nil)
	if err != nil {
		return nil, err
	}
	ws.EnableWriteCompression(config.EnableCompression)
	defer func() {
		if connector == nil {
			ws.Close()
		}
	}()
	if config.MessageTimeout <= 0 {
		config.MessageTimeout = common.DefaultMessageTimeout
	}
	err = connect(ws, config.User, config.Password, config.DB, writeTimeout, readTimeout)
	if err != nil {
		return nil, err
	}
	wsClient := client.NewClient(ws, config.ChanLength)
	connector = &Connector{
		client:              wsClient,
		requestID:           0,
		listLock:            sync.RWMutex{},
		sendChanList:        list.New(),
		writeTimeout:        writeTimeout,
		readTimeout:         readTimeout,
		config:              config,
		closeOnce:           sync.Once{},
		closeChan:           make(chan struct{}),
		customErrorHandler:  config.ErrorHandler,
		customCloseHandler:  config.CloseHandler,
		url:                 u.String(),
		dialer:              &dialer,
		chanLength:          config.ChanLength,
		autoReconnect:       config.AutoReconnect,
		reconnectIntervalMs: config.ReconnectIntervalMs,
		reconnectRetryCount: config.ReconnectRetryCount,
		user:                config.User,
		password:            config.Password,
		db:                  config.DB,
	}
	connector.initClient(connector.client)
	return connector, nil
}

func (c *Connector) initClient(client *client.Client) {
	if c.writeTimeout > 0 {
		client.WriteWait = c.writeTimeout
	}
	client.TextMessageHandler = c.handleTextMessage
	client.BinaryMessageHandler = c.handleBinaryMessage
	client.ErrorHandler = c.handleError
	go client.WritePump()
	go client.ReadPump()
}

func connect(ws *websocket.Conn, user string, password string, db string, writeTimeout time.Duration, readTimeout time.Duration) error {
	req := &ConnectReq{
		ReqID:    0,
		User:     user,
		Password: password,
		DB:       db,
	}
	args, err := client.JsonI.Marshal(req)
	if err != nil {
		return err
	}
	action := &client.WSAction{
		Action: STMTConnect,
		Args:   args,
	}
	connectAction, err := client.JsonI.Marshal(action)
	if err != nil {
		return err
	}
	ws.SetWriteDeadline(time.Now().Add(writeTimeout))
	err = ws.WriteMessage(websocket.TextMessage, connectAction)
	if err != nil {
		return err
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
		return ConnectTimeoutErr
	}
	if err != nil {
		return err
	}
	var resp ConnectResp
	err = client.JsonI.Unmarshal(respBytes, &resp)
	if err != nil {
		return err
	}
	if resp.Code != 0 {
		return taosErrors.NewError(resp.Code, resp.Message)
	}
	return nil
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

func (c *Connector) handleBinaryMessage(message []byte) {
	reqID := binary.LittleEndian.Uint64(message[8:16])
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
	<-envelope.ErrorChan
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
	//c.Close()
}

func (c *Connector) generateReqID() uint64 {
	return atomic.AddUint64(&c.requestID, 1)
}

func (c *Connector) reconnect() error {
	reconnected := false
	for i := 0; i < c.reconnectRetryCount; i++ {
		time.Sleep(time.Duration(c.reconnectIntervalMs) * time.Millisecond)
		conn, _, err := c.dialer.Dial(c.url, nil)
		if err != nil {
			continue
		}
		conn.EnableWriteCompression(c.dialer.EnableCompression)
		err = connect(conn, c.user, c.password, c.db, c.writeTimeout, c.readTimeout)
		if err != nil {
			conn.Close()
			continue
		}
		if c.client != nil {
			c.client.Close()
		}
		cl := client.NewClient(conn, c.chanLength)
		c.initClient(cl)
		c.client = cl
		reconnected = true
		break
	}
	if !reconnected {
		if c.client != nil {
			c.client.Close()
		}
		return errors.New("reconnect failed")
	}
	return nil
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
	envelope := client.GlobalEnvelopePool.Get()
	defer client.GlobalEnvelopePool.Put(envelope)
	err = client.JsonI.NewEncoder(envelope.Msg).Encode(action)
	if err != nil {
		return nil, err
	}
	respBytes, err := c.sendText(reqID, envelope)
	if err != nil {
		if !c.autoReconnect {
			return nil, err
		}
		var opError *net.OpError
		if errors.Is(err, client.ClosedError) || errors.As(err, &opError) {
			err = c.reconnect()
			if err != nil {
				return nil, err
			}
			respBytes, err = c.sendText(reqID, envelope)
			if err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
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
