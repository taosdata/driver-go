package schemaless

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	jsoniter "github.com/json-iterator/go"
	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/ws/client"
)

const (
	InfluxDBLineProtocol       = 1
	OpenTSDBTelnetLineProtocol = 2
	OpenTSDBJsonFormatProtocol = 3
)

type Schemaless struct {
	client              *client.Client
	sendList            *list.List
	url                 string
	user                string
	password            string
	db                  string
	readTimeout         time.Duration
	writeTimeout        time.Duration
	lock                sync.Mutex
	once                sync.Once
	closeChan           chan struct{}
	errorHandler        func(error)
	dialer              *websocket.Dialer
	chanLength          uint
	autoReconnect       bool
	reconnectIntervalMs int
	reconnectRetryCount int
}

func NewSchemaless(config *Config) (*Schemaless, error) {
	wsUrl, err := url.Parse(config.url)
	if err != nil {
		return nil, fmt.Errorf("config url error: %s", err)
	}
	if wsUrl.Scheme != "ws" && wsUrl.Scheme != "wss" {
		return nil, errors.New("config url scheme error")
	}
	wsUrl.Path = "/ws"
	dialer := common.DefaultDialer
	dialer.EnableCompression = config.enableCompression
	conn, _, err := dialer.Dial(wsUrl.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("dial ws error: %s", err)
	}
	conn.EnableWriteCompression(config.enableCompression)
	s := Schemaless{
		client:       client.NewClient(conn, config.chanLength),
		sendList:     list.New(),
		url:          wsUrl.String(),
		user:         config.user,
		password:     config.password,
		db:           config.db,
		closeChan:    make(chan struct{}),
		errorHandler: config.errorHandler,
		dialer:       &dialer,
		chanLength:   config.chanLength,
	}

	if config.autoReconnect {
		s.autoReconnect = true
		s.reconnectIntervalMs = config.reconnectIntervalMs
		s.reconnectRetryCount = config.reconnectRetryCount
	}

	if config.readTimeout > 0 {
		s.readTimeout = config.readTimeout
	}

	if config.writeTimeout > 0 {
		s.writeTimeout = config.writeTimeout
	}

	if err = connect(conn, s.user, s.password, s.db, s.writeTimeout, s.readTimeout); err != nil {
		return nil, fmt.Errorf("connect ws error: %s", err)
	}
	s.initClient(s.client)

	return &s, nil
}

func (s *Schemaless) initClient(c *client.Client) {
	if s.writeTimeout > 0 {
		c.WriteWait = s.writeTimeout
	}
	c.ErrorHandler = s.handleError
	c.TextMessageHandler = s.handleTextMessage

	go c.ReadPump()
	go c.WritePump()
}

func (s *Schemaless) reconnect() error {
	reconnected := false
	for i := 0; i < s.reconnectRetryCount; i++ {
		time.Sleep(time.Duration(s.reconnectIntervalMs) * time.Millisecond)
		conn, _, err := s.dialer.Dial(s.url, nil)
		if err != nil {
			continue
		}
		conn.EnableWriteCompression(s.dialer.EnableCompression)
		if err = connect(conn, s.user, s.password, s.db, s.writeTimeout, s.readTimeout); err != nil {
			_ = conn.Close()
			continue
		}
		if s.client != nil {
			s.client.Close()
		}
		c := client.NewClient(conn, s.chanLength)
		s.initClient(c)
		s.client = c
		reconnected = true
		break
	}
	if !reconnected {
		if s.client != nil {
			s.client.Close()
		}
		return errors.New("reconnect failed")
	}
	return nil
}

func (s *Schemaless) Insert(lines string, protocol int, precision string, ttl int, reqID int64) error {
	if reqID == 0 {
		reqID = common.GetReqID()
	}
	req := &schemalessReq{
		ReqID:     uint64(reqID),
		DB:        s.db,
		Protocol:  protocol,
		Precision: precision,
		TTL:       ttl,
		Data:      lines,
	}

	args, err := client.JsonI.Marshal(req)
	if err != nil {
		return err
	}
	action := &client.WSAction{Action: insertAction, Args: args}
	envelope := client.GlobalEnvelopePool.Get()
	defer client.GlobalEnvelopePool.Put(envelope)
	err = client.JsonI.NewEncoder(envelope.Msg).Encode(action)
	if err != nil {
		return err
	}
	respBytes, err := s.sendText(uint64(reqID), envelope)
	if err != nil {
		if !s.autoReconnect {
			return err
		}
		var opError *net.OpError
		if errors.Is(err, client.ClosedError) || errors.As(err, &opError) {
			err = s.reconnect()
			if err != nil {
				return err
			}
			respBytes, err = s.sendText(uint64(reqID), envelope)
			if err != nil {
				return err
			}
		} else {
			return err
		}
	}
	var resp schemalessResp
	err = client.JsonI.Unmarshal(respBytes, &resp)
	return client.HandleResponseError(err, resp.Code, resp.Message)
}

func (s *Schemaless) Close() {
	s.once.Do(func() {
		close(s.closeChan)
		if s.client != nil {
			s.client.Close()
		}
		s.client = nil
	})
}

var (
	//revive:disable-next-line
	ConnectTimeoutErr = errors.New("schemaless connect timeout")
)

func connect(ws *websocket.Conn, user string, password string, db string, writeTimeout time.Duration, readTimeout time.Duration) error {
	req := &wsConnectReq{
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
		Action: connAction,
		Args:   args,
	}
	connectAction, err := client.JsonI.Marshal(action)
	if err != nil {
		return err
	}
	_ = ws.SetWriteDeadline(time.Now().Add(writeTimeout))
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
	var resp wsConnectResp
	err = client.JsonI.Unmarshal(respBytes, &resp)
	return client.HandleResponseError(err, resp.Code, resp.Message)
}

func (s *Schemaless) sendText(reqID uint64, envelope *client.Envelope) ([]byte, error) {
	envelope.Type = websocket.TextMessage
	return s.send(reqID, envelope)
}

func (s *Schemaless) send(reqID uint64, envelope *client.Envelope) ([]byte, error) {
	channel := &IndexedChan{
		index:   reqID,
		channel: make(chan []byte, 1),
	}
	element := s.addMessageOutChan(channel)
	err := s.client.Send(envelope)
	if err != nil {
		s.lock.Lock()
		s.sendList.Remove(element)
		s.lock.Unlock()
		return nil, err
	}
	err = <-envelope.ErrorChan
	if err != nil {
		s.lock.Lock()
		s.sendList.Remove(element)
		s.lock.Unlock()
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.readTimeout)
	defer cancel()
	select {
	case <-s.closeChan:
		return nil, errors.New("connection closed")
	case resp := <-channel.channel:
		return resp, nil
	case <-ctx.Done():
		s.lock.Lock()
		s.sendList.Remove(element)
		s.lock.Unlock()
		return nil, fmt.Errorf("message timeout :%s", envelope.Msg.String())
	}
}

type IndexedChan struct {
	index   uint64
	channel chan []byte
}

func (s *Schemaless) addMessageOutChan(outChan *IndexedChan) *list.Element {
	s.lock.Lock()
	defer s.lock.Unlock()
	element := s.sendList.PushBack(outChan)
	return element
}

func (s *Schemaless) handleTextMessage(message []byte) {
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
	s.lock.Lock()
	defer s.lock.Unlock()

	element := s.findOutChanByID(reqID)
	if element != nil {
		element.Value.(*IndexedChan).channel <- message
		s.sendList.Remove(element)
	}
}

func (s *Schemaless) findOutChanByID(index uint64) *list.Element {
	root := s.sendList.Front()
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

func (s *Schemaless) handleError(err error) {
	if s.errorHandler != nil {
		s.errorHandler(err)
	}
}
