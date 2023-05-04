package schemaless

import (
	"container/list"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	jsoniter "github.com/json-iterator/go"
	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/ws/client"
)

type Schemaless struct {
	client       *client.Client
	sendList     *list.List
	url          string
	user         string
	password     string
	db           string
	readTimeout  time.Duration
	lock         sync.Mutex
	once         sync.Once
	errorHandler func(error)
}

func NewSchemaless(config *Config) (*Schemaless, error) {
	wsUrl, err := url.Parse(config.url)
	if err != nil {
		return nil, fmt.Errorf("config url error: %s", err)
	}
	if wsUrl.Scheme != "ws" && wsUrl.Scheme != "wss" {
		return nil, errors.New("config url scheme error")
	}
	if len(wsUrl.Path) == 0 || wsUrl.Path != "/rest/schemaless" {
		wsUrl.Path = "/rest/schemaless"
	}
	ws, _, err := common.DefaultDialer.Dial(wsUrl.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("dial ws error: %s", err)
	}

	s := Schemaless{
		client:       client.NewClient(ws, config.chanLength),
		sendList:     list.New(),
		url:          config.url,
		user:         config.user,
		password:     config.password,
		db:           config.db,
		errorHandler: config.errorHandler,
	}

	if config.readTimeout > 0 {
		s.readTimeout = config.readTimeout
	}

	if config.writeTimeout > 0 {
		s.client.WriteWait = config.writeTimeout
	}
	s.client.ErrorHandler = s.handleError
	s.client.TextMessageHandler = s.handleTextMessage

	go s.client.ReadPump()
	go s.client.WritePump()

	if err = s.connect(); err != nil {
		return nil, fmt.Errorf("connect ws error: %s", err)
	}

	return &s, nil
}

func (s *Schemaless) Insert(lines string, protocol int, precision string, ttl int, reqID int64) error {
	if reqID == 0 {
		reqID = common.GetReqID()
	}
	req := schemalessReq{ReqID: uint64(reqID), DB: s.db, Protocol: protocol, Precision: precision, TTL: ttl, Data: lines}

	args, err := json.Marshal(req)
	if err != nil {
		return err
	}
	_, err = s.send(req.ReqID, &wsAction{Action: insertAction, Args: args})
	return err
}

func (s *Schemaless) Close() {
	s.once.Do(func() {
		if s.client != nil {
			s.client.Close()
		}
		s.client = nil
	})
}

func (s *Schemaless) connect() error {
	req := &wsConnectReq{
		ReqID:    uint64(common.GetReqID()),
		User:     s.user,
		Password: s.password,
		DB:       s.db,
	}
	args, err := json.Marshal(req)
	if err != nil {
		return err
	}

	_, err = s.send(req.ReqID, &wsAction{Action: connAction, Args: args})
	return err
}

func (s *Schemaless) send(reqID uint64, action *wsAction) ([]byte, error) {
	envelope := s.client.GetEnvelope()
	defer s.client.PutEnvelope(envelope)
	err := client.JsonI.NewEncoder(envelope.Msg).Encode(action)
	if err != nil {
		return nil, err
	}
	envelope.Type = websocket.TextMessage

	channel := &IndexedChan{index: reqID, channel: make(chan []byte, 1)}
	element := s.addMessageOutChan(channel)

	s.client.Send(envelope)

	ctx, cancel := context.WithTimeout(context.Background(), s.readTimeout)
	defer cancel()

	select {
	case resp := <-channel.channel:
		return resp, nil
	case <-ctx.Done():
		s.lock.Lock()
		defer s.lock.Unlock()
		s.sendList.Remove(element)
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
	s.Close()
}
