package schemaless

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"net/url"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	jsoniter "github.com/json-iterator/go"
	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/common/tdversion"
	"github.com/taosdata/driver-go/v3/ws/client"
	wsreconnect "github.com/taosdata/driver-go/v3/ws/internal/reconnect"
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
	totpCode            string
	bearerToken         string
	readTimeout         time.Duration
	writeTimeout        time.Duration
	lock                sync.Mutex
	clientLock          sync.RWMutex
	reconnectLock       sync.Mutex
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
	if err = tdversion.WSCheckVersion(conn); err != nil {
		_ = conn.Close()
		return nil, err
	}
	s := Schemaless{
		client:       client.NewClient(conn, config.chanLength),
		sendList:     list.New(),
		url:          wsUrl.String(),
		user:         config.user,
		password:     config.password,
		db:           config.db,
		totpCode:     config.totpCode,
		bearerToken:  config.bearerToken,
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

	if err = connect(conn, s.user, s.password, s.db, s.totpCode, s.bearerToken, s.writeTimeout, s.readTimeout); err != nil {
		return nil, fmt.Errorf("connect ws error: %s", err)
	}
	s.initClient(s.client)

	return &s, nil
}

func (s *Schemaless) initClient(c *client.Client) {
	if s.writeTimeout > 0 {
		c.WriteWait = s.writeTimeout
	}
	c.AsyncCallbacks = false
	c.ErrorHandler = s.handleError
	c.TextMessageHandler = s.handleTextMessage

	go c.ReadPump()
	go c.WritePump()
}

func (s *Schemaless) reconnect(failedClient *client.Client) error {
	s.reconnectLock.Lock()
	defer s.reconnectLock.Unlock()
	if s.isClosed() {
		return SchemalessClosedErr
	}
	if wsreconnect.HasHealthyReplacement(s.loadClient(), failedClient) {
		return nil
	}
	reconnected := false
	for i := 0; i < s.reconnectRetryCount; i++ {
		if s.isClosed() {
			return SchemalessClosedErr
		}
		time.Sleep(time.Duration(s.reconnectIntervalMs) * time.Millisecond)
		if s.isClosed() {
			return SchemalessClosedErr
		}
		conn, _, err := s.dialer.Dial(s.url, nil)
		if err != nil {
			continue
		}
		conn.EnableWriteCompression(s.dialer.EnableCompression)
		if err = connect(conn, s.user, s.password, s.db, s.totpCode, s.bearerToken, s.writeTimeout, s.readTimeout); err != nil {
			_ = conn.Close()
			continue
		}
		if tdversion.WSCheckVersion(conn) != nil {
			_ = conn.Close()
			continue
		}
		if s.isClosed() {
			_ = conn.Close()
			return SchemalessClosedErr
		}
		c := client.NewClient(conn, s.chanLength)
		s.initClient(c)
		oldClient, ok := wsreconnect.ReplaceClientOrClose(c, s.replaceClient)
		if !ok {
			return SchemalessClosedErr
		}
		wsreconnect.CloseClient(oldClient)
		reconnected = true
		break
	}
	if !reconnected {
		wsreconnect.CloseMatchedClient(s.clearClientIf, failedClient)
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
	respBytes, err := s.sendTextWithReconnect(uint64(reqID), envelope)
	if err != nil {
		return err
	}
	var resp schemalessResp
	err = client.JsonI.Unmarshal(respBytes, &resp)
	return client.HandleResponseError(err, resp.Code, resp.Message)
}

func (s *Schemaless) Close() {
	s.once.Do(func() {
		close(s.closeChan)
		if currentClient := s.clearClient(); currentClient != nil {
			currentClient.Close()
		}
	})
}

var (
	//revive:disable-next-line
	ConnectTimeoutErr = errors.New("schemaless connect timeout")
	//revive:disable-next-line
	SchemalessClosedErr = errors.New("connection closed")
)

func connect(ws *websocket.Conn, user string, password string, db string, totpCode string, bearerToken string, writeTimeout time.Duration, readTimeout time.Duration) error {
	req := &wsConnectReq{
		ReqID:       0,
		User:        user,
		Password:    password,
		DB:          db,
		TOTPCode:    totpCode,
		BearerToken: bearerToken,
		App:         common.GetProcessName(),
		Connector:   common.GetConnectorInfo("ws"),
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
	resp, _, _, err := s.sendTextWithClient(reqID, envelope)
	return resp, err
}

func (s *Schemaless) sendTextWithReconnect(reqID uint64, envelope *client.Envelope) ([]byte, error) {
	respBytes, failedClient, writeAcked, err := s.sendTextWithClient(reqID, envelope)
	if err == nil {
		return respBytes, nil
	}
	if !s.autoReconnect {
		return nil, err
	}
	if s.isClosed() {
		return nil, SchemalessClosedErr
	}
	// Schemaless insert is not safe to replay once the websocket write completed.
	if writeAcked {
		return nil, err
	}
	if !wsreconnect.IsReconnectableError(err) {
		return nil, err
	}
	if err = s.reconnect(failedClient); err != nil {
		return nil, err
	}
	respBytes, _, _, err = s.sendTextWithClient(reqID, envelope)
	if err != nil {
		return nil, err
	}
	return respBytes, nil
}

func (s *Schemaless) sendTextWithClient(reqID uint64, envelope *client.Envelope) ([]byte, *client.Client, bool, error) {
	envelope.Type = websocket.TextMessage
	return s.sendWithClient(reqID, envelope)
}

func (s *Schemaless) send(reqID uint64, envelope *client.Envelope) ([]byte, error) {
	resp, _, _, err := s.sendWithClient(reqID, envelope)
	return resp, err
}

func (s *Schemaless) sendWithClient(reqID uint64, envelope *client.Envelope) ([]byte, *client.Client, bool, error) {
	currentClient := s.loadClient()
	if currentClient == nil {
		if s.isClosed() {
			return nil, nil, false, SchemalessClosedErr
		}
		return nil, nil, false, client.ClosedError
	}
	channel := &IndexedChan{
		index:   reqID,
		channel: make(chan []byte, 1),
	}
	element := s.addMessageOutChan(channel)
	err := currentClient.Send(envelope)
	if err != nil {
		s.lock.Lock()
		s.sendList.Remove(element)
		s.lock.Unlock()
		return nil, currentClient, false, err
	}
	err = <-envelope.ErrorChan
	if err != nil {
		s.lock.Lock()
		s.sendList.Remove(element)
		s.lock.Unlock()
		return nil, currentClient, false, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.readTimeout)
	defer cancel()
	if resp, ok := tryReadSchemalessResponse(channel.channel); ok {
		return resp, currentClient, true, nil
	}
	select {
	case resp := <-channel.channel:
		return resp, currentClient, true, nil
	case <-s.closeChan:
		if resp, ok := tryReadSchemalessResponse(channel.channel); ok {
			return resp, currentClient, true, nil
		}
		s.lock.Lock()
		s.sendList.Remove(element)
		s.lock.Unlock()
		if resp, ok := tryReadSchemalessResponse(channel.channel); ok {
			return resp, currentClient, true, nil
		}
		return nil, currentClient, true, SchemalessClosedErr
	case <-currentClient.Done():
		if resp, ok := tryReadSchemalessResponse(channel.channel); ok {
			return resp, currentClient, true, nil
		}
		s.lock.Lock()
		s.sendList.Remove(element)
		s.lock.Unlock()
		if resp, ok := tryReadSchemalessResponse(channel.channel); ok {
			return resp, currentClient, true, nil
		}
		if s.isClosed() {
			return nil, currentClient, true, SchemalessClosedErr
		}
		return nil, currentClient, true, client.WrapClosedError(client.ClosedError, currentClient.LastError())
	case <-ctx.Done():
		s.lock.Lock()
		s.sendList.Remove(element)
		s.lock.Unlock()
		return nil, currentClient, true, fmt.Errorf("message timeout :%s", envelope.Msg.String())
	}
}

func tryReadSchemalessResponse(ch <-chan []byte) ([]byte, bool) {
	select {
	case resp := <-ch:
		return resp, true
	default:
		return nil, false
	}
}

func (s *Schemaless) isClosed() bool {
	select {
	case <-s.closeChan:
		return true
	default:
		return false
	}
}

func (s *Schemaless) loadClient() *client.Client {
	s.clientLock.RLock()
	currentClient := s.client
	s.clientLock.RUnlock()
	return currentClient
}

func (s *Schemaless) replaceClient(next *client.Client) (*client.Client, bool) {
	s.clientLock.Lock()
	defer s.clientLock.Unlock()
	if s.isClosed() {
		return nil, false
	}
	currentClient := s.client
	s.client = next
	return currentClient, true
}

func (s *Schemaless) clearClient() *client.Client {
	s.clientLock.Lock()
	currentClient := s.client
	s.client = nil
	s.clientLock.Unlock()
	return currentClient
}

func (s *Schemaless) clearClientIf(target *client.Client) *client.Client {
	s.clientLock.Lock()
	defer s.clientLock.Unlock()
	if s.client != target {
		return nil
	}
	currentClient := s.client
	s.client = nil
	return currentClient
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
	element := s.findOutChanByID(reqID)
	if element != nil {
		s.sendList.Remove(element)
		ch := element.Value.(*IndexedChan).channel
		s.lock.Unlock()
		ch <- message
		return
	}
	s.lock.Unlock()
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
