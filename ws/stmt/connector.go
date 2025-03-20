package stmt

import (
	"context"
	"errors"
	"net"
	"net/url"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/ws/client"
)

type Connector struct {
	client              *WSConn
	writeTimeout        time.Duration
	readTimeout         time.Duration
	config              *Config
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
	closed              bool
	sync.Mutex
}

var (
	//revive:disable-next-line
	ConnectTimeoutErr = errors.New("stmt connect timeout")
	ErrConnIsClosed   = errors.New("stmt Connector is closed")
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
			_ = ws.Close()
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
	wsConn := NewWSConn(wsClient, writeTimeout, readTimeout)
	connector = &Connector{
		client:              wsConn,
		writeTimeout:        writeTimeout,
		readTimeout:         readTimeout,
		config:              config,
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
	wsClient.ErrorHandler = connector.handleError
	wsConn.initClient()
	return connector, nil
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
	var resp ConnectResp
	err = client.JsonI.Unmarshal(respBytes, &resp)
	return client.HandleResponseError(err, resp.Code, resp.Message)
}

func (c *Connector) handleError(err error) {
	if c.customErrorHandler != nil {
		c.customErrorHandler(c, err)
	}
	//c.Close()
}

func (c *Connector) generateReqID() uint64 {
	return uint64(common.GetReqID())
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
			_ = conn.Close()
			continue
		}
		if c.client != nil {
			c.client.Close()
		}
		cl := client.NewClient(conn, c.chanLength)
		cl.ErrorHandler = c.handleError
		wsConn := NewWSConn(cl, c.writeTimeout, c.readTimeout)
		wsConn.initClient()
		reconnected = true
		c.client = wsConn
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
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return nil, ErrConnIsClosed
	}
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
	respBytes, err := c.client.sendText(reqID, envelope)
	if err != nil {
		if !c.autoReconnect {
			return nil, err
		}

		var opError *net.OpError
		if !c.client.client.IsRunning() || errors.Is(err, client.ClosedError) || errors.As(err, &opError) {
			err = c.reconnect()
			if err != nil {
				return nil, err
			}
			respBytes, err = c.client.sendText(reqID, envelope)
			if err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	}
	var resp InitResp
	err = client.JsonI.Unmarshal(respBytes, &resp)
	err = client.HandleResponseError(err, resp.Code, resp.Message)
	if err != nil {
		return nil, err
	}
	s := &Stmt{
		id:        resp.StmtID,
		connector: c.client,
	}
	return s, nil
}

func (c *Connector) Close() error {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	c.client.Close()
	if c.customCloseHandler != nil {
		c.customCloseHandler()
	}
	return nil
}
