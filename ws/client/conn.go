package client

import (
	"bytes"
	"encoding/json"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	jsoniter "github.com/json-iterator/go"
)

const BufferSize4M = 4 * 1024 * 1024
const DefaultMessageTimeout = time.Minute * 5
const DefaultPongWait = 60 * time.Second
const DefaultPingPeriod = (60 * time.Second * 9) / 10
const DefaultWriteWait = 10 * time.Second

const (
	StatusNormal = uint32(1)
	StatusStop   = uint32(2)
)

var JsonI = jsoniter.ConfigCompatibleWithStandardLibrary

var DefaultDialer = websocket.Dialer{
	Proxy:            http.ProxyFromEnvironment,
	HandshakeTimeout: 45 * time.Second,
	ReadBufferSize:   BufferSize4M,
	WriteBufferSize:  BufferSize4M,
	WriteBufferPool:  &sync.Pool{},
}

type WSAction struct {
	Action string          `json:"action"`
	Args   json.RawMessage `json:"args"`
}

var GlobalEnvelopePool EnvelopePool

type EnvelopePool struct {
	p sync.Pool
}

func (ep *EnvelopePool) Get() *Envelope {
	epv := ep.p.Get()
	if epv == nil {
		return &Envelope{Msg: new(bytes.Buffer)}
	}
	return epv.(*Envelope)
}

func (ep *EnvelopePool) Put(epv *Envelope) {
	epv.Reset()
	ep.p.Put(epv)
}

type Envelope struct {
	Type int
	Msg  *bytes.Buffer
}

func (e *Envelope) Reset() {
	e.Msg.Reset()
}

type Client struct {
	conn                 *websocket.Conn
	status               uint32
	sendChan             chan *Envelope
	BufferSize           int
	WriteWait            time.Duration
	PingPeriod           time.Duration
	PongWait             time.Duration
	TextMessageHandler   func(message []byte)
	BinaryMessageHandler func(message []byte)
	ErrorHandler         func(err error)
	SendMessageHandler   func(envelope *Envelope)
	once                 sync.Once
}

func NewClient(conn *websocket.Conn, sendChanLength uint) *Client {
	return &Client{
		conn:                 conn,
		status:               StatusNormal,
		BufferSize:           BufferSize4M,
		sendChan:             make(chan *Envelope, sendChanLength),
		WriteWait:            DefaultWriteWait,
		PingPeriod:           DefaultPingPeriod,
		PongWait:             DefaultPongWait,
		TextMessageHandler:   func(message []byte) {},
		BinaryMessageHandler: func(message []byte) {},
		ErrorHandler:         func(err error) {},
		SendMessageHandler: func(envelope *Envelope) {
			GlobalEnvelopePool.Put(envelope)
		},
		once: sync.Once{},
	}
}

func (c *Client) ReadPump() {
	c.conn.SetReadLimit(BufferSize4M)
	c.conn.SetReadDeadline(time.Now().Add(c.PongWait))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(c.PongWait))
		return nil
	})
	c.conn.SetCloseHandler(nil)
	for {
		messageType, message, err := c.conn.ReadMessage()
		if err != nil {
			if e, ok := err.(*websocket.CloseError); ok && e.Code == websocket.CloseAbnormalClosure {
				break
			}
			c.ErrorHandler(err)
			break
		}
		switch messageType {
		case websocket.TextMessage:
			c.TextMessageHandler(message)
		case websocket.BinaryMessage:
			c.BinaryMessageHandler(message)
		}
	}
}

func (c *Client) WritePump() {
	ticker := time.NewTicker(c.PingPeriod)
	defer func() {
		ticker.Stop()
	}()
	for {
		select {
		case message, ok := <-c.sendChan:
			c.conn.SetWriteDeadline(time.Now().Add(c.WriteWait))
			if !ok {
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			err := c.conn.WriteMessage(message.Type, message.Msg.Bytes())
			if err != nil {
				c.ErrorHandler(err)
				return
			}
			c.SendMessageHandler(message)
		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(c.WriteWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

func (c *Client) Send(envelope *Envelope) {
	if !c.IsRunning() {
		return
	}
	defer func() {
		// maybe closed
		if recover() != nil {
			return
		}
	}()
	c.sendChan <- envelope
}

func (c *Client) GetEnvelope() *Envelope {
	return GlobalEnvelopePool.Get()
}

func (c *Client) PutEnvelope(envelope *Envelope) {
	GlobalEnvelopePool.Put(envelope)
}

func (c *Client) IsRunning() bool {
	return atomic.LoadUint32(&c.status) == StatusNormal
}

func (c *Client) Close() {
	c.once.Do(func() {
		close(c.sendChan)
		atomic.StoreUint32(&c.status, StatusStop)
	})
}
