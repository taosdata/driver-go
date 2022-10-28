package client

import (
	"bytes"
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	jsoniter "github.com/json-iterator/go"
	"github.com/taosdata/driver-go/v3/common"
)

const (
	StatusNormal = uint32(1)
	StatusStop   = uint32(2)
)

var JsonI = jsoniter.ConfigCompatibleWithStandardLibrary

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
	errHandlerOnce       sync.Once
}

func NewClient(conn *websocket.Conn, sendChanLength uint) *Client {
	return &Client{
		conn:                 conn,
		status:               StatusNormal,
		BufferSize:           common.BufferSize4M,
		sendChan:             make(chan *Envelope, sendChanLength),
		WriteWait:            common.DefaultWriteWait,
		PingPeriod:           common.DefaultPingPeriod,
		PongWait:             common.DefaultPongWait,
		TextMessageHandler:   func(message []byte) {},
		BinaryMessageHandler: func(message []byte) {},
		ErrorHandler:         func(err error) {},
		SendMessageHandler: func(envelope *Envelope) {
			GlobalEnvelopePool.Put(envelope)
		},
	}
}

func (c *Client) ReadPump() {
	c.conn.SetReadLimit(common.BufferSize4M)
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
			c.handleError(err)
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
			if !ok {
				return
			}
			c.conn.SetWriteDeadline(time.Now().Add(c.WriteWait))
			err := c.conn.WriteMessage(message.Type, message.Msg.Bytes())
			if err != nil {
				c.handleError(err)
				return
			}
			c.SendMessageHandler(message)
		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(c.WriteWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				c.handleError(err)
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
		if c.conn != nil {
			c.conn.Close()
		}
	})
}

func (c *Client) handleError(err error) {
	c.errHandlerOnce.Do(func() { c.ErrorHandler(err) })
}
