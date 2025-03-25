package stmt

import (
	"container/list"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	jsoniter "github.com/json-iterator/go"
	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/ws/client"
)

// WSConn is a websocket connection, which is used to communicate with the server.
type WSConn struct {
	client       *client.Client
	listLock     sync.RWMutex
	sendChanList *list.List
	writeTimeout time.Duration
	readTimeout  time.Duration
	closeChan    chan struct{}
	closeOnce    sync.Once
}

func NewWSConn(client *client.Client, writeTimeout time.Duration, readTimeout time.Duration) *WSConn {
	return &WSConn{
		client:       client,
		sendChanList: list.New(),
		listLock:     sync.RWMutex{},
		writeTimeout: writeTimeout,
		readTimeout:  readTimeout,
		closeChan:    make(chan struct{}),
	}
}

func (c *WSConn) initClient() {
	if c.writeTimeout > 0 {
		c.client.WriteWait = c.writeTimeout
	}
	c.client.TextMessageHandler = c.handleTextMessage
	c.client.BinaryMessageHandler = c.handleBinaryMessage
	go c.client.WritePump()
	go c.client.ReadPump()
}

type IndexedChan struct {
	index   uint64
	channel chan []byte
}

func (c *WSConn) handleTextMessage(message []byte) {
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

func (c *WSConn) handleBinaryMessage(message []byte) {
	reqID := binary.LittleEndian.Uint64(message[8:16])
	c.listLock.Lock()
	element := c.findOutChanByID(reqID)
	if element != nil {
		element.Value.(*IndexedChan).channel <- message
		c.sendChanList.Remove(element)
	}
	c.listLock.Unlock()
}

func (c *WSConn) findOutChanByID(index uint64) *list.Element {
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

func (c *WSConn) generateReqID() uint64 {
	return uint64(common.GetReqID())
}

func (c *WSConn) sendText(reqID uint64, envelope *client.Envelope) ([]byte, error) {
	envelope.Type = websocket.TextMessage
	return c.send(reqID, envelope)
}

func (c *WSConn) sendBinary(reqID uint64, envelope *client.Envelope) ([]byte, error) {
	envelope.Type = websocket.BinaryMessage
	return c.send(reqID, envelope)
}

func (c *WSConn) send(reqID uint64, envelope *client.Envelope) ([]byte, error) {
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

func (c *WSConn) sendTextWithoutResp(envelope *client.Envelope) {
	envelope.Type = websocket.TextMessage
	err := c.client.Send(envelope)
	if err != nil {
		return
	}
	<-envelope.ErrorChan
}

func (c *WSConn) addMessageOutChan(outChan *IndexedChan) *list.Element {
	c.listLock.Lock()
	element := c.sendChanList.PushBack(outChan)
	c.listLock.Unlock()
	return element
}

func (c *WSConn) Close() {
	c.closeOnce.Do(func() {
		close(c.closeChan)
		c.client.Close()
	})
}
