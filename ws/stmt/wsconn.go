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
	"github.com/taosdata/driver-go/v3/ws/client"
)

// WSConn is a websocket connection, which is used to communicate with the server.
// Deprecated: use unified.Client internals from package ws/unified instead.
type WSConn struct {
	client       *client.Client
	listLock     sync.RWMutex
	sendChanList *list.List
	writeTimeout time.Duration
	readTimeout  time.Duration
	closeChan    chan struct{}
	closeOnce    sync.Once
}

// Deprecated: use unified.NewClient from package ws/unified instead.
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
	c.client.AsyncCallbacks = false
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
		c.sendChanList.Remove(element)
		ch := element.Value.(*IndexedChan).channel
		c.listLock.Unlock()
		ch <- message
		return
	}
	c.listLock.Unlock()
}

func (c *WSConn) handleBinaryMessage(message []byte) {
	reqID := binary.LittleEndian.Uint64(message[8:16])
	c.listLock.Lock()
	element := c.findOutChanByID(reqID)
	if element != nil {
		c.sendChanList.Remove(element)
		ch := element.Value.(*IndexedChan).channel
		c.listLock.Unlock()
		ch <- message
		return
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

func (c *WSConn) sendText(reqID uint64, envelope *client.Envelope) ([]byte, error) {
	envelope.Type = websocket.TextMessage
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
		c.removeMessageOutChan(element)
		return nil, err
	}
	err = <-envelope.ErrorChan
	if err != nil {
		c.removeMessageOutChan(element)
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.readTimeout)
	defer cancel()
	if resp, ok := tryReadWSResponse(channel.channel); ok {
		return resp, nil
	}
	select {
	case resp := <-channel.channel:
		return resp, nil
	case <-c.closeChan:
		if resp, ok := tryReadWSResponse(channel.channel); ok {
			return resp, nil
		}
		c.removeMessageOutChan(element)
		if resp, ok := tryReadWSResponse(channel.channel); ok {
			return resp, nil
		}
		return nil, errors.New("connection closed")
	case <-c.client.Done():
		if resp, ok := tryReadWSResponse(channel.channel); ok {
			return resp, nil
		}
		c.removeMessageOutChan(element)
		if resp, ok := tryReadWSResponse(channel.channel); ok {
			return resp, nil
		}
		return nil, client.WrapClosedError(client.ClosedError, c.client.LastError())
	case <-ctx.Done():
		c.removeMessageOutChan(element)
		return nil, fmt.Errorf("message timeout :%s", envelope.Msg.String())
	}
}

func tryReadWSResponse(ch <-chan []byte) ([]byte, bool) {
	select {
	case resp := <-ch:
		return resp, true
	default:
		return nil, false
	}
}

func (c *WSConn) addMessageOutChan(outChan *IndexedChan) *list.Element {
	c.listLock.Lock()
	element := c.sendChanList.PushBack(outChan)
	c.listLock.Unlock()
	return element
}

func (c *WSConn) removeMessageOutChan(element *list.Element) {
	c.listLock.Lock()
	defer c.listLock.Unlock()
	c.sendChanList.Remove(element)
}

// Deprecated: use unified.Client internals from package ws/unified instead.
func (c *WSConn) Close() {
	c.closeOnce.Do(func() {
		close(c.closeChan)
		c.client.Close()
	})
}
