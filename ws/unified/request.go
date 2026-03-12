package unified

import (
	"bytes"
	"container/list"
	"context"
	"time"

	"github.com/taosdata/driver-go/v3/ws/client"
)

// sendEnvelopeWithRuntime sends one request on a specific runtime and waits for one routed response.
// It returns whether the websocket write has been acknowledged and the runtime generation used.
func (c *Client) sendEnvelopeWithRuntime(runtime *client.Client, reqID uint64, envelope *client.Envelope, timeout time.Duration, timeoutErr error) ([]byte, bool, uint64, error) {
	if runtime == nil {
		return nil, false, 0, client.ClosedError
	}
	if timeout <= 0 {
		timeout = c.config.ReadTimeout
	}
	if timeout <= 0 {
		timeout = c.config.MessageTimeout
	}
	if timeoutErr == nil {
		timeoutErr = ErrQueryMessageTimeout
	}

	respChan := make(chan []byte, 1)
	pendingReq := &PendingRequest{
		reqID:   reqID,
		channel: respChan,
	}
	var element *list.Element
	var runtimeGen uint64

	// Keep runtime validation, generation read, and pending registration in one critical section.
	c.lock.RLock()
	c.pendingLock.Lock()
	if c.runtime != runtime {
		c.pendingLock.Unlock()
		c.lock.RUnlock()
		return nil, false, 0, client.ClosedError
	}
	runtimeGen = c.runtimeGen
	pendingReq.runtimeGen = runtimeGen
	element = c.pendingRequests.PushBack(pendingReq)
	c.pendingLock.Unlock()
	c.lock.RUnlock()

	defer func() {
		c.pendingLock.Lock()
		c.pendingRequests.Remove(element)
		c.pendingLock.Unlock()
	}()

	err := runtime.Send(envelope)
	if err != nil {
		return nil, false, runtimeGen, err
	}

	err = <-envelope.ErrorChan
	if err != nil {
		return nil, false, runtimeGen, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	select {
	case resp := <-respChan:
		if resp == nil {
			// nil means connection was lost during runtime swap
			return nil, true, runtimeGen, client.ClosedError
		}
		return resp, true, runtimeGen, nil
	case <-runtime.Done():
		// Prefer an already-routed response over disconnect if both race.
		select {
		case resp := <-respChan:
			if resp == nil {
				return nil, true, runtimeGen, client.ClosedError
			}
			return resp, true, runtimeGen, nil
		default:
		}
		return nil, true, runtimeGen, client.ClosedError
	case <-ctx.Done():
		// Prefer an already-routed response over timeout if both race.
		select {
		case resp := <-respChan:
			if resp == nil {
				return nil, true, runtimeGen, client.ClosedError
			}
			return resp, true, runtimeGen, nil
		default:
		}
		return nil, true, runtimeGen, timeoutErr
	}
}

// sendEnvelopeNoResponse sends one request on a specific runtime and only waits for write-ack.
func (c *Client) sendEnvelopeNoResponse(runtime *client.Client, envelope *client.Envelope) error {
	if runtime == nil {
		return client.ClosedError
	}

	c.lock.RLock()
	runtimeMatched := c.runtime == runtime
	c.lock.RUnlock()
	if !runtimeMatched {
		return client.ClosedError
	}

	if err := runtime.Send(envelope); err != nil {
		return err
	}
	return <-envelope.ErrorChan
}

func writeUint64(buffer *bytes.Buffer, v uint64) {
	buffer.WriteByte(byte(v))
	buffer.WriteByte(byte(v >> 8))
	buffer.WriteByte(byte(v >> 16))
	buffer.WriteByte(byte(v >> 24))
	buffer.WriteByte(byte(v >> 32))
	buffer.WriteByte(byte(v >> 40))
	buffer.WriteByte(byte(v >> 48))
	buffer.WriteByte(byte(v >> 56))
}

func writeUint32(buffer *bytes.Buffer, v uint32) {
	buffer.WriteByte(byte(v))
	buffer.WriteByte(byte(v >> 8))
	buffer.WriteByte(byte(v >> 16))
	buffer.WriteByte(byte(v >> 24))
}

func writeUint16(buffer *bytes.Buffer, v uint16) {
	buffer.WriteByte(byte(v))
	buffer.WriteByte(byte(v >> 8))
}
