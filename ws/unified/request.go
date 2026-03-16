package unified

import (
	"bytes"
	"context"
	"time"

	"github.com/taosdata/driver-go/v3/ws/client"
)

var errNilEnvelope = &Error{
	Type:    ErrorTypeInvalidState,
	Message: "nil envelope",
}

// sendEnvelopeWithRuntime sends one request on a specific runtime and waits for one routed response.
// It returns whether the websocket write has been acknowledged and the runtime generation used.
// Timeout only bounds local waiting for a routed response. It does not cancel an in-flight
// websocket write already queued in the runtime send path.
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
	pendingReq := &pendingRequest{
		reqID:   reqID,
		channel: respChan,
	}
	var runtimeGen uint64

	// Fast path: first atomic snapshot read is an early reject optimization so
	// stale runtimes can fail without contending on pendingLock.
	// We re-check snapshot again after pendingLock is held to close the TOCTOU
	// window before registering pendingReq.
	if snapshot, ok := c.loadRuntimeSnapshotAtomic(); ok {
		if snapshot.runtime != runtime {
			return nil, false, 0, client.ClosedError
		}
		runtimeGen = snapshot.generation

		c.pendingLock.Lock()
		currentSnapshot, currentOK := c.loadRuntimeSnapshotAtomic()
		if !currentOK || currentSnapshot.runtime != runtime || currentSnapshot.generation != runtimeGen {
			c.pendingLock.Unlock()
			return nil, false, 0, client.ClosedError
		}
		if c.pendingRequests == nil {
			c.pendingRequests = make(map[uint64]*pendingRequest)
		}
		c.pendingRequests[reqID] = pendingReq
		c.pendingLock.Unlock()
	} else {
		// Compatibility fallback for tests that create zero-value Client literals.
		// Keep c.lock -> pendingLock order with swapRuntime.
		c.lock.RLock()
		c.pendingLock.Lock()
		if c.runtime != runtime {
			c.pendingLock.Unlock()
			c.lock.RUnlock()
			return nil, false, 0, client.ClosedError
		}
		runtimeGen = c.runtimeGen
		if c.pendingRequests == nil {
			c.pendingRequests = make(map[uint64]*pendingRequest)
		}
		c.pendingRequests[reqID] = pendingReq
		c.pendingLock.Unlock()
		c.lock.RUnlock()
	}

	defer func() {
		_ = c.removePendingRequest(reqID, pendingReq)
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
		// A timeout here means caller stop-waiting, not guaranteed server-side cancellation.
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
	if envelope == nil {
		return errNilEnvelope
	}

	if snapshot, ok := c.loadRuntimeSnapshotAtomic(); ok {
		if snapshot.runtime != runtime {
			return client.ClosedError
		}
	} else {
		c.lock.RLock()
		runtimeMatched := c.runtime == runtime
		c.lock.RUnlock()
		if !runtimeMatched {
			return client.ClosedError
		}
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
