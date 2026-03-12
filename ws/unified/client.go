package unified

import (
	"container/list"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/ws/client"
)

var (
	defaultUnifiedErrHandler = func(error) {}
)

type BootstrapFunc func(conn *websocket.Conn) error

type DialFunc func(endpoint string) (*websocket.Conn, error)

type ClientFactory func(conn *websocket.Conn, chanLength uint) *client.Client

type Option func(c *Client)

// PendingRequest represents a pending request waiting for response.
type PendingRequest struct {
	reqID      uint64
	channel    chan []byte
	runtimeGen uint64 // runtime generation number when this request was created
}

// WithDialFunc overrides how websocket connections are created.
func WithDialFunc(dialFunc DialFunc) Option {
	return func(c *Client) {
		c.dial = dialFunc
	}
}

// WithClientFactory overrides runtime client construction after websocket bootstrap.
func WithClientFactory(factory ClientFactory) Option {
	return func(c *Client) {
		c.clientFactory = factory
	}
}

// Client is the shared websocket client holder used by adapters.
type Client struct {
	config        Config
	failover      *FailoverState
	dialer        *websocket.Dialer
	dial          DialFunc
	clientFactory ClientFactory

	lock         sync.RWMutex
	runtime      *client.Client
	closed       bool
	runtimeGen   uint64 // incremented on each runtime swap
	closeChan    chan struct{}
	errorHandler func(error)

	// normal connect support
	normalConnectLock sync.Mutex
	connected         bool

	// Message routing for request-response pattern
	pendingLock     sync.RWMutex
	pendingRequests *list.List

	// Reconnect protection
	reconnectLock sync.Mutex
	reconnecting  bool
	reconnectDone chan struct{}
	reconnectErr  error
}

// NewClient builds a failover-capable unified websocket client from normalized config input.
func NewClient(cfg *Config, defaultPath string, opts ...Option) (*Client, error) {
	if cfg == nil {
		return nil, ErrNilConfig
	}
	config := *cfg
	config.Endpoints = append([]string(nil), cfg.Endpoints...)
	if err := config.Normalize(defaultPath); err != nil {
		return nil, err
	}
	failoverState, err := NewFailoverState(config.Endpoints)
	if err != nil {
		return nil, err
	}
	dialer := common.DefaultDialer
	dialer.EnableCompression = config.EnableCompression
	c := &Client{
		config:          config,
		failover:        failoverState,
		dialer:          &dialer,
		clientFactory:   client.NewClient,
		pendingRequests: list.New(),
		closeChan:       make(chan struct{}),
		errorHandler:    defaultUnifiedErrHandler,
	}
	c.dial = c.dialWithDialer
	for i := 0; i < len(opts); i++ {
		opts[i](c)
	}
	return c, nil
}

// NewClientFromDSN builds Config from DSN and returns a unified Client.
func NewClientFromDSN(dsn string, defaultPath string, opts ...Option) (*Client, error) {
	cfg, err := NewConfigFromDSN(dsn, defaultPath)
	if err != nil {
		return nil, err
	}
	return NewClient(cfg, defaultPath, opts...)
}

// dialWithDialer dials endpoint using configured gorilla dialer options.
func (c *Client) dialWithDialer(endpoint string) (*websocket.Conn, error) {
	conn, _, err := c.dialer.Dial(endpoint, nil)
	if err != nil {
		return nil, err
	}
	conn.EnableWriteCompression(c.dialer.EnableCompression)
	return conn, nil
}

// ConnectWithBootstrap dials endpoints in initial order and replaces runtime client on success.
func (c *Client) ConnectWithBootstrap(bootstrap BootstrapFunc) error {
	return c.connectWithCandidates(c.failover.InitialCandidates(), bootstrap)
}

// ReconnectWithBootstrap dials endpoints in reconnect order and replaces runtime client on success.
// It prevents concurrent reconnect attempts using reconnectLock.
func (c *Client) ReconnectWithBootstrap(bootstrap BootstrapFunc) error {
	return c.reconnectWithBootstrap(bootstrap, nil)
}

// reconnectWithBootstrap performs reconnection with concurrent protection.
// If failedRuntime is provided, reconnect is skipped if current runtime is different and healthy.
func (c *Client) reconnectWithBootstrap(bootstrap BootstrapFunc, failedRuntime *client.Client) error {
	c.reconnectLock.Lock()

	// Check current runtime after acquiring lock
	currentRuntime := c.Runtime()
	// Check if current runtime is different from failed one and still healthy
	if failedRuntime != nil && currentRuntime != nil && currentRuntime != failedRuntime && currentRuntime.IsRunning() {
		c.reconnectLock.Unlock()
		return nil
	}

	// If reconnect is already in progress, wait for its completion.
	if c.reconnecting {
		done := c.reconnectDone
		c.reconnectLock.Unlock()

		select {
		case <-done:
		case <-c.closeChan:
			return ErrUnifiedClosed
		}

		c.reconnectLock.Lock()
		reconnectErr := c.reconnectErr
		c.reconnectLock.Unlock()

		runtime := c.Runtime()
		if runtime != nil && runtime.IsRunning() {
			return nil
		}
		if reconnectErr != nil {
			return reconnectErr
		}
		return ErrUnifiedConnectFailed
	}

	c.reconnecting = true
	c.reconnectDone = make(chan struct{})
	done := c.reconnectDone
	c.reconnectErr = nil
	c.reconnectLock.Unlock()

	err := c.connectWithCandidatesWithRetry(c.failover.ReconnectCandidates(), bootstrap)
	if err != nil && !errors.Is(err, ErrUnifiedClosed) && !IsReconnectFailedError(err) {
		err = &Error{
			Type:              ErrorTypeReconnectFailed,
			Message:           ErrUnifiedConnectFailed.Message,
			Cause:             err,
			ConnectionRelated: true,
			ReconnectFailed:   true,
		}
	}

	c.reconnectLock.Lock()
	c.reconnecting = false
	c.reconnectErr = err
	close(done)
	c.reconnectLock.Unlock()

	return err
}

// connectWithCandidates dials candidates until one succeeds and swaps in a new runtime.
func (c *Client) connectWithCandidates(candidates []EndpointCandidate, bootstrap BootstrapFunc) error {
	var lastErr error
	for i := 0; i < len(candidates); i++ {
		if c.IsClosed() {
			return ErrUnifiedClosed
		}
		candidate := candidates[i]
		conn, err := c.dial(candidate.URL)
		if err != nil {
			lastErr = err
			continue
		}
		if bootstrap != nil {
			if err = bootstrap(conn); err != nil {
				if conn != nil {
					_ = conn.Close()
				}
				lastErr = err
				continue
			}
		}
		nextRuntime := c.clientFactory(conn, c.config.ChanLength)
		oldRuntime, err := c.swapRuntime(nextRuntime, candidate.Index)
		if err != nil {
			nextRuntime.Close()
			lastErr = err
			continue
		}

		// Initialize the new runtime with handlers and pumps
		c.initializeRuntime(nextRuntime)

		if oldRuntime != nil {
			oldRuntime.Close()
		}
		return nil
	}
	if lastErr != nil {
		return lastErr
	}
	return ErrUnifiedConnectFailed
}

// connectWithCandidatesWithRetry dials candidates with retry logic based on config.
func (c *Client) connectWithCandidatesWithRetry(candidates []EndpointCandidate, bootstrap BootstrapFunc) error {
	retryCount := c.config.ReconnectRetryCount
	if retryCount <= 0 {
		retryCount = 1
	}

	var lastErr error
	for attempt := 0; attempt < retryCount; attempt++ {
		if c.IsClosed() {
			return ErrUnifiedClosed
		}

		// Try all candidates
		err := c.connectWithCandidates(candidates, bootstrap)
		if err == nil {
			return nil
		}
		lastErr = err

		// Don't sleep after last attempt
		if attempt < retryCount-1 {
			interval := time.Duration(c.config.ReconnectIntervalMs) * time.Millisecond
			if interval <= 0 {
				interval = 2000 * time.Millisecond
			}
			if err = c.waitReconnectInterval(interval); err != nil {
				return err
			}
		}
	}

	return lastErr
}

// initializeRuntime sets up handlers and starts pumps for a new runtime.
func (c *Client) initializeRuntime(runtime *client.Client) {
	if runtime == nil {
		return
	}

	c.lock.RLock()
	handler := c.errorHandler
	c.lock.RUnlock()

	runtime.AsyncCallbacks = false
	if c.config.WriteTimeout > 0 {
		runtime.WriteWait = c.config.WriteTimeout
	}
	runtime.ErrorHandler = normalizeErrorHandler(handler)

	// Set unified message handlers for routing responses
	runtime.TextMessageHandler = c.handleTextMessage
	runtime.BinaryMessageHandler = c.HandleBinaryMessage

	// Some unit tests build runtimes without an underlying websocket connection.
	// Skip pump startup in that case to avoid nil-pointer panics in ws/client.
	if !runtime.HasConnection() {
		return
	}

	// Start pumps
	go runtime.ReadPump()
	go runtime.WritePump()
}

// swapRuntime marks endpoint active and atomically replaces current runtime client.
// It also cleans up pending requests from the old runtime using generation numbers.
func (c *Client) swapRuntime(next *client.Client, endpointIndex int) (*client.Client, error) {
	c.lock.Lock()
	if c.closed {
		c.lock.Unlock()
		if next != nil {
			next.Close() // Close the new runtime since we can't use it
		}
		return nil, ErrUnifiedClosed
	}
	if next == nil {
		c.lock.Unlock()
		return nil, ErrNilRuntime
	}
	if err := c.failover.MarkActive(endpointIndex); err != nil {
		c.lock.Unlock()
		return nil, err
	}

	oldRuntime := c.runtime
	currentGen := c.runtimeGen
	c.runtime = next
	c.runtimeGen++ // Increment generation for new runtime
	c.lock.Unlock()

	// Clean up pending requests from old runtime (generation <= currentGen)
	// New requests will have generation > currentGen and won't be cleaned
	c.pendingLock.Lock()
	for e := c.pendingRequests.Front(); e != nil; {
		nextElem := e.Next()
		req := e.Value.(*PendingRequest)
		if req.runtimeGen <= currentGen {
			c.pendingRequests.Remove(e)
			// Send nil to signal connection lost
			select {
			case req.channel <- nil:
			default:
				// Channel full or closed, skip
			}
		}
		e = nextElem
	}
	c.pendingLock.Unlock()

	return oldRuntime, nil
}

// ActiveEndpoint returns the current active endpoint candidate.
func (c *Client) ActiveEndpoint() EndpointCandidate {
	return c.failover.Active()
}

// Runtime returns the currently active runtime client pointer.
func (c *Client) Runtime() *client.Client {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.runtime
}

// Close marks client closed and closes active runtime if present.
func (c *Client) Close() {
	c.lock.Lock()
	if c.closed {
		c.lock.Unlock()
		return
	}
	c.closed = true
	c.connected = false
	runtime := c.runtime
	c.runtime = nil
	close(c.closeChan)
	c.lock.Unlock()
	if runtime != nil {
		runtime.Close()
	}
}

// IsClosed reports whether client has been closed.
func (c *Client) IsClosed() bool {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.closed
}

// Config returns current normalized client config by value.
func (c *Client) Config() Config {
	return c.config
}

// SetErrorHandler sets the error handler callback for the runtime client.
func (c *Client) SetErrorHandler(handler func(error)) {
	normalized := normalizeErrorHandler(handler)

	c.lock.Lock()
	c.errorHandler = normalized
	runtime := c.runtime
	c.lock.Unlock()
	if runtime != nil {
		runtime.ErrorHandler = normalized
	}
}

func (c *Client) waitReconnectInterval(interval time.Duration) error {
	if interval <= 0 {
		return nil
	}
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-c.closeChan:
		return ErrUnifiedClosed
	}
}

func normalizeErrorHandler(handler func(error)) func(error) {
	if handler == nil {
		return defaultUnifiedErrHandler
	}
	return handler
}

// isReconnectableError checks if an error should trigger reconnect.
func isReconnectableError(err error) bool {
	if err == nil {
		return false
	}
	var opError *net.OpError
	var closeError *websocket.CloseError
	if errors.Is(err, client.ClosedError) || errors.As(err, &opError) || errors.As(err, &closeError) {
		return true
	}
	return false
}
