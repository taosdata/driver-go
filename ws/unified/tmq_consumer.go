package unified

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/gorilla/websocket"
	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/common/parser"
	"github.com/taosdata/driver-go/v3/common/tdversion"
	"github.com/taosdata/driver-go/v3/common/tmq"
	taosErrors "github.com/taosdata/driver-go/v3/errors"
	"github.com/taosdata/driver-go/v3/ws/client"
	"github.com/taosdata/driver-go/v3/ws/unified/proto"
)

type TMQConsumer struct {
	client             *Client
	requestID          uint64
	stateLock          sync.RWMutex
	err                error
	timezone           *time.Location
	dataParser         *parser.TMQRawDataParser
	messageTimeout     time.Duration
	autoCommit         bool
	autoCommitInterval time.Duration
	nextAutoCommitTime time.Time
	user               string
	password           string
	groupID            string
	clientID           string
	offsetRest         string
	snapshotEnable     string
	withTableName      string
	sessionTimeoutMS   string
	maxPollIntervalMS  string
	otherOptions       map[string]string
	closeOnce          sync.Once
	topics             []string
	autoReconnect      bool
	lastMessageID      uint64
}

type WSError struct {
	err error
}

func (e *WSError) Error() string {
	return fmt.Sprintf("websocket close with error %v", e.err)
}

var ErrTMQConsumerUninitialized = &Error{
	Type:    ErrorTypeInvalidState,
	Message: "unified tmq consumer is not initialized",
}

type SubscribeReq = proto.SubscribeReq
type SubscribeResp = proto.SubscribeResp
type PollReq = proto.PollReq
type PollResp = proto.PollResp
type FetchJsonMetaReq = proto.FetchJSONMetaReq
type FetchJsonMetaResp = proto.FetchJSONMetaResp
type TMQFetchRawMetaReq = proto.FetchRawMetaReq
type CommitReq = proto.CommitReq
type CommitResp = proto.CommitResp
type UnsubscribeReq = proto.UnsubscribeReq
type AssignmentReq = proto.AssignmentReq
type AssignmentResp = proto.AssignmentResp
type OffsetSeekReq = proto.OffsetSeekReq
type OffsetSeekResp = proto.OffsetSeekResp
type CommittedReq = proto.CommittedReq
type CommittedResp = proto.CommittedResp
type TopicVgroupID = proto.TopicVgroupID
type CommitOffsetReq = proto.CommitOffsetReq
type CommitOffsetResp = proto.CommitOffsetResp
type PositionReq = proto.PositionReq
type PositionResp = proto.PositionResp

// NewTMQConsumer creates a tmq consumer backed by unified client reconnect/failover runtime.
func NewTMQConsumer(conf *tmq.ConfigMap) (*TMQConsumer, error) {
	if conf == nil {
		return nil, ErrNilConfig
	}
	confCopy := conf.Clone()
	config, err := configMapToConfig(confCopy)
	if err != nil {
		return nil, err
	}
	autoCommit := config.AutoCommit != "false"
	autoCommitInterval := time.Second * 5
	if config.AutoCommitIntervalMS != "" {
		interval, err := strconv.ParseUint(config.AutoCommitIntervalMS, 10, 64)
		if err != nil {
			return nil, err
		}
		autoCommitInterval = time.Millisecond * time.Duration(interval)
	}
	unifiedCfg := NewConfig(config.Endpoints)
	unifiedCfg.ChanLength = config.ChanLength
	unifiedCfg.MessageTimeout = config.MessageTimeout
	unifiedCfg.ReadTimeout = config.MessageTimeout
	unifiedCfg.WriteTimeout = config.WriteWait
	unifiedCfg.EnableCompression = config.EnableCompression
	unifiedCfg.AutoReconnect = config.AutoReconnect
	unifiedCfg.ReconnectIntervalMs = config.ReconnectIntervalMs
	unifiedCfg.ReconnectRetryCount = config.ReconnectRetryCount

	wsClient, err := NewClient(unifiedCfg, "/rest/tmq")
	if err != nil {
		return nil, err
	}
	consumer := &TMQConsumer{
		client:             wsClient,
		messageTimeout:     config.MessageTimeout,
		user:               config.User,
		password:           config.Password,
		groupID:            config.GroupID,
		clientID:           config.ClientID,
		offsetRest:         config.OffsetRest,
		autoCommit:         autoCommit,
		autoCommitInterval: autoCommitInterval,
		snapshotEnable:     config.SnapshotEnable,
		withTableName:      config.WithTableName,
		dataParser:         parser.NewTMQRawDataParser(),
		autoReconnect:      config.AutoReconnect,
		otherOptions:       config.OtherOptions,
		timezone:           config.Timezone,
		sessionTimeoutMS:   config.SessionTimeoutMS,
		maxPollIntervalMS:  config.MaxPollIntervalMS,
	}
	consumer.client.SetErrorHandler(consumer.handleError)
	if err = consumer.client.ConnectWithBootstrap(consumer.bootstrapTMQ); err != nil {
		consumer.client.Close()
		return nil, err
	}
	return consumer, nil
}

func (c *TMQConsumer) bootstrapTMQ(conn *websocket.Conn) error {
	return tdversion.WSCheckVersion(conn)
}

func (c *TMQConsumer) reconnect(failedRuntime *client.Client) error {
	if c.isClosed() {
		return ClosedErr
	}
	if err := c.client.reconnectWithBootstrap(c.bootstrapTMQ, failedRuntime); err != nil {
		if errors.Is(err, ErrUnifiedClosed) {
			return ClosedErr
		}
		return &Error{
			Type:              ErrorTypeReconnectFailed,
			Message:           "reconnect failed",
			ConnectionRelated: true,
			ReconnectFailed:   true,
		}
	}
	topics := c.topicsSnapshot()
	if len(topics) > 0 {
		if err := c.doSubscribe(topics, false); err != nil {
			return err
		}
	}
	return nil
}

var excludeConfig = map[string]struct{}{
	"ws.url":                       {},
	"ws.message.channelLen":        {},
	"ws.message.timeout":           {},
	"ws.message.writeWait":         {},
	"td.connect.user":              {},
	"td.connect.pass":              {},
	"group.id":                     {},
	"client.id":                    {},
	"auto.offset.reset":            {},
	"enable.auto.commit":           {},
	"auto.commit.interval.ms":      {},
	"experimental.snapshot.enable": {},
	"msg.with.table.name":          {},
	"ws.message.enableCompression": {},
	"ws.autoReconnect":             {},
	"ws.reconnectIntervalMs":       {},
	"ws.reconnectRetryCount":       {},
	"session.timeout.ms":           {},
	"max.poll.interval.ms":         {},
	"timezone":                     {},
}

func configMapToConfig(m tmq.ConfigMap) (*config, error) {
	url, err := m.Get("ws.url", "")
	if err != nil {
		return nil, err
	}
	if url == "" {
		return nil, newInvalidConfigErrorf("ws.url required")
	}
	endpoints, err := parseTMQEndpoints(url.(string))
	if err != nil {
		return nil, err
	}
	chanLen, err := m.Get("ws.message.channelLen", uint(0))
	if err != nil {
		return nil, err
	}
	messageTimeout, err := m.Get("ws.message.timeout", common.DefaultMessageTimeout)
	if err != nil {
		return nil, err
	}
	writeWait, err := m.Get("ws.message.writeWait", common.DefaultWriteWait)
	if err != nil {
		return nil, err
	}
	user, err := m.Get("td.connect.user", "")
	if err != nil {
		return nil, err
	}
	pass, err := m.Get("td.connect.pass", "")
	if err != nil {
		return nil, err
	}
	groupID, err := m.Get("group.id", "")
	if err != nil {
		return nil, err
	}
	clientID, err := m.Get("client.id", "")
	if err != nil {
		return nil, err
	}
	offsetReset, err := m.Get("auto.offset.reset", "")
	if err != nil {
		return nil, err
	}
	enableAutoCommit, err := m.Get("enable.auto.commit", "")
	if err != nil {
		return nil, err
	}
	//auto.commit.interval.ms
	autoCommitIntervalMS, err := m.Get("auto.commit.interval.ms", "")
	if err != nil {
		return nil, err
	}
	enableSnapshot, err := m.Get("experimental.snapshot.enable", "")
	if err != nil {
		return nil, err
	}
	withTableName, err := m.Get("msg.with.table.name", "")
	if err != nil {
		return nil, err
	}
	enableCompression, err := m.Get("ws.message.enableCompression", false)
	if err != nil {
		return nil, err
	}
	autoReconnect, err := m.Get("ws.autoReconnect", false)
	if err != nil {
		return nil, err
	}
	reconnectIntervalMs, err := m.Get("ws.reconnectIntervalMs", int(2000))
	if err != nil {
		return nil, err
	}
	reconnectRetryCount, err := m.Get("ws.reconnectRetryCount", int(3))
	if err != nil {
		return nil, err
	}
	sessionTimeoutMS, err := m.Get("session.timeout.ms", "")
	if err != nil {
		return nil, err
	}
	maxPollIntervalMS, err := m.Get("max.poll.interval.ms", "")
	if err != nil {
		return nil, err
	}
	timezone, err := m.Get("timezone", "")
	if err != nil {
		return nil, err
	}
	config := newConfig(endpoints[0], chanLen.(uint))
	config.setEndpoints(endpoints)
	err = config.setMessageTimeout(messageTimeout.(time.Duration))
	if err != nil {
		return nil, err
	}
	err = config.setWriteWait(writeWait.(time.Duration))
	if err != nil {
		return nil, err
	}
	config.setConnectUser(user.(string))
	config.setConnectPass(pass.(string))
	config.setGroupID(groupID.(string))
	config.setClientID(clientID.(string))
	config.setAutoOffsetReset(offsetReset.(string))
	config.setAutoCommit(enableAutoCommit.(string))
	config.setAutoCommitIntervalMS(autoCommitIntervalMS.(string))
	config.setSnapshotEnable(enableSnapshot.(string))
	config.setWithTableName(withTableName.(string))
	config.setEnableCompression(enableCompression.(bool))
	config.setAutoReconnect(autoReconnect.(bool))
	config.setReconnectIntervalMs(reconnectIntervalMs.(int))
	config.setReconnectRetryCount(reconnectRetryCount.(int))
	config.setSessionTimeoutMS(sessionTimeoutMS.(string))
	config.setMaxPollIntervalMS(maxPollIntervalMS.(string))
	err = config.setTimezone(timezone.(string))
	if err != nil {
		return nil, err
	}
	for k, v := range m {
		if _, ok := excludeConfig[k]; ok {
			continue
		}
		if strV, ok := v.(string); ok {
			config.OtherOptions[k] = strV
		} else {
			return nil, newInvalidConfigErrorf("config %s value must be string", k)
		}
	}
	return config, nil
}

func parseTMQEndpoints(raw string) ([]string, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, newInvalidConfigErrorf("ws.url required")
	}
	items := strings.Split(raw, ",")
	endpoints := make([]string, 0, len(items))
	for i := 0; i < len(items); i++ {
		item := strings.TrimSpace(items[i])
		if item == "" {
			continue
		}
		u, err := url.Parse(item)
		if err != nil {
			return nil, err
		}
		// tmq endpoint is always /rest/tmq regardless of user-provided path.
		u.Path = ""
		endpoints = append(endpoints, u.String())
	}
	if len(endpoints) == 0 {
		return nil, newInvalidConfigErrorf("ws.url required")
	}
	seen := make(map[string]struct{}, len(endpoints))
	normalized := make([]string, 0, len(endpoints))
	for i := 0; i < len(endpoints); i++ {
		u, err := url.Parse(endpoints[i])
		if err != nil {
			return nil, err
		}
		if u.Scheme == "" || u.Host == "" {
			return nil, newInvalidConfigErrorf("invalid websocket endpoint: %s", endpoints[i])
		}
		switch strings.ToLower(u.Scheme) {
		case "ws", "wss":
			u.Scheme = strings.ToLower(u.Scheme)
		default:
			return nil, newInvalidConfigErrorf("invalid websocket endpoint scheme: %s", endpoints[i])
		}
		u.Path = "/rest/tmq"
		s := u.String()
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		normalized = append(normalized, s)
	}
	if len(normalized) == 0 {
		return nil, newInvalidConfigErrorf("ws.url required")
	}
	return normalized, nil
}

func (c *TMQConsumer) handleError(err error) {
	if !c.autoReconnect {
		c.setErr(&WSError{err: err})
	}
}

func (c *TMQConsumer) generateReqID() uint64 {
	return atomic.AddUint64(&c.requestID, 1)
}

func (c *TMQConsumer) ensureInitialized() error {
	if c == nil || c.client == nil {
		return ErrTMQConsumerUninitialized
	}
	return nil
}

// Close consumer. This function can be called multiple times.
func (c *TMQConsumer) Close() error {
	if err := c.ensureInitialized(); err != nil {
		return err
	}
	c.closeOnce.Do(func() {
		c.client.Close()
	})
	return nil
}

//revive:disable-next-line
var ClosedErr = &Error{
	Type:                   ErrorTypeClientClosed,
	Message:                "connection closed",
	ConnectionRelated:      true,
	ConnectionDisconnected: true,
}

func (c *TMQConsumer) sendText(reqID uint64, envelope *client.Envelope) ([]byte, error) {
	resp, _, err := c.sendTextWithClient(reqID, envelope)
	return resp, err
}

func (c *TMQConsumer) sendTextWithReconnect(reqID uint64, envelope *client.Envelope, reconnect bool) ([]byte, error) {
	respBytes, failedRuntime, err := c.sendTextWithClient(reqID, envelope)
	if err == nil {
		return respBytes, nil
	}
	if !reconnect {
		return nil, err
	}
	if !isReconnectableError(err) && !errors.Is(err, ClosedErr) {
		return nil, err
	}
	if err = c.reconnect(failedRuntime); err != nil {
		return nil, err
	}
	respBytes, _, err = c.sendTextWithClient(reqID, envelope)
	if err != nil {
		return nil, err
	}
	return respBytes, nil
}

func (c *TMQConsumer) sendTextWithClient(reqID uint64, envelope *client.Envelope) ([]byte, *client.Client, error) {
	currentRuntime := c.runtime()
	if currentRuntime == nil {
		if c.isClosed() {
			return nil, nil, ClosedErr
		}
		return nil, nil, client.ClosedError
	}
	envelope.Type = websocket.TextMessage
	timeoutErr := &Error{
		Type:              ErrorTypeMessageTimeout,
		Message:           fmt.Sprintf("message timeout :%s", envelope.Msg.String()),
		ConnectionRelated: true,
	}
	resp, _, _, err := c.client.sendEnvelopeWithRuntime(currentRuntime, reqID, envelope, c.messageTimeout, timeoutErr)
	if err != nil {
		if c.isClosed() || errors.Is(err, ErrUnifiedClosed) {
			return nil, currentRuntime, ClosedErr
		}
		if errors.Is(err, client.ClosedError) {
			return nil, currentRuntime, client.WrapClosedError(ClosedErr, currentRuntime.LastError())
		}
		return nil, currentRuntime, err
	}
	return resp, currentRuntime, nil
}

func (c *TMQConsumer) isClosed() bool {
	if c == nil || c.client == nil {
		return true
	}
	return c.client.IsClosed()
}

func (c *TMQConsumer) runtime() *client.Client {
	if c == nil || c.client == nil {
		return nil
	}
	return c.client.Runtime()
}

type RebalanceCb func(*TMQConsumer, tmq.Event) error

func (c *TMQConsumer) Subscribe(topic string, rebalanceCb RebalanceCb) error {
	if err := c.ensureInitialized(); err != nil {
		return err
	}
	return c.SubscribeTopics([]string{topic}, rebalanceCb)
}

func (c *TMQConsumer) SubscribeTopics(topics []string, rebalanceCb RebalanceCb) error {
	if err := c.ensureInitialized(); err != nil {
		return err
	}
	return c.doSubscribe(topics, c.autoReconnect)
}

func (c *TMQConsumer) doSubscribe(topics []string, reconnect bool) error {
	if currentErr := c.getErr(); currentErr != nil {
		return currentErr
	}
	reqID := c.generateReqID()
	req := &SubscribeReq{
		ReqID:             reqID,
		User:              c.user,
		Password:          c.password,
		GroupID:           c.groupID,
		ClientID:          c.clientID,
		OffsetRest:        c.offsetRest,
		Topics:            topics,
		AutoCommit:        "false",
		SnapshotEnable:    c.snapshotEnable,
		WithTableName:     c.withTableName,
		SessionTimeoutMS:  c.sessionTimeoutMS,
		MaxPollIntervalMS: c.maxPollIntervalMS,
		App:               common.GetProcessName(),
		Connector:         common.GetConnectorInfo("ws"),
		Config:            c.otherOptions,
	}
	args, err := client.JsonI.Marshal(req)
	if err != nil {
		return err
	}
	action := &client.WSAction{
		Action: proto.TMQActionSubscribe,
		Args:   args,
	}
	envelope := client.GlobalEnvelopePool.Get()
	defer client.GlobalEnvelopePool.Put(envelope)
	err = client.JsonI.NewEncoder(envelope.Msg).Encode(action)
	if err != nil {
		return err
	}
	respBytes, err := c.sendTextWithReconnect(reqID, envelope, reconnect)
	if err != nil {
		return err
	}
	var resp SubscribeResp
	err = client.JsonI.Unmarshal(respBytes, &resp)
	err = client.HandleResponseError(err, resp.Code, resp.Message)
	if err != nil {
		return err
	}
	c.setTopics(topics)
	return nil
}

// Poll messages
func (c *TMQConsumer) Poll(timeoutMs int) tmq.Event {
	if err := c.ensureInitialized(); err != nil {
		return tmq.NewTMQErrorWithErr(err)
	}
	if currentErr := c.getErr(); currentErr != nil {
		return tmq.NewTMQErrorWithErr(currentErr)
	}
	if c.autoCommit && c.tryScheduleAutoCommit(time.Now()) {
		_ = c.doCommit()
	}
	reqID := c.generateReqID()
	req := &PollReq{
		ReqID:        reqID,
		BlockingTime: int64(timeoutMs),
		MessageID:    c.getLastMessageID(),
	}
	args, err := client.JsonI.Marshal(req)
	if err != nil {
		return tmq.NewTMQErrorWithErr(err)
	}
	action := &client.WSAction{
		Action: proto.TMQActionPoll,
		Args:   args,
	}
	envelope := client.GlobalEnvelopePool.Get()
	defer client.GlobalEnvelopePool.Put(envelope)
	err = client.JsonI.NewEncoder(envelope.Msg).Encode(action)
	if err != nil {
		return tmq.NewTMQErrorWithErr(err)
	}
	respBytes, err := c.sendTextWithReconnect(reqID, envelope, c.autoReconnect)
	if err != nil {
		return tmq.NewTMQErrorWithErr(err)
	}
	var resp PollResp
	err = client.JsonI.Unmarshal(respBytes, &resp)
	if err != nil {
		return tmq.NewTMQErrorWithErr(err)
	}
	if resp.Code != 0 {
		return tmq.NewTMQErrorWithErr(taosErrors.NewError(resp.Code, resp.Message))
	}
	if resp.HaveMessage {
		c.setLastMessageID(resp.MessageID)
		switch resp.MessageType {
		case common.TMQ_RES_DATA:
			result := &tmq.DataMessage{}
			result.SetDbName(resp.Database)
			result.SetTopic(resp.Topic)
			result.SetOffset(tmq.Offset(resp.Offset))
			data, err := c.fetch(resp.MessageID)
			if err != nil {
				return tmq.NewTMQErrorWithErr(err)
			}
			result.SetData(data)
			topic := resp.Topic
			result.TopicPartition = tmq.TopicPartition{
				Topic:     &topic,
				Partition: resp.VgroupID,
				Offset:    tmq.Offset(resp.Offset),
			}
			return result
		case common.TMQ_RES_TABLE_META:
			result := &tmq.MetaMessage{}
			result.SetDbName(resp.Database)
			result.SetTopic(resp.Topic)
			result.SetOffset(tmq.Offset(resp.Offset))
			meta, err := c.fetchJsonMeta(resp.MessageID)
			if err != nil {
				return tmq.NewTMQErrorWithErr(err)
			}
			topic := resp.Topic
			result.TopicPartition = tmq.TopicPartition{
				Topic:     &topic,
				Partition: resp.VgroupID,
				Offset:    tmq.Offset(resp.Offset),
			}
			result.SetMeta(meta)
			return result
		case common.TMQ_RES_METADATA:
			result := &tmq.MetaDataMessage{}
			result.SetDbName(resp.Database)
			result.SetTopic(resp.Topic)
			result.SetOffset(tmq.Offset(resp.Offset))
			meta, err := c.fetchJsonMeta(resp.MessageID)
			if err != nil {
				return tmq.NewTMQErrorWithErr(err)
			}
			data, err := c.fetch(resp.MessageID)
			if err != nil {
				return tmq.NewTMQErrorWithErr(err)
			}
			result.SetMetaData(&tmq.MetaData{
				Meta: meta,
				Data: data,
			})
			topic := resp.Topic
			result.TopicPartition = tmq.TopicPartition{
				Topic:     &topic,
				Partition: resp.VgroupID,
				Offset:    tmq.Offset(resp.Offset),
			}
			return result
		default:
			return tmq.NewTMQError(0xfffff, "invalid tmq message type")
		}
	} else {
		return nil
	}
}

func (c *TMQConsumer) fetchJsonMeta(messageID uint64) (*tmq.Meta, error) {
	reqID := c.generateReqID()
	req := &FetchJsonMetaReq{
		ReqID:     reqID,
		MessageID: messageID,
	}
	args, err := client.JsonI.Marshal(req)
	if err != nil {
		return nil, err
	}
	action := &client.WSAction{
		Action: proto.TMQActionFetchJSONMeta,
		Args:   args,
	}
	envelope := client.GlobalEnvelopePool.Get()
	defer client.GlobalEnvelopePool.Put(envelope)
	err = client.JsonI.NewEncoder(envelope.Msg).Encode(action)
	if err != nil {
		return nil, err
	}
	respBytes, err := c.sendText(reqID, envelope)
	if err != nil {
		return nil, err
	}
	var resp FetchJsonMetaResp
	err = client.JsonI.Unmarshal(respBytes, &resp)
	err = client.HandleResponseError(err, resp.Code, resp.Message)
	if err != nil {
		return nil, err
	}
	var meta tmq.Meta
	err = client.JsonI.Unmarshal(resp.Data, &meta)
	if err != nil {
		return nil, err
	}
	return &meta, nil
}

func (c *TMQConsumer) fetch(messageID uint64) ([]*tmq.Data, error) {
	reqID := c.generateReqID()
	req := &TMQFetchRawMetaReq{
		ReqID:     reqID,
		MessageID: messageID,
	}
	args, err := client.JsonI.Marshal(req)
	if err != nil {
		return nil, err
	}
	action := &client.WSAction{
		Action: proto.TMQActionFetchRaw,
		Args:   args,
	}
	envelope := client.GlobalEnvelopePool.Get()
	defer client.GlobalEnvelopePool.Put(envelope)
	err = client.JsonI.NewEncoder(envelope.Msg).Encode(action)
	if err != nil {
		return nil, err
	}
	respBytes, err := c.sendText(reqID, envelope)
	if err != nil {
		return nil, err
	}
	blockInfo, err := c.dataParser.Parse(unsafe.Pointer(&respBytes[38]))
	if err != nil {
		return nil, err
	}
	tmqData := make([]*tmq.Data, len(blockInfo))
	for i := 0; i < len(blockInfo); i++ {
		var data [][]driver.Value
		if c.timezone == nil {
			data, err = parser.ReadBlockSimple(blockInfo[i].RawBlock, blockInfo[i].Precision)
		} else {
			data, err = parser.ReadBlockSimpleWithTimeFormat(blockInfo[i].RawBlock, blockInfo[i].Precision, c.FormatTime)
		}
		if err != nil {
			return nil, err
		}
		tmqData[i] = &tmq.Data{
			TableName: blockInfo[i].TableName,
			Data:      data,
		}
	}
	return tmqData, nil
}

func (c *TMQConsumer) FormatTime(ts int64, precision int) driver.Value {
	return common.TimestampConvertToTimeWithLocation(ts, precision, c.timezone)
}

func (c *TMQConsumer) Commit() ([]tmq.TopicPartition, error) {
	if err := c.ensureInitialized(); err != nil {
		return nil, err
	}
	err := c.doCommit()
	if err != nil {
		return nil, err
	}
	partitions, err := c.Assignment()
	if err != nil {
		return nil, err
	}
	return c.Committed(partitions, 0)
}

func (c *TMQConsumer) doCommit() error {
	if currentErr := c.getErr(); currentErr != nil {
		return currentErr
	}
	reqID := c.generateReqID()
	req := &CommitReq{
		ReqID:     reqID,
		MessageID: 0,
	}
	args, err := client.JsonI.Marshal(req)
	if err != nil {
		return err
	}
	action := &client.WSAction{
		Action: proto.TMQActionCommit,
		Args:   args,
	}
	envelope := client.GlobalEnvelopePool.Get()
	defer client.GlobalEnvelopePool.Put(envelope)
	err = client.JsonI.NewEncoder(envelope.Msg).Encode(action)
	if err != nil {
		return err
	}
	respBytes, err := c.sendText(reqID, envelope)
	if err != nil {
		return err
	}
	var resp CommitResp
	err = client.JsonI.Unmarshal(respBytes, &resp)
	return client.HandleResponseError(err, resp.Code, resp.Message)
}

func (c *TMQConsumer) Unsubscribe() error {
	if err := c.ensureInitialized(); err != nil {
		return err
	}
	if currentErr := c.getErr(); currentErr != nil {
		return currentErr
	}
	reqID := c.generateReqID()
	req := &UnsubscribeReq{
		ReqID: reqID,
	}
	args, err := client.JsonI.Marshal(req)
	if err != nil {
		return err
	}
	action := &client.WSAction{
		Action: proto.TMQActionUnsubscribe,
		Args:   args,
	}
	envelope := client.GlobalEnvelopePool.Get()
	defer client.GlobalEnvelopePool.Put(envelope)
	err = client.JsonI.NewEncoder(envelope.Msg).Encode(action)
	if err != nil {
		return err
	}
	respBytes, err := c.sendText(reqID, envelope)
	if err != nil {
		return err
	}
	var resp CommitResp
	err = client.JsonI.Unmarshal(respBytes, &resp)
	return client.HandleResponseError(err, resp.Code, resp.Message)
}

func (c *TMQConsumer) Assignment() (partitions []tmq.TopicPartition, err error) {
	if err = c.ensureInitialized(); err != nil {
		return nil, err
	}
	if currentErr := c.getErr(); currentErr != nil {
		return nil, currentErr
	}
	topics := c.topicsSnapshot()
	for _, topic := range topics {
		reqID := c.generateReqID()
		req := &AssignmentReq{
			ReqID: reqID,
			Topic: topic,
		}
		args, err := client.JsonI.Marshal(req)
		if err != nil {
			return nil, err
		}
		action := &client.WSAction{
			Action: proto.TMQActionAssignment,
			Args:   args,
		}
		envelope := client.GlobalEnvelopePool.Get()
		defer client.GlobalEnvelopePool.Put(envelope)
		err = client.JsonI.NewEncoder(envelope.Msg).Encode(action)
		if err != nil {
			return nil, err
		}
		respBytes, err := c.sendText(reqID, envelope)
		if err != nil {
			return nil, err
		}
		var resp AssignmentResp
		err = client.JsonI.Unmarshal(respBytes, &resp)
		err = client.HandleResponseError(err, resp.Code, resp.Message)
		if err != nil {
			return nil, err
		}
		topicName := topic
		for i := 0; i < len(resp.Assignment); i++ {
			offset := tmq.Offset(resp.Assignment[i].Offset)
			partitions = append(partitions, tmq.TopicPartition{
				Topic:     &topicName,
				Partition: resp.Assignment[i].VGroupID,
				Offset:    offset,
			})
		}
	}
	return partitions, nil
}

func (c *TMQConsumer) Seek(partition tmq.TopicPartition, ignoredTimeoutMs int) error {
	if err := c.ensureInitialized(); err != nil {
		return err
	}
	if currentErr := c.getErr(); currentErr != nil {
		return currentErr
	}
	reqID := c.generateReqID()
	req := &OffsetSeekReq{
		ReqID:    reqID,
		Topic:    *partition.Topic,
		VgroupID: partition.Partition,
		Offset:   int64(partition.Offset),
	}
	args, err := client.JsonI.Marshal(req)
	if err != nil {
		return err
	}
	action := &client.WSAction{
		Action: proto.TMQActionSeek,
		Args:   args,
	}
	envelope := client.GlobalEnvelopePool.Get()
	defer client.GlobalEnvelopePool.Put(envelope)
	err = client.JsonI.NewEncoder(envelope.Msg).Encode(action)
	if err != nil {
		return err
	}
	respBytes, err := c.sendText(reqID, envelope)
	if err != nil {
		return err
	}
	var resp OffsetSeekResp
	err = client.JsonI.Unmarshal(respBytes, &resp)
	return client.HandleResponseError(err, resp.Code, resp.Message)
}

func (c *TMQConsumer) Committed(partitions []tmq.TopicPartition, timeoutMs int) (offsets []tmq.TopicPartition, err error) {
	if err = c.ensureInitialized(); err != nil {
		return nil, err
	}
	offsets = make([]tmq.TopicPartition, len(partitions))
	reqID := c.generateReqID()
	req := &CommittedReq{
		ReqID:          reqID,
		TopicVgroupIDs: make([]TopicVgroupID, len(partitions)),
	}
	for i := 0; i < len(partitions); i++ {
		req.TopicVgroupIDs[i] = TopicVgroupID{
			Topic:    *partitions[i].Topic,
			VgroupID: partitions[i].Partition,
		}
	}
	args, err := client.JsonI.Marshal(req)
	if err != nil {
		return nil, err
	}
	action := &client.WSAction{
		Action: proto.TMQActionCommitted,
		Args:   args,
	}
	envelope := client.GlobalEnvelopePool.Get()
	defer client.GlobalEnvelopePool.Put(envelope)
	err = client.JsonI.NewEncoder(envelope.Msg).Encode(action)
	if err != nil {
		return nil, err
	}
	respBytes, err := c.sendText(reqID, envelope)
	if err != nil {
		return nil, err
	}
	var resp CommittedResp
	err = client.JsonI.Unmarshal(respBytes, &resp)
	err = client.HandleResponseError(err, resp.Code, resp.Message)
	if err != nil {
		return nil, err
	}
	for i := 0; i < len(resp.Committed); i++ {
		offsets[i] = tmq.TopicPartition{
			Topic:     partitions[i].Topic,
			Partition: partitions[i].Partition,
			Offset:    tmq.Offset(resp.Committed[i]),
		}
	}
	return offsets, nil
}

func (c *TMQConsumer) CommitOffsets(offsets []tmq.TopicPartition) ([]tmq.TopicPartition, error) {
	if err := c.ensureInitialized(); err != nil {
		return nil, err
	}
	if currentErr := c.getErr(); currentErr != nil {
		return nil, currentErr
	}
	envelope := client.GlobalEnvelopePool.Get()
	defer client.GlobalEnvelopePool.Put(envelope)
	for i := 0; i < len(offsets); i++ {
		reqID := c.generateReqID()
		req := &CommitOffsetReq{
			ReqID:    reqID,
			Topic:    *offsets[i].Topic,
			VgroupID: offsets[i].Partition,
			Offset:   int64(offsets[i].Offset),
		}
		args, err := client.JsonI.Marshal(req)
		if err != nil {
			return nil, err
		}
		action := &client.WSAction{
			Action: proto.TMQActionCommitOffset,
			Args:   args,
		}
		envelope.Reset()
		err = client.JsonI.NewEncoder(envelope.Msg).Encode(action)
		if err != nil {
			return nil, err
		}
		respBytes, err := c.sendText(reqID, envelope)
		if err != nil {
			return nil, err
		}
		var resp CommitOffsetResp
		err = client.JsonI.Unmarshal(respBytes, &resp)
		err = client.HandleResponseError(err, resp.Code, resp.Message)
		if err != nil {
			return nil, err
		}
	}
	return c.Committed(offsets, 0)
}

func (c *TMQConsumer) Position(partitions []tmq.TopicPartition) (offsets []tmq.TopicPartition, err error) {
	if err = c.ensureInitialized(); err != nil {
		return nil, err
	}
	offsets = make([]tmq.TopicPartition, len(partitions))
	reqID := c.generateReqID()
	req := &PositionReq{
		ReqID:          reqID,
		TopicVgroupIDs: make([]TopicVgroupID, len(partitions)),
	}
	for i := 0; i < len(partitions); i++ {
		req.TopicVgroupIDs[i] = TopicVgroupID{
			Topic:    *partitions[i].Topic,
			VgroupID: partitions[i].Partition,
		}
	}
	args, err := client.JsonI.Marshal(req)
	if err != nil {
		return nil, err
	}
	action := &client.WSAction{
		Action: proto.TMQActionPosition,
		Args:   args,
	}
	envelope := client.GlobalEnvelopePool.Get()
	defer client.GlobalEnvelopePool.Put(envelope)
	err = client.JsonI.NewEncoder(envelope.Msg).Encode(action)
	if err != nil {
		return nil, err
	}
	respBytes, err := c.sendText(reqID, envelope)
	if err != nil {
		return nil, err
	}
	var resp PositionResp
	err = client.JsonI.Unmarshal(respBytes, &resp)
	err = client.HandleResponseError(err, resp.Code, resp.Message)
	if err != nil {
		return nil, err
	}
	for i := 0; i < len(resp.Position); i++ {
		offsets[i] = tmq.TopicPartition{
			Topic:     partitions[i].Topic,
			Partition: partitions[i].Partition,
			Offset:    tmq.Offset(resp.Position[i]),
		}
	}
	return offsets, nil
}

func (c *TMQConsumer) getErr() error {
	c.stateLock.RLock()
	defer c.stateLock.RUnlock()
	return c.err
}

func (c *TMQConsumer) setErr(err error) {
	c.stateLock.Lock()
	c.err = err
	c.stateLock.Unlock()
}

func (c *TMQConsumer) getLastMessageID() uint64 {
	c.stateLock.RLock()
	defer c.stateLock.RUnlock()
	return c.lastMessageID
}

func (c *TMQConsumer) setLastMessageID(messageID uint64) {
	c.stateLock.Lock()
	c.lastMessageID = messageID
	c.stateLock.Unlock()
}

func (c *TMQConsumer) setTopics(topics []string) {
	c.stateLock.Lock()
	c.topics = make([]string, len(topics))
	copy(c.topics, topics)
	c.stateLock.Unlock()
}

func (c *TMQConsumer) topicsSnapshot() []string {
	c.stateLock.RLock()
	defer c.stateLock.RUnlock()
	if len(c.topics) == 0 {
		return nil
	}
	topics := make([]string, len(c.topics))
	copy(topics, c.topics)
	return topics
}

func (c *TMQConsumer) tryScheduleAutoCommit(now time.Time) bool {
	c.stateLock.Lock()
	defer c.stateLock.Unlock()
	if c.nextAutoCommitTime.IsZero() {
		c.nextAutoCommitTime = now.Add(c.autoCommitInterval)
		return false
	}
	if now.After(c.nextAutoCommitTime) {
		c.nextAutoCommitTime = now.Add(c.autoCommitInterval)
		return true
	}
	return false
}
