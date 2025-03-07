package taosWS

import (
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	jsoniter "github.com/json-iterator/go"
	"github.com/taosdata/driver-go/v3/common"
	stmtCommon "github.com/taosdata/driver-go/v3/common/stmt"
	taosErrors "github.com/taosdata/driver-go/v3/errors"
)

var jsonI = jsoniter.ConfigCompatibleWithStandardLibrary

const (
	WSConnect    = "conn"
	WSFreeResult = "free_result"

	STMTInit         = "init"
	STMTPrepare      = "prepare"
	STMTAddBatch     = "add_batch"
	STMTExec         = "exec"
	STMTClose        = "close"
	STMTGetColFields = "get_col_fields"
	STMTUseResult    = "use_result"
)

const (
	BinaryQueryMessage   uint64 = 6
	FetchRawBlockMessage uint64 = 7
)

//revive:disable
var (
	NotQueryError    = errors.New("sql is an update statement not a query statement")
	ReadTimeoutError = errors.New("read timeout")
)

//revive:enable

type taosConn struct {
	buf          *bytes.Buffer
	client       *websocket.Conn
	writeLock    sync.Mutex
	readTimeout  time.Duration
	writeTimeout time.Duration
	cfg          *Config
	messageChan  chan *message
	messageError error
	endpoint     string
	closed       uint32
	closeCh      chan struct{}
}

type message struct {
	mt      int
	message []byte
	err     error
}

func newTaosConn(cfg *Config) (*taosConn, error) {
	endpointUrl := &url.URL{
		Scheme: cfg.Net,
		Host:   fmt.Sprintf("%s:%d", cfg.Addr, cfg.Port),
		Path:   "/ws",
	}
	if cfg.Token != "" {
		endpointUrl.RawQuery = fmt.Sprintf("token=%s", cfg.Token)
	}
	endpoint := endpointUrl.String()
	dialer := common.DefaultDialer
	dialer.EnableCompression = cfg.EnableCompression
	ws, _, err := dialer.Dial(endpoint, nil)
	if err != nil {
		return nil, err
	}
	ws.EnableWriteCompression(cfg.EnableCompression)
	err = ws.SetReadDeadline(time.Now().Add(common.DefaultPongWait))
	if err != nil {
		return nil, err
	}
	ws.SetPongHandler(func(string) error {
		_ = ws.SetReadDeadline(time.Now().Add(common.DefaultPongWait))
		return nil
	})
	tc := &taosConn{
		buf:          &bytes.Buffer{},
		client:       ws,
		readTimeout:  cfg.ReadTimeout,
		writeTimeout: cfg.WriteTimeout,
		cfg:          cfg,
		endpoint:     endpoint,
		closeCh:      make(chan struct{}),
		messageChan:  make(chan *message, 10),
	}

	go tc.ping()
	go tc.read()
	err = tc.connect()
	if err != nil {
		_ = tc.Close()
	}
	return tc, nil
}

func (tc *taosConn) ping() {
	ticker := time.NewTicker(common.DefaultPingPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-tc.closeCh:
			return
		case <-ticker.C:
			_ = tc.writePing()
		}
	}
}

func (tc *taosConn) read() {
	for {
		if tc.client == nil || tc.isClosed() {
			break
		}
		mt, msg, err := tc.client.ReadMessage()
		tc.messageChan <- &message{
			mt:      mt,
			message: msg,
			err:     err,
		}
		if err != nil {
			tc.messageError = NewBadConnError(err)
			break
		}
		if tc.isClosed() {
			break
		}
	}
}

func (tc *taosConn) Begin() (driver.Tx, error) {
	return nil, &taosErrors.TaosError{Code: 0xffff, ErrStr: "websocket does not support transaction"}
}

func (tc *taosConn) Close() (err error) {
	if !tc.isClosed() {
		atomic.StoreUint32(&tc.closed, 1)
		close(tc.closeCh)
	}
	if tc.client != nil {
		err = tc.client.Close()
	}
	tc.client = nil
	tc.cfg = nil
	tc.endpoint = ""
	return err
}

func (tc *taosConn) isClosed() bool {
	return atomic.LoadUint32(&tc.closed) != 0
}

func (tc *taosConn) Prepare(query string) (driver.Stmt, error) {
	return tc.PrepareContext(context.Background(), query)
}

func getReqID(ctx context.Context) (uint64, error) {
	reqID, err := common.GetReqIDFromCtx(ctx)
	if err != nil {
		return 0, err
	}
	if reqID == 0 {
		return uint64(common.GetReqID()), nil
	}
	return uint64(reqID), nil
}
func (tc *taosConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if tc.isClosed() {
		return nil, driver.ErrBadConn
	}
	reqID, err := getReqID(ctx)
	if err != nil {
		return nil, err
	}
	stmtID, err := tc.stmtInit(reqID)
	if err != nil {
		return nil, err
	}
	isInsert, err := tc.stmtPrepare(stmtID, query)
	if err != nil {
		_ = tc.stmtClose(stmtID)
		return nil, err
	}
	stmt := &Stmt{
		conn:     tc,
		stmtID:   stmtID,
		isInsert: isInsert,
		pSql:     query,
	}
	return stmt, nil
}

func (tc *taosConn) stmtInit(reqID uint64) (uint64, error) {
	req := &StmtInitReq{
		ReqID: reqID,
	}
	reqArgs, err := json.Marshal(req)
	if err != nil {
		return 0, err
	}
	action := &WSAction{
		Action: STMTInit,
		Args:   reqArgs,
	}
	tc.buf.Reset()
	err = jsonI.NewEncoder(tc.buf).Encode(action)
	if err != nil {
		return 0, err
	}
	err = tc.writeText(tc.buf.Bytes())
	if err != nil {
		return 0, err
	}
	var resp StmtInitResp
	err = tc.readTo(&resp, reqID)
	err = handleResponseError(err, resp.Code, resp.Message)
	if err != nil {
		return 0, err
	}
	return resp.StmtID, nil
}

func (tc *taosConn) stmtPrepare(stmtID uint64, sql string) (bool, error) {
	reqID := uint64(common.GetReqID())
	req := &StmtPrepareRequest{
		ReqID:  reqID,
		StmtID: stmtID,
		SQL:    sql,
	}
	reqArgs, err := json.Marshal(req)
	if err != nil {
		return false, err
	}
	action := &WSAction{
		Action: STMTPrepare,
		Args:   reqArgs,
	}
	tc.buf.Reset()
	err = jsonI.NewEncoder(tc.buf).Encode(action)
	if err != nil {
		return false, err
	}
	err = tc.writeText(tc.buf.Bytes())
	if err != nil {
		return false, err
	}
	var resp StmtPrepareResponse
	err = tc.readTo(&resp, reqID)
	err = handleResponseError(err, resp.Code, resp.Message)
	if err != nil {
		return false, err
	}
	return resp.IsInsert, nil
}

func (tc *taosConn) stmtClose(stmtID uint64) error {
	reqID := uint64(common.GetReqID())
	req := &StmtCloseRequest{
		ReqID:  reqID,
		StmtID: stmtID,
	}
	reqArgs, err := json.Marshal(req)
	if err != nil {
		return err
	}
	action := &WSAction{
		Action: STMTClose,
		Args:   reqArgs,
	}
	tc.buf.Reset()
	err = jsonI.NewEncoder(tc.buf).Encode(action)
	if err != nil {
		return err
	}
	err = tc.writeText(tc.buf.Bytes())
	if err != nil {
		return err
	}
	return nil
}

func (tc *taosConn) stmtGetColFields(stmtID uint64) ([]*stmtCommon.StmtField, error) {
	reqID := uint64(common.GetReqID())
	req := &StmtGetColFieldsRequest{
		ReqID:  reqID,
		StmtID: stmtID,
	}
	reqArgs, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	action := &WSAction{
		Action: STMTGetColFields,
		Args:   reqArgs,
	}
	tc.buf.Reset()
	err = jsonI.NewEncoder(tc.buf).Encode(action)
	if err != nil {
		return nil, err
	}
	err = tc.writeText(tc.buf.Bytes())
	if err != nil {
		return nil, err
	}
	var resp StmtGetColFieldsResponse
	err = tc.readTo(&resp, reqID)
	err = handleResponseError(err, resp.Code, resp.Message)
	if err != nil {
		return nil, err
	}
	return resp.Fields, nil
}

func (tc *taosConn) stmtBindParam(stmtID uint64, block []byte) error {
	reqID := uint64(common.GetReqID())
	tc.buf.Reset()
	WriteUint64(tc.buf, reqID)
	WriteUint64(tc.buf, stmtID)
	WriteUint64(tc.buf, BindMessage)
	tc.buf.Write(block)
	err := tc.writeBinary(tc.buf.Bytes())
	if err != nil {
		return err
	}
	var resp StmtBindResponse
	err = tc.readTo(&resp, reqID)
	return handleResponseError(err, resp.Code, resp.Message)
}

func WriteUint64(buffer *bytes.Buffer, v uint64) {
	buffer.WriteByte(byte(v))
	buffer.WriteByte(byte(v >> 8))
	buffer.WriteByte(byte(v >> 16))
	buffer.WriteByte(byte(v >> 24))
	buffer.WriteByte(byte(v >> 32))
	buffer.WriteByte(byte(v >> 40))
	buffer.WriteByte(byte(v >> 48))
	buffer.WriteByte(byte(v >> 56))
}

func WriteUint32(buffer *bytes.Buffer, v uint32) {
	buffer.WriteByte(byte(v))
	buffer.WriteByte(byte(v >> 8))
	buffer.WriteByte(byte(v >> 16))
	buffer.WriteByte(byte(v >> 24))
}

func WriteUint16(buffer *bytes.Buffer, v uint16) {
	buffer.WriteByte(byte(v))
	buffer.WriteByte(byte(v >> 8))
}

func (tc *taosConn) stmtAddBatch(stmtID uint64) error {
	reqID := uint64(common.GetReqID())
	req := &StmtAddBatchRequest{
		ReqID:  reqID,
		StmtID: stmtID,
	}
	reqArgs, err := json.Marshal(req)
	if err != nil {
		return err
	}
	action := &WSAction{
		Action: STMTAddBatch,
		Args:   reqArgs,
	}
	tc.buf.Reset()
	err = jsonI.NewEncoder(tc.buf).Encode(action)
	if err != nil {
		return err
	}
	err = tc.writeText(tc.buf.Bytes())
	if err != nil {
		return err
	}
	var resp StmtAddBatchResponse
	err = tc.readTo(&resp, reqID)
	return handleResponseError(err, resp.Code, resp.Message)
}

func (tc *taosConn) stmtExec(stmtID uint64) (int, error) {
	reqID := uint64(common.GetReqID())
	req := &StmtExecRequest{
		ReqID:  reqID,
		StmtID: stmtID,
	}
	reqArgs, err := json.Marshal(req)
	if err != nil {
		return 0, err
	}
	action := &WSAction{
		Action: STMTExec,
		Args:   reqArgs,
	}
	tc.buf.Reset()
	err = jsonI.NewEncoder(tc.buf).Encode(action)
	if err != nil {
		return 0, err
	}
	err = tc.writeText(tc.buf.Bytes())
	if err != nil {
		return 0, err
	}
	var resp StmtExecResponse
	err = tc.readTo(&resp, reqID)
	err = handleResponseError(err, resp.Code, resp.Message)
	if err != nil {
		return 0, err
	}
	return resp.Affected, nil
}

func (tc *taosConn) stmtUseResult(stmtID uint64) (*rows, error) {
	reqID := uint64(common.GetReqID())
	req := &StmtUseResultRequest{
		ReqID:  reqID,
		StmtID: stmtID,
	}
	reqArgs, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	action := &WSAction{
		Action: STMTUseResult,
		Args:   reqArgs,
	}
	tc.buf.Reset()
	err = jsonI.NewEncoder(tc.buf).Encode(action)
	if err != nil {
		return nil, err
	}
	err = tc.writeText(tc.buf.Bytes())
	if err != nil {
		return nil, err
	}
	var resp StmtUseResultResponse
	err = tc.readTo(&resp, reqID)
	err = handleResponseError(err, resp.Code, resp.Message)
	if err != nil {
		return nil, err
	}
	rs := &rows{
		buf:           &bytes.Buffer{},
		conn:          tc,
		resultID:      resp.ResultID,
		fieldsCount:   resp.FieldsCount,
		fieldsNames:   resp.FieldsNames,
		fieldsTypes:   resp.FieldsTypes,
		fieldsLengths: resp.FieldsLengths,
		precision:     resp.Precision,
		isStmt:        true,
	}
	return rs, nil
}

func (tc *taosConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (result driver.Result, err error) {
	return tc.execCtx(ctx, query, args)
}

func (tc *taosConn) execCtx(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	resp, err := tc.doQuery(ctx, query, args)
	if err != nil {
		return nil, err
	}
	if resp.Code != 0 {
		return nil, taosErrors.NewError(resp.Code, resp.Message)
	}
	return driver.RowsAffected(resp.AffectedRows), nil
}

func (tc *taosConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (rows driver.Rows, err error) {
	return tc.queryCtx(ctx, query, args)
}

func (tc *taosConn) queryCtx(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	resp, err := tc.doQuery(ctx, query, args)
	if err != nil {
		return nil, err
	}
	if resp.Code != 0 {
		return nil, taosErrors.NewError(resp.Code, resp.Message)
	}
	if resp.IsUpdate {
		return nil, NotQueryError
	}
	rs := &rows{
		buf:              &bytes.Buffer{},
		conn:             tc,
		resultID:         resp.ID,
		fieldsCount:      resp.FieldsCount,
		fieldsNames:      resp.FieldsNames,
		fieldsTypes:      resp.FieldsTypes,
		fieldsLengths:    resp.FieldsLengths,
		precision:        resp.Precision,
		fieldsPrecisions: resp.FieldsPrecisions,
		fieldsScales:     resp.FieldsScales,
	}
	return rs, err
}

func (tc *taosConn) doQuery(ctx context.Context, query string, args []driver.NamedValue) (*WSQueryResp, error) {
	if tc.isClosed() {
		return nil, driver.ErrBadConn
	}
	reqID, err := getReqID(ctx)
	if err != nil {
		return nil, err
	}
	if len(args) != 0 {
		if !tc.cfg.InterpolateParams {
			return nil, driver.ErrSkip
		}
		// try client-side prepare to reduce round trip
		prepared, err := common.InterpolateParams(query, args)
		if err != nil {
			return nil, err
		}
		query = prepared
	}
	tc.buf.Reset()

	WriteUint64(tc.buf, reqID) // req id
	WriteUint64(tc.buf, 0)     // message id
	WriteUint64(tc.buf, BinaryQueryMessage)
	WriteUint16(tc.buf, 1)                  // version
	WriteUint32(tc.buf, uint32(len(query))) // sql length
	tc.buf.WriteString(query)
	err = tc.writeBinary(tc.buf.Bytes())
	if err != nil {
		return nil, err
	}
	var resp WSQueryResp
	err = tc.readTo(&resp, reqID)
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

func (tc *taosConn) Ping(ctx context.Context) (err error) {
	if tc.isClosed() {
		return driver.ErrBadConn
	}
	return tc.writePing()
}

func (tc *taosConn) connect() error {
	redID := uint64(common.GetReqID())
	req := &WSConnectReq{
		ReqID:    redID,
		User:     tc.cfg.User,
		Password: tc.cfg.Passwd,
		DB:       tc.cfg.DbName,
	}
	args, err := jsonI.Marshal(req)
	if err != nil {
		return err
	}
	action := &WSAction{
		Action: WSConnect,
		Args:   args,
	}
	tc.buf.Reset()
	err = jsonI.NewEncoder(tc.buf).Encode(action)
	if err != nil {
		return err
	}
	err = tc.writeText(tc.buf.Bytes())
	if err != nil {
		return err
	}
	var resp WSConnectResp
	err = tc.readTo(&resp, redID)
	return handleResponseError(err, resp.Code, resp.Message)
}

func (tc *taosConn) writeText(data []byte) error {
	return tc.write(websocket.TextMessage, data)
}

func (tc *taosConn) writeBinary(data []byte) error {
	return tc.write(websocket.BinaryMessage, data)
}

func (tc *taosConn) writePing() error {
	return tc.write(websocket.PingMessage, nil)
}

func (tc *taosConn) write(messageType int, data []byte) error {
	tc.writeLock.Lock()
	defer tc.writeLock.Unlock()
	if tc.isClosed() {
		return driver.ErrBadConn
	}
	if tc.messageError != nil {
		return tc.messageError
	}
	err := tc.client.SetWriteDeadline(time.Now().Add(tc.writeTimeout))
	if err != nil {
		return NewBadConnError(err)
	}
	err = tc.client.WriteMessage(messageType, data)
	if err != nil {
		return NewBadConnErrorWithCtx(err, string(data))
	}
	return nil
}

func (tc *taosConn) readTo(to interface{}, expectRedID uint64) error {
	mt, respBytes, err := tc.readResponse()
	if err != nil {
		return err
	}
	if mt != websocket.TextMessage {
		return NewBadConnErrorWithCtx(fmt.Errorf("readTo: got wrong message type %d", mt), formatBytes(respBytes))
	}
	err = jsonI.Unmarshal(respBytes, to)
	if err != nil {
		return NewBadConnErrorWithCtx(err, string(respBytes))
	}
	if respI, ok := to.(RespInterface); ok {
		if respI.GetReqID() != expectRedID {
			return NewBadConnErrorWithCtx(fmt.Errorf("readTo: got wrong reqID %d, expect %d", respI.GetReqID(), expectRedID), string(respBytes))
		}
	}
	return nil
}

func (tc *taosConn) readBytes() ([]byte, error) {
	mt, respBytes, err := tc.readResponse()
	if err != nil {
		return nil, err
	}
	if mt != websocket.BinaryMessage {
		return nil, NewBadConnErrorWithCtx(fmt.Errorf("readBytes: got wrong message type %d", mt), string(respBytes))
	}
	return respBytes, err
}

func (tc *taosConn) readResponse() (int, []byte, error) {
	if tc.isClosed() {
		return 0, nil, driver.ErrBadConn
	}
	if tc.messageError != nil {
		return 0, nil, tc.messageError
	}
	ctx, cancel := context.WithTimeout(context.Background(), tc.readTimeout)
	defer cancel()
	select {
	case <-tc.closeCh:
		return 0, nil, driver.ErrBadConn
	case msg := <-tc.messageChan:
		if msg.err != nil {
			return 0, nil, NewBadConnError(msg.err)
		}
		return msg.mt, msg.message, nil
	case <-ctx.Done():
		return 0, nil, NewBadConnError(ReadTimeoutError)
	}
}

func formatBytes(bs []byte) string {
	if len(bs) == 0 {
		return ""
	}
	buffer := &strings.Builder{}
	buffer.WriteByte('[')
	for i := 0; i < len(bs); i++ {
		fmt.Fprintf(buffer, "0x%02x", bs[i])
		if i != len(bs)-1 {
			buffer.WriteByte(',')
		}
	}
	buffer.WriteByte(']')
	return buffer.String()
}

func handleResponseError(err error, code int, msg string) error {
	if err != nil {
		return err
	}
	if code != 0 {
		return taosErrors.NewError(code, msg)
	}
	return nil
}
