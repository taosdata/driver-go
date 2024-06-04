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
	WSQuery      = "query"
	WSFetch      = "fetch"
	WSFetchBlock = "fetch_block"
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

var (
	NotQueryError    = errors.New("sql is an update statement not a query statement")
	ReadTimeoutError = errors.New("read timeout")
)

type taosConn struct {
	buf          *bytes.Buffer
	client       *websocket.Conn
	requestID    uint64
	writeLock    sync.Mutex
	readTimeout  time.Duration
	writeTimeout time.Duration
	cfg          *config
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

func (tc *taosConn) generateReqID() uint64 {
	return atomic.AddUint64(&tc.requestID, 1)
}

func newTaosConn(cfg *config) (*taosConn, error) {
	endpointUrl := &url.URL{
		Scheme: cfg.net,
		Host:   fmt.Sprintf("%s:%d", cfg.addr, cfg.port),
		Path:   "/ws",
	}
	if cfg.token != "" {
		endpointUrl.RawQuery = fmt.Sprintf("token=%s", cfg.token)
	}
	endpoint := endpointUrl.String()
	dialer := common.DefaultDialer
	dialer.EnableCompression = cfg.enableCompression
	ws, _, err := dialer.Dial(endpoint, nil)
	if err != nil {
		return nil, err
	}
	ws.EnableWriteCompression(cfg.enableCompression)
	ws.SetReadDeadline(time.Now().Add(common.DefaultPongWait))
	ws.SetPongHandler(func(string) error {
		ws.SetReadDeadline(time.Now().Add(common.DefaultPongWait))
		return nil
	})
	tc := &taosConn{
		buf:          &bytes.Buffer{},
		client:       ws,
		requestID:    0,
		readTimeout:  cfg.readTimeout,
		writeTimeout: cfg.writeTimeout,
		cfg:          cfg,
		endpoint:     endpoint,
		closeCh:      make(chan struct{}),
		messageChan:  make(chan *message, 10),
	}

	go tc.ping()
	go tc.read()
	err = tc.connect()
	if err != nil {
		tc.Close()
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
			tc.writePing()
		}
	}
}

func (tc *taosConn) read() {
	for {
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
	if tc.isClosed() {
		return nil, driver.ErrBadConn
	}
	stmtID, err := tc.stmtInit()
	if err != nil {
		return nil, err
	}
	isInsert, err := tc.stmtPrepare(stmtID, query)
	if err != nil {
		tc.stmtClose(stmtID)
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

func (tc *taosConn) stmtInit() (uint64, error) {
	reqID := tc.generateReqID()
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
	err = tc.readTo(&resp)
	if err != nil {
		return 0, err
	}
	if resp.Code != 0 {
		return 0, taosErrors.NewError(resp.Code, resp.Message)
	}
	return resp.StmtID, nil
}

func (tc *taosConn) stmtPrepare(stmtID uint64, sql string) (bool, error) {
	reqID := tc.generateReqID()
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
	err = tc.readTo(&resp)
	if err != nil {
		return false, err
	}
	if resp.Code != 0 {
		return false, taosErrors.NewError(resp.Code, resp.Message)
	}
	return resp.IsInsert, nil
}

func (tc *taosConn) stmtClose(stmtID uint64) error {
	reqID := tc.generateReqID()
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
	reqID := tc.generateReqID()
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
	err = tc.readTo(&resp)
	if err != nil {
		return nil, err
	}
	if resp.Code != 0 {
		return nil, taosErrors.NewError(resp.Code, resp.Message)
	}
	return resp.Fields, nil
}

func (tc *taosConn) stmtBindParam(stmtID uint64, block []byte) error {
	reqID := tc.generateReqID()
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
	err = tc.readTo(&resp)
	if err != nil {
		return err
	}
	if resp.Code != 0 {
		return taosErrors.NewError(resp.Code, resp.Message)
	}
	return nil
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
	reqID := tc.generateReqID()
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
	err = tc.readTo(&resp)
	if err != nil {
		return err
	}
	if resp.Code != 0 {
		return taosErrors.NewError(resp.Code, resp.Message)
	}
	return nil
}

func (tc *taosConn) stmtExec(stmtID uint64) (int, error) {
	reqID := tc.generateReqID()
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
	err = tc.readTo(&resp)
	if err != nil {
		return 0, err
	}
	if resp.Code != 0 {
		return 0, taosErrors.NewError(resp.Code, resp.Message)
	}
	return resp.Affected, nil
}

func (tc *taosConn) stmtUseResult(stmtID uint64) (*rows, error) {
	reqID := tc.generateReqID()
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
	err = tc.readTo(&resp)
	if err != nil {
		return nil, err
	}
	if resp.Code != 0 {
		return nil, taosErrors.NewError(resp.Code, resp.Message)
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
func (tc *taosConn) Exec(query string, args []driver.Value) (driver.Result, error) {
	return tc.execCtx(context.Background(), query, common.ValueArgsToNamedValueArgs(args))
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

func (tc *taosConn) Query(query string, args []driver.Value) (driver.Rows, error) {
	return tc.QueryContext(context.Background(), query, common.ValueArgsToNamedValueArgs(args))
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
		buf:           &bytes.Buffer{},
		conn:          tc,
		resultID:      resp.ID,
		fieldsCount:   resp.FieldsCount,
		fieldsNames:   resp.FieldsNames,
		fieldsTypes:   resp.FieldsTypes,
		fieldsLengths: resp.FieldsLengths,
		precision:     resp.Precision,
	}
	return rs, err
}

func (tc *taosConn) doQuery(_ context.Context, query string, args []driver.NamedValue) (*WSQueryResp, error) {
	if tc.isClosed() {
		return nil, driver.ErrBadConn
	}
	if len(args) != 0 {
		if !tc.cfg.interpolateParams {
			return nil, driver.ErrSkip
		}
		// try client-side prepare to reduce round trip
		prepared, err := common.InterpolateParams(query, args)
		if err != nil {
			return nil, err
		}
		query = prepared
	}
	reqID := tc.generateReqID()
	tc.buf.Reset()

	WriteUint64(tc.buf, reqID) // req id
	WriteUint64(tc.buf, 0)     // message id
	WriteUint64(tc.buf, BinaryQueryMessage)
	WriteUint16(tc.buf, 1)                  // version
	WriteUint32(tc.buf, uint32(len(query))) // sql length
	tc.buf.WriteString(query)
	err := tc.writeBinary(tc.buf.Bytes())
	if err != nil {
		return nil, err
	}
	var resp WSQueryResp
	err = tc.readTo(&resp)
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
	req := &WSConnectReq{
		ReqID:    0,
		User:     tc.cfg.user,
		Password: tc.cfg.passwd,
		DB:       tc.cfg.dbName,
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
	err = tc.readTo(&resp)
	if err != nil {
		return err
	}
	if resp.Code != 0 {
		return taosErrors.NewError(resp.Code, resp.Message)
	}
	return nil
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
	tc.client.SetWriteDeadline(time.Now().Add(tc.writeTimeout))
	err := tc.client.WriteMessage(messageType, data)
	if err != nil {
		return NewBadConnErrorWithCtx(err, string(data))
	}
	return nil
}

func (tc *taosConn) readTo(to interface{}) error {
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
