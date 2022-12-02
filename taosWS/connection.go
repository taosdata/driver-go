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
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	jsoniter "github.com/json-iterator/go"
	"github.com/taosdata/driver-go/v3/common"
	taosErrors "github.com/taosdata/driver-go/v3/errors"
)

var jsonI = jsoniter.ConfigCompatibleWithStandardLibrary

const (
	WSConnect    = "conn"
	WSQuery      = "query"
	WSFetch      = "fetch"
	WSFetchBlock = "fetch_block"
	WSFreeResult = "free_result"
)

var (
	NotQueryError    = errors.New("sql is an update statement not a query statement")
	ReadTimeoutError = errors.New("read timeout")
)

type taosConn struct {
	buf          *bytes.Buffer
	client       *websocket.Conn
	requestID    uint64
	readTimeout  time.Duration
	writeTimeout time.Duration
	cfg          *config
	endpoint     string
}

func (tc *taosConn) generateReqID() uint64 {
	return atomic.AddUint64(&tc.requestID, 1)
}

func newTaosConn(cfg *config) (*taosConn, error) {
	endpointUrl := &url.URL{
		Scheme: cfg.net,
		Host:   fmt.Sprintf("%s:%d", cfg.addr, cfg.port),
		Path:   "/rest/ws",
	}
	if cfg.token != "" {
		endpointUrl.RawQuery = fmt.Sprintf("token=%s", cfg.token)
	}
	endpoint := endpointUrl.String()
	ws, _, err := common.DefaultDialer.Dial(endpoint, nil)
	if err != nil {
		return nil, err
	}
	ws.SetReadLimit(common.BufferSize4M)
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
	}

	err = tc.connect()
	if err != nil {
		tc.Close()
	}
	return tc, nil
}

func (tc *taosConn) Begin() (driver.Tx, error) {
	return nil, &taosErrors.TaosError{Code: 0xffff, ErrStr: "websocket does not support transaction"}
}

func (tc *taosConn) Close() (err error) {
	if tc.client != nil {
		err = tc.client.Close()
	}
	tc.client = nil
	tc.cfg = nil
	tc.endpoint = ""
	return err
}

func (tc *taosConn) Prepare(query string) (driver.Stmt, error) {
	return nil, &taosErrors.TaosError{Code: 0xffff, ErrStr: "websocket does not support stmt"}
}

func (tc *taosConn) Exec(query string, args []driver.Value) (driver.Result, error) {
	return tc.execCtx(context.Background(), query, common.ValueArgsToNamedValueArgs(args))
}

func (tc *taosConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (result driver.Result, err error) {
	return tc.execCtx(ctx, query, args)
}

func (tc *taosConn) execCtx(_ context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if len(args) != 0 {
		if !tc.cfg.interpolateParams {
			return nil, driver.ErrSkip
		}
		// try to interpolate the parameters to save extra round trips for preparing and closing a statement
		prepared, err := common.InterpolateParams(query, args)
		if err != nil {
			return nil, err
		}
		query = prepared
	}
	reqID := tc.generateReqID()
	req := &WSQueryReq{
		ReqID: reqID,
		SQL:   query,
	}
	reqArgs, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	action := &WSAction{
		Action: WSQuery,
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
	var resp WSQueryResp
	err = tc.readTo(&resp)
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

func (tc *taosConn) queryCtx(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
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
	req := &WSQueryReq{
		ReqID: reqID,
		SQL:   query,
	}
	reqArgs, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	action := &WSAction{
		Action: WSQuery,
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
	var resp WSQueryResp
	err = tc.readTo(&resp)
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

func (tc *taosConn) Ping(ctx context.Context) (err error) {
	return nil
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
	tc.client.SetWriteDeadline(time.Now().Add(tc.writeTimeout))
	err := tc.client.WriteMessage(websocket.TextMessage, data)
	if err != nil {
		return NewBadConnErrorWithCtx(err, string(data))
	}
	return nil
}

func (tc *taosConn) readTo(to interface{}) error {
	var outErr error
	done := make(chan struct{})
	go func() {
		defer func() {
			close(done)
		}()
		mt, respBytes, err := tc.client.ReadMessage()
		if err != nil {
			outErr = NewBadConnError(err)
			return
		}
		if mt != websocket.TextMessage {
			outErr = NewBadConnErrorWithCtx(fmt.Errorf("readTo: got wrong message type %d", mt), formatBytes(respBytes))
			return
		}
		err = jsonI.Unmarshal(respBytes, to)
		if err != nil {
			outErr = NewBadConnErrorWithCtx(err, string(respBytes))
			return
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), tc.readTimeout)
	defer cancel()
	select {
	case <-done:
		return outErr
	case <-ctx.Done():
		return NewBadConnError(ReadTimeoutError)
	}
}

func (tc *taosConn) readBytes() ([]byte, error) {
	var respBytes []byte
	var outErr error
	done := make(chan struct{})
	go func() {
		defer func() {
			close(done)
		}()
		mt, message, err := tc.client.ReadMessage()
		if err != nil {
			outErr = NewBadConnError(err)
			return
		}
		if mt != websocket.BinaryMessage {
			outErr = NewBadConnErrorWithCtx(fmt.Errorf("readBytes: got wrong message type %d", mt), string(respBytes))
			return
		}
		respBytes = message
	}()
	ctx, cancel := context.WithTimeout(context.Background(), tc.readTimeout)
	defer cancel()
	select {
	case <-done:
		return respBytes, outErr
	case <-ctx.Done():
		return nil, NewBadConnError(ReadTimeoutError)
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
