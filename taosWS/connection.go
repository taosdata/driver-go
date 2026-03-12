package taosWS

import (
	"context"
	"database/sql/driver"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"

	"github.com/gorilla/websocket"
	"github.com/taosdata/driver-go/v3/common"
	taosErrors "github.com/taosdata/driver-go/v3/errors"
	wsClient "github.com/taosdata/driver-go/v3/ws/client"
	"github.com/taosdata/driver-go/v3/ws/unified"
)

//revive:disable
var (
	NotQueryError = errors.New("sql is an update statement not a query statement")
)

//revive:enable

type taosConn struct {
	unifiedClient *unified.Client
	cfg           *Config
	closed        uint32
	closeOnce     sync.Once
}

func newTaosConn(cfg *Config) (*taosConn, error) {
	if cfg == nil {
		return nil, unified.ErrNilConfig
	}
	// taosWS Config aliases unified.Config; keep a local copy for taosWS defaults.
	unifiedCfg := *cfg
	unifiedCfg.Endpoints = append([]string(nil), cfg.Endpoints...)
	if unifiedCfg.ChanLength == 0 {
		unifiedCfg.ChanLength = 1
	}
	if unifiedCfg.ReconnectRetryCount <= 0 {
		unifiedCfg.ReconnectRetryCount = 1
	}
	unifiedConnector, err := unified.NewConnector(&unifiedCfg, "/ws")
	if err != nil {
		return nil, err
	}
	unifiedClient, err := unifiedConnector.Connect()
	if err != nil {
		return nil, NewBadConnError(err)
	}

	return &taosConn{
		unifiedClient: unifiedClient,
		cfg:           cfg,
	}, nil
}

func (tc *taosConn) Begin() (driver.Tx, error) {
	return nil, &taosErrors.TaosError{Code: 0xffff, ErrStr: "websocket does not support transaction"}
}

func (tc *taosConn) Close() (err error) {
	tc.closeOnce.Do(func() {
		atomic.StoreUint32(&tc.closed, 1)
		if tc.unifiedClient != nil {
			tc.unifiedClient.Close()
		}
	})
	return nil
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
	if tc.unifiedClient == nil {
		return nil, driver.ErrBadConn
	}
	reqID, err := getReqID(ctx)
	if err != nil {
		return nil, err
	}
	stmtHandle, err := tc.unifiedClient.InitStmt(int64(reqID))
	if err != nil {
		return nil, mapUnifiedConnError(err)
	}
	err = stmtHandle.Prepare(query, int64(reqID))
	if err != nil {
		_ = stmtHandle.Close(int64(reqID))
		return nil, mapUnifiedConnError(err)
	}
	isInsert, err := stmtHandle.IsInsert()
	if err != nil {
		_ = stmtHandle.Close(int64(reqID))
		return nil, mapUnifiedConnError(err)
	}
	stmt := &Stmt{
		conn:       tc,
		stmtHandle: stmtHandle,
		isInsert:   isInsert,
	}
	return stmt, nil
}

func (tc *taosConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (result driver.Result, err error) {
	return tc.execCtx(ctx, query, args)
}

func (tc *taosConn) execCtx(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if tc.isClosed() || tc.unifiedClient == nil {
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
		prepared, iErr := common.InterpolateParams(query, args)
		if iErr != nil {
			return nil, iErr
		}
		query = prepared
	}
	affected, err := tc.unifiedClient.Exec(query, int64(reqID))
	if err != nil {
		return nil, mapUnifiedConnError(err)
	}
	return driver.RowsAffected(affected), nil
}

func (tc *taosConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (rows driver.Rows, err error) {
	return tc.queryCtx(ctx, query, args)
}

func (tc *taosConn) queryCtx(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if tc.isClosed() || tc.unifiedClient == nil {
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
		prepared, iErr := common.InterpolateParams(query, args)
		if iErr != nil {
			return nil, iErr
		}
		query = prepared
	}
	rs, err := tc.unifiedClient.Query(query, int64(reqID))
	if err != nil {
		return nil, mapUnifiedConnError(err)
	}
	if rs == nil {
		return nil, NotQueryError
	}
	return newRowsFromUnified(rs), nil
}

func (tc *taosConn) Ping(ctx context.Context) (err error) {
	_ = ctx
	if tc.isClosed() {
		return driver.ErrBadConn
	}
	if tc.unifiedClient == nil {
		return driver.ErrBadConn
	}
	return mapUnifiedConnError(tc.unifiedClient.Ping())
}

func mapUnifiedConnError(err error) error {
	if err == nil {
		return nil
	}
	var badConnErr *BadConnError
	if errors.As(err, &badConnErr) {
		return err
	}
	if errors.Is(err, driver.ErrBadConn) {
		return err
	}
	if errors.Is(err, wsClient.ClosedError) ||
		errors.Is(err, io.ErrClosedPipe) ||
		isNetOrWebsocketError(err) ||
		unified.IsConnectionRelatedError(err) ||
		unified.IsConnectionDisconnectedError(err) ||
		unified.IsReconnectFailedError(err) {
		return NewBadConnError(err)
	}
	return err
}

func isNetOrWebsocketError(err error) bool {
	var opError *net.OpError
	var closeError *websocket.CloseError
	return errors.As(err, &opError) || errors.As(err, &closeError)
}
