package taosSql

import (
	"context"
	"database/sql/driver"
	"unsafe"

	"github.com/taosdata/driver-go/v2/common"
	"github.com/taosdata/driver-go/v2/errors"
	"github.com/taosdata/driver-go/v2/wrapper"
	"github.com/taosdata/driver-go/v2/wrapper/handler"
)

type taosConn struct {
	taos unsafe.Pointer
	cfg  *config
}

func (tc *taosConn) Begin() (driver.Tx, error) {
	return nil, &errors.TaosError{Code: 0xffff, ErrStr: "taosSql does not support transaction"}
}

func (tc *taosConn) Close() (err error) {
	if tc.taos != nil {
		locker.Lock()
		wrapper.TaosClose(tc.taos)
		locker.Unlock()
	}
	tc.taos = nil
	return nil
}

func (tc *taosConn) Prepare(query string) (driver.Stmt, error) {
	if tc.taos == nil {
		return nil, errors.ErrTscInvalidConnection
	}
	locker.Lock()
	stmtP := wrapper.TaosStmtInit(tc.taos)
	code := wrapper.TaosStmtPrepare(stmtP, query)
	locker.Unlock()
	if code != 0 {
		errStr := wrapper.TaosStmtErrStr(stmtP)
		err := errors.NewError(code, errStr)
		locker.Lock()
		wrapper.TaosStmtClose(stmtP)
		locker.Unlock()
		return nil, err
	}
	locker.Lock()
	isInsert, code := wrapper.TaosStmtIsInsert(stmtP)
	if code != 0 {
		errStr := wrapper.TaosStmtErrStr(stmtP)
		err := errors.NewError(code, errStr)
		locker.Lock()
		wrapper.TaosStmtClose(stmtP)
		locker.Unlock()
		return nil, err
	}
	locker.Unlock()
	stmt := &Stmt{
		tc:       tc,
		pSql:     query,
		stmt:     stmtP,
		isInsert: isInsert,
	}
	return stmt, nil
}

func (tc *taosConn) Exec(query string, args []driver.Value) (driver.Result, error) {
	if tc.taos == nil {
		return nil, driver.ErrBadConn
	}
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
	handler := asyncHandlerPool.Get()
	defer asyncHandlerPool.Put(handler)
	result := tc.taosQuery(query, handler)
	defer func() {
		if result != nil && result.Res != nil {
			locker.Lock()
			wrapper.TaosFreeResult(result.Res)
			locker.Unlock()
		}
	}()
	res := result.Res
	code := wrapper.TaosError(res)
	if code != int(errors.SUCCESS) {
		errStr := wrapper.TaosErrorStr(res)
		return nil, errors.NewError(code, errStr)
	}
	affectRows := wrapper.TaosAffectedRows(res)
	return driver.RowsAffected(affectRows), nil

}

func (tc *taosConn) Query(query string, args []driver.Value) (driver.Rows, error) {
	if tc.taos == nil {
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
	handler := asyncHandlerPool.Get()
	result := tc.taosQuery(query, handler)
	res := result.Res
	code := wrapper.TaosError(res)
	if code != int(errors.SUCCESS) {
		errStr := wrapper.TaosErrorStr(res)
		locker.Lock()
		wrapper.TaosFreeResult(result.Res)
		locker.Unlock()
		return nil, errors.NewError(code, errStr)
	}
	numFields := wrapper.TaosNumFields(res)
	rowsHeader, err := wrapper.ReadColumn(res, numFields)
	if err != nil {
		return nil, err
	}
	rs := &rows{
		handler:    handler,
		rowsHeader: rowsHeader,
		result:     res,
	}
	return rs, nil
}

// Ping implements driver.Pinger interface
func (tc *taosConn) Ping(ctx context.Context) (err error) {
	if tc.taos != nil {
		return nil
	}
	return errors.ErrTscInvalidConnection
}

// BeginTx implements driver.ConnBeginTx interface
func (tc *taosConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	return nil, &errors.TaosError{Code: 0xffff, ErrStr: "taosSql does not support transaction"}
}

func (tc *taosConn) taosQuery(sqlStr string, handler *handler.Handler) *handler.AsyncResult {
	locker.Lock()
	wrapper.TaosQueryA(tc.taos, sqlStr, handler.Handler)
	locker.Unlock()
	r := <-handler.Caller.QueryResult
	return r
}
