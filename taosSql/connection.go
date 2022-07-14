package taosSql

import (
	"context"
	"database/sql/driver"
	"strings"
	"unsafe"

	"github.com/taosdata/driver-go/v2/common"
	"github.com/taosdata/driver-go/v2/errors"
	"github.com/taosdata/driver-go/v2/wrapper"
	"github.com/taosdata/driver-go/v2/wrapper/handler"
)

type taosConn struct {
	taos    unsafe.Pointer
	cfg     *config
	invaild bool
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

	stmt := &taosSqlStmt{
		tc:   tc,
		pSql: query,
	}

	// find ? count and save  to stmt.paramCount
	stmt.paramCount = strings.Count(query, "?")

	return stmt, nil
}

func (tc *taosConn) Exec(query string, args []driver.Value) (driver.Result, error) {
	return tc.execContext(context.Background(), query, args)
}

func (tc *taosConn) ExecContext(ctx context.Context, query string, nvargs []driver.NamedValue) (driver.Result, error) {
	var args []driver.Value
	var err error
	if len(nvargs) != 0 {
		args, err = namedValueToValue(nvargs)
		if err != nil {
			return nil, err
		}
	}
	return tc.execContext(ctx, query, args)
}

func (tc *taosConn) execContext(ctx context.Context, query string, args []driver.Value) (driver.Result, error) {
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

	h, err := asyncHandlerPool.GetContext(ctx)
	if err != nil {
		return nil, err
	}

	result, err := tc.taosQueryContext(ctx, query, h)
	if err != nil {
		handlerRecycle.Put(h)
		return nil, err
	}
	defer asyncHandlerPool.Put(h)

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
	return tc.queryContext(context.Background(), query, args)
}

func (tc *taosConn) QueryContext(ctx context.Context, query string, nvargs []driver.NamedValue) (driver.Rows, error) {
	var args []driver.Value
	var err error
	if len(nvargs) != 0 {
		args, err = namedValueToValue(nvargs)
		if err != nil {
			return nil, err
		}
	}
	return tc.queryContext(ctx, query, args)
}

func (tc *taosConn) queryContext(ctx context.Context, query string, args []driver.Value) (driver.Rows, error) {

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

	h, err := asyncHandlerPool.GetContext(ctx)
	if err != nil {
		return nil, err
	}
	result, err := tc.taosQueryContext(ctx, query, h)
	if err != nil {
		handlerRecycle.Put(h)
		return nil, err
	}

	res := result.Res
	code := wrapper.TaosError(res)
	if code != int(errors.SUCCESS) {
		asyncHandlerPool.Put(h)
		errStr := wrapper.TaosErrorStr(res)
		locker.Lock()
		wrapper.TaosFreeResult(result.Res)
		locker.Unlock()
		return nil, errors.NewError(code, errStr)
	}
	numFields := wrapper.TaosNumFields(res)
	rowsHeader, err := wrapper.ReadColumn(res, numFields)
	if err != nil {
		asyncHandlerPool.Put(h)
		return nil, err
	}
	return newRowsWithContext(ctx, h, rowsHeader, res), nil
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

func (tc *taosConn) CheckNamedValue(nv *driver.NamedValue) (err error) {
	nv.Value, err = converter{}.ConvertValue(nv.Value)
	return
}

func (tc *taosConn) taosQuery(sqlStr string, handler *handler.Handler) *handler.AsyncResult {
	locker.Lock()
	wrapper.TaosQueryA(tc.taos, sqlStr, handler.Handler)
	locker.Unlock()
	r := <-handler.Caller.QueryResult
	return r
}

func (tc *taosConn) taosQueryContext(ctx context.Context, sqlStr string, handler *handler.Handler) (*handler.AsyncResult, error) {
	locker.Lock()
	wrapper.TaosQueryA(tc.taos, sqlStr, handler.Handler)
	locker.Unlock()
	select {
	case <-ctx.Done():
		tc.invaild = true
		return nil, ctx.Err()
	case r := <-handler.Caller.QueryResult:
		return r, nil
	}
}

func (tc *taosConn) IsValid() bool {
	return !tc.invaild
}
