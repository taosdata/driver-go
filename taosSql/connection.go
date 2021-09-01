package taosSql

import (
	"context"
	"database/sql/driver"
	"github.com/taosdata/driver-go/v2/common"
	"github.com/taosdata/driver-go/v2/wrapper"
	"strings"
	"unsafe"

	"github.com/taosdata/driver-go/v2/errors"
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
		wrapper.TaosClose(tc.taos)
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

	result, _, affectedRows, err := tc.taosQuery(query)
	if err != nil {
		return nil, err
	}
	defer wrapper.TaosFreeResult(result)
	return driver.RowsAffected(affectedRows), nil

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
	result, numFields, _, err := tc.taosQuery(query)
	if err != nil {
		return nil, err
	}
	// Read Result
	rs := &rows{
		result: result,
	}
	// Columns field
	rs.rowsHeader, err = wrapper.ReadColumn(result, numFields)
	return rs, err
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

func (tc *taosConn) taosQuery(sqlStr string) (result unsafe.Pointer, numFields int, affectedRows int, err error) {
	result = wrapper.TaosQuery(tc.taos, sqlStr)
	code := wrapper.TaosError(result)
	if code != 0 {
		errStr := wrapper.TaosErrorStr(result)
		wrapper.TaosFreeResult(result)
		return nil, 0, 0, &errors.TaosError{
			Code:   int32(code) & 0xffff,
			ErrStr: errStr,
		}
	}

	// read result and save into tc struct
	numFields = wrapper.TaosFieldCount(result)
	if numFields == 0 {
		// there are no select and show kinds of commands
		affectedRows = wrapper.TaosAffectedRows(result)
	}

	return result, numFields, affectedRows, nil
}
