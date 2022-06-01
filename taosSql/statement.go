package taosSql

import (
	"context"
	"database/sql/driver"

	"github.com/taosdata/driver-go/v2/errors"
)

type taosSqlStmt struct {
	tc         *taosConn
	id         uint32
	pSql       string
	paramCount int
}

func (stmt *taosSqlStmt) Close() error {
	return nil
}

func (stmt *taosSqlStmt) NumInput() int {
	return stmt.paramCount
}

func (stmt *taosSqlStmt) Exec(args []driver.Value) (driver.Result, error) {
	if stmt.tc == nil || stmt.tc.taos == nil {
		return nil, errors.ErrTscInvalidConnection
	}
	return stmt.tc.Exec(stmt.pSql, args)
}

func (stmt *taosSqlStmt) Query(args []driver.Value) (driver.Rows, error) {
	if stmt.tc == nil || stmt.tc.taos == nil {
		return nil, errors.ErrTscInvalidConnection
	}
	return stmt.tc.Query(stmt.pSql, args)
}

func (stmt *taosSqlStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	if stmt.tc == nil {
		return nil, errors.ErrTscInvalidConnection
	}
	driverArgs, err := namedValueToValue(args)

	if err != nil {
		return nil, err
	}

	rs, err := stmt.Query(driverArgs)
	if err != nil {
		return nil, err
	}
	return rs, err
}

func (stmt *taosSqlStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	if stmt.tc == nil {
		return nil, errors.ErrTscInvalidConnection
	}

	driverArgs, err := namedValueToValue(args)
	if err != nil {
		return nil, err
	}

	return stmt.Exec(driverArgs)
}
