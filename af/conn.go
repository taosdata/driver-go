package af

import (
	"database/sql/driver"
	"github.com/taosdata/driver-go/v2/common"
	"github.com/taosdata/driver-go/v2/errors"
	"github.com/taosdata/driver-go/v2/wrapper"
	"time"
	"unsafe"
)

type Connector struct {
	taos unsafe.Pointer
}

func Open(host, user, pass, db string, port int) (*Connector, error) {
	if len(user) == 0 {
		user = common.DefaultUser
	}
	if len(pass) == 0 {
		pass = common.DefaultPassword
	}
	tc, err := wrapper.TaosConnect(host, user, pass, db, port)
	if err != nil {
		return nil, err
	}
	return &Connector{taos: tc}, nil
}

func (conn *Connector) Close() error {
	wrapper.TaosClose(conn.taos)
	conn.taos = nil
	return nil
}

func (conn *Connector) StmtExecute(sql string, params *Param) (res driver.Result, err error) {
	stmt := NewStmt(conn.taos)
	if stmt == nil {
		err = &errors.TaosError{Code: 0xffff, ErrStr: "failed to init stmt"}
		return
	}

	defer stmt.Close()
	err = stmt.Prepare(sql)
	if err != nil {
		return nil, err
	}
	err = stmt.BindRow(params)
	if err != nil {
		return nil, err
	}
	err = stmt.AddBatch()
	if err != nil {
		return nil, err
	}
	err = stmt.Execute()
	if err != nil {
		return nil, err
	}
	result := stmt.GetAffectedRows()
	return driver.RowsAffected(result), nil
}

func (conn *Connector) StmtQuery(sql string, params *Param) (rows driver.Rows, err error) {
	stmt := NewStmt(conn.taos)
	if stmt == nil {
		err = &errors.TaosError{Code: 0xffff, ErrStr: "failed to init stmt"}
		return
	}

	defer stmt.Close()
	err = stmt.Prepare(sql)
	if err != nil {
		return nil, err
	}
	err = stmt.BindRow(params)
	if err != nil {
		return nil, err
	}
	err = stmt.Execute()
	if err != nil {
		return nil, err
	}
	return stmt.GetResultRows()
}

func (conn *Connector) Exec(query string, args ...driver.Value) (driver.Result, error) {
	if conn.taos == nil {
		return nil, driver.ErrBadConn
	}
	if len(args) != 0 {
		prepared, err := common.InterpolateParams(query, args)
		if err != nil {
			return nil, err
		}
		query = prepared
	}

	result, _, affectedRows, err := conn.taosQuery(query)
	if err != nil {
		return nil, err
	}
	defer wrapper.TaosFreeResult(result)
	return driver.RowsAffected(affectedRows), nil
}

func (conn *Connector) Query(query string, args ...driver.Value) (driver.Rows, error) {
	if conn.taos == nil {
		return nil, driver.ErrBadConn
	}
	if len(args) != 0 {
		prepared, err := common.InterpolateParams(query, args)
		if err != nil {
			return nil, err
		}
		query = prepared
	}

	result, numFields, _, err := conn.taosQuery(query)
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

func (conn *Connector) Subscribe(restart bool, topic string, sql string, interval time.Duration) (Subscriber, error) {
	sub := wrapper.TaosSubscribe(conn.taos, topic, sql, restart, interval)
	return &taosSubscriber{sub: sub}, nil
}

func (conn *Connector) taosQuery(sqlStr string) (result unsafe.Pointer, numFields int, affectedRows int, err error) {
	result = wrapper.TaosQuery(conn.taos, sqlStr)
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
