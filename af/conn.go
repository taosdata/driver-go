package af

import "C"
import (
	"database/sql/driver"
	"time"
	"unsafe"

	"github.com/taosdata/driver-go/v2/af/async"
	"github.com/taosdata/driver-go/v2/af/insertstmt"
	"github.com/taosdata/driver-go/v2/af/locker"
	"github.com/taosdata/driver-go/v2/common"
	"github.com/taosdata/driver-go/v2/common/param"
	"github.com/taosdata/driver-go/v2/errors"
	taosError "github.com/taosdata/driver-go/v2/errors"
	"github.com/taosdata/driver-go/v2/wrapper"
	"github.com/taosdata/driver-go/v2/wrapper/handler"
)

type Connector struct {
	taos unsafe.Pointer
}

func NewConnector(taos unsafe.Pointer) (*Connector, error) {
	if taos == nil {
		return nil, errors.ErrTscInvalidConnection
	}
	return &Connector{taos: taos}, nil
}

func Open(host, user, pass, db string, port int) (*Connector, error) {
	if len(user) == 0 {
		user = common.DefaultUser
	}
	if len(pass) == 0 {
		pass = common.DefaultPassword
	}
	locker.Lock()
	tc, err := wrapper.TaosConnect(host, user, pass, db, port)
	locker.Unlock()
	if err != nil {
		return nil, err
	}
	return &Connector{taos: tc}, nil
}

func (conn *Connector) Close() error {
	locker.Lock()
	wrapper.TaosClose(conn.taos)
	locker.Unlock()
	conn.taos = nil
	return nil
}

func (conn *Connector) StmtExecute(sql string, params *param.Param) (res driver.Result, err error) {
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

func (conn *Connector) StmtQuery(sql string, params *param.Param) (rows driver.Rows, err error) {
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
	asyncHandler := async.GetHandler()
	defer async.PutHandler(asyncHandler)
	result := conn.taosQuery(query, asyncHandler)
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

	handler := async.GetHandler()
	result := conn.taosQuery(query, handler)
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

func (conn *Connector) Subscribe(restart bool, topic string, sql string, interval time.Duration) (Subscriber, error) {
	sub := wrapper.TaosSubscribe(conn.taos, topic, sql, restart, interval)
	return &taosSubscriber{sub: sub}, nil
}

func (conn *Connector) taosQuery(sqlStr string, handler *handler.Handler) *handler.AsyncResult {
	locker.Lock()
	wrapper.TaosQueryA(conn.taos, sqlStr, handler.Handler)
	locker.Unlock()
	r := <-handler.Caller.QueryResult
	return r
}

func (conn *Connector) InsertStmt() *insertstmt.InsertStmt {
	return insertstmt.NewInsertStmt(conn.taos)
}

func (conn *Connector) LoadTableInfo(tableNameList []string) error {
	locker.Lock()
	code := wrapper.TaosLoadTableInfo(conn.taos, tableNameList)
	locker.Unlock()
	err := taosError.GetError(code)
	if err != nil {
		return err
	}
	return nil
}

func (conn *Connector) SelectDB(db string) error {
	locker.Lock()
	code := wrapper.TaosSelectDB(conn.taos, db)
	locker.Unlock()
	err := taosError.GetError(code)
	if err != nil {
		return err
	}
	return nil
}

func (conn *Connector) InfluxDBInsertLines(lines []string, precision string) error {
	locker.Lock()
	result := wrapper.TaosSchemalessInsert(conn.taos, lines, wrapper.InfluxDBLineProtocol, precision)
	locker.Unlock()
	code := wrapper.TaosError(result)
	if code != 0 {
		errStr := wrapper.TaosErrorStr(result)
		locker.Lock()
		wrapper.TaosFreeResult(result)
		locker.Unlock()
		return errors.NewError(code, errStr)
	}
	locker.Lock()
	wrapper.TaosFreeResult(result)
	locker.Unlock()
	return nil
}

func (conn *Connector) OpenTSDBInsertTelnetLines(lines []string) error {
	locker.Lock()
	result := wrapper.TaosSchemalessInsert(conn.taos, lines, wrapper.OpenTSDBTelnetLineProtocol, "")
	locker.Unlock()
	code := wrapper.TaosError(result)
	if code != 0 {
		errStr := wrapper.TaosErrorStr(result)
		locker.Lock()
		wrapper.TaosFreeResult(result)
		locker.Unlock()
		return errors.NewError(code, errStr)
	}
	wrapper.TaosFreeResult(result)
	return nil
}

func (conn *Connector) OpenTSDBInsertJsonPayload(payload string) error {
	result := wrapper.TaosSchemalessInsert(conn.taos, []string{payload}, wrapper.OpenTSDBJsonFormatProtocol, "")
	code := wrapper.TaosError(result)
	if code != 0 {
		errStr := wrapper.TaosErrorStr(result)
		locker.Lock()
		wrapper.TaosFreeResult(result)
		locker.Unlock()
		return errors.NewError(code, errStr)
	}
	locker.Lock()
	wrapper.TaosFreeResult(result)
	locker.Unlock()
	return nil
}
