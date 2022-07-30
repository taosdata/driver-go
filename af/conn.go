package af

import "C"
import (
	"database/sql/driver"
	"unsafe"

	"github.com/taosdata/driver-go/v3/af/async"
	"github.com/taosdata/driver-go/v3/af/insertstmt"
	"github.com/taosdata/driver-go/v3/af/locker"
	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/common/param"
	"github.com/taosdata/driver-go/v3/errors"
	taosError "github.com/taosdata/driver-go/v3/errors"
	"github.com/taosdata/driver-go/v3/wrapper"
	"github.com/taosdata/driver-go/v3/wrapper/handler"
)

type Connector struct {
	taos unsafe.Pointer
}

// NewConnector New connector with TDengine connection
func NewConnector(taos unsafe.Pointer) (*Connector, error) {
	if taos == nil {
		return nil, errors.ErrTscInvalidConnection
	}
	return &Connector{taos: taos}, nil
}

// Open New connector with TDengine connection information
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

// Close Release TDengine connection
func (conn *Connector) Close() error {
	locker.Lock()
	wrapper.TaosClose(conn.taos)
	locker.Unlock()
	conn.taos = nil
	return nil
}

// StmtExecute Execute sql through stmt
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

// Exec Execute sql
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

// Query Execute query sql
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
		async.PutHandler(handler)
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
	precision := wrapper.TaosResultPrecision(res)
	rs := &rows{
		handler:    handler,
		rowsHeader: rowsHeader,
		result:     res,
		precision:  precision,
	}
	return rs, nil

}

func (conn *Connector) taosQuery(sqlStr string, handler *handler.Handler) *handler.AsyncResult {
	locker.Lock()
	wrapper.TaosQueryA(conn.taos, sqlStr, handler.Handler)
	locker.Unlock()
	r := <-handler.Caller.QueryResult
	return r
}

// InsertStmt Prepare batch insert stmt
func (conn *Connector) InsertStmt() *insertstmt.InsertStmt {
	return insertstmt.NewInsertStmt(conn.taos)
}

// SelectDB Execute `use db`
func (conn *Connector) SelectDB(db string) error {
	locker.Lock()
	code := wrapper.TaosSelectDB(conn.taos, db)
	locker.Unlock()
	if code != 0 {
		err := taosError.NewError(code, wrapper.TaosErrorStr(nil))
		return err
	}
	return nil
}

// InfluxDBInsertLines Insert data using influxdb line format
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

// OpenTSDBInsertTelnetLines Insert data using opentsdb telnet format
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

// OpenTSDBInsertJsonPayload Insert data using opentsdb json format
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
