package insertstmt

import (
	"errors"
	"unsafe"

	"github.com/taosdata/driver-go/v2/af/locker"
	"github.com/taosdata/driver-go/v2/af/param"
	taosError "github.com/taosdata/driver-go/v2/errors"
	"github.com/taosdata/driver-go/v2/wrapper"
)

type InsertStmt struct {
	stmt unsafe.Pointer
}

func NewInsertStmt(taosConn unsafe.Pointer) *InsertStmt {
	locker.Lock()
	stmt := wrapper.TaosStmtInit(taosConn)
	locker.Unlock()
	return &InsertStmt{stmt: stmt}
}

func (stmt *InsertStmt) Prepare(sql string) error {
	locker.Lock()
	code := wrapper.TaosStmtPrepare(stmt.stmt, sql)
	locker.Unlock()
	err := taosError.GetError(code)
	if err != nil {
		return err
	}
	isInsert, code := wrapper.TaosStmtIsInsert(stmt.stmt)
	err = taosError.GetError(code)
	if err != nil {
		return err
	}
	if !isInsert {
		return errors.New("only support insert")
	}
	return nil
}

func (stmt *InsertStmt) SetTableName(name string) error {
	locker.Lock()
	code := wrapper.TaosStmtSetTBName(stmt.stmt, name)
	locker.Unlock()
	err := taosError.GetError(code)
	if err != nil {
		return err
	}
	return nil
}

func (stmt *InsertStmt) SetSubTableName(name string) error {
	locker.Lock()
	code := wrapper.TaosStmtSetSubTBName(stmt.stmt, name)
	locker.Unlock()
	err := taosError.GetError(code)
	if err != nil {
		return err
	}
	return nil
}

func (stmt *InsertStmt) SetTableNameWithTags(tableName string, tags *param.Param) error {
	locker.Lock()
	code := wrapper.TaosStmtSetTBNameTags(stmt.stmt, tableName, tags.GetValues())
	locker.Unlock()
	err := taosError.GetError(code)
	if err != nil {
		return err
	}
	return nil
}

func (stmt *InsertStmt) BindParam(params []*param.Param, bindType *param.ColumnType) error {
	data := make([][]interface{}, len(params))
	for columnIndex, columnData := range params {
		value := columnData.GetValues()
		data[columnIndex] = value
	}
	columnTypes, err := bindType.GetValue()
	if err != nil {
		return err
	}
	locker.Lock()
	code := wrapper.TaosStmtBindParamBatch(stmt.stmt, data, columnTypes)
	locker.Unlock()
	err = taosError.GetError(code)
	if err != nil {
		return err
	}
	return nil
}

func (stmt *InsertStmt) AddBatch() error {
	locker.Lock()
	code := wrapper.TaosStmtAddBatch(stmt.stmt)
	locker.Unlock()
	err := taosError.GetError(code)
	return err
}

func (stmt *InsertStmt) Execute() error {
	locker.Lock()
	code := wrapper.TaosStmtExecute(stmt.stmt)
	locker.Unlock()
	err := taosError.GetError(code)
	return err
}

func (stmt *InsertStmt) GetAffectedRows() int {
	return wrapper.TaosStmtAffectedRowsOnce(stmt.stmt)
}

func (stmt *InsertStmt) Close() error {
	locker.Lock()
	code := wrapper.TaosStmtClose(stmt.stmt)
	locker.Unlock()
	err := taosError.GetError(code)
	stmt.stmt = nil
	return err
}
