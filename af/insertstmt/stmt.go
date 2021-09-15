package insertstmt

import (
	"errors"
	"github.com/taosdata/driver-go/v2/af/param"
	taosError "github.com/taosdata/driver-go/v2/errors"
	"github.com/taosdata/driver-go/v2/wrapper"
	"unsafe"
)

type InsertStmt struct {
	stmt unsafe.Pointer
}

func NewInsertStmt(taosConn unsafe.Pointer) *InsertStmt {
	stmt := wrapper.TaosStmtInit(taosConn)
	return &InsertStmt{stmt: stmt}
}

func (stmt *InsertStmt) Prepare(sql string) error {
	code := wrapper.TaosStmtPrepare(stmt.stmt, sql)
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
	code := wrapper.TaosStmtSetTBName(stmt.stmt, name)
	err := taosError.GetError(code)
	if err != nil {
		return err
	}
	return nil
}

func (stmt *InsertStmt) SetSubTableName(name string) error {
	code := wrapper.TaosStmtSetSubTBName(stmt.stmt, name)
	err := taosError.GetError(code)
	if err != nil {
		return err
	}
	return nil
}

func (stmt *InsertStmt) SetTableNameWithTags(tableName string, tags *param.Param) error {
	code := wrapper.TaosStmtSetTBNameTags(stmt.stmt, tableName, tags.GetValues())
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
	code := wrapper.TaosStmtBindParamBatch(stmt.stmt, data, columnTypes)
	err = taosError.GetError(code)
	if err != nil {
		return err
	}
	return nil
}

func (stmt *InsertStmt) AddBatch() error {
	code := wrapper.TaosStmtAddBatch(stmt.stmt)
	err := taosError.GetError(code)
	return err
}

func (stmt *InsertStmt) Execute() error {
	code := wrapper.TaosStmtExecute(stmt.stmt)
	err := taosError.GetError(code)
	return err
}

func (stmt *InsertStmt) GetAffectedRows() int {
	result := wrapper.TaosStmtUseResult(stmt.stmt)
	defer wrapper.TaosFreeResult(result)
	affectedRows := wrapper.TaosAffectedRows(result)
	return affectedRows
}

func (stmt *InsertStmt) Close() error {
	code := wrapper.TaosStmtClose(stmt.stmt)
	err := taosError.GetError(code)
	stmt.stmt = nil
	return err
}
