package af

import "C"
import (
	"database/sql/driver"
	"errors"
	"fmt"
	"github.com/taosdata/driver-go/v2/af/param"
	taosError "github.com/taosdata/driver-go/v2/errors"
	"github.com/taosdata/driver-go/v2/wrapper"
	"unsafe"
)

type Stmt struct {
	stmt       unsafe.Pointer
	isInsert   bool
	paramCount int
}

func NewStmt(taosConn unsafe.Pointer) *Stmt {
	stmt := wrapper.TaosStmtInit(taosConn)
	return &Stmt{stmt: stmt}
}

func (s *Stmt) Prepare(sql string) error {
	code := wrapper.TaosStmtPrepare(s.stmt, sql)
	err := taosError.GetError(code)
	if err != nil {
		return err
	}
	isInsert, code := wrapper.TaosStmtIsInsert(s.stmt)
	err = taosError.GetError(code)
	if err != nil {
		return err
	}
	s.isInsert = isInsert
	numParams, code := wrapper.TaosStmtNumParams(s.stmt)
	err = taosError.GetError(code)
	if err != nil {
		return err
	}
	s.paramCount = numParams
	return err
}

func (s *Stmt) SetTableNameWithTags(tableName string, tags *param.Param) error {
	code := wrapper.TaosStmtSetTBNameTags(s.stmt, tableName, tags.GetValues())
	err := taosError.GetError(code)
	return err
}

func (s *Stmt) SetTableName(tableName string) error {
	code := wrapper.TaosStmtSetTBName(s.stmt, tableName)
	err := taosError.GetError(code)
	return err
}

func (s *Stmt) BindRow(row *param.Param) error {
	if s.paramCount == 0 {
		code := wrapper.TaosStmtBindParam(s.stmt, nil)
		err := taosError.GetError(code)
		return err
	}
	if row == nil {
		return fmt.Errorf("row param got nil")
	}
	value := row.GetValues()
	if len(value) != s.paramCount {
		return fmt.Errorf("row param count error : expect %d got %d", s.paramCount, len(value))
	}
	code := wrapper.TaosStmtBindParam(s.stmt, value)
	err := taosError.GetError(code)
	return err
}

func (s *Stmt) GetAffectedRows() int {
	if s.isInsert {
		return 0
	}
	result := wrapper.TaosStmtUseResult(s.stmt)
	defer wrapper.TaosFreeResult(result)
	affectedRows := wrapper.TaosAffectedRows(result)
	return affectedRows
}

func (s *Stmt) GetResultRows() (driver.Rows, error) {
	if s.isInsert {
		return nil, errors.New("not support on insert")
	}
	result := wrapper.TaosStmtUseResult(s.stmt)
	numFields := wrapper.TaosFieldCount(result)
	rs := &rows{
		result: result,
	}
	var err error
	rs.rowsHeader, err = wrapper.ReadColumn(result, numFields)
	return rs, err
}

func (s *Stmt) AddBatch() error {
	code := wrapper.TaosStmtAddBatch(s.stmt)
	err := taosError.GetError(code)
	return err
}

func (s *Stmt) Execute() error {
	code := wrapper.TaosStmtExecute(s.stmt)
	err := taosError.GetError(code)
	return err
}

func (s *Stmt) Close() error {
	code := wrapper.TaosStmtClose(s.stmt)
	err := taosError.GetError(code)
	s.stmt = nil
	return err
}
