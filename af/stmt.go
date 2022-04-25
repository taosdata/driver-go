package af

import "C"
import (
	"database/sql/driver"
	"errors"
	"fmt"
	"unsafe"

	"github.com/taosdata/driver-go/v2/af/async"
	"github.com/taosdata/driver-go/v2/af/locker"
	"github.com/taosdata/driver-go/v2/af/param"
	taosError "github.com/taosdata/driver-go/v2/errors"
	"github.com/taosdata/driver-go/v2/wrapper"
)

type Stmt struct {
	stmt       unsafe.Pointer
	isInsert   bool
	paramCount int
}

func NewStmt(taosConn unsafe.Pointer) *Stmt {
	locker.Lock()
	stmt := wrapper.TaosStmtInit(taosConn)
	locker.Unlock()
	return &Stmt{stmt: stmt}
}

func (s *Stmt) Prepare(sql string) error {
	locker.Lock()
	code := wrapper.TaosStmtPrepare(s.stmt, sql)
	locker.Unlock()
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
	locker.Lock()
	code := wrapper.TaosStmtSetTBNameTags(s.stmt, tableName, tags.GetValues())
	locker.Unlock()
	err := taosError.GetError(code)
	return err
}

func (s *Stmt) SetTableName(tableName string) error {
	locker.Lock()
	code := wrapper.TaosStmtSetTBName(s.stmt, tableName)
	locker.Unlock()
	err := taosError.GetError(code)
	return err
}

func (s *Stmt) BindRow(row *param.Param) error {
	if s.paramCount == 0 {
		locker.Lock()
		code := wrapper.TaosStmtBindParam(s.stmt, nil)
		locker.Unlock()
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
	locker.Lock()
	code := wrapper.TaosStmtBindParam(s.stmt, value)
	locker.Unlock()
	err := taosError.GetError(code)
	return err
}

func (s *Stmt) GetAffectedRows() int {
	if !s.isInsert {
		return 0
	}
	return wrapper.TaosStmtAffectedRowsOnce(s.stmt)
}

func (s *Stmt) GetResultRows() (driver.Rows, error) {
	if s.isInsert {
		return nil, errors.New("not support on insert")
	}
	result := wrapper.TaosStmtUseResult(s.stmt)
	numFields := wrapper.TaosFieldCount(result)
	rs := &rows{
		handler: async.GetHandler(),
		result:  result,
	}
	var err error
	rs.rowsHeader, err = wrapper.ReadColumn(result, numFields)
	return rs, err
}

func (s *Stmt) AddBatch() error {
	locker.Lock()
	code := wrapper.TaosStmtAddBatch(s.stmt)
	locker.Unlock()
	err := taosError.GetError(code)
	return err
}

func (s *Stmt) Execute() error {
	locker.Lock()
	code := wrapper.TaosStmtExecute(s.stmt)
	locker.Unlock()
	err := taosError.GetError(code)
	return err
}

func (s *Stmt) Close() error {
	locker.Lock()
	code := wrapper.TaosStmtClose(s.stmt)
	locker.Unlock()
	err := taosError.GetError(code)
	s.stmt = nil
	return err
}
