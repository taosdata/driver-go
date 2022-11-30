package af

import "C"
import (
	"fmt"
	"unsafe"

	"github.com/taosdata/driver-go/v3/af/locker"
	"github.com/taosdata/driver-go/v3/common/param"
	taosError "github.com/taosdata/driver-go/v3/errors"
	"github.com/taosdata/driver-go/v3/wrapper"
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

func NewStmtWithReqID(taosConn unsafe.Pointer, reqID int64) *Stmt {
	locker.Lock()
	stmt := wrapper.TaosStmtInitWithReqID(taosConn, reqID)
	locker.Unlock()
	return &Stmt{stmt: stmt}
}

func (s *Stmt) Prepare(sql string) error {
	locker.Lock()
	code := wrapper.TaosStmtPrepare(s.stmt, sql)
	locker.Unlock()
	if code != 0 {
		errStr := wrapper.TaosStmtErrStr(s.stmt)
		return taosError.NewError(code, errStr)
	}
	isInsert, code := wrapper.TaosStmtIsInsert(s.stmt)
	if code != 0 {
		errStr := wrapper.TaosStmtErrStr(s.stmt)
		return taosError.NewError(code, errStr)
	}
	s.isInsert = isInsert
	numParams, code := wrapper.TaosStmtNumParams(s.stmt)
	if code != 0 {
		errStr := wrapper.TaosStmtErrStr(s.stmt)
		return taosError.NewError(code, errStr)
	}
	s.paramCount = numParams
	return nil
}

func (s *Stmt) SetTableNameWithTags(tableName string, tags *param.Param) error {
	locker.Lock()
	code := wrapper.TaosStmtSetTBNameTags(s.stmt, tableName, tags.GetValues())
	locker.Unlock()
	if code != 0 {
		errStr := wrapper.TaosStmtErrStr(s.stmt)
		return taosError.NewError(code, errStr)
	}
	return nil
}

func (s *Stmt) SetTableName(tableName string) error {
	locker.Lock()
	code := wrapper.TaosStmtSetTBName(s.stmt, tableName)
	locker.Unlock()
	if code != 0 {
		errStr := wrapper.TaosStmtErrStr(s.stmt)
		return taosError.NewError(code, errStr)
	}
	return nil
}

func (s *Stmt) BindRow(row *param.Param) error {
	if s.paramCount == 0 {
		locker.Lock()
		code := wrapper.TaosStmtBindParam(s.stmt, nil)
		locker.Unlock()
		if code != 0 {
			errStr := wrapper.TaosStmtErrStr(s.stmt)
			return taosError.NewError(code, errStr)
		}
		return nil
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
	if code != 0 {
		errStr := wrapper.TaosStmtErrStr(s.stmt)
		return taosError.NewError(code, errStr)
	}
	return nil
}

func (s *Stmt) GetAffectedRows() int {
	if !s.isInsert {
		return 0
	}
	return wrapper.TaosStmtAffectedRowsOnce(s.stmt)
}

func (s *Stmt) AddBatch() error {
	locker.Lock()
	code := wrapper.TaosStmtAddBatch(s.stmt)
	locker.Unlock()
	if code != 0 {
		errStr := wrapper.TaosStmtErrStr(s.stmt)
		return taosError.NewError(code, errStr)
	}
	return nil
}

func (s *Stmt) Execute() error {
	locker.Lock()
	code := wrapper.TaosStmtExecute(s.stmt)
	locker.Unlock()
	if code != 0 {
		errStr := wrapper.TaosStmtErrStr(s.stmt)
		return taosError.NewError(code, errStr)
	}
	return nil
}

func (s *Stmt) Close() error {
	locker.Lock()
	code := wrapper.TaosStmtClose(s.stmt)
	locker.Unlock()
	s.stmt = nil
	if code != 0 {
		errStr := wrapper.TaosStmtErrStr(s.stmt)
		return taosError.NewError(code, errStr)
	}
	return nil
}
