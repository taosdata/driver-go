package insertstmt

import (
	"errors"
	taosError "github.com/taosdata/driver-go/v2/errors"
	"github.com/taosdata/driver-go/v2/wrapper"
	"sync"
	"unsafe"
)

type InsertStmt struct {
	taos     unsafe.Pointer
	stmt     unsafe.Pointer
	prepared bool
	sync.RWMutex
}

func NewInsertStmt(taosConn unsafe.Pointer) *InsertStmt {
	stmt := wrapper.TaosStmtInit(taosConn)
	return &InsertStmt{stmt: stmt, taos: taosConn}
}

func (stmt *InsertStmt) Prepare(sql string) (*PreparedStmt, error) {
	//must lock
	stmt.Lock()
	defer stmt.Unlock()
	if stmt.prepared {
		return nil, errors.New("duplicate prepare")
	}
	code := wrapper.TaosStmtPrepare(stmt.stmt, sql)
	err := taosError.GetError(code)
	if err != nil {
		return nil, err
	}
	isInsert, code := wrapper.TaosStmtIsInsert(stmt.stmt)
	err = taosError.GetError(code)
	if err != nil {
		return nil, err
	}
	if !isInsert {
		return nil, errors.New("only support insert")
	}
	stmt.prepared = true
	return NewPreparedStmt(stmt.stmt, stmt.taos), nil
}
