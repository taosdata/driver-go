package insertstmt

import (
	"errors"
	"github.com/taosdata/driver-go/v2/af/param"
	taosError "github.com/taosdata/driver-go/v2/errors"
	"github.com/taosdata/driver-go/v2/wrapper"
	"sync"
	"unsafe"
)

type PreparedStmt struct {
	taos unsafe.Pointer
	stmt unsafe.Pointer
	sync.RWMutex
	setTableName bool
}

func NewPreparedStmt(stmt, taos unsafe.Pointer) *PreparedStmt {
	return &PreparedStmt{stmt: stmt, taos: taos}
}

func (p *PreparedStmt) SetTableName(name string) error {
	if p.setTableName {
		return errors.New("duplicate set table name")
	}
	p.Lock()
	defer p.Unlock()
	code := wrapper.TaosStmtSetTBName(p.stmt, name)
	err := taosError.GetError(code)
	if err != nil {
		return err
	}
	p.setTableName = true
	return nil
}

func (p *PreparedStmt) SetSubTableName(name string) error {
	if p.setTableName {
		return errors.New("duplicate set table name")
	}
	p.Lock()
	defer p.Unlock()
	code := wrapper.TaosLoadTableInfo(p.taos, []string{name})
	err := taosError.GetError(code)
	if err != nil {
		return err
	}
	code = wrapper.TaosStmtSetSubTBName(p.stmt, name)
	err = taosError.GetError(code)
	if err != nil {
		return err
	}
	p.setTableName = true
	return nil
}

func (p *PreparedStmt) SetTableNameWithTags(tableName string, tags *param.Param) error {
	if p.setTableName {
		return errors.New("duplicate set table name")
	}
	p.Lock()
	defer p.Unlock()
	code := wrapper.TaosStmtSetTBNameTags(p.stmt, tableName, tags.GetValues())
	err := taosError.GetError(code)
	if err != nil {
		return err
	}
	p.setTableName = true
	return nil
}

func (p *PreparedStmt) BindParam(params []*param.Param, bindType *param.ColumnType) error {
	data := make([][]interface{}, len(params))
	for columnIndex, columnData := range params {
		value := columnData.GetValues()
		data[columnIndex] = value
	}
	columnTypes, err := bindType.GetValue()
	if err != nil {
		return err
	}
	code := wrapper.TaosStmtBindParamBatch(p.stmt, data, columnTypes)
	err = taosError.GetError(code)
	if err != nil {
		return err
	}
	return nil
}

func (p *PreparedStmt) AddBatch() error {
	code := wrapper.TaosStmtAddBatch(p.stmt)
	err := taosError.GetError(code)
	return err
}

func (p *PreparedStmt) Execute() error {
	code := wrapper.TaosStmtExecute(p.stmt)
	err := taosError.GetError(code)
	return err
}

func (p *PreparedStmt) GetAffectedRows() int {
	result := wrapper.TaosStmtUseResult(p.stmt)
	defer wrapper.TaosFreeResult(result)
	affectedRows := wrapper.TaosAffectedRows(result)
	return affectedRows
}

func (p *PreparedStmt) Close() error {
	code := wrapper.TaosStmtClose(p.stmt)
	err := taosError.GetError(code)
	p.stmt = nil
	return err
}
