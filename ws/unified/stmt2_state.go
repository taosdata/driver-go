package unified

import (
	"github.com/taosdata/driver-go/v3/common/param"
	commonstmt "github.com/taosdata/driver-go/v3/common/stmt"
)

type stmtCompatBatchState struct {
	TableName     string
	Tags          *param.Param
	TagBindType   *param.ColumnType
	Params        []*param.Param
	ParamBindType *param.ColumnType
}

type stmtCompatState struct {
	Current stmtCompatBatchState
	Tables  map[string]*commonstmt.TaosStmt2BindData
	Order   []string
	Query   *commonstmt.TaosStmt2BindData
	RawMode bool
}

func newStmtCompatState() *stmtCompatState {
	return &stmtCompatState{
		Tables: make(map[string]*commonstmt.TaosStmt2BindData, 4),
		Order:  make([]string, 0, 4),
	}
}

func (s *stmtCompatState) reset() {
	s.Current = stmtCompatBatchState{}
	for k := range s.Tables {
		delete(s.Tables, k)
	}
	s.Order = s.Order[:0]
	s.Query = nil
	s.RawMode = false
}

func (s *stmtCompatState) setTableName(name string) {
	s.Current.TableName = name
}

func (s *stmtCompatState) setTags(tags *param.Param, bindType *param.ColumnType) {
	s.Current.Tags = tags
	s.Current.TagBindType = bindType
}

func (s *stmtCompatState) bindParams(params []*param.Param, bindType *param.ColumnType) {
	if len(params) == 0 {
		s.Current.Params = nil
	} else {
		s.Current.Params = append(s.Current.Params[:0], params...)
	}
	s.Current.ParamBindType = bindType
}

func (s *stmtCompatState) addBatch(isInsert bool) error {
	if isInsert {
		bindData, err := buildStmt2InsertBindData(s.Current.TableName, s.Current.Tags, s.Current.Params)
		if err != nil {
			return err
		}
		if err = s.upsertTable(bindData); err != nil {
			return err
		}
	} else {
		bindData, err := buildStmt2QueryBindData(s.Current.Params)
		if err != nil {
			return err
		}
		s.Query = bindData[0]
	}
	s.Current = stmtCompatBatchState{}
	s.RawMode = false
	return nil
}

func (s *stmtCompatState) setRawBindData(bindData []*commonstmt.TaosStmt2BindData, isInsert bool) error {
	s.RawMode = true
	if !isInsert {
		for k := range s.Tables {
			delete(s.Tables, k)
		}
		s.Order = s.Order[:0]
		s.Query = nil
		if len(bindData) > 0 {
			s.Query = bindData[0]
		}
		return nil
	}
	s.Query = nil
	for i := 0; i < len(bindData); i++ {
		if err := s.upsertTable(bindData[i]); err != nil {
			return err
		}
	}
	return nil
}

func (s *stmtCompatState) clearBindData() {
	for k := range s.Tables {
		delete(s.Tables, k)
	}
	s.Order = s.Order[:0]
	s.Query = nil
	s.RawMode = false
}

func (s *stmtCompatState) hasBindData(isInsert bool) bool {
	if isInsert {
		return len(s.Order) > 0
	}
	return s.Query != nil
}

func (s *stmtCompatState) bindData(isInsert bool) []*commonstmt.TaosStmt2BindData {
	if !isInsert {
		if s.Query == nil {
			return nil
		}
		return []*commonstmt.TaosStmt2BindData{s.Query}
	}
	if len(s.Order) == 0 {
		return nil
	}
	out := make([]*commonstmt.TaosStmt2BindData, 0, len(s.Order))
	for i := 0; i < len(s.Order); i++ {
		key := s.Order[i]
		if data, ok := s.Tables[key]; ok && data != nil {
			out = append(out, data)
		}
	}
	return out
}

func (s *stmtCompatState) upsertTable(incoming *commonstmt.TaosStmt2BindData) error {
	if incoming == nil {
		return nil
	}
	if s.Tables == nil {
		s.Tables = make(map[string]*commonstmt.TaosStmt2BindData, 4)
	}
	if s.Order == nil {
		s.Order = make([]string, 0, 4)
	}
	key := incoming.TableName
	current, ok := s.Tables[key]
	if !ok || current == nil {
		s.Tables[key] = incoming
		s.Order = append(s.Order, key)
		return nil
	}
	if current.Tags == nil && len(incoming.Tags) > 0 {
		current.Tags = incoming.Tags
	}
	if len(current.Cols) != len(incoming.Cols) {
		return newInvalidStateErrorf("col count not match for table %q, got %d expect %d", key, len(incoming.Cols), len(current.Cols))
	}
	for i := 0; i < len(current.Cols); i++ {
		current.Cols[i] = append(current.Cols[i], incoming.Cols[i]...)
	}
	return nil
}
