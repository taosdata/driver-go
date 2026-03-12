package unified

import (
	"database/sql/driver"
	"testing"

	"github.com/taosdata/driver-go/v3/common/param"
	commonstmt "github.com/taosdata/driver-go/v3/common/stmt"
)

func TestStmtCompatStateSetTableNameOverrideBeforeAddBatch(t *testing.T) {
	state := NewStmtCompatState()
	state.SetTableName("tb1")
	state.SetTableName("tb2")
	state.BindParams([]*param.Param{param.NewParam(1).AddInt(1)}, param.NewColumnType(1).AddInt())
	if err := state.AddBatch(true); err != nil {
		t.Fatalf("unexpected add batch error: %v", err)
	}

	data := state.BindData(true)
	if len(data) != 1 {
		t.Fatalf("expect 1 batch, got %d", len(data))
	}
	if data[0].TableName != "tb2" {
		t.Fatalf("expect overwritten table name, got %s", data[0].TableName)
	}
}

func TestStmtCompatStateBindParamsOverwriteBeforeAddBatch(t *testing.T) {
	state := NewStmtCompatState()
	first := []*param.Param{param.NewParam(1).AddInt(1)}
	second := []*param.Param{param.NewParam(1).AddInt(2)}
	state.BindParams(first, param.NewColumnType(1).AddInt())
	state.BindParams(second, param.NewColumnType(1).AddInt())
	if err := state.AddBatch(true); err != nil {
		t.Fatalf("unexpected add batch error: %v", err)
	}

	data := state.BindData(true)
	if len(data) != 1 {
		t.Fatalf("expect 1 batch, got %d", len(data))
	}
	if len(data[0].Cols) != 1 {
		t.Fatalf("expect 1 param col, got %d", len(data[0].Cols))
	}
	if got, ok := data[0].Cols[0][0].(int32); !ok || got != int32(2) {
		t.Fatalf("expect last bind params to win, got %v", data[0].Cols[0][0])
	}
}

func TestStmtCompatStateAddBatchResetsCurrent(t *testing.T) {
	state := NewStmtCompatState()
	state.SetTableName("tb")
	state.SetTags(param.NewParam(1).AddNchar("tag"), param.NewColumnType(1).AddNchar(16))
	state.BindParams([]*param.Param{param.NewParam(1).AddInt(3)}, param.NewColumnType(1).AddInt())
	if err := state.AddBatch(true); err != nil {
		t.Fatalf("unexpected add batch error: %v", err)
	}

	if state.Current.TableName != "" {
		t.Fatalf("expect empty current table name, got %s", state.Current.TableName)
	}
	if state.Current.Tags != nil {
		t.Fatal("expect current tags reset")
	}
	if state.Current.Params != nil {
		t.Fatal("expect current params reset")
	}
}

func TestStmtCompatStateResetClearsBatches(t *testing.T) {
	state := NewStmtCompatState()
	err := state.SetRawBindData([]*commonstmt.TaosStmt2BindData{
		{
			TableName: "tb",
			Cols:      [][]driver.Value{{int32(1)}},
		},
	}, true)
	if err != nil {
		t.Fatalf("unexpected set raw bind data error: %v", err)
	}
	state.Reset()

	if len(state.BindData(true)) != 0 {
		t.Fatalf("expect no batches after reset, got %d", len(state.BindData(true)))
	}
	if state.Current.TableName != "" {
		t.Fatalf("expect empty current table name, got %s", state.Current.TableName)
	}
}

func TestStmtCompatStateMergeSameTable(t *testing.T) {
	state := NewStmtCompatState()
	err := state.SetRawBindData([]*commonstmt.TaosStmt2BindData{
		{
			TableName: "tb",
			Cols:      [][]driver.Value{{int32(1)}, {int32(10)}},
		},
		{
			TableName: "tb",
			Cols:      [][]driver.Value{{int32(2)}, {int32(20)}},
		},
	}, true)
	if err != nil {
		t.Fatalf("unexpected set raw bind data error: %v", err)
	}
	data := state.BindData(true)
	if len(data) != 1 {
		t.Fatalf("expect merged single table batch, got %d", len(data))
	}
	if len(data[0].Cols) != 2 || len(data[0].Cols[0]) != 2 || len(data[0].Cols[1]) != 2 {
		t.Fatalf("expect merged row count 2, got %+v", data[0].Cols)
	}
}

func TestStmtCompatStateMergeSameTableAcrossSetRawBindDataCalls(t *testing.T) {
	state := NewStmtCompatState()
	err := state.SetRawBindData([]*commonstmt.TaosStmt2BindData{
		{
			TableName: "tb",
			Cols:      [][]driver.Value{{int32(1)}, {int32(10)}},
		},
	}, true)
	if err != nil {
		t.Fatalf("unexpected set raw bind data error: %v", err)
	}
	err = state.SetRawBindData([]*commonstmt.TaosStmt2BindData{
		{
			TableName: "tb",
			Cols:      [][]driver.Value{{int32(2)}, {int32(20)}},
		},
	}, true)
	if err != nil {
		t.Fatalf("unexpected set raw bind data error: %v", err)
	}
	data := state.BindData(true)
	if len(data) != 1 {
		t.Fatalf("expect merged single table batch, got %d", len(data))
	}
	if len(data[0].Cols) != 2 || len(data[0].Cols[0]) != 2 || len(data[0].Cols[1]) != 2 {
		t.Fatalf("expect merged row count 2, got %+v", data[0].Cols)
	}
}
