package unified

import (
	"database/sql/driver"
	"testing"

	"github.com/taosdata/driver-go/v3/common"
	commonstmt "github.com/taosdata/driver-go/v3/common/stmt"
)

// BenchmarkStmtToSQL measures SQL reconstruction for a typical multi-row insert.
func BenchmarkStmtToSQL(b *testing.B) {
	stmt := &Stmt{
		sql:      "insert into meters values (?, ?, ?)",
		isInsert: true,
		fields: []*commonstmt.Stmt2AllField{
			{FieldType: common.TSDB_DATA_TYPE_TIMESTAMP, BindType: commonstmt.TAOS_FIELD_COL},
			{FieldType: common.TSDB_DATA_TYPE_NCHAR, BindType: commonstmt.TAOS_FIELD_COL},
			{FieldType: common.TSDB_DATA_TYPE_DOUBLE, BindType: commonstmt.TAOS_FIELD_COL},
		},
		bindMode: stmtBindModeRaw,
		state:    newStmtCompatState(),
	}
	data := &commonstmt.TaosStmt2BindData{
		Cols: [][]driver.Value{
			{int64(1700000000000), int64(1700000000001), int64(1700000000002), int64(1700000000003)},
			{"beijing", "shanghai", "guangzhou", "shenzhen"},
			{12.5, 13.5, 14.5, 15.5},
		},
	}
	if err := stmt.state.setRawBindData([]*commonstmt.TaosStmt2BindData{data}, true); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sqls, err := stmt.ToSQL()
		if err != nil {
			b.Fatal(err)
		}
		if len(sqls) != 1 {
			b.Fatalf("got %d SQL statements, want 1", len(sqls))
		}
	}
}
