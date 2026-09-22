package unified

import (
	"database/sql/driver"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/taosdata/driver-go/v3/common"
	commonstmt "github.com/taosdata/driver-go/v3/common/stmt"
)

func rawSQLStmt(t *testing.T, sql string, insert bool, fields []*commonstmt.Stmt2AllField, data []*commonstmt.TaosStmt2BindData) *Stmt {
	t.Helper()
	s := &Stmt{sql: sql, isInsert: insert, fields: fields, bindMode: stmtBindModeRaw, state: newStmtCompatState()}
	require.NoError(t, s.state.setRawBindData(data, insert))
	return s
}

func TestStmtToSQL(t *testing.T) {
	tests := []struct {
		name   string
		sql    string
		insert bool
		fields []*commonstmt.Stmt2AllField
		data   []*commonstmt.TaosStmt2BindData
		want   []string
	}{
		{
			name: "query string and timestamp",
			sql:  "select * from meters where name = ? and ts >= ?",
			data: []*commonstmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{{"O'Reilly\\test"}, {time.Unix(1700000000, 123).UTC()}},
			}},
			want: []string{"select * from meters where name = 'O\\'Reilly\\\\test' and ts >= '2023-11-14T22:13:20.000000123Z'"},
		},
		{
			name:   "query NULL",
			sql:    "select * from meters where value is ?",
			fields: []*commonstmt.Stmt2AllField{{FieldType: common.TSDB_DATA_TYPE_INT, BindType: commonstmt.TAOS_FIELD_QUERY}},
			data:   []*commonstmt.TaosStmt2BindData{{Cols: [][]driver.Value{{nil}}}},
			want:   []string{"select * from meters where value is NULL"},
		},
		{
			name:   "uppercase VALUES with nested expression",
			sql:    "INSERT INTO meters VALUES\n(coalesce(?, 0), ?)",
			insert: true,
			fields: []*commonstmt.Stmt2AllField{
				{FieldType: common.TSDB_DATA_TYPE_BIGINT, BindType: commonstmt.TAOS_FIELD_COL},
				{FieldType: common.TSDB_DATA_TYPE_INT, BindType: commonstmt.TAOS_FIELD_COL},
			},
			data: []*commonstmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{{int64(1)}, {int32(2)}},
			}},
			want: []string{"INSERT INTO meters VALUES\n(coalesce(1, 0), 2)"},
		},
		{
			name:   "fixed table insert multiple rows",
			sql:    "insert into meters values (?, ?)",
			insert: true,
			fields: []*commonstmt.Stmt2AllField{
				{FieldType: common.TSDB_DATA_TYPE_TIMESTAMP, BindType: commonstmt.TAOS_FIELD_COL},
				{FieldType: common.TSDB_DATA_TYPE_INT, BindType: commonstmt.TAOS_FIELD_COL},
			},
			data: []*commonstmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{{int64(1), int64(2)}, {int32(10), int32(20)}},
			}},
			want: []string{"insert into meters values (1, 10) (2, 20)"},
		},
		{
			name:   "subtable insert multiple tables",
			sql:    "insert into ? using meters tags(?) values (?, ?)",
			insert: true,
			fields: []*commonstmt.Stmt2AllField{
				{FieldType: common.TSDB_DATA_TYPE_BINARY, BindType: commonstmt.TAOS_FIELD_TBNAME},
				{FieldType: common.TSDB_DATA_TYPE_NCHAR, BindType: commonstmt.TAOS_FIELD_TAG},
				{FieldType: common.TSDB_DATA_TYPE_TIMESTAMP, BindType: commonstmt.TAOS_FIELD_COL},
				{FieldType: common.TSDB_DATA_TYPE_INT, BindType: commonstmt.TAOS_FIELD_COL},
			},
			data: []*commonstmt.TaosStmt2BindData{
				{TableName: "d1", Tags: []driver.Value{"beijing"}, Cols: [][]driver.Value{{int64(1), int64(2)}, {int32(10), int32(20)}}},
				{TableName: "db.d`2", Tags: []driver.Value{"shanghai"}, Cols: [][]driver.Value{{int64(3)}, {int32(30)}}},
			},
			want: []string{
				"insert into `d1` using meters tags('beijing') values (1, 10) (2, 20)",
				"insert into `db`.`d``2` using meters tags('shanghai') values (3, 30)",
			},
		},
		{
			name:   "subtable insert multiple tags",
			sql:    "insert into ? using meters tags(?, ?) values (?, ?)",
			insert: true,
			fields: []*commonstmt.Stmt2AllField{
				{FieldType: common.TSDB_DATA_TYPE_BINARY, BindType: commonstmt.TAOS_FIELD_TBNAME},
				{FieldType: common.TSDB_DATA_TYPE_NCHAR, BindType: commonstmt.TAOS_FIELD_TAG},
				{FieldType: common.TSDB_DATA_TYPE_INT, BindType: commonstmt.TAOS_FIELD_TAG},
				{FieldType: common.TSDB_DATA_TYPE_TIMESTAMP, BindType: commonstmt.TAOS_FIELD_COL},
				{FieldType: common.TSDB_DATA_TYPE_BOOL, BindType: commonstmt.TAOS_FIELD_COL},
			},
			data: []*commonstmt.TaosStmt2BindData{{
				TableName: "d1", Tags: []driver.Value{"beijing", int32(1)}, Cols: [][]driver.Value{{int64(1)}, {true}},
			}},
			want: []string{"insert into `d1` using meters tags('beijing', 1) values (1, 1)"},
		},
		{
			name:   "binary and decimal",
			sql:    "insert into t values (?, ?, ?, ?)",
			insert: true,
			fields: []*commonstmt.Stmt2AllField{
				{FieldType: common.TSDB_DATA_TYPE_TIMESTAMP, BindType: commonstmt.TAOS_FIELD_COL},
				{FieldType: common.TSDB_DATA_TYPE_VARBINARY, BindType: commonstmt.TAOS_FIELD_COL},
				{FieldType: common.TSDB_DATA_TYPE_BLOB, BindType: commonstmt.TAOS_FIELD_COL},
				{FieldType: common.TSDB_DATA_TYPE_DECIMAL, BindType: commonstmt.TAOS_FIELD_COL},
			},
			data: []*commonstmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{{int64(1)}, {[]byte{0, 1, 255}}, {[]byte("blob")}, {"12.3400"}},
			}},
			want: []string{"insert into t values (1, '\\x0001FF', '\\x626C6F62', 12.3400)"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := rawSQLStmt(t, tt.sql, tt.insert, tt.fields, tt.data)

			got, err := s.ToSQL()
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
			require.True(t, s.state.hasBindData(tt.insert), "ToSQL must not consume bind data")
		})
	}
}

func TestStmtToSQLErrors(t *testing.T) {
	tests := []struct {
		name     string
		stmt     func(t *testing.T) *Stmt
		want     error
		wantText string
	}{
		{
			name: "not prepared",
			stmt: func(*testing.T) *Stmt { return &Stmt{state: newStmtCompatState()} },
			want: ErrStmtNotPrepared,
		},
		{
			name: "compatibility binding",
			stmt: func(*testing.T) *Stmt {
				return &Stmt{sql: "insert into t values(?)", isInsert: true, bindMode: stmtBindModeCompat, state: newStmtCompatState()}
			},
			want: ErrStmtToSQLRequiresBind,
		},
		{
			name: "closed statement",
			stmt: func(*testing.T) *Stmt {
				return &Stmt{sql: "select ?", closed: true, state: newStmtCompatState()}
			},
			want: ErrUnifiedClosed,
		},
		{
			name: "no raw binding",
			stmt: func(*testing.T) *Stmt {
				return &Stmt{sql: "insert into t values(?)", isInsert: true, bindMode: stmtBindModeRaw, state: newStmtCompatState()}
			},
			want: ErrStmtToSQLRequiresBind,
		},
		{
			name: "geometry WKB",
			stmt: func(t *testing.T) *Stmt {
				fields := []*commonstmt.Stmt2AllField{{FieldType: common.TSDB_DATA_TYPE_GEOMETRY, BindType: commonstmt.TAOS_FIELD_COL}}
				return rawSQLStmt(t, "insert into t values(?)", true, fields, []*commonstmt.TaosStmt2BindData{{Cols: [][]driver.Value{{[]byte{1, 2}}}}})
			},
			wantText: "does not support GEOMETRY",
		},
		{
			name: "query parameter has multiple values",
			stmt: func(t *testing.T) *Stmt {
				return rawSQLStmt(t, "select * from t where v = ?", false, nil, []*commonstmt.TaosStmt2BindData{{Cols: [][]driver.Value{{int32(1), int32(2)}}}})
			},
			wantText: "must contain exactly one value",
		},
		{
			name: "query marker count does not match bind data",
			stmt: func(t *testing.T) *Stmt {
				return rawSQLStmt(t, "select * from t where a = ? and b = ?", false, nil, []*commonstmt.TaosStmt2BindData{{Cols: [][]driver.Value{{int32(1)}}}})
			},
			want: driver.ErrSkip,
		},
		{
			name: "schema changed",
			stmt: func(*testing.T) *Stmt {
				return &Stmt{sql: "insert into t values(?)", schemaChanged: true, state: newStmtCompatState()}
			},
			want: ErrStmtSchemaChanged,
		},
		{
			name: "invalid table name",
			stmt: func(t *testing.T) *Stmt {
				fields := []*commonstmt.Stmt2AllField{
					{FieldType: common.TSDB_DATA_TYPE_BINARY, BindType: commonstmt.TAOS_FIELD_TBNAME},
					{FieldType: common.TSDB_DATA_TYPE_INT, BindType: commonstmt.TAOS_FIELD_COL},
				}
				return rawSQLStmt(t, "insert into ? values(?)", true, fields, []*commonstmt.TaosStmt2BindData{{TableName: "d1;drop", Cols: [][]driver.Value{{int32(1)}}}})
			},
			wantText: "invalid table name",
		},
		{
			name: "invalid decimal",
			stmt: func(t *testing.T) *Stmt {
				fields := []*commonstmt.Stmt2AllField{{FieldType: common.TSDB_DATA_TYPE_DECIMAL, BindType: commonstmt.TAOS_FIELD_COL}}
				return rawSQLStmt(t, "insert into t values(?)", true, fields, []*commonstmt.TaosStmt2BindData{{Cols: [][]driver.Value{{"not-a-number"}}}})
			},
			wantText: "invalid decimal value",
		},
		{
			name: "decimal has wrong Go type",
			stmt: func(t *testing.T) *Stmt {
				fields := []*commonstmt.Stmt2AllField{{FieldType: common.TSDB_DATA_TYPE_DECIMAL, BindType: commonstmt.TAOS_FIELD_COL}}
				return rawSQLStmt(t, "insert into t values(?)", true, fields, []*commonstmt.TaosStmt2BindData{{Cols: [][]driver.Value{{int32(1)}}}})
			},
			wantText: "decimal value must be string",
		},
		{
			name: "binary has wrong Go type",
			stmt: func(t *testing.T) *Stmt {
				fields := []*commonstmt.Stmt2AllField{{FieldType: common.TSDB_DATA_TYPE_VARBINARY, BindType: commonstmt.TAOS_FIELD_COL}}
				return rawSQLStmt(t, "insert into t values(?)", true, fields, []*commonstmt.TaosStmt2BindData{{Cols: [][]driver.Value{{"binary"}}}})
			},
			wantText: "binary value must be []byte",
		},
		{
			name: "NUL in text",
			stmt: func(t *testing.T) *Stmt {
				fields := []*commonstmt.Stmt2AllField{{FieldType: common.TSDB_DATA_TYPE_NCHAR, BindType: commonstmt.TAOS_FIELD_COL}}
				return rawSQLStmt(t, "insert into t values(?)", true, fields, []*commonstmt.TaosStmt2BindData{{Cols: [][]driver.Value{{"a\x00b"}}}})
			},
			wantText: "string value contains NUL",
		},
		{
			name: "parameter marker after VALUES tuple",
			stmt: func(t *testing.T) *Stmt {
				fields := []*commonstmt.Stmt2AllField{
					{FieldType: common.TSDB_DATA_TYPE_INT, BindType: commonstmt.TAOS_FIELD_COL},
					{FieldType: common.TSDB_DATA_TYPE_INT, BindType: commonstmt.TAOS_FIELD_COL},
				}
				return rawSQLStmt(t, "insert into t values(?) ?", true, fields, []*commonstmt.TaosStmt2BindData{{Cols: [][]driver.Value{{int32(1)}, {int32(2)}}}})
			},
			wantText: "unsupported insert SQL parameter layout",
		},
		{
			name: "insert marker count does not match fields",
			stmt: func(t *testing.T) *Stmt {
				fields := []*commonstmt.Stmt2AllField{{FieldType: common.TSDB_DATA_TYPE_INT, BindType: commonstmt.TAOS_FIELD_COL}}
				return rawSQLStmt(t, "insert into t values(?, ?)", true, fields, []*commonstmt.TaosStmt2BindData{{Cols: [][]driver.Value{{int32(1)}}}})
			},
			wantText: "marker count does not match field count",
		},
		{
			name: "insert has no VALUES clause",
			stmt: func(t *testing.T) *Stmt {
				fields := []*commonstmt.Stmt2AllField{{FieldType: common.TSDB_DATA_TYPE_INT, BindType: commonstmt.TAOS_FIELD_COL}}
				return rawSQLStmt(t, "insert into t select ?", true, fields, []*commonstmt.TaosStmt2BindData{{Cols: [][]driver.Value{{int32(1)}}}})
			},
			wantText: "insert SQL has no VALUES clause",
		},
		{
			name: "VALUES has no row tuple",
			stmt: func(t *testing.T) *Stmt {
				fields := []*commonstmt.Stmt2AllField{{FieldType: common.TSDB_DATA_TYPE_INT, BindType: commonstmt.TAOS_FIELD_COL}}
				return rawSQLStmt(t, "insert into t values ?", true, fields, []*commonstmt.TaosStmt2BindData{{Cols: [][]driver.Value{{int32(1)}}}})
			},
			wantText: "VALUES clause has no row tuple",
		},
		{
			name: "unterminated VALUES tuple",
			stmt: func(t *testing.T) *Stmt {
				fields := []*commonstmt.Stmt2AllField{{FieldType: common.TSDB_DATA_TYPE_INT, BindType: commonstmt.TAOS_FIELD_COL}}
				return rawSQLStmt(t, "insert into t values(?", true, fields, []*commonstmt.TaosStmt2BindData{{Cols: [][]driver.Value{{int32(1)}}}})
			},
			wantText: "unterminated VALUES tuple",
		},
		{
			name: "combined rows exceed SQL length limit",
			stmt: func(t *testing.T) *Stmt {
				value := strings.Repeat("x", common.MaxTaosSqlLen/2)
				fields := []*commonstmt.Stmt2AllField{{FieldType: common.TSDB_DATA_TYPE_NCHAR, BindType: commonstmt.TAOS_FIELD_COL}}
				return rawSQLStmt(t, "insert into t values(?)", true, fields, []*commonstmt.TaosStmt2BindData{{Cols: [][]driver.Value{{value, value}}}})
			},
			wantText: "sql statement exceeds the maximum length",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tt.stmt(t).ToSQL()
			if tt.want != nil {
				require.ErrorIs(t, err, tt.want)
			} else {
				require.ErrorContains(t, err, tt.wantText)
			}
		})
	}
}

func TestBuildStmtSQL(t *testing.T) {
	queryFields := []*commonstmt.Stmt2AllField{{FieldType: common.TSDB_DATA_TYPE_NCHAR, BindType: commonstmt.TAOS_FIELD_QUERY}}
	insertFields := []*commonstmt.Stmt2AllField{{FieldType: common.TSDB_DATA_TYPE_INT, BindType: commonstmt.TAOS_FIELD_COL}}
	tests := []struct {
		name     string
		sql      string
		insert   bool
		fields   []*commonstmt.Stmt2AllField
		data     []*commonstmt.TaosStmt2BindData
		want     []string
		wantText string
	}{
		{
			name:   "query",
			sql:    "select * from t where name = ?",
			fields: queryFields,
			data:   []*commonstmt.TaosStmt2BindData{{Cols: [][]driver.Value{{"name"}}}},
			want:   []string{"select * from t where name = 'name'"},
		},
		{
			name:     "query missing bind data",
			sql:      "select ?",
			fields:   queryFields,
			wantText: "query bind data is missing",
		},
		{
			name:   "insert",
			sql:    "insert into t values(?)",
			insert: true,
			fields: insertFields,
			data:   []*commonstmt.TaosStmt2BindData{{Cols: [][]driver.Value{{int32(1), int32(2)}}}},
			want:   []string{"insert into t values(1) (2)"},
		},
		{
			name:     "insert marker count mismatch",
			sql:      "insert into t values(?, ?)",
			insert:   true,
			fields:   insertFields,
			data:     []*commonstmt.TaosStmt2BindData{{Cols: [][]driver.Value{{int32(1)}}}},
			wantText: "marker count does not match field count",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := buildStmtSQL(tt.sql, tt.insert, tt.fields, tt.data)
			if tt.wantText != "" {
				require.ErrorContains(t, err, tt.wantText)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestStmtQueryArgs(t *testing.T) {
	tests := []struct {
		name     string
		cols     [][]driver.Value
		fields   []*commonstmt.Stmt2AllField
		want     []driver.Value
		wantText string
	}{
		{
			name:   "serializes values by field type",
			cols:   [][]driver.Value{{"O'Reilly"}, {int32(1)}},
			fields: []*commonstmt.Stmt2AllField{{FieldType: common.TSDB_DATA_TYPE_NCHAR}, {FieldType: common.TSDB_DATA_TYPE_INT}},
			want:   []driver.Value{"'O\\'Reilly'", int32(1)},
		},
		{
			name:     "parameter has multiple values",
			cols:     [][]driver.Value{{int32(1), int32(2)}},
			wantText: "query parameter 0 must contain exactly one value",
		},
		{
			name:     "text contains NUL",
			cols:     [][]driver.Value{{"a\x00b"}},
			fields:   []*commonstmt.Stmt2AllField{{FieldType: common.TSDB_DATA_TYPE_NCHAR}},
			wantText: "string value contains NUL",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := stmtQueryArgs(tt.cols, tt.fields)
			if tt.wantText != "" {
				require.ErrorContains(t, err, tt.wantText)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestStmtInsertArgs(t *testing.T) {
	validFields := []*commonstmt.Stmt2AllField{
		{FieldType: common.TSDB_DATA_TYPE_BINARY, BindType: commonstmt.TAOS_FIELD_TBNAME},
		{FieldType: common.TSDB_DATA_TYPE_NCHAR, BindType: commonstmt.TAOS_FIELD_TAG},
		{FieldType: common.TSDB_DATA_TYPE_INT, BindType: commonstmt.TAOS_FIELD_COL},
	}
	validData := &commonstmt.TaosStmt2BindData{TableName: "db.d1", Tags: []driver.Value{"tag"}, Cols: [][]driver.Value{{int32(1)}}}
	tests := []struct {
		name     string
		data     *commonstmt.TaosStmt2BindData
		fields   []*commonstmt.Stmt2AllField
		row      int
		colBase  int
		want     []driver.Value
		wantText string
	}{
		{
			name:   "table tag and column",
			data:   validData,
			fields: validFields,
			want:   []driver.Value{"`db`.`d1`", "'tag'", int32(1)},
		},
		{
			name:     "missing tag",
			data:     &commonstmt.TaosStmt2BindData{TableName: "d1", Cols: [][]driver.Value{{int32(1)}}},
			fields:   validFields,
			wantText: "missing tag value 0",
		},
		{
			name:     "missing column row",
			data:     validData,
			fields:   validFields,
			row:      1,
			wantText: "missing column value 0 row 1",
		},
		{
			name:     "unsupported bind type",
			data:     validData,
			fields:   []*commonstmt.Stmt2AllField{{BindType: 99}},
			wantText: "unsupported bind type 99",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := stmtInsertArgs(tt.data, tt.fields, tt.row, tt.colBase)
			if tt.wantText != "" {
				require.ErrorContains(t, err, tt.wantText)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestStmtInsertTemplate(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		prefix   string
		tuple    string
		suffix   string
		wantText string
	}{
		{
			name:   "nested tuple",
			sql:    "insert into t VALUES (coalesce(?, 0), ?) suffix",
			prefix: "insert into t VALUES ",
			tuple:  "(coalesce(?, 0), ?)",
			suffix: " suffix",
		},
		{name: "no VALUES", sql: "insert into t select ?", wantText: "insert SQL has no VALUES clause"},
		{name: "no row tuple", sql: "insert into t values ?", wantText: "VALUES clause has no row tuple"},
		{name: "unterminated tuple", sql: "insert into t values (?", wantText: "unterminated VALUES tuple"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			prefix, tuple, suffix, err := stmtInsertTemplate(tt.sql)
			if tt.wantText != "" {
				require.ErrorContains(t, err, tt.wantText)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.prefix, prefix)
			require.Equal(t, tt.tuple, tuple)
			require.Equal(t, tt.suffix, suffix)
		})
	}
}
