package wrapper

import (
	"database/sql/driver"
	"fmt"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/common/parser"
	"github.com/taosdata/driver-go/v3/common/stmt"
	taosError "github.com/taosdata/driver-go/v3/errors"
	"github.com/taosdata/driver-go/v3/wrapper/cgo"
)

type stmt2Result struct {
	res      unsafe.Pointer
	affected int
	n        int
}
type StmtCallBackTest struct {
	ExecResult chan *stmt2Result
}

func (s *StmtCallBackTest) ExecCall(res unsafe.Pointer, affected int, code int) {
	s.ExecResult <- &stmt2Result{
		res:      res,
		affected: affected,
		n:        code,
	}
}

func NewStmtCallBackTest() *StmtCallBackTest {
	return &StmtCallBackTest{
		ExecResult: make(chan *stmt2Result, 1),
	}
}

func TestStmt2BindData(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer TaosClose(conn)
	defer func() {
		err = exec(conn, "drop database if exists test_stmt2")
		if err != nil {
			t.Error(err)
			return
		}
	}()
	err = exec(conn, "create database if not exists test_stmt2 precision 'ms' keep 36500")
	if err != nil {
		t.Error(err)
		return
	}
	err = exec(conn, "use test_stmt2")
	if err != nil {
		t.Error(err)
		return
	}
	now := time.Now().Round(time.Millisecond)
	next1S := now.Add(time.Second)
	next2S := now.Add(2 * time.Second)

	tests := []struct {
		name        string
		tbType      string
		pos         string
		params      []*stmt.TaosStmt2BindData
		expectValue [][]driver.Value
	}{
		{
			name:   "int",
			tbType: "ts timestamp, v int",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{now},
					{int32(1)},
				},
			}},
			expectValue: [][]driver.Value{
				{now, int32(1)},
			},
		},
		{
			name:   "int null",
			tbType: "ts timestamp, v int",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{now},
					{nil},
				},
			}},
			expectValue: [][]driver.Value{
				{now, nil},
			},
		},
		{
			name:   "int null 3 cols",
			tbType: "ts timestamp, v int",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
						next1S,
						next2S,
					},
					{
						int32(1),
						nil,
						int32(2),
					},
				},
			}},
			expectValue: [][]driver.Value{
				{now, int32(1)},
				{next1S, nil},
				{next2S, int32(2)},
			},
		},
		{
			name:   "bool",
			tbType: "ts timestamp, v bool",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{{now}, {bool(true)}},
			}},

			expectValue: [][]driver.Value{{now, true}},
		},
		{
			name:   "bool null",
			tbType: "ts timestamp, v bool",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{{now}, {nil}},
			}},

			expectValue: [][]driver.Value{{now, nil}},
		},
		{
			name:   "bool null 3 cols",
			tbType: "ts timestamp, v bool",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
						next1S,
						next2S,
					},
					{
						bool(true),
						nil,
						bool(false),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, true},
				{next1S, nil},
				{next2S, false},
			},
		},
		{
			name:   "tinyint",
			tbType: "ts timestamp, v tinyint",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{{now}, {int8(1)}},
			}},

			expectValue: [][]driver.Value{
				{now, int8(1)},
			},
		},
		{
			name:   "tinyint null",
			tbType: "ts timestamp, v tinyint",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					}, {
						nil,
					}},
			}},

			expectValue: [][]driver.Value{
				{now, nil},
			},
		},
		{
			name:   "tinyint null 3 cols",
			tbType: "ts timestamp, v tinyint",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
						next1S,
						next2S,
					}, {
						int8(1),
						nil,
						int8(2),
					}},
			}},

			expectValue: [][]driver.Value{
				{now, int8(1)},
				{next1S, nil},
				{next2S, int8(2)},
			},
		},
		{
			name:   "smallint",
			tbType: "ts timestamp, v smallint",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						int16(1),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, int16(1)},
			},
		},
		{
			name:   "smallint null",
			tbType: "ts timestamp, v smallint",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						nil,
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, nil},
			},
		},
		{
			name:   "smallint null 3 cols",
			tbType: "ts timestamp, v smallint",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
						next1S,
						next2S,
					},
					{
						int16(1),
						nil,
						int16(2),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, int16(1)},
				{next1S, nil},
				{next2S, int16(2)},
			},
		},
		{
			name:   "bigint",
			tbType: "ts timestamp, v bigint",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						int64(1),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, int64(1)},
			},
		},
		{
			name:   "bigint null",
			tbType: "ts timestamp, v bigint",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						nil,
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, nil},
			},
		},
		{
			name:   "bigint null 3 cols",
			tbType: "ts timestamp, v bigint",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
						next1S,
						next2S,
					},
					{
						int64(1),
						nil,
						int64(2),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, int64(1)},
				{next1S, nil},
				{next2S, int64(2)},
			},
		},

		{
			name:   "tinyint unsigned",
			tbType: "ts timestamp, v tinyint unsigned",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						uint8(1),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, uint8(1)},
			},
		},
		{
			name:   "tinyint unsigned null",
			tbType: "ts timestamp, v tinyint unsigned",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						nil,
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, nil},
			},
		},
		{
			name:   "tinyint unsigned null 3 cols",
			tbType: "ts timestamp, v tinyint unsigned",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
						next1S,
						next2S,
					},
					{
						uint8(1),
						nil,
						uint8(2),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, uint8(1)},
				{next1S, nil},
				{next2S, uint8(2)},
			},
		},

		{
			name:   "smallint unsigned",
			tbType: "ts timestamp, v smallint unsigned",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						uint16(1),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, uint16(1)},
			},
		},
		{
			name:   "smallint unsigned null",
			tbType: "ts timestamp, v smallint unsigned",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						nil,
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, nil},
			},
		},
		{
			name:   "smallint unsigned null 3 cols",
			tbType: "ts timestamp, v smallint unsigned",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
						next1S,
						next2S,
					},
					{
						uint16(1),
						nil,
						uint16(2),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, uint16(1)},
				{next1S, nil},
				{next2S, uint16(2)},
			},
		},

		{
			name:   "int unsigned",
			tbType: "ts timestamp, v int unsigned",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						uint32(1),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, uint32(1)},
			},
		},
		{
			name:   "int unsigned null",
			tbType: "ts timestamp, v int unsigned",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						nil,
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, nil},
			},
		},
		{
			name:   "int unsigned null 3 cols",
			tbType: "ts timestamp, v int unsigned",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
						next1S,
						next2S,
					},
					{
						uint32(1),
						nil,
						uint32(2),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, uint32(1)},
				{next1S, nil},
				{next2S, uint32(2)},
			},
		},

		{
			name:   "bigint unsigned",
			tbType: "ts timestamp, v bigint unsigned",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						uint64(1),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, uint64(1)},
			},
		},
		{
			name:   "bigint unsigned null",
			tbType: "ts timestamp, v bigint unsigned",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						nil,
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, nil},
			},
		},
		{
			name:   "bigint unsigned null 3 cols",
			tbType: "ts timestamp, v bigint unsigned",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
						next1S,
						next2S,
					},
					{
						uint64(1),
						nil,
						uint64(2),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, uint64(1)},
				{next1S, nil},
				{next2S, uint64(2)},
			},
		},

		{
			name:   "float",
			tbType: "ts timestamp, v float",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						float32(1.2),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, float32(1.2)},
			},
		},
		{
			name:   "float null",
			tbType: "ts timestamp, v float",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						nil,
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, nil},
			},
		},
		{
			name:   "float null 3 cols",
			tbType: "ts timestamp, v float",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
						next1S,
						next2S,
					},
					{
						float32(1.2),
						nil,
						float32(2.2),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, float32(1.2)},
				{next1S, nil},
				{next2S, float32(2.2)},
			},
		},

		{
			name:   "double",
			tbType: "ts timestamp, v double",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						float64(1.2),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, float64(1.2)},
			},
		},
		{
			name:   "double null",
			tbType: "ts timestamp, v double",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						nil,
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, nil},
			},
		},
		{
			name:   "double null 3 cols",
			tbType: "ts timestamp, v double",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
						next1S,
						next2S,
					},
					{
						float64(1.2),
						nil,
						float64(2.2),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, float64(1.2)},
				{next1S, nil},
				{next2S, float64(2.2)},
			},
		},

		{
			name:   "binary",
			tbType: "ts timestamp, v binary(20)",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						[]byte("yes"),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, "yes"},
			},
		},
		{
			name:   "binary null",
			tbType: "ts timestamp, v binary(20)",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						nil,
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, nil},
			},
		},
		{
			name:   "binary null 3 cols",
			tbType: "ts timestamp, v binary(20)",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
						next1S,
						next2S,
					},
					{
						[]byte("yes"),
						nil,
						[]byte("中文"),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, "yes"},
				{next1S, nil},
				{next2S, "中文"},
			},
		},

		{
			name:   "varbinary",
			tbType: "ts timestamp, v varbinary(20)",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						[]byte("yes"),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, []byte("yes")},
			},
		},
		{
			name:   "varbinary null",
			tbType: "ts timestamp, v varbinary(20)",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						nil,
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, nil},
			},
		},

		{
			name:   "varbinary null 3 cols",
			tbType: "ts timestamp, v varbinary(20)",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
						next1S,
						next2S,
					},
					{
						[]byte("yes"),
						nil,
						[]byte("中文"),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, []byte("yes")},
				{next1S, nil},
				{next2S, []byte("中文")},
			},
		},

		{
			name:   "geometry",
			tbType: "ts timestamp, v geometry(100)",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						[]byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40},
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, []byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40}},
			},
		},
		{
			name:   "geometry null",
			tbType: "ts timestamp, v geometry(100)",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						nil,
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, nil},
			},
		},
		{
			name:   "geometry null 3 cols",
			tbType: "ts timestamp, v geometry(100)",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
						next1S,
						next2S,
					},
					{
						[]byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40},
						nil,
						[]byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40},
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, []byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40}},
				{next1S, nil},
				{next2S, []byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40}},
			},
		},

		{
			name:   "nchar",
			tbType: "ts timestamp, v nchar(20)",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						[]byte("yes"),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, "yes"},
			},
		},
		{
			name:   "nchar null",
			tbType: "ts timestamp, v nchar(20)",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						nil,
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, nil},
			},
		},
		{
			name:   "nchar null 3 cols",
			tbType: "ts timestamp, v nchar(20)",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
						next1S,
						next2S,
					},
					{
						[]byte("yes"),
						nil,
						[]byte("中文"),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, "yes"},
				{next1S, nil},
				{next2S, "中文"},
			},
		},

		{
			name:   "nchar bind string",
			tbType: "ts timestamp, v nchar(20)",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						"yes",
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, "yes"},
			},
		},

		{
			name:   "nchar bind string null 3 cols",
			tbType: "ts timestamp, v nchar(20)",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
						next1S,
						next2S,
					},
					{
						"yes",
						nil,
						"中文",
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, "yes"},
				{next1S, nil},
				{next2S, "中文"},
			},
		},

		{
			name:   "binary bind string",
			tbType: "ts timestamp, v binary(20)",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						"yes",
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, "yes"},
			},
		},

		{
			name:   "binary bind string null 3 cols",
			tbType: "ts timestamp, v binary(20)",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
						next1S,
						next2S,
					},
					{
						"yes",
						nil,
						"中文",
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, "yes"},
				{next1S, nil},
				{next2S, "中文"},
			},
		},

		{
			name:   "varbinary bind string",
			tbType: "ts timestamp, v varbinary(20)",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						"yes",
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, []byte("yes")},
			},
		},

		{
			name:   "varbinary bind string null 3 cols",
			tbType: "ts timestamp, v varbinary(20)",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
						next1S,
						next2S,
					},
					{
						"yes",
						nil,
						"中文",
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, []byte("yes")},
				{next1S, nil},
				{next2S, []byte("中文")},
			},
		},
	}
	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tbType := tc.tbType
			tbName := fmt.Sprintf("test_fast_insert_%02d", i)
			drop := fmt.Sprintf("drop table if exists %s", tbName)
			create := fmt.Sprintf("create table if not exists %s(%s)", tbName, tbType)
			pos := tc.pos
			sql := fmt.Sprintf("insert into %s values(%s)", tbName, pos)
			var err error
			if err = exec(conn, drop); err != nil {
				t.Error(err)
				return
			}
			if err = exec(conn, create); err != nil {
				t.Error(err)
				return
			}
			caller := NewStmtCallBackTest()
			handler := cgo.NewHandle(caller)
			insertStmt := TaosStmt2Init(conn, 0xcc123, false, false, handler)
			code := TaosStmt2Prepare(insertStmt, sql)
			if code != 0 {
				errStr := TaosStmt2Error(insertStmt)
				err = taosError.NewError(code, errStr)
				t.Error(err)
				return
			}
			isInsert, code := TaosStmt2IsInsert(insertStmt)
			if code != 0 {
				errStr := TaosStmt2Error(insertStmt)
				err = taosError.NewError(code, errStr)
				t.Error(err)
				return
			}
			assert.True(t, isInsert)
			code, count, cfields := TaosStmt2GetFields(insertStmt, stmt.TAOS_FIELD_COL)
			if code != 0 {
				errStr := TaosStmt2Error(insertStmt)
				err = taosError.NewError(code, errStr)
				t.Error(err)
				return
			}
			defer TaosStmt2FreeFields(insertStmt, cfields)
			assert.Equal(t, 2, count)
			fields := StmtParseFields(count, cfields)
			err = TaosStmt2BindParam(insertStmt, true, tc.params, fields, nil, -1)
			if err != nil {
				t.Error(err)
				return
			}
			code = TaosStmt2Exec(insertStmt)
			if code != 0 {
				errStr := TaosStmt2Error(insertStmt)
				err = taosError.NewError(code, errStr)
				t.Error(err)
				return
			}
			r := <-caller.ExecResult
			if r.n != 0 {
				errStr := TaosStmt2Error(insertStmt)
				err = taosError.NewError(r.n, errStr)
				t.Error(err)
				return
			}
			t.Log(r.affected)
			//time.Sleep(time.Second)
			code = TaosStmt2Close(insertStmt)
			if code != 0 {
				errStr := TaosStmt2Error(insertStmt)
				err = taosError.NewError(code, errStr)
				t.Error(err)
				return
			}
			result, err := query(conn, fmt.Sprintf("select * from %s order by ts asc", tbName))
			if err != nil {
				t.Error(err)
				return
			}
			assert.Equal(t, tc.expectValue, result)
		})
	}

}

func TestStmt2BindBytes(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer TaosClose(conn)
	defer func() {
		err = exec(conn, "drop database if exists test_stmt2_binary")
		if err != nil {
			t.Error(err)
			return
		}
	}()
	err = exec(conn, "create database if not exists test_stmt2_binary precision 'ms' keep 36500")
	if err != nil {
		t.Error(err)
		return
	}
	err = exec(conn, "use test_stmt2_binary")
	if err != nil {
		t.Error(err)
		return
	}
	now := time.Now().Round(time.Millisecond)
	next1S := now.Add(time.Second)
	next2S := now.Add(2 * time.Second)

	tests := []struct {
		name        string
		tbType      string
		pos         string
		params      []*stmt.TaosStmt2BindData
		expectValue [][]driver.Value
	}{
		{
			name:   "int",
			tbType: "ts timestamp, v int",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{now},
					{int32(1)},
				},
			}},
			expectValue: [][]driver.Value{
				{now, int32(1)},
			},
		},
		{
			name:   "int null",
			tbType: "ts timestamp, v int",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{now},
					{nil},
				},
			}},
			expectValue: [][]driver.Value{
				{now, nil},
			},
		},
		{
			name:   "int null 3 cols",
			tbType: "ts timestamp, v int",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
						next1S,
						next2S,
					},
					{
						int32(1),
						nil,
						int32(2),
					},
				},
			}},
			expectValue: [][]driver.Value{
				{now, int32(1)},
				{next1S, nil},
				{next2S, int32(2)},
			},
		},
		{
			name:   "bool",
			tbType: "ts timestamp, v bool",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{{now}, {bool(true)}},
			}},

			expectValue: [][]driver.Value{{now, true}},
		},
		{
			name:   "bool null",
			tbType: "ts timestamp, v bool",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{{now}, {nil}},
			}},

			expectValue: [][]driver.Value{{now, nil}},
		},
		{
			name:   "bool null 3 cols",
			tbType: "ts timestamp, v bool",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
						next1S,
						next2S,
					},
					{
						bool(true),
						nil,
						bool(false),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, true},
				{next1S, nil},
				{next2S, false},
			},
		},
		{
			name:   "tinyint",
			tbType: "ts timestamp, v tinyint",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{{now}, {int8(1)}},
			}},

			expectValue: [][]driver.Value{
				{now, int8(1)},
			},
		},
		{
			name:   "tinyint null",
			tbType: "ts timestamp, v tinyint",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					}, {
						nil,
					}},
			}},

			expectValue: [][]driver.Value{
				{now, nil},
			},
		},
		{
			name:   "tinyint null 3 cols",
			tbType: "ts timestamp, v tinyint",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
						next1S,
						next2S,
					}, {
						int8(1),
						nil,
						int8(2),
					}},
			}},

			expectValue: [][]driver.Value{
				{now, int8(1)},
				{next1S, nil},
				{next2S, int8(2)},
			},
		},
		{
			name:   "smallint",
			tbType: "ts timestamp, v smallint",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						int16(1),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, int16(1)},
			},
		},
		{
			name:   "smallint null",
			tbType: "ts timestamp, v smallint",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						nil,
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, nil},
			},
		},
		{
			name:   "smallint null 3 cols",
			tbType: "ts timestamp, v smallint",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
						next1S,
						next2S,
					},
					{
						int16(1),
						nil,
						int16(2),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, int16(1)},
				{next1S, nil},
				{next2S, int16(2)},
			},
		},
		{
			name:   "bigint",
			tbType: "ts timestamp, v bigint",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						int64(1),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, int64(1)},
			},
		},
		{
			name:   "bigint null",
			tbType: "ts timestamp, v bigint",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						nil,
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, nil},
			},
		},
		{
			name:   "bigint null 3 cols",
			tbType: "ts timestamp, v bigint",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
						next1S,
						next2S,
					},
					{
						int64(1),
						nil,
						int64(2),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, int64(1)},
				{next1S, nil},
				{next2S, int64(2)},
			},
		},

		{
			name:   "tinyint unsigned",
			tbType: "ts timestamp, v tinyint unsigned",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						uint8(1),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, uint8(1)},
			},
		},
		{
			name:   "tinyint unsigned null",
			tbType: "ts timestamp, v tinyint unsigned",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						nil,
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, nil},
			},
		},
		{
			name:   "tinyint unsigned null 3 cols",
			tbType: "ts timestamp, v tinyint unsigned",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
						next1S,
						next2S,
					},
					{
						uint8(1),
						nil,
						uint8(2),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, uint8(1)},
				{next1S, nil},
				{next2S, uint8(2)},
			},
		},

		{
			name:   "smallint unsigned",
			tbType: "ts timestamp, v smallint unsigned",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						uint16(1),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, uint16(1)},
			},
		},
		{
			name:   "smallint unsigned null",
			tbType: "ts timestamp, v smallint unsigned",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						nil,
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, nil},
			},
		},
		{
			name:   "smallint unsigned null 3 cols",
			tbType: "ts timestamp, v smallint unsigned",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
						next1S,
						next2S,
					},
					{
						uint16(1),
						nil,
						uint16(2),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, uint16(1)},
				{next1S, nil},
				{next2S, uint16(2)},
			},
		},

		{
			name:   "int unsigned",
			tbType: "ts timestamp, v int unsigned",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						uint32(1),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, uint32(1)},
			},
		},
		{
			name:   "int unsigned null",
			tbType: "ts timestamp, v int unsigned",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						nil,
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, nil},
			},
		},
		{
			name:   "int unsigned null 3 cols",
			tbType: "ts timestamp, v int unsigned",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
						next1S,
						next2S,
					},
					{
						uint32(1),
						nil,
						uint32(2),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, uint32(1)},
				{next1S, nil},
				{next2S, uint32(2)},
			},
		},

		{
			name:   "bigint unsigned",
			tbType: "ts timestamp, v bigint unsigned",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						uint64(1),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, uint64(1)},
			},
		},
		{
			name:   "bigint unsigned null",
			tbType: "ts timestamp, v bigint unsigned",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						nil,
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, nil},
			},
		},
		{
			name:   "bigint unsigned null 3 cols",
			tbType: "ts timestamp, v bigint unsigned",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
						next1S,
						next2S,
					},
					{
						uint64(1),
						nil,
						uint64(2),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, uint64(1)},
				{next1S, nil},
				{next2S, uint64(2)},
			},
		},

		{
			name:   "float",
			tbType: "ts timestamp, v float",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						float32(1.2),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, float32(1.2)},
			},
		},
		{
			name:   "float null",
			tbType: "ts timestamp, v float",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						nil,
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, nil},
			},
		},
		{
			name:   "float null 3 cols",
			tbType: "ts timestamp, v float",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
						next1S,
						next2S,
					},
					{
						float32(1.2),
						nil,
						float32(2.2),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, float32(1.2)},
				{next1S, nil},
				{next2S, float32(2.2)},
			},
		},

		{
			name:   "double",
			tbType: "ts timestamp, v double",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						float64(1.2),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, float64(1.2)},
			},
		},
		{
			name:   "double null",
			tbType: "ts timestamp, v double",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						nil,
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, nil},
			},
		},
		{
			name:   "double null 3 cols",
			tbType: "ts timestamp, v double",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
						next1S,
						next2S,
					},
					{
						float64(1.2),
						nil,
						float64(2.2),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, float64(1.2)},
				{next1S, nil},
				{next2S, float64(2.2)},
			},
		},

		{
			name:   "binary",
			tbType: "ts timestamp, v binary(20)",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						[]byte("yes"),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, "yes"},
			},
		},
		{
			name:   "binary null",
			tbType: "ts timestamp, v binary(20)",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						nil,
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, nil},
			},
		},
		{
			name:   "binary null 3 cols",
			tbType: "ts timestamp, v binary(20)",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
						next1S,
						next2S,
					},
					{
						[]byte("yes"),
						nil,
						[]byte("中文"),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, "yes"},
				{next1S, nil},
				{next2S, "中文"},
			},
		},

		{
			name:   "varbinary",
			tbType: "ts timestamp, v varbinary(20)",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						[]byte("yes"),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, []byte("yes")},
			},
		},
		{
			name:   "varbinary null",
			tbType: "ts timestamp, v varbinary(20)",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						nil,
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, nil},
			},
		},

		{
			name:   "varbinary null 3 cols",
			tbType: "ts timestamp, v varbinary(20)",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
						next1S,
						next2S,
					},
					{
						[]byte("yes"),
						nil,
						[]byte("中文"),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, []byte("yes")},
				{next1S, nil},
				{next2S, []byte("中文")},
			},
		},

		{
			name:   "geometry",
			tbType: "ts timestamp, v geometry(100)",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						[]byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40},
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, []byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40}},
			},
		},
		{
			name:   "geometry null",
			tbType: "ts timestamp, v geometry(100)",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						nil,
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, nil},
			},
		},
		{
			name:   "geometry null 3 cols",
			tbType: "ts timestamp, v geometry(100)",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
						next1S,
						next2S,
					},
					{
						[]byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40},
						nil,
						[]byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40},
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, []byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40}},
				{next1S, nil},
				{next2S, []byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40}},
			},
		},

		{
			name:   "nchar",
			tbType: "ts timestamp, v nchar(20)",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						[]byte("yes"),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, "yes"},
			},
		},
		{
			name:   "nchar null",
			tbType: "ts timestamp, v nchar(20)",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						nil,
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, nil},
			},
		},
		{
			name:   "nchar null 3 cols",
			tbType: "ts timestamp, v nchar(20)",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
						next1S,
						next2S,
					},
					{
						[]byte("yes"),
						nil,
						[]byte("中文"),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, "yes"},
				{next1S, nil},
				{next2S, "中文"},
			},
		},

		{
			name:   "nchar bind string",
			tbType: "ts timestamp, v nchar(20)",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						"yes",
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, "yes"},
			},
		},

		{
			name:   "nchar bind string null 3 cols",
			tbType: "ts timestamp, v nchar(20)",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
						next1S,
						next2S,
					},
					{
						"yes",
						nil,
						"中文",
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, "yes"},
				{next1S, nil},
				{next2S, "中文"},
			},
		},

		{
			name:   "binary bind string",
			tbType: "ts timestamp, v binary(20)",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						"yes",
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, "yes"},
			},
		},

		{
			name:   "binary bind string null 3 cols",
			tbType: "ts timestamp, v binary(20)",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
						next1S,
						next2S,
					},
					{
						"yes",
						nil,
						"中文",
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, "yes"},
				{next1S, nil},
				{next2S, "中文"},
			},
		},

		{
			name:   "varbinary bind string",
			tbType: "ts timestamp, v varbinary(20)",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						"yes",
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, []byte("yes")},
			},
		},

		{
			name:   "varbinary bind string null 3 cols",
			tbType: "ts timestamp, v varbinary(20)",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
						next1S,
						next2S,
					},
					{
						"yes",
						nil,
						"中文",
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, []byte("yes")},
				{next1S, nil},
				{next2S, []byte("中文")},
			},
		},
	}
	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tbType := tc.tbType
			tbName := fmt.Sprintf("test_fast_insert_%02d", i)
			drop := fmt.Sprintf("drop table if exists %s", tbName)
			create := fmt.Sprintf("create table if not exists %s(%s)", tbName, tbType)
			pos := tc.pos
			sql := fmt.Sprintf("insert into %s values(%s)", tbName, pos)
			var err error
			if err = exec(conn, drop); err != nil {
				t.Error(err)
				return
			}
			if err = exec(conn, create); err != nil {
				t.Error(err)
				return
			}
			caller := NewStmtCallBackTest()
			handler := cgo.NewHandle(caller)
			insertStmt := TaosStmt2Init(conn, 0xcc123, false, false, handler)
			code := TaosStmt2Prepare(insertStmt, sql)
			if code != 0 {
				errStr := TaosStmt2Error(insertStmt)
				err = taosError.NewError(code, errStr)
				t.Error(err)
				return
			}
			isInsert, code := TaosStmt2IsInsert(insertStmt)
			if code != 0 {
				errStr := TaosStmt2Error(insertStmt)
				err = taosError.NewError(code, errStr)
				t.Error(err)
				return
			}
			assert.True(t, isInsert)
			code, count, cfields := TaosStmt2GetFields(insertStmt, stmt.TAOS_FIELD_COL)
			if code != 0 {
				errStr := TaosStmt2Error(insertStmt)
				err = taosError.NewError(code, errStr)
				t.Error(err)
				return
			}
			defer TaosStmt2FreeFields(insertStmt, cfields)
			assert.Equal(t, 2, count)
			fields := StmtParseFields(count, cfields)
			bs, err := stmt.MarshalStmt2Binary(tc.params, true, fields, nil)
			if err != nil {
				t.Error("marshal binary error:", err)
				return
			}
			//for j := 0; j < len(bs); j++ {
			//	fmt.Printf("0x%02x, ", bs[j])
			//	if j%16 == 15 {
			//		fmt.Println()
			//	}
			//}
			err = TaosStmt2BindBinary(insertStmt, bs, -1)
			if !assert.NoError(t, err, bs) {
				return
			}
			//return
			code = TaosStmt2Exec(insertStmt)
			if code != 0 {
				errStr := TaosStmt2Error(insertStmt)
				err = taosError.NewError(code, errStr)
				t.Error(err)
				return
			}
			r := <-caller.ExecResult
			if r.n != 0 {
				errStr := TaosStmt2Error(insertStmt)
				err = taosError.NewError(r.n, errStr)
				t.Error(err)
				return
			}
			t.Log(r.affected)
			code = TaosStmt2Close(insertStmt)
			if code != 0 {
				errStr := TaosStmt2Error(insertStmt)
				err = taosError.NewError(code, errStr)
				t.Error(err)
				return
			}
			result, err := query(conn, fmt.Sprintf("select * from %s order by ts asc", tbName))
			if err != nil {
				t.Error(err)
				return
			}
			assert.Equal(t, tc.expectValue, result)
		})
	}

}

func TestStmt2AllType(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer TaosClose(conn)
	defer func() {
		err = exec(conn, "drop database if exists test_stmt2_all")
		if err != nil {
			t.Error(err)
			return
		}
	}()
	err = exec(conn, "create database if not exists test_stmt2_all precision 'ms' keep 36500")
	if err != nil {
		t.Error(err)
		return
	}
	err = exec(conn, "use test_stmt2_all")
	if err != nil {
		t.Error(err)
		return
	}
	err = exec(conn, "create table if not exists all_stb("+
		"ts timestamp, "+
		"v1 bool, "+
		"v2 tinyint, "+
		"v3 smallint, "+
		"v4 int, "+
		"v5 bigint, "+
		"v6 tinyint unsigned, "+
		"v7 smallint unsigned, "+
		"v8 int unsigned, "+
		"v9 bigint unsigned, "+
		"v10 float, "+
		"v11 double, "+
		"v12 binary(20), "+
		"v13 varbinary(20), "+
		"v14 geometry(100), "+
		"v15 nchar(20))"+
		"tags("+
		"tts timestamp, "+
		"tv1 bool, "+
		"tv2 tinyint, "+
		"tv3 smallint, "+
		"tv4 int, "+
		"tv5 bigint, "+
		"tv6 tinyint unsigned, "+
		"tv7 smallint unsigned, "+
		"tv8 int unsigned, "+
		"tv9 bigint unsigned, "+
		"tv10 float, "+
		"tv11 double, "+
		"tv12 binary(20), "+
		"tv13 varbinary(20), "+
		"tv14 geometry(100), "+
		"tv15 nchar(20))")
	if err != nil {
		t.Error(err)
		return
	}
	caller := NewStmtCallBackTest()
	handler := cgo.NewHandle(caller)
	insertStmt := TaosStmt2Init(conn, 0xcc123, false, false, handler)
	prepareInsertSql := "insert into ? using all_stb tags(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
	code := TaosStmt2Prepare(insertStmt, prepareInsertSql)
	if code != 0 {
		errStr := TaosStmt2Error(insertStmt)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	params := []*stmt.TaosStmt2BindData{{
		TableName: "ctb1",
	}}
	err = TaosStmt2BindParam(insertStmt, true, params, nil, nil, -1)
	if err != nil {
		t.Error(err)
		return
	}

	code, count, cTablefields := TaosStmt2GetFields(insertStmt, stmt.TAOS_FIELD_TBNAME)
	if code != 0 {
		errStr := TaosStmt2Error(insertStmt)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	assert.Equal(t, 1, count)
	assert.Equal(t, unsafe.Pointer(nil), cTablefields)

	isInsert, code := TaosStmt2IsInsert(insertStmt)
	if code != 0 {
		errStr := TaosStmt2Error(insertStmt)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	assert.True(t, isInsert)
	code, count, cColFields := TaosStmt2GetFields(insertStmt, stmt.TAOS_FIELD_COL)
	if code != 0 {
		errStr := TaosStmt2Error(insertStmt)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	defer TaosStmt2FreeFields(insertStmt, cColFields)
	assert.Equal(t, 16, count)
	colFields := StmtParseFields(count, cColFields)
	t.Log(colFields)
	code, count, cTagfields := TaosStmt2GetFields(insertStmt, stmt.TAOS_FIELD_TAG)
	if code != 0 {
		errStr := TaosStmt2Error(insertStmt)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	defer TaosStmt2FreeFields(insertStmt, cTagfields)
	assert.Equal(t, 16, count)
	tagFields := StmtParseFields(count, cTagfields)
	t.Log(tagFields)
	now := time.Now()
	//colTypes := []int8{
	//	common.TSDB_DATA_TYPE_TIMESTAMP,
	//	common.TSDB_DATA_TYPE_BOOL,
	//	common.TSDB_DATA_TYPE_TINYINT,
	//	common.TSDB_DATA_TYPE_SMALLINT,
	//	common.TSDB_DATA_TYPE_INT,
	//	common.TSDB_DATA_TYPE_BIGINT,
	//	common.TSDB_DATA_TYPE_UTINYINT,
	//	common.TSDB_DATA_TYPE_USMALLINT,
	//	common.TSDB_DATA_TYPE_UINT,
	//	common.TSDB_DATA_TYPE_UBIGINT,
	//	common.TSDB_DATA_TYPE_FLOAT,
	//	common.TSDB_DATA_TYPE_DOUBLE,
	//	common.TSDB_DATA_TYPE_BINARY,
	//	common.TSDB_DATA_TYPE_VARBINARY,
	//	common.TSDB_DATA_TYPE_GEOMETRY,
	//	common.TSDB_DATA_TYPE_NCHAR,
	//}
	params2 := []*stmt.TaosStmt2BindData{{
		TableName: "ctb1",
		Tags: []driver.Value{
			// TIMESTAMP
			now,
			// BOOL
			true,
			// TINYINT
			int8(1),
			// SMALLINT
			int16(1),
			// INT
			int32(1),
			// BIGINT
			int64(1),
			// UTINYINT
			uint8(1),
			// USMALLINT
			uint16(1),
			// UINT
			uint32(1),
			// UBIGINT
			uint64(1),
			// FLOAT
			float32(1.2),
			// DOUBLE
			float64(1.2),
			// BINARY
			[]byte("binary"),
			// VARBINARY
			[]byte("varbinary"),
			// GEOMETRY
			[]byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40},
			// NCHAR
			"nchar",
		},
		Cols: [][]driver.Value{
			{
				now,
				now.Add(time.Second),
				now.Add(time.Second * 2),
			},
			{
				true,
				nil,
				false,
			},
			{
				int8(11),
				nil,
				int8(12),
			},
			{
				int16(11),
				nil,
				int16(12),
			},
			{
				int32(11),
				nil,
				int32(12),
			},
			{
				int64(11),
				nil,
				int64(12),
			},
			{
				uint8(11),
				nil,
				uint8(12),
			},
			{
				uint16(11),
				nil,
				uint16(12),
			},
			{
				uint32(11),
				nil,
				uint32(12),
			},
			{
				uint64(11),
				nil,
				uint64(12),
			},
			{
				float32(11.2),
				nil,
				float32(12.2),
			},
			{
				float64(11.2),
				nil,
				float64(12.2),
			},
			{
				[]byte("binary1"),
				nil,
				[]byte("binary2"),
			},
			{
				[]byte("varbinary1"),
				nil,
				[]byte("varbinary2"),
			},
			{
				[]byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40},
				nil,
				[]byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40},
			},
			{
				"nchar1",
				nil,
				"nchar2",
			},
		},
	}}

	err = TaosStmt2BindParam(insertStmt, true, params2, colFields, tagFields, -1)
	if err != nil {
		t.Error(err)
		return
	}
	code = TaosStmt2Exec(insertStmt)
	if code != 0 {
		errStr := TaosStmt2Error(insertStmt)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	r := <-caller.ExecResult
	if r.n != 0 {
		errStr := TaosStmt2Error(insertStmt)
		err = taosError.NewError(r.n, errStr)
		t.Error(err)
		return
	}
	t.Log(r.affected)

	code = TaosStmt2Close(insertStmt)
	if code != 0 {
		errStr := TaosStmt2Error(insertStmt)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
}

func TestStmt2AllTypeBytes(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer TaosClose(conn)
	defer func() {
		err = exec(conn, "drop database if exists test_stmt2_all")
		if err != nil {
			t.Error(err)
			return
		}
	}()
	err = exec(conn, "create database if not exists test_stmt2_all_bytes precision 'ms' keep 36500")
	if err != nil {
		t.Error(err)
		return
	}
	err = exec(conn, "use test_stmt2_all_bytes")
	if err != nil {
		t.Error(err)
		return
	}
	err = exec(conn, "create table if not exists all_stb("+
		"ts timestamp, "+
		"v1 bool, "+
		"v2 tinyint, "+
		"v3 smallint, "+
		"v4 int, "+
		"v5 bigint, "+
		"v6 tinyint unsigned, "+
		"v7 smallint unsigned, "+
		"v8 int unsigned, "+
		"v9 bigint unsigned, "+
		"v10 float, "+
		"v11 double, "+
		"v12 binary(20), "+
		"v13 varbinary(20), "+
		"v14 geometry(100), "+
		"v15 nchar(20))"+
		"tags("+
		"tts timestamp, "+
		"tv1 bool, "+
		"tv2 tinyint, "+
		"tv3 smallint, "+
		"tv4 int, "+
		"tv5 bigint, "+
		"tv6 tinyint unsigned, "+
		"tv7 smallint unsigned, "+
		"tv8 int unsigned, "+
		"tv9 bigint unsigned, "+
		"tv10 float, "+
		"tv11 double, "+
		"tv12 binary(20), "+
		"tv13 varbinary(20), "+
		"tv14 geometry(100), "+
		"tv15 nchar(20))")
	if err != nil {
		t.Error(err)
		return
	}
	caller := NewStmtCallBackTest()
	handler := cgo.NewHandle(caller)
	insertStmt := TaosStmt2Init(conn, 0xcc123, false, false, handler)
	prepareInsertSql := "insert into ? using all_stb tags(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
	code := TaosStmt2Prepare(insertStmt, prepareInsertSql)
	if code != 0 {
		errStr := TaosStmt2Error(insertStmt)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	params := []*stmt.TaosStmt2BindData{{
		TableName: "ctb1",
	}}
	bs, err := stmt.MarshalStmt2Binary(params, true, nil, nil)
	if err != nil {
		t.Error(err)
		return
	}
	err = TaosStmt2BindBinary(insertStmt, bs, -1)
	if err != nil {
		t.Error(err)
		return
	}

	code, count, cTablefields := TaosStmt2GetFields(insertStmt, stmt.TAOS_FIELD_TBNAME)
	if code != 0 {
		errStr := TaosStmt2Error(insertStmt)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	assert.Equal(t, 1, count)
	assert.Equal(t, unsafe.Pointer(nil), cTablefields)

	isInsert, code := TaosStmt2IsInsert(insertStmt)
	if code != 0 {
		errStr := TaosStmt2Error(insertStmt)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	assert.True(t, isInsert)
	code, count, cColFields := TaosStmt2GetFields(insertStmt, stmt.TAOS_FIELD_COL)
	if code != 0 {
		errStr := TaosStmt2Error(insertStmt)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	defer TaosStmt2FreeFields(insertStmt, cColFields)
	assert.Equal(t, 16, count)
	colFields := StmtParseFields(count, cColFields)
	t.Log(colFields)
	code, count, cTagfields := TaosStmt2GetFields(insertStmt, stmt.TAOS_FIELD_TAG)
	if code != 0 {
		errStr := TaosStmt2Error(insertStmt)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	defer TaosStmt2FreeFields(insertStmt, cTagfields)
	assert.Equal(t, 16, count)
	tagFields := StmtParseFields(count, cTagfields)
	t.Log(tagFields)
	now := time.Now()
	//colTypes := []int8{
	//	common.TSDB_DATA_TYPE_TIMESTAMP,
	//	common.TSDB_DATA_TYPE_BOOL,
	//	common.TSDB_DATA_TYPE_TINYINT,
	//	common.TSDB_DATA_TYPE_SMALLINT,
	//	common.TSDB_DATA_TYPE_INT,
	//	common.TSDB_DATA_TYPE_BIGINT,
	//	common.TSDB_DATA_TYPE_UTINYINT,
	//	common.TSDB_DATA_TYPE_USMALLINT,
	//	common.TSDB_DATA_TYPE_UINT,
	//	common.TSDB_DATA_TYPE_UBIGINT,
	//	common.TSDB_DATA_TYPE_FLOAT,
	//	common.TSDB_DATA_TYPE_DOUBLE,
	//	common.TSDB_DATA_TYPE_BINARY,
	//	common.TSDB_DATA_TYPE_VARBINARY,
	//	common.TSDB_DATA_TYPE_GEOMETRY,
	//	common.TSDB_DATA_TYPE_NCHAR,
	//}
	params2 := []*stmt.TaosStmt2BindData{{
		TableName: "ctb1",
		Tags: []driver.Value{
			// TIMESTAMP
			now,
			// BOOL
			true,
			// TINYINT
			int8(1),
			// SMALLINT
			int16(1),
			// INT
			int32(1),
			// BIGINT
			int64(1),
			// UTINYINT
			uint8(1),
			// USMALLINT
			uint16(1),
			// UINT
			uint32(1),
			// UBIGINT
			uint64(1),
			// FLOAT
			float32(1.2),
			// DOUBLE
			float64(1.2),
			// BINARY
			[]byte("binary"),
			// VARBINARY
			[]byte("varbinary"),
			// GEOMETRY
			[]byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40},
			// NCHAR
			"nchar",
		},
		Cols: [][]driver.Value{
			{
				now,
				now.Add(time.Second),
				now.Add(time.Second * 2),
			},
			{
				true,
				nil,
				false,
			},
			{
				int8(11),
				nil,
				int8(12),
			},
			{
				int16(11),
				nil,
				int16(12),
			},
			{
				int32(11),
				nil,
				int32(12),
			},
			{
				int64(11),
				nil,
				int64(12),
			},
			{
				uint8(11),
				nil,
				uint8(12),
			},
			{
				uint16(11),
				nil,
				uint16(12),
			},
			{
				uint32(11),
				nil,
				uint32(12),
			},
			{
				uint64(11),
				nil,
				uint64(12),
			},
			{
				float32(11.2),
				nil,
				float32(12.2),
			},
			{
				float64(11.2),
				nil,
				float64(12.2),
			},
			{
				[]byte("binary1"),
				nil,
				[]byte("binary2"),
			},
			{
				[]byte("varbinary1"),
				nil,
				[]byte("varbinary2"),
			},
			{
				[]byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40},
				nil,
				[]byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40},
			},
			{
				"nchar1",
				nil,
				"nchar2",
			},
		},
	}}
	bs, err = stmt.MarshalStmt2Binary(params2, true, colFields, tagFields)
	if err != nil {
		t.Error(err)
		return
	}
	err = TaosStmt2BindBinary(insertStmt, bs, -1)
	if err != nil {
		t.Error(err)
		return
	}
	code = TaosStmt2Exec(insertStmt)
	if code != 0 {
		errStr := TaosStmt2Error(insertStmt)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	r := <-caller.ExecResult
	if r.n != 0 {
		errStr := TaosStmt2Error(insertStmt)
		err = taosError.NewError(r.n, errStr)
		t.Error(err)
		return
	}
	t.Log(r.affected)

	code = TaosStmt2Close(insertStmt)
	if code != 0 {
		errStr := TaosStmt2Error(insertStmt)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
}

func TestStmt2Query(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer TaosClose(conn)
	defer func() {
		err = exec(conn, "drop database if exists test_stmt2_query")
		if err != nil {
			t.Error(err)
			return
		}
	}()
	err = exec(conn, "create database if not exists test_stmt2_query precision 'ms' keep 36500")
	if err != nil {
		t.Error(err)
		return
	}
	err = exec(conn, "use test_stmt2_query")
	if err != nil {
		t.Error(err)
		return
	}
	err = exec(conn, "create table if not exists t(ts timestamp,v int)")
	if err != nil {
		t.Error(err)
		return
	}
	caller := NewStmtCallBackTest()
	handler := cgo.NewHandle(caller)
	stmt2 := TaosStmt2Init(conn, 0xcc123, false, false, handler)
	prepareInsertSql := "insert into t values (?,?)"
	code := TaosStmt2Prepare(stmt2, prepareInsertSql)
	if code != 0 {
		errStr := TaosStmt2Error(stmt2)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	isInsert, code := TaosStmt2IsInsert(stmt2)
	if code != 0 {
		errStr := TaosStmt2Error(stmt2)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	assert.True(t, isInsert)
	now := time.Now().Round(time.Millisecond)
	colTypes := []*stmt.StmtField{
		{
			FieldType: common.TSDB_DATA_TYPE_TIMESTAMP,
			Precision: common.PrecisionMilliSecond,
		},
		{
			FieldType: common.TSDB_DATA_TYPE_INT,
		},
	}
	params := []*stmt.TaosStmt2BindData{
		{
			TableName: "t",
			Cols: [][]driver.Value{
				{
					now,
					now.Add(time.Second),
				},
				{
					int32(1),
					int32(2),
				},
			},
		},
		{
			TableName: "t",
			Cols: [][]driver.Value{
				{
					now.Add(time.Second * 2),
					now.Add(time.Second * 3),
				},
				{
					int32(3),
					int32(4),
				},
			},
		},
	}
	err = TaosStmt2BindParam(stmt2, true, params, colTypes, nil, -1)
	if err != nil {
		t.Error(err)
		return
	}
	code = TaosStmt2Exec(stmt2)
	if code != 0 {
		errStr := TaosStmt2Error(stmt2)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	r := <-caller.ExecResult
	if r.n != 0 {
		errStr := TaosStmt2Error(stmt2)
		err = taosError.NewError(r.n, errStr)
		t.Error(err)
		return
	}
	assert.Equal(t, 4, r.affected)
	code = TaosStmt2Prepare(stmt2, "select * from t where ts >= ? and ts <= ?")
	if code != 0 {
		errStr := TaosStmt2Error(stmt2)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	isInsert, code = TaosStmt2IsInsert(stmt2)
	if code != 0 {
		errStr := TaosStmt2Error(stmt2)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	assert.False(t, isInsert)
	params = []*stmt.TaosStmt2BindData{
		{
			Cols: [][]driver.Value{
				{
					now,
				},
				{
					now.Add(time.Second * 3),
				},
			},
		},
	}

	err = TaosStmt2BindParam(stmt2, false, params, nil, nil, -1)
	if err != nil {
		t.Error(err)
		return
	}
	code = TaosStmt2Exec(stmt2)
	if code != 0 {
		errStr := TaosStmt2Error(stmt2)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	r = <-caller.ExecResult
	if r.n != 0 {
		errStr := TaosStmt2Error(stmt2)
		err = taosError.NewError(r.n, errStr)
		t.Error(err)
		return
	}
	res := r.res
	fileCount := TaosNumFields(res)
	rh, err := ReadColumn(res, fileCount)
	if err != nil {
		t.Error(err)
		return
	}
	precision := TaosResultPrecision(res)
	var result [][]driver.Value
	for {
		columns, errCode, block := TaosFetchRawBlock(res)
		if errCode != 0 {
			errStr := TaosErrorStr(res)
			err = taosError.NewError(errCode, errStr)
			t.Error(err)
			return
		}
		if columns == 0 {
			break
		}
		r := parser.ReadBlock(block, columns, rh.ColTypes, precision)
		result = append(result, r...)
	}
	assert.Equal(t, 4, len(result))
	assert.Equal(t, now, result[0][0])
	assert.Equal(t, now.Add(time.Second), result[1][0])
	assert.Equal(t, now.Add(time.Second*2), result[2][0])
	assert.Equal(t, now.Add(time.Second*3), result[3][0])
	assert.Equal(t, int32(1), result[0][1])
	assert.Equal(t, int32(2), result[1][1])
	assert.Equal(t, int32(3), result[2][1])
	assert.Equal(t, int32(4), result[3][1])
	code = TaosStmt2Close(stmt2)
	if code != 0 {
		errStr := TaosStmt2Error(stmt2)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
}

func TestStmt2QueryBytes(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer TaosClose(conn)
	defer func() {
		err = exec(conn, "drop database if exists test_stmt2_query_bytes")
		if err != nil {
			t.Error(err)
			return
		}
	}()
	err = exec(conn, "create database if not exists test_stmt2_query_bytes precision 'ms' keep 36500")
	if err != nil {
		t.Error(err)
		return
	}
	err = exec(conn, "use test_stmt2_query_bytes")
	if err != nil {
		t.Error(err)
		return
	}
	err = exec(conn, "create table if not exists t(ts timestamp,v int)")
	if err != nil {
		t.Error(err)
		return
	}
	caller := NewStmtCallBackTest()
	handler := cgo.NewHandle(caller)
	stmt2 := TaosStmt2Init(conn, 0xcc123, false, false, handler)
	prepareInsertSql := "insert into t values (?,?)"
	code := TaosStmt2Prepare(stmt2, prepareInsertSql)
	if code != 0 {
		errStr := TaosStmt2Error(stmt2)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	isInsert, code := TaosStmt2IsInsert(stmt2)
	if code != 0 {
		errStr := TaosStmt2Error(stmt2)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	assert.True(t, isInsert)
	now := time.Now().Round(time.Millisecond)
	colTypes := []*stmt.StmtField{
		{
			FieldType: common.TSDB_DATA_TYPE_TIMESTAMP,
			Precision: common.PrecisionMilliSecond,
		},
		{
			FieldType: common.TSDB_DATA_TYPE_INT,
		},
	}
	params := []*stmt.TaosStmt2BindData{
		{
			TableName: "t",
			Cols: [][]driver.Value{
				{
					now,
					now.Add(time.Second),
				},
				{
					int32(1),
					int32(2),
				},
			},
		},
		{
			TableName: "t",
			Cols: [][]driver.Value{
				{
					now.Add(time.Second * 2),
					now.Add(time.Second * 3),
				},
				{
					int32(3),
					int32(4),
				},
			},
		},
	}
	bs, err := stmt.MarshalStmt2Binary(params, true, colTypes, nil)
	if err != nil {
		t.Error(err)
		return
	}
	err = TaosStmt2BindBinary(stmt2, bs, -1)
	if err != nil {
		t.Error(err)
		return
	}
	code = TaosStmt2Exec(stmt2)
	if code != 0 {
		errStr := TaosStmt2Error(stmt2)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	r := <-caller.ExecResult
	if r.n != 0 {
		errStr := TaosStmt2Error(stmt2)
		err = taosError.NewError(r.n, errStr)
		t.Error(err)
		return
	}
	assert.Equal(t, 4, r.affected)
	code = TaosStmt2Prepare(stmt2, "select * from t where ts >= ? and ts <= ?")
	if code != 0 {
		errStr := TaosStmt2Error(stmt2)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	isInsert, code = TaosStmt2IsInsert(stmt2)
	if code != 0 {
		errStr := TaosStmt2Error(stmt2)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	assert.False(t, isInsert)
	params = []*stmt.TaosStmt2BindData{
		{
			Cols: [][]driver.Value{
				{
					now,
				},
				{
					now.Add(time.Second * 3),
				},
			},
		},
	}
	bs, err = stmt.MarshalStmt2Binary(params, false, nil, nil)
	if err != nil {
		t.Error(err)
		return
	}
	err = TaosStmt2BindBinary(stmt2, bs, -1)
	if err != nil {
		t.Error(err)
		return
	}
	code = TaosStmt2Exec(stmt2)
	if code != 0 {
		errStr := TaosStmt2Error(stmt2)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	r = <-caller.ExecResult
	if r.n != 0 {
		errStr := TaosStmt2Error(stmt2)
		err = taosError.NewError(r.n, errStr)
		t.Error(err)
		return
	}
	res := r.res
	fileCount := TaosNumFields(res)
	rh, err := ReadColumn(res, fileCount)
	if err != nil {
		t.Error(err)
		return
	}
	precision := TaosResultPrecision(res)
	var result [][]driver.Value
	for {
		columns, errCode, block := TaosFetchRawBlock(res)
		if errCode != 0 {
			errStr := TaosErrorStr(res)
			err = taosError.NewError(errCode, errStr)
			t.Error(err)
			return
		}
		if columns == 0 {
			break
		}
		r := parser.ReadBlock(block, columns, rh.ColTypes, precision)
		result = append(result, r...)
	}
	assert.Equal(t, 4, len(result))
	assert.Equal(t, now, result[0][0])
	assert.Equal(t, now.Add(time.Second), result[1][0])
	assert.Equal(t, now.Add(time.Second*2), result[2][0])
	assert.Equal(t, now.Add(time.Second*3), result[3][0])
	assert.Equal(t, int32(1), result[0][1])
	assert.Equal(t, int32(2), result[1][1])
	assert.Equal(t, int32(3), result[2][1])
	assert.Equal(t, int32(4), result[3][1])
	code = TaosStmt2Close(stmt2)
	if code != 0 {
		errStr := TaosStmt2Error(stmt2)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
}

func TestStmt2QueryAllType(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer TaosClose(conn)
	defer func() {
		err = exec(conn, "drop database if exists test_stmt2_query_all")
		if err != nil {
			t.Error(err)
			return
		}
	}()
	err = exec(conn, "create database if not exists test_stmt2_query_all precision 'ms' keep 36500")
	if err != nil {
		t.Error(err)
		return
	}
	err = exec(conn, "use test_stmt2_query_all")
	if err != nil {
		t.Error(err)
		return
	}

	err = exec(conn, "create table if not exists t("+
		"ts timestamp, "+
		"v1 bool, "+
		"v2 tinyint, "+
		"v3 smallint, "+
		"v4 int, "+
		"v5 bigint, "+
		"v6 tinyint unsigned, "+
		"v7 smallint unsigned, "+
		"v8 int unsigned, "+
		"v9 bigint unsigned, "+
		"v10 float, "+
		"v11 double, "+
		"v12 binary(20), "+
		"v13 varbinary(20), "+
		"v14 geometry(100), "+
		"v15 nchar(20))")
	if err != nil {
		t.Error(err)
		return
	}
	caller := NewStmtCallBackTest()
	handler := cgo.NewHandle(caller)
	stmt2 := TaosStmt2Init(conn, 0xcc123, false, false, handler)
	prepareInsertSql := "insert into t values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
	colTypes := []*stmt.StmtField{
		{FieldType: common.TSDB_DATA_TYPE_TIMESTAMP, Precision: common.PrecisionMilliSecond},
		{FieldType: common.TSDB_DATA_TYPE_BOOL},
		{FieldType: common.TSDB_DATA_TYPE_TINYINT},
		{FieldType: common.TSDB_DATA_TYPE_SMALLINT},
		{FieldType: common.TSDB_DATA_TYPE_INT},
		{FieldType: common.TSDB_DATA_TYPE_BIGINT},
		{FieldType: common.TSDB_DATA_TYPE_UTINYINT},
		{FieldType: common.TSDB_DATA_TYPE_USMALLINT},
		{FieldType: common.TSDB_DATA_TYPE_UINT},
		{FieldType: common.TSDB_DATA_TYPE_UBIGINT},
		{FieldType: common.TSDB_DATA_TYPE_FLOAT},
		{FieldType: common.TSDB_DATA_TYPE_DOUBLE},
		{FieldType: common.TSDB_DATA_TYPE_BINARY},
		{FieldType: common.TSDB_DATA_TYPE_VARBINARY},
		{FieldType: common.TSDB_DATA_TYPE_GEOMETRY},
		{FieldType: common.TSDB_DATA_TYPE_NCHAR},
	}

	now := time.Now()
	params2 := []*stmt.TaosStmt2BindData{{
		TableName: "t",
		Cols: [][]driver.Value{
			{
				now,
				now.Add(time.Second),
				now.Add(time.Second * 2),
			},
			{
				true,
				nil,
				false,
			},
			{
				int8(11),
				nil,
				int8(12),
			},
			{
				int16(11),
				nil,
				int16(12),
			},
			{
				int32(11),
				nil,
				int32(12),
			},
			{
				int64(11),
				nil,
				int64(12),
			},
			{
				uint8(11),
				nil,
				uint8(12),
			},
			{
				uint16(11),
				nil,
				uint16(12),
			},
			{
				uint32(11),
				nil,
				uint32(12),
			},
			{
				uint64(11),
				nil,
				uint64(12),
			},
			{
				float32(11.2),
				nil,
				float32(12.2),
			},
			{
				float64(11.2),
				nil,
				float64(12.2),
			},
			{
				[]byte("binary1"),
				nil,
				[]byte("binary2"),
			},
			{
				[]byte("varbinary1"),
				nil,
				[]byte("varbinary2"),
			},
			{
				[]byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40},
				nil,
				[]byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40},
			},
			{
				"nchar1",
				nil,
				"nchar2",
			},
		},
	}}
	code := TaosStmt2Prepare(stmt2, prepareInsertSql)
	if code != 0 {
		errStr := TaosStmt2Error(stmt2)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	isInsert, code := TaosStmt2IsInsert(stmt2)
	if code != 0 {
		errStr := TaosStmt2Error(stmt2)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	assert.True(t, isInsert)
	err = TaosStmt2BindParam(stmt2, true, params2, colTypes, nil, -1)
	if err != nil {
		t.Error(err)
		return
	}
	code = TaosStmt2Exec(stmt2)
	if code != 0 {
		errStr := TaosStmt2Error(stmt2)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	r := <-caller.ExecResult
	if r.n != 0 {
		errStr := TaosStmt2Error(stmt2)
		err = taosError.NewError(r.n, errStr)
		t.Error(err)
		return
	}
	t.Log(r.affected)
	assert.Equal(t, 3, r.affected)
	code = TaosStmt2Prepare(stmt2, "select * from t where ts =? and v1 = ? and v2 = ? and v3 = ? and v4 = ? and v5 = ? and v6 = ? and v7 = ? and v8 = ? and v9 = ? and v10 = ? and v11 = ? and v12 = ? and v13 = ? and v14 = ? and v15 = ? ")
	if code != 0 {
		errStr := TaosStmt2Error(stmt2)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	isInsert, code = TaosStmt2IsInsert(stmt2)
	if code != 0 {
		errStr := TaosStmt2Error(stmt2)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	assert.False(t, isInsert)
	params := []*stmt.TaosStmt2BindData{
		{
			Cols: [][]driver.Value{
				{now},
				{true},
				{int8(11)},
				{int16(11)},
				{int32(11)},
				{int64(11)},
				{uint8(11)},
				{uint16(11)},
				{uint32(11)},
				{uint64(11)},
				{float32(11.2)},
				{float64(11.2)},
				{[]byte("binary1")},
				{[]byte("varbinary1")},
				{"point(100 100)"},
				{"nchar1"},
			},
		},
	}
	err = TaosStmt2BindParam(stmt2, false, params, nil, nil, -1)
	if err != nil {
		t.Error(err)
		return
	}
	code = TaosStmt2Exec(stmt2)
	if code != 0 {
		errStr := TaosStmt2Error(stmt2)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	r = <-caller.ExecResult
	if r.n != 0 {
		errStr := TaosStmt2Error(stmt2)
		err = taosError.NewError(r.n, errStr)
		t.Error(err)
		return
	}
	res := r.res
	fileCount := TaosNumFields(res)
	rh, err := ReadColumn(res, fileCount)
	if err != nil {
		t.Error(err)
		return
	}
	precision := TaosResultPrecision(res)
	var result [][]driver.Value
	for {
		columns, errCode, block := TaosFetchRawBlock(res)
		if errCode != 0 {
			errStr := TaosErrorStr(res)
			err = taosError.NewError(errCode, errStr)
			t.Error(err)
			return
		}
		if columns == 0 {
			break
		}
		r := parser.ReadBlock(block, columns, rh.ColTypes, precision)
		result = append(result, r...)
	}
	t.Log(result)
	assert.Len(t, result, 1)
}

func TestStmt2QueryAllTypeBytes(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer TaosClose(conn)
	defer func() {
		err = exec(conn, "drop database if exists test_stmt2_query_all_bytes")
		if err != nil {
			t.Error(err)
			return
		}
	}()
	err = exec(conn, "create database if not exists test_stmt2_query_all_bytes precision 'ms' keep 36500")
	if err != nil {
		t.Error(err)
		return
	}
	err = exec(conn, "use test_stmt2_query_all_bytes")
	if err != nil {
		t.Error(err)
		return
	}

	err = exec(conn, "create table if not exists t("+
		"ts timestamp, "+
		"v1 bool, "+
		"v2 tinyint, "+
		"v3 smallint, "+
		"v4 int, "+
		"v5 bigint, "+
		"v6 tinyint unsigned, "+
		"v7 smallint unsigned, "+
		"v8 int unsigned, "+
		"v9 bigint unsigned, "+
		"v10 float, "+
		"v11 double, "+
		"v12 binary(20), "+
		"v13 varbinary(20), "+
		"v14 geometry(100), "+
		"v15 nchar(20))")
	if err != nil {
		t.Error(err)
		return
	}
	caller := NewStmtCallBackTest()
	handler := cgo.NewHandle(caller)
	stmt2 := TaosStmt2Init(conn, 0xcc123, false, false, handler)
	prepareInsertSql := "insert into t values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
	colTypes := []*stmt.StmtField{
		{FieldType: common.TSDB_DATA_TYPE_TIMESTAMP, Precision: common.PrecisionMilliSecond},
		{FieldType: common.TSDB_DATA_TYPE_BOOL},
		{FieldType: common.TSDB_DATA_TYPE_TINYINT},
		{FieldType: common.TSDB_DATA_TYPE_SMALLINT},
		{FieldType: common.TSDB_DATA_TYPE_INT},
		{FieldType: common.TSDB_DATA_TYPE_BIGINT},
		{FieldType: common.TSDB_DATA_TYPE_UTINYINT},
		{FieldType: common.TSDB_DATA_TYPE_USMALLINT},
		{FieldType: common.TSDB_DATA_TYPE_UINT},
		{FieldType: common.TSDB_DATA_TYPE_UBIGINT},
		{FieldType: common.TSDB_DATA_TYPE_FLOAT},
		{FieldType: common.TSDB_DATA_TYPE_DOUBLE},
		{FieldType: common.TSDB_DATA_TYPE_BINARY},
		{FieldType: common.TSDB_DATA_TYPE_VARBINARY},
		{FieldType: common.TSDB_DATA_TYPE_GEOMETRY},
		{FieldType: common.TSDB_DATA_TYPE_NCHAR},
	}

	now := time.Now()
	params2 := []*stmt.TaosStmt2BindData{{
		TableName: "t",
		Cols: [][]driver.Value{
			{
				now,
				now.Add(time.Second),
				now.Add(time.Second * 2),
			},
			{
				true,
				nil,
				false,
			},
			{
				int8(11),
				nil,
				int8(12),
			},
			{
				int16(11),
				nil,
				int16(12),
			},
			{
				int32(11),
				nil,
				int32(12),
			},
			{
				int64(11),
				nil,
				int64(12),
			},
			{
				uint8(11),
				nil,
				uint8(12),
			},
			{
				uint16(11),
				nil,
				uint16(12),
			},
			{
				uint32(11),
				nil,
				uint32(12),
			},
			{
				uint64(11),
				nil,
				uint64(12),
			},
			{
				float32(11.2),
				nil,
				float32(12.2),
			},
			{
				float64(11.2),
				nil,
				float64(12.2),
			},
			{
				[]byte("binary1"),
				nil,
				[]byte("binary2"),
			},
			{
				[]byte("varbinary1"),
				nil,
				[]byte("varbinary2"),
			},
			{
				[]byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40},
				nil,
				[]byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40},
			},
			{
				"nchar1",
				nil,
				"nchar2",
			},
		},
	}}
	code := TaosStmt2Prepare(stmt2, prepareInsertSql)
	if code != 0 {
		errStr := TaosStmt2Error(stmt2)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	isInsert, code := TaosStmt2IsInsert(stmt2)
	if code != 0 {
		errStr := TaosStmt2Error(stmt2)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	assert.True(t, isInsert)
	bs, err := stmt.MarshalStmt2Binary(params2, true, colTypes, nil)
	if err != nil {
		t.Error(err)
		return
	}
	err = TaosStmt2BindBinary(stmt2, bs, -1)
	if err != nil {
		t.Error(err)
		return
	}
	code = TaosStmt2Exec(stmt2)
	if code != 0 {
		errStr := TaosStmt2Error(stmt2)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	r := <-caller.ExecResult
	if r.n != 0 {
		errStr := TaosStmt2Error(stmt2)
		err = taosError.NewError(r.n, errStr)
		t.Error(err)
		return
	}
	t.Log(r.affected)
	assert.Equal(t, 3, r.affected)
	code = TaosStmt2Prepare(stmt2, "select * from t where ts =? and v1 = ? and v2 = ? and v3 = ? and v4 = ? and v5 = ? and v6 = ? and v7 = ? and v8 = ? and v9 = ? and v10 = ? and v11 = ? and v12 = ? and v13 = ? and v14 = ? and v15 = ? ")
	if code != 0 {
		errStr := TaosStmt2Error(stmt2)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	isInsert, code = TaosStmt2IsInsert(stmt2)
	if code != 0 {
		errStr := TaosStmt2Error(stmt2)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	assert.False(t, isInsert)
	params := []*stmt.TaosStmt2BindData{
		{
			Cols: [][]driver.Value{
				{now},
				{true},
				{int8(11)},
				{int16(11)},
				{int32(11)},
				{int64(11)},
				{uint8(11)},
				{uint16(11)},
				{uint32(11)},
				{uint64(11)},
				{float32(11.2)},
				{float64(11.2)},
				{[]byte("binary1")},
				{[]byte("varbinary1")},
				{"point(100 100)"},
				{"nchar1"},
			},
		},
	}
	bs, err = stmt.MarshalStmt2Binary(params, false, nil, nil)
	if err != nil {
		t.Error(err)
		return
	}
	err = TaosStmt2BindBinary(stmt2, bs, -1)
	if err != nil {
		t.Error(err)
		return
	}
	code = TaosStmt2Exec(stmt2)
	if code != 0 {
		errStr := TaosStmt2Error(stmt2)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	r = <-caller.ExecResult
	if r.n != 0 {
		errStr := TaosStmt2Error(stmt2)
		err = taosError.NewError(r.n, errStr)
		t.Error(err)
		return
	}
	res := r.res
	fileCount := TaosNumFields(res)
	rh, err := ReadColumn(res, fileCount)
	if err != nil {
		t.Error(err)
		return
	}
	precision := TaosResultPrecision(res)
	var result [][]driver.Value
	for {
		columns, errCode, block := TaosFetchRawBlock(res)
		if errCode != 0 {
			errStr := TaosErrorStr(res)
			err = taosError.NewError(errCode, errStr)
			t.Error(err)
			return
		}
		if columns == 0 {
			break
		}
		r := parser.ReadBlock(block, columns, rh.ColTypes, precision)
		result = append(result, r...)
	}
	t.Log(result)
	assert.Len(t, result, 1)
}

func TestStmt2Json(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer TaosClose(conn)
	defer func() {
		err = exec(conn, "drop database if exists test_stmt2_json")
		if err != nil {
			t.Error(err)
			return
		}
	}()
	err = exec(conn, "create database if not exists test_stmt2_json precision 'ms' keep 36500")
	if err != nil {
		t.Error(err)
		return
	}
	err = exec(conn, "use test_stmt2_json")
	if err != nil {
		t.Error(err)
		return
	}
	err = exec(conn, "create table if not exists test_json_stb(ts timestamp, v int) tags (t json)")
	if err != nil {
		t.Error(err)
		return
	}
	caller := NewStmtCallBackTest()
	handler := cgo.NewHandle(caller)
	stmt2 := TaosStmt2Init(conn, 0xcc123, false, false, handler)
	defer func() {
		code := TaosStmt2Close(stmt2)
		if code != 0 {
			errStr := TaosStmt2Error(stmt2)
			err = taosError.NewError(code, errStr)
			t.Error(err)
			return
		}
	}()
	prepareInsertSql := "insert into ? using test_json_stb tags(?) values (?,?)"
	code := TaosStmt2Prepare(stmt2, prepareInsertSql)
	if code != 0 {
		errStr := TaosStmt2Error(stmt2)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	now := time.Now().Round(time.Millisecond)
	params := []*stmt.TaosStmt2BindData{{
		TableName: "ctb1",
		Tags:      []driver.Value{[]byte(`{"a":1,"b":"xx"}`)},
		Cols: [][]driver.Value{
			{now},
			{int32(1)},
		},
	}}
	colTypes := []*stmt.StmtField{
		{FieldType: common.TSDB_DATA_TYPE_TIMESTAMP, Precision: common.PrecisionMilliSecond},
		{FieldType: common.TSDB_DATA_TYPE_INT},
	}
	tagTypes := []*stmt.StmtField{
		{FieldType: common.TSDB_DATA_TYPE_JSON},
	}
	err = TaosStmt2BindParam(stmt2, true, params, colTypes, tagTypes, -1)
	if err != nil {
		t.Error(err)
		return
	}
	code = TaosStmt2Exec(stmt2)
	if code != 0 {
		errStr := TaosStmt2Error(stmt2)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	r := <-caller.ExecResult
	if r.n != 0 {
		errStr := TaosStmt2Error(stmt2)
		err = taosError.NewError(r.n, errStr)
		t.Error(err)
		return
	}
	assert.Equal(t, 1, r.affected)

	TaosStmt2Prepare(stmt2, "select * from test_json_stb where t->'a' = ?")
	params = []*stmt.TaosStmt2BindData{{
		Cols: [][]driver.Value{
			{int32(1)},
		},
	}}
	err = TaosStmt2BindParam(stmt2, false, params, nil, nil, -1)
	if err != nil {
		t.Error(err)
		return
	}
	code = TaosStmt2Exec(stmt2)
	if code != 0 {
		errStr := TaosStmt2Error(stmt2)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	r = <-caller.ExecResult
	if r.n != 0 {
		errStr := TaosStmt2Error(stmt2)
		err = taosError.NewError(r.n, errStr)
		t.Error(err)
		return
	}
	res := r.res
	fileCount := TaosNumFields(res)
	rh, err := ReadColumn(res, fileCount)
	if err != nil {
		t.Error(err)
		return
	}
	precision := TaosResultPrecision(res)
	var result [][]driver.Value
	for {
		columns, errCode, block := TaosFetchRawBlock(res)
		if errCode != 0 {
			errStr := TaosErrorStr(res)
			err = taosError.NewError(errCode, errStr)
			t.Error(err)
			return
		}
		if columns == 0 {
			break
		}
		r := parser.ReadBlock(block, columns, rh.ColTypes, precision)
		result = append(result, r...)
	}
	t.Log(result)
	assert.Equal(t, 1, len(result))
}

func TestStmt2BindMultiTables(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer TaosClose(conn)
	defer func() {
		err = exec(conn, "drop database if exists test_stmt2_multi")
		if err != nil {
			t.Error(err)
			return
		}
	}()
	err = exec(conn, "create database if not exists test_stmt2_multi precision 'ms' keep 36500")
	if err != nil {
		t.Error(err)
		return
	}
	err = exec(conn, "use test_stmt2_multi")
	if err != nil {
		t.Error(err)
		return
	}
	err = exec(conn, "create table if not exists stb(ts timestamp, v bigint) tags(tv int)")
	if err != nil {
		t.Error(err)
		return
	}
	caller := NewStmtCallBackTest()
	handler := cgo.NewHandle(caller)
	insertStmt := TaosStmt2Init(conn, 0xcc123, false, false, handler)
	prepareInsertSql := "insert into ? using stb tags(?) values (?,?)"
	code := TaosStmt2Prepare(insertStmt, prepareInsertSql)
	if code != 0 {
		errStr := TaosStmt2Error(insertStmt)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	binds := []*stmt.TaosStmt2BindData{
		{
			TableName: "table1",
			Cols: [][]driver.Value{
				{
					// ts 1726803356466
					time.Unix(1726803356, 466000000),
				},
				{
					int64(1),
				},
			},
			Tags: []driver.Value{int32(1)},
		},
		{
			TableName: "table2",
			Cols: [][]driver.Value{
				{
					// ts 1726803356466
					time.Unix(1726803356, 466000000),
				},
				{
					int64(2),
				},
			},
			Tags: []driver.Value{int32(2)},
		},
		{
			TableName: "table3",
			Cols: [][]driver.Value{
				{
					// ts 1726803356466
					time.Unix(1726803356, 466000000),
				},
				{
					int64(3),
				},
			},
			Tags: []driver.Value{int32(3)},
		},
	}
	colType := []*stmt.StmtField{
		{
			FieldType: common.TSDB_DATA_TYPE_TIMESTAMP,
			Precision: common.PrecisionMilliSecond,
		},
		{
			FieldType: common.TSDB_DATA_TYPE_BIGINT,
		},
	}
	tagType := []*stmt.StmtField{
		{
			FieldType: common.TSDB_DATA_TYPE_INT,
		},
	}

	isInsert, code := TaosStmt2IsInsert(insertStmt)
	if code != 0 {
		errStr := TaosStmt2Error(insertStmt)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	assert.True(t, isInsert)

	err = TaosStmt2BindParam(insertStmt, true, binds, colType, tagType, -1)
	if err != nil {
		t.Error(err)
		return
	}
	code = TaosStmt2Exec(insertStmt)
	if code != 0 {
		errStr := TaosStmt2Error(insertStmt)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	r := <-caller.ExecResult
	if r.n != 0 {
		errStr := TaosStmt2Error(insertStmt)
		err = taosError.NewError(r.n, errStr)
		t.Error(err)
		return
	}
	t.Log(r.affected)

	code = TaosStmt2Close(insertStmt)
	if code != 0 {
		errStr := TaosStmt2Error(insertStmt)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
}
