package wrapper

import (
	"database/sql/driver"
	"io"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/taosdata/driver-go/v2/common"
	"github.com/taosdata/driver-go/v2/errors"
	"github.com/taosdata/driver-go/v2/wrapper/cgo"
)

func TestTaosOptions(t *testing.T) {
	type args struct {
		option int
		value  string
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{
			name: "test_options",
			args: args{
				option: common.TSDB_OPTION_CONFIGDIR,
				value:  "/etc/taos",
			},
			want: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := TaosOptions(tt.args.option, tt.args.value); got != tt.want {
				t.Errorf("TaosOptions() = %v, want %v", got, tt.want)
			}
		})
	}
}

type result struct {
	res unsafe.Pointer
	n   int
}

type TestCaller struct {
	QueryResult chan *result
	FetchResult chan *result
}

func NewTestCaller() *TestCaller {
	return &TestCaller{
		QueryResult: make(chan *result),
		FetchResult: make(chan *result),
	}
}

func (t *TestCaller) QueryCall(res unsafe.Pointer, code int) {
	t.QueryResult <- &result{
		res: res,
		n:   code,
	}
}

func (t *TestCaller) FetchCall(res unsafe.Pointer, numOfRows int) {
	t.FetchResult <- &result{
		res: res,
		n:   numOfRows,
	}
}

func TestTaosQueryA(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer TaosClose(conn)
	var caller = NewTestCaller()
	type args struct {
		taosConnect unsafe.Pointer
		sql         string
		caller      *TestCaller
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "test",
			args: args{
				taosConnect: conn,
				sql:         "show databases",
				caller:      caller,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := cgo.NewHandle(tt.args.caller)
			go TaosQueryA(tt.args.taosConnect, tt.args.sql, p)
			r := <-tt.args.caller.QueryResult
			t.Log("query finish")
			count := TaosNumFields(r.res)
			rowsHeader, err := ReadColumn(r.res, count)
			precision := TaosResultPrecision(r.res)
			if err != nil {
				t.Error(err)
				return
			}
			t.Logf("%#v", rowsHeader)
			if r.n != 0 {
				t.Error("query result", r.n)
				return
			}
			res := r.res
			for {
				go TaosFetchRowsA(res, p)
				r = <-tt.args.caller.FetchResult
				if r.n == 0 {
					t.Log("success")
					TaosFreeResult(r.res)
					break
				} else {
					res = r.res
					for i := 0; i < r.n; i++ {
						values := make([]driver.Value, len(rowsHeader.ColNames))
						row := TaosFetchRow(res)
						lengths := FetchLengths(res, len(rowsHeader.ColNames))
						for j := range rowsHeader.ColTypes {
							if row == nil {
								t.Error(io.EOF)
								return
							}
							values[j] = FetchRow(row, j, rowsHeader.ColTypes[j], lengths[j], precision)
						}
						t.Log(values)
					}
					t.Log("fetch rows a", r.n)
				}
			}
		})
	}
}

func TestTaosResetCurrentDB(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer TaosClose(conn)
	type args struct {
		taosConnect unsafe.Pointer
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "test",
			args: args{
				taosConnect: conn,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			TaosSelectDB(tt.args.taosConnect, "log")
			result := TaosQuery(tt.args.taosConnect, "select database()")
			code := TaosError(result)
			if code != 0 {
				errStr := TaosErrorStr(result)
				TaosFreeResult(result)
				t.Error(errors.TaosError{Code: int32(code), ErrStr: errStr})
				return
			}
			row := TaosFetchRow(result)
			lengths := FetchLengths(result, 1)
			currentDB := FetchRow(row, 0, 10, lengths[0])
			assert.Equal(t, "log", currentDB)
			TaosFreeResult(result)
			TaosResetCurrentDB(tt.args.taosConnect)
			result = TaosQuery(tt.args.taosConnect, "select database()")
			code = TaosError(result)
			if code != 0 {
				errStr := TaosErrorStr(result)
				TaosFreeResult(result)
				t.Error(errors.TaosError{Code: int32(code), ErrStr: errStr})
				return
			}
			row = TaosFetchRow(result)
			lengths = FetchLengths(result, 1)
			currentDB = FetchRow(row, 0, 10, lengths[0])
			assert.Nil(t, currentDB)
			TaosFreeResult(result)
		})
	}
}

func TestTaosValidateSql(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer TaosClose(conn)
	type args struct {
		taosConnect unsafe.Pointer
		sql         string
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{
			name: "valid",
			args: args{
				taosConnect: conn,
				sql:         "show log.stables",
			},
			want: 0,
		},
		{
			name: "TSC_SQL_SYNTAX_ERROR",
			args: args{
				taosConnect: conn,
				sql:         "slect 1",
			},
			want: int(errors.TSC_SQL_SYNTAX_ERROR),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := TaosValidateSql(tt.args.taosConnect, tt.args.sql); got&0xffff != tt.want {
				t.Errorf("TaosValidateSql() = %v, want %v", got&0xffff, tt.want)
			}
		})
	}
}

func TestTaosIsUpdateQuery(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer TaosClose(conn)
	tests := []struct {
		name string
		want bool
	}{
		{
			name: "create database if not exists is_update",
			want: true,
		},
		{
			name: "show log.stables",
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := TaosQuery(conn, tt.name)
			defer TaosFreeResult(result)
			if got := TaosIsUpdateQuery(result); got != tt.want {
				t.Errorf("TaosIsUpdateQuery() = %v, want %v", got, tt.want)
			}
		})
	}
}
