package wrapper

import (
	"database/sql/driver"
	"io"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/errors"
	"github.com/taosdata/driver-go/v3/wrapper/cgo"
)

// @author: xftan
// @date: 2022/1/27 17:29
// @description: test taos_options
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

// @author: xftan
// @date: 2022/1/27 17:29
// @description: test taos_query_a
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
					}
					t.Log("fetch rows a", r.n)
				}
			}
		})
	}
}

func TestError(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	res := TaosQuery(conn, "asd")
	code := TaosError(res)
	assert.NotEqual(t, code, 0)
	errStr := TaosErrorStr(res)
	assert.NotEmpty(t, errStr)
}

func TestAffectedRows(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer TaosClose(conn)
	defer func() {
		res := TaosQuery(conn, "drop database if exists affected_rows_test")
		code := TaosError(res)
		if code != 0 {
			t.Error(errors.NewError(code, TaosErrorStr(res)))
			return
		}
		TaosFreeResult(res)
	}()
	res := TaosQuery(conn, "create database if not exists affected_rows_test")
	code := TaosError(res)
	if code != 0 {
		t.Error(errors.NewError(code, TaosErrorStr(res)))
		return
	}
	TaosFreeResult(res)
	res = TaosQuery(conn, "create table if not exists affected_rows_test.t0(ts timestamp,v int)")
	code = TaosError(res)
	if code != 0 {
		t.Error(errors.NewError(code, TaosErrorStr(res)))
		return
	}
	TaosFreeResult(res)
	res = TaosQuery(conn, "insert into affected_rows_test.t0 values(now,1)")
	code = TaosError(res)
	if code != 0 {
		t.Error(errors.NewError(code, TaosErrorStr(res)))
		return
	}
	affected := TaosAffectedRows(res)
	assert.Equal(t, 1, affected)
}

// @author: xftan
// @date: 2022/1/27 17:29
// @description: test taos_reset_current_db
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
			err = exec(tt.args.taosConnect, "create database if not exists log")
			if err != nil {
				t.Error(err)
				return
			}
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

// @author: xftan
// @date: 2022/1/27 17:30
// @description: test taos_validate_sql
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
				sql:         "show grants",
			},
			want: 0,
		},
		{
			name: "TSC_SQL_SYNTAX_ERROR",
			args: args{
				taosConnect: conn,
				sql:         "slect 1",
			},
			want: 9728,
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

// @author: xftan
// @date: 2022/1/27 17:30
// @description: test taos_is_update_query
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
			name: "drop database if exists is_update",
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

// @author: xftan
// @date: 2022/1/27 17:30
// @description: taos async raw block
func TestTaosResultBlock(t *testing.T) {
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
				sql:         "show users",
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
			if err != nil {
				t.Error(err)
				return
			}
			//t.Logf("%#v", rowsHeader)
			if r.n != 0 {
				t.Error("query result", r.n)
				return
			}
			res := r.res
			precision := TaosResultPrecision(res)
			for {
				go TaosFetchRawBlockA(res, p)
				r = <-tt.args.caller.FetchResult
				if r.n == 0 {
					t.Log("success")
					TaosFreeResult(r.res)
					break
				} else {
					res = r.res
					block := TaosGetRawBlock(res)
					assert.NotNil(t, block)
					values := ReadBlock(block, r.n, rowsHeader.ColTypes, precision)
					_ = values
					t.Log(values)
				}
			}
		})
	}
}

func TestTaosGetClientInfo(t *testing.T) {
	s := TaosGetClientInfo()
	assert.NotEmpty(t, s)
}
