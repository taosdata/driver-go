package unified

import (
	"database/sql/driver"
	"testing"
	"time"

	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/common/param"
	"github.com/taosdata/driver-go/v3/types"
)

func TestNormalizeStmt2Value(t *testing.T) {
	now := time.Unix(1711111111, 123456789)
	testCases := []struct {
		name      string
		value     driver.Value
		queryMode bool
		expect    driver.Value
	}{
		{name: "taos bool", value: types.TaosBool(true), expect: true},
		{name: "taos int", value: types.TaosInt(7), expect: int32(7)},
		{name: "taos binary", value: types.TaosBinary([]byte("b")), expect: []byte("b")},
		{name: "taos nchar", value: types.TaosNchar("n"), expect: "n"},
		{name: "builtin int64", value: int64(9), expect: int64(9)},
		{
			name:   "taos timestamp insert mode",
			value:  types.TaosTimestamp{T: now, Precision: common.PrecisionMicroSecond},
			expect: common.TimeToTimestamp(now, common.PrecisionMicroSecond),
		},
		{
			name:      "taos timestamp query mode",
			value:     types.TaosTimestamp{T: now, Precision: common.PrecisionNanoSecond},
			queryMode: true,
			expect:    now.Format(time.RFC3339Nano),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := NormalizeStmt2Value(tc.value, tc.queryMode)
			if err != nil {
				t.Fatal(err)
			}
			switch expect := tc.expect.(type) {
			case []byte:
				actual, ok := got.([]byte)
				if !ok || string(actual) != string(expect) {
					t.Fatalf("expect %v, got %v", expect, got)
				}
			default:
				if got != tc.expect {
					t.Fatalf("expect %v, got %v", tc.expect, got)
				}
			}
		})
	}
}

func TestNormalizeStmt2ValueUnsupportedType(t *testing.T) {
	_, err := NormalizeStmt2Value(struct{}{}, false)
	if err == nil {
		t.Fatal("expect unsupported type error")
	}
}

func TestNormalizeStmt2Columns(t *testing.T) {
	now := time.Unix(1711111111, 0)
	columns := []*param.Param{
		param.NewParam(2).AddInt(1).AddInt(2),
		param.NewParam(2).AddTimestamp(now, common.PrecisionMilliSecond).AddNull(),
	}
	normalized, err := NormalizeStmt2Columns(columns, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(normalized) != 2 {
		t.Fatalf("expect 2 columns, got %d", len(normalized))
	}
	if normalized[0][0] != int32(1) || normalized[0][1] != int32(2) {
		t.Fatalf("unexpected int column: %+v", normalized[0])
	}
	expectTS := common.TimeToTimestamp(now, common.PrecisionMilliSecond)
	if normalized[1][0] != expectTS || normalized[1][1] != nil {
		t.Fatalf("unexpected timestamp column: %+v", normalized[1])
	}
}
