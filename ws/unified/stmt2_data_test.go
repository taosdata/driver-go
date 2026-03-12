package unified

import (
	"testing"
	"time"

	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/common/param"
)

func TestBuildStmt2InsertBindData(t *testing.T) {
	ts := time.Unix(1711111111, 0)
	data, err := BuildStmt2InsertBindData(
		"tb1",
		param.NewParam(1).AddNchar("tag1"),
		[]*param.Param{
			param.NewParam(2).AddTimestamp(ts, common.PrecisionMilliSecond).AddNull(),
			param.NewParam(2).AddInt(1).AddInt(2),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if data.TableName != "tb1" {
		t.Fatalf("unexpected table name: %s", data.TableName)
	}
	if len(data.Tags) != 1 || data.Tags[0] != "tag1" {
		t.Fatalf("unexpected tags: %+v", data.Tags)
	}
	if len(data.Cols) != 2 {
		t.Fatalf("unexpected col count: %d", len(data.Cols))
	}
	expectTS := common.TimeToTimestamp(ts, common.PrecisionMilliSecond)
	if data.Cols[0][0] != expectTS || data.Cols[0][1] != nil {
		t.Fatalf("unexpected timestamp col: %+v", data.Cols[0])
	}
	if data.Cols[1][0] != int32(1) || data.Cols[1][1] != int32(2) {
		t.Fatalf("unexpected int col: %+v", data.Cols[1])
	}
}

func TestBuildStmt2QueryBindData(t *testing.T) {
	ts := time.Unix(1711111111, 123456789)
	data, err := BuildStmt2QueryBindData([]*param.Param{
		param.NewParam(1).AddTimestamp(ts, common.PrecisionNanoSecond),
		param.NewParam(1).AddInt(9),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(data) != 1 {
		t.Fatalf("expect one query bind item, got %d", len(data))
	}
	if len(data[0].Cols) != 2 {
		t.Fatalf("unexpected query col count: %d", len(data[0].Cols))
	}
	if data[0].Cols[0][0] != ts.Format(time.RFC3339Nano) {
		t.Fatalf("unexpected query timestamp value: %+v", data[0].Cols[0][0])
	}
	if data[0].Cols[1][0] != int32(9) {
		t.Fatalf("unexpected query int value: %+v", data[0].Cols[1][0])
	}
}

func TestBuildStmt2DataErrors(t *testing.T) {
	_, err := BuildStmt2InsertBindData("", nil, []*param.Param{
		param.NewParam(1).AddValue(struct{}{}),
	})
	if err == nil {
		t.Fatal("expect insert normalize error")
	}
	_, err = BuildStmt2QueryBindData(nil)
	if err == nil {
		t.Fatal("expect query error")
	}
}
