package wrapper

import (
	"fmt"
	"testing"

	"github.com/taosdata/driver-go/v2/errors"
)

func TestReadBlock(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}

	defer TaosClose(conn)

	res := TaosQuery(conn, "create database if not exists test_block_raw")
	code := TaosError(res)
	if code != 0 {
		errStr := TaosErrorStr(res)
		TaosFreeResult(res)
		t.Error(errors.NewError(code, errStr))
		return
	}
	TaosFreeResult(res)

	res = TaosQuery(conn, "drop table if exists test_block_raw.all_type")
	code = TaosError(res)
	if code != 0 {
		errStr := TaosErrorStr(res)
		TaosFreeResult(res)
		t.Error(errors.NewError(code, errStr))
		return
	}
	TaosFreeResult(res)

	res = TaosQuery(conn, "create table if not exists test_block_raw.all_type (ts timestamp,"+
		"c1 bool,"+
		"c2 tinyint,"+
		"c3 smallint,"+
		"c4 int,"+
		"c5 bigint,"+
		"c6 tinyint unsigned,"+
		"c7 smallint unsigned,"+
		"c8 int unsigned,"+
		"c9 bigint unsigned,"+
		"c10 float,"+
		"c11 double,"+
		"c12 binary(20),"+
		"c13 nchar(20)"+
		")")
	code = TaosError(res)
	if code != 0 {
		errStr := TaosErrorStr(res)
		TaosFreeResult(res)
		t.Error(errors.NewError(code, errStr))
		return
	}
	TaosFreeResult(res)

	sql := fmt.Sprintf("insert into test_block_raw.all_type values(now,1,1,1,1,1,1,1,1,1,1,1,'test_binary','test_nchar')(now+1s,null,null,null,null,null,null,null,null,null,null,null,null,null)")
	res = TaosQuery(conn, sql)
	code = TaosError(res)
	if code != 0 {
		errStr := TaosErrorStr(res)
		TaosFreeResult(res)
		t.Error(errors.NewError(code, errStr))
		return
	}
	TaosFreeResult(res)

	//sql = "select c1 ,c2 ,c3 ,c4 ,c5 ,c6 ,c7 ,c8 ,c9 ,c10,c11,c12,c13 from test_block_raw.all_type"
	sql = "select * from test_block_raw.all_type"
	res = TaosQuery(conn, sql)
	code = TaosError(res)
	if code != 0 {
		errStr := TaosErrorStr(res)
		TaosFreeResult(res)
		t.Error(errors.NewError(code, errStr))
		return
	}
	fileCount := TaosNumFields(res)
	rh, err := ReadColumn(res, fileCount)
	if err != nil {
		t.Error(err)
		return
	}
	precision := TaosResultPrecision(res)
	for {
		blockSize, errCode, block := TaosFetchRawBlock(res)
		if errCode != int(errors.SUCCESS) {
			errStr := TaosErrorStr(res)
			err := errors.NewError(code, errStr)
			t.Error(err)
			TaosFreeResult(res)
			return
		}
		if blockSize == 0 {
			break
		}
		data := ReadBlock(block, blockSize, rh.ColTypes, precision)
		t.Log(data)
	}
}
