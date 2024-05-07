package af

import (
	"database/sql/driver"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/common/param"
)

func TestNewStmt(t *testing.T) {
	db := testDatabase(t)
	_, err := db.Exec("create table test_stmt (ts timestamp,v int)")
	assert.NoError(t, err)
	stmt := db.Stmt()
	err = stmt.Prepare("insert into ? values(?,?)")
	assert.NoError(t, err)
	err = stmt.SetTableName("test_stmt")
	assert.NoError(t, err)
	ts := time.Now().UnixNano() / 1e3
	err = stmt.BindRow(param.NewParam(2).AddTimestamp(time.Unix(0, ts*1e3), common.PrecisionMicroSecond).AddInt(1))
	assert.NoError(t, err)
	err = stmt.AddBatch()
	assert.NoError(t, err)
	err = stmt.Execute()
	assert.NoError(t, err)
	affected := stmt.GetAffectedRows()
	assert.Equal(t, int(1), affected)
	err = stmt.Prepare("select * from test_stmt where v = ?")
	assert.NoError(t, err)
	err = stmt.BindRow(param.NewParam(1).AddInt(1))
	assert.NoError(t, err)
	err = stmt.AddBatch()
	assert.NoError(t, err)
	err = stmt.Execute()
	assert.NoError(t, err)
	rows, err := stmt.UseResult()
	assert.NoError(t, err)
	dest := make([]driver.Value, 2)
	err = rows.Next(dest)
	assert.NoError(t, err)
	assert.Equal(t, ts, dest[0].(time.Time).UnixNano()/1e3)
	assert.Equal(t, int32(1), dest[1].(int32))
	err = rows.Next(dest)
	assert.ErrorIs(t, err, io.EOF)
	err = rows.Close()
	assert.NoError(t, err)
	err = stmt.Close()
	assert.NoError(t, err)
	err = db.Close()
	assert.NoError(t, err)
}
