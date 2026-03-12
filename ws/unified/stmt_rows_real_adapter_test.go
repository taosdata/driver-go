package unified

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
	commonstmt "github.com/taosdata/driver-go/v3/common/stmt"
	"github.com/taosdata/driver-go/v3/ws/client"
)

func TestUnifiedStmtAndRowsRealAdapterCoverage(t *testing.T) {
	if testing.Short() {
		t.Skip("skip integration test in short mode")
	}
	ensureTaosadapterBinary(t)

	ports, stops := startAdapters(t, 2)
	t.Cleanup(func() {
		for i := len(ports) - 1; i >= 0; i-- {
			if stop, ok := stops[ports[i]]; ok && stop != nil {
				stop()
				delete(stops, ports[i])
			}
		}
	})

	db := createTestDatabase(t, ports)
	c := newIntegrationUnifiedClient(t, ports, db)
	defer c.Close()

	table := fmt.Sprintf("unified_stmt_rows_cov_%d", time.Now().UnixNano())
	_, err := c.Exec(fmt.Sprintf("create table if not exists %s.%s(ts timestamp, v int, note nchar(16))", db, table), 0)
	require.NoError(t, err)

	insertSQL := fmt.Sprintf("insert into %s.%s values(?,?,?)", db, table)
	querySQL := fmt.Sprintf("select ts,v,note from %s.%s where v >= ? order by ts", db, table)

	t.Run("stmt_bind_exec_use_result_rows", func(t *testing.T) {
		stmt, stmtErr := c.InitStmt(0)
		require.NoError(t, stmtErr)
		defer func() {
			_ = stmt.Close(0)
		}()

		require.NoError(t, stmt.Prepare(insertSQL, 0))
		isInsert, stmtErr := stmt.IsInsert()
		require.NoError(t, stmtErr)
		require.True(t, isInsert)
		insertFields, stmtErr := stmt.ColFields()
		require.NoError(t, stmtErr)
		require.Len(t, insertFields, 3)

		baseTS := time.Now().UTC().Round(time.Millisecond)
		require.NoError(t, stmt.Bind([]*commonstmt.TaosStmt2BindData{
			{
				Cols: [][]driver.Value{
					{baseTS, baseTS.Add(time.Second)},
					{int32(11), int32(22)},
					{"note_1", "note_2"},
				},
			},
		}))
		affected, stmtErr := stmt.Exec()
		require.NoError(t, stmtErr)
		require.Equal(t, 2, affected)
		require.Equal(t, 2, stmt.AffectedRows())

		require.NoError(t, stmt.Close(0))
		require.NoError(t, stmt.Close(0))

		queryStmt, queryErr := c.InitStmt(0)
		require.NoError(t, queryErr)
		defer func() {
			_ = queryStmt.Close(0)
		}()

		require.NoError(t, queryStmt.Prepare(querySQL, 0))
		queryInsert, queryErr := queryStmt.IsInsert()
		require.NoError(t, queryErr)
		require.False(t, queryInsert)
		queryFields, queryErr := queryStmt.ColFields()
		require.NoError(t, queryErr)
		require.Nil(t, queryFields)

		require.NoError(t, queryStmt.Bind([]*commonstmt.TaosStmt2BindData{
			{
				Cols: [][]driver.Value{{int32(10)}},
			},
		}))
		_, queryErr = queryStmt.Exec()
		require.NoError(t, queryErr)

		rows, queryErr := queryStmt.UseResult(0)
		require.NoError(t, queryErr)
		require.NotNil(t, rows)
		require.NotZero(t, rows.ResultID())
		require.Equal(t, []string{"ts", "v", "note"}, rows.Columns())
		require.NotEmpty(t, rows.ColumnTypeDatabaseTypeName(0))
		_, _ = rows.ColumnTypeLength(1)
		_, _, _ = rows.ColumnTypePrecisionScale(1)
		require.NotNil(t, rows.ColumnTypeScanType(1))

		result := make([][]driver.Value, 0, 4)
		for {
			values := make([]driver.Value, 3)
			nextErr := rows.Next(values)
			if errors.Is(nextErr, io.EOF) {
				break
			}
			require.NoError(t, nextErr)
			result = append(result, append([]driver.Value(nil), values...))
		}
		require.Len(t, result, 2)
		require.Equal(t, int32(11), result[0][1])
		require.Equal(t, int32(22), result[1][1])

		require.NoError(t, rows.FreeResult(0))
		require.NoError(t, rows.FreeResult(0))
		require.ErrorIs(t, rows.Next(make([]driver.Value, 3)), ErrQueryResultClosed)
		require.NoError(t, rows.Close())
	})

	t.Run("query_exec_and_runtime_mismatch_result", func(t *testing.T) {
		_, err = c.Exec(fmt.Sprintf("insert into %s.%s values(now, 33, 'note_3')", db, table), 0)
		require.NoError(t, err)

		affected, execErr := c.Exec(fmt.Sprintf("select ts,v,note from %s.%s limit 1", db, table), 0)
		require.NoError(t, execErr)
		require.Equal(t, 0, affected)

		rows, queryErr := c.Query(fmt.Sprintf("select ts,v,note from %s.%s order by ts limit 1", db, table), 0)
		require.NoError(t, queryErr)
		require.NotNil(t, rows)
		require.NotZero(t, rows.ResultID())

		oldRuntime := c.Runtime()
		require.NotNil(t, oldRuntime)
		require.NoError(t, c.ReconnectWithBootstrap(c.defaultBootstrap))

		_, _, queryErr = rows.FetchRawBlock(0)
		require.ErrorIs(t, queryErr, ErrQueryResultConnectionLost)
		require.ErrorIs(t, rows.FreeResult(0), ErrQueryResultConnectionLost)
		require.NoError(t, rows.FreeResult(0))
	})

	t.Run("stmt_reconnect_paths", func(t *testing.T) {
		// init reconnect path
		runtime := c.Runtime()
		require.NotNil(t, runtime)
		runtime.Close()
		_, initErr := c.InitStmt(0)
		require.NoError(t, initErr)

		// prepare reconnect path
		prepareStmt, prepareErr := c.InitStmt(0)
		require.NoError(t, prepareErr)
		active := activeAdapterPort(t, c)
		stopByPort(t, active, stops)
		require.NoError(t, prepareStmt.Prepare(insertSQL, 0))
		require.NoError(t, prepareStmt.Close(0))
		stops[active] = restartAdapterOnPort(t, active)

		// exec reconnect path
		execStmt, execInitErr := c.InitStmt(0)
		require.NoError(t, execInitErr)
		require.NoError(t, execStmt.Prepare(insertSQL, 0))
		require.NoError(t, execStmt.Bind([]*commonstmt.TaosStmt2BindData{
			{
				Cols: [][]driver.Value{
					{time.Now().UTC().Round(time.Millisecond)},
					{int32(44)},
					{"note_4"},
				},
			},
		}))
		active = activeAdapterPort(t, c)
		stopByPort(t, active, stops)
		affected, execErr := execStmt.Exec()
		require.NoError(t, execErr)
		require.Equal(t, 1, affected)
		require.NoError(t, execStmt.Close(0))
		stops[active] = restartAdapterOnPort(t, active)

		// schema-changed branch during reprepare after reconnect.
		schemaStmt, schemaErr := c.InitStmt(0)
		require.NoError(t, schemaErr)
		require.NoError(t, schemaStmt.Prepare(insertSQL, 0))
		schemaStmt.fieldsCount += 1
		require.NoError(t, schemaStmt.Bind([]*commonstmt.TaosStmt2BindData{
			{
				Cols: [][]driver.Value{
					{time.Now().UTC().Round(time.Millisecond)},
					{int32(55)},
					{"note_5"},
				},
			},
		}))
		active = activeAdapterPort(t, c)
		stopByPort(t, active, stops)
		_, schemaErr = schemaStmt.Exec()
		require.ErrorIs(t, schemaErr, ErrStmtReprepareSchemaChanged)
		_, schemaErr = schemaStmt.IsInsert()
		require.ErrorIs(t, schemaErr, ErrStmtSchemaChanged)
		require.NoError(t, schemaStmt.Prepare(insertSQL, 0))
		require.NoError(t, schemaStmt.Close(0))
		stops[active] = restartAdapterOnPort(t, active)
	})

	t.Run("request_runtime_mismatch_paths", func(t *testing.T) {
		oldRuntime := c.Runtime()
		require.NotNil(t, oldRuntime)
		require.NoError(t, c.ReconnectWithBootstrap(c.defaultBootstrap))

		envelope := client.GlobalEnvelopePool.Get()
		defer client.GlobalEnvelopePool.Put(envelope)
		envelope.Type = websocket.TextMessage
		envelope.Msg.Reset()
		_, _ = envelope.Msg.WriteString("{}")

		_, acked, _, sendErr := c.sendEnvelopeWithRuntime(oldRuntime, uint64(time.Now().UnixNano()), envelope, 0, nil)
		require.ErrorIs(t, sendErr, client.ClosedError)
		require.False(t, acked)

		sendErr = c.sendEnvelopeNoResponse(oldRuntime, envelope)
		require.ErrorIs(t, sendErr, client.ClosedError)
	})
}

func TestUnifiedSmallCoverageEdges(t *testing.T) {
	t.Run("failover_endpoints_copy", func(t *testing.T) {
		state, err := NewFailoverState([]string{"ws://a", "ws://b"})
		require.NoError(t, err)
		endpoints := state.Endpoints()
		require.Equal(t, []string{"ws://a", "ws://b"}, endpoints)
		endpoints[0] = "changed"
		require.Equal(t, "ws://a", state.Endpoints()[0])
	})

	t.Run("stmt_compat_state_clear_bind_data", func(t *testing.T) {
		state := NewStmtCompatState()
		require.NoError(t, state.SetRawBindData([]*commonstmt.TaosStmt2BindData{
			{
				TableName: "tb1",
				Cols:      [][]driver.Value{{int32(1)}},
			},
		}, true))
		require.True(t, state.HasBindData(true))
		state.ClearBindData()
		require.False(t, state.HasBindData(true))
		require.Nil(t, state.BindData(true))
	})

	t.Run("rows_and_error_nil_branches", func(t *testing.T) {
		var rs *ResultSet
		require.Equal(t, uint64(0), rs.ResultID())
		require.ErrorIs(t, rs.FreeResult(0), ErrQueryResultClosed)
		require.ErrorIs(t, rs.Close(), ErrQueryResultClosed)

		var unifiedErr *Error
		require.Equal(t, "", unifiedErr.Error())
		require.Nil(t, unifiedErr.Unwrap())

		baseErr := errors.New("root cause")
		wrapped := &Error{Type: ErrorTypeProtocol, Cause: baseErr}
		require.Equal(t, "root cause", wrapped.Error())
		require.ErrorIs(t, wrapped, baseErr)
	})

	t.Run("connector_nil_receiver", func(t *testing.T) {
		var connector *Connector
		require.Equal(t, Config{}, connector.Config())
		_, err := connector.Connect()
		require.ErrorIs(t, err, ErrNilConfig)
	})
}
