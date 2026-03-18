package unified

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/taosdata/driver-go/v3/ws/client"
)

// TestBuildTextRequestSummaryRedactsSensitiveFields verifies the expected behavior for this scenario.
func TestBuildTextRequestSummaryRedactsSensitiveFields(t *testing.T) {
	args := []byte(`{
		"user":"root",
		"password":"plain-password",
		"token":"plain-token",
		"td.connect.token":"cfg-token",
		"url":"ws://127.0.0.1:6041/rest/tmq?token=query-token&x=1",
		"sql":"select * from t where password='secret-pass' and token=\"secret-token\"",
		"safe":"ok"
	}`)
	summary := buildTextRequestSummary("subscribe", 101, args)

	require.Contains(t, summary, "action=subscribe")
	require.Contains(t, summary, "req_id=101")
	require.Contains(t, summary, `"password":"***"`)
	require.Contains(t, summary, `"token":"***"`)
	require.Contains(t, summary, `"td.connect.token":"***"`)
	require.Contains(t, summary, `"safe":"ok"`)
	require.NotContains(t, summary, "plain-password")
	require.NotContains(t, summary, "plain-token")
	require.NotContains(t, summary, "cfg-token")
	require.NotContains(t, summary, "query-token")
	require.NotContains(t, summary, "secret-pass")
	require.NotContains(t, summary, "secret-token")
}

// TestBuildBinaryQueryRequestSummaryRedactsSensitiveText verifies the expected behavior for this scenario.
func TestBuildBinaryQueryRequestSummaryRedactsSensitiveText(t *testing.T) {
	sql := `insert into t values(now, 1) password='abc' token=def authorization:"ghi"`
	summary := buildBinaryQueryRequestSummary(88, sql)

	require.Contains(t, summary, "binary_action=query")
	require.Contains(t, summary, "req_id=88")
	require.Contains(t, summary, "password=***")
	require.Contains(t, summary, "token=***")
	require.Contains(t, summary, "authorization:***")
	require.NotContains(t, summary, "abc")
	require.NotContains(t, summary, "def")
	require.NotContains(t, summary, "ghi")
}

// TestWrapRequestErrorKeepsErrorIs verifies wrapped errors still support errors.Is checks.
func TestWrapRequestErrorKeepsErrorIs(t *testing.T) {
	wrapped := wrapRequestError(client.ClosedError, "action=ping")
	require.Error(t, wrapped)
	require.True(t, errors.Is(wrapped, client.ClosedError))
	require.Contains(t, wrapped.Error(), "action=ping")
}

// TestWrapRequestErrorSummaryFuncLazy verifies summary function is evaluated only when needed.
func TestWrapRequestErrorSummaryFuncLazy(t *testing.T) {
	called := 0
	summaryFunc := func() string {
		called++
		return "action=query"
	}

	require.NoError(t, wrapRequestErrorWithSummaryFunc(nil, summaryFunc))
	require.Equal(t, 0, called)

	wrapped := wrapRequestErrorWithSummaryFunc(client.ClosedError, summaryFunc)
	require.Error(t, wrapped)
	require.Equal(t, 1, called)
	require.Contains(t, wrapped.Error(), "action=query")
}
