package unified

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	taosErrors "github.com/taosdata/driver-go/v3/errors"
)

// TestUnifiedOpenRealAdapter verifies Open wrappers with real adapter.
func TestUnifiedOpenRealAdapter(t *testing.T) {
	if testing.Short() {
		t.Skip("skip integration test in short mode")
	}
	tmqIntegrationSQL(t, "select 1")

	dsn := strings.TrimSpace(os.Getenv("UNIFIED_IT_DSN"))
	if dsn == "" {
		dsn = "root:taosdata@ws(127.0.0.1:6041)/"
	}

	client, err := Open(dsn)
	if err != nil {
		t.Fatalf("integration test requires taosadapter/taosd: unified.Open failed: %v", err)
	}
	_, err = client.Exec(0, "select 1")
	if err != nil {
		client.Close()
		t.Fatalf("select failed after Open: %v", err)
	}
	client.Close()

	driverOpenClient, err := NewDSNDriver("").Open(dsn)
	if err != nil {
		t.Fatalf("integration test requires taosadapter/taosd: DSNDriver.Open failed: %v", err)
	}
	_, err = driverOpenClient.Exec(0, "select 1")
	if err != nil {
		driverOpenClient.Close()
		t.Fatalf("select failed after DSNDriver.Open: %v", err)
	}
	driverOpenClient.Close()
}

// TestUnifiedExecIllegalSQLReturnsTaosError verifies illegal SQL returns driver-go taos error type instead of unified.Error.
func TestUnifiedExecIllegalSQLReturnsTaosError(t *testing.T) {
	if testing.Short() {
		t.Skip("skip integration test in short mode")
	}
	tmqIntegrationSQL(t, "select 1")

	dsn := strings.TrimSpace(os.Getenv("UNIFIED_IT_DSN"))
	if dsn == "" {
		dsn = "root:taosdata@ws(127.0.0.1:6041)/"
	}

	client, err := Open(dsn)
	if err != nil {
		t.Fatalf("integration test requires taosadapter/taosd: unified.Open failed: %v", err)
	}
	defer client.Close()

	_, err = client.Exec(0, "xxxxxxx inot")
	if err == nil {
		t.Fatal("expected illegal SQL to fail")
	}

	var taosErr *taosErrors.TaosError
	if !errors.As(err, &taosErr) {
		t.Fatalf("expected taos error type, got %T: %v", err, err)
	}

	var unifiedErr *Error
	if errors.As(err, &unifiedErr) {
		t.Fatalf("expected non-unified error for illegal SQL, got unified error: %+v", unifiedErr)
	}
}

func TestQueryWithAdapterHA(t *testing.T) {
	if testing.Short() {
		t.Skip("skip integration test in short mode")
	}
	withFreshClusterRegistry(t)

	cfg, err := NewConfigFromDSN("root:taosdata@ws(127.0.0.1:6041)/?adapterHa=true", defaultDSNPath)
	require.NoError(t, err)
	require.True(t, cfg.AdapterHA)

	client, err := NewClient(cfg, defaultDSNPath)
	require.NoError(t, err)
	t.Cleanup(client.Close)

	require.Equal(t, uint32(0), atomic.LoadUint32(&client.instancesFetched))
	err = client.Connect()
	require.NoError(t, err)
	require.Equal(t, uint32(1), atomic.LoadUint32(&client.instancesFetched))
	require.NotEmpty(t, client.failover.endpointsCopy())

	rows, err := client.Query(0, "select 42")
	require.NoError(t, err)
	require.NotNil(t, rows)
	t.Cleanup(func() { _ = rows.Close() })

	values := make([]driver.Value, 1)
	require.NoError(t, rows.Next(values))
	require.Equal(t, "42", fmt.Sprint(values[0]))
	require.ErrorIs(t, rows.Next(values), io.EOF)
}
