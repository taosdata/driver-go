package unified

import (
	"os"
	"strings"
	"testing"
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
