package unified

import (
	"reflect"
	"testing"
	"time"
)

// TestParseDSNInvalidNoSlash verifies the expected behavior for this scenario.
func TestParseDSNInvalidNoSlash(t *testing.T) {
	_, err := ParseDSN("abcd")
	if err == nil {
		t.Fatal("expect invalid dsn error")
	}
	if err.Error() != "invalid DSN: missing the slash separating the database name" {
		t.Fatalf("unexpected error: %s", err.Error())
	}
}

// TestParseDSNCommon verifies the expected behavior for this scenario.
func TestParseDSNCommon(t *testing.T) {
	cfg, err := ParseDSN("user:passwd@ws(fqdn:6041)/dbname")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.User != "user" || cfg.Passwd != "passwd" || cfg.Net != "ws" || cfg.Addr != "fqdn" || cfg.Port != 6041 || cfg.DbName != "dbname" {
		t.Fatalf("unexpected cfg: %+v", cfg)
	}
	if !cfg.InterpolateParams {
		t.Fatal("expect interpolate params default true")
	}
}

// TestParseDSNMultiAddrList verifies the expected behavior for this scenario.
func TestParseDSNMultiAddrList(t *testing.T) {
	cfg, err := ParseDSN("user:passwd@ws(a:6041,b:6042)/db")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Addr != "a" || cfg.Port != 6041 {
		t.Fatalf("unexpected first addr: host=%s port=%d", cfg.Addr, cfg.Port)
	}
	want := []string{"ws://a:6041", "ws://b:6042"}
	if !reflect.DeepEqual(want, cfg.Endpoints) {
		t.Fatalf("want %v, got %v", want, cfg.Endpoints)
	}
}

// TestParseDSNMultiAddrListWithToken verifies the expected behavior for this scenario.
func TestParseDSNMultiAddrListWithToken(t *testing.T) {
	cfg, err := ParseDSN("user:passwd@ws(a:6041,b:6042)/db?token=abc")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"ws://a:6041?token=abc", "ws://b:6042?token=abc"}
	if !reflect.DeepEqual(want, cfg.Endpoints) {
		t.Fatalf("want %v, got %v", want, cfg.Endpoints)
	}
}

// TestNewConfigFromDSN verifies the expected behavior for this scenario.
func TestNewConfigFromDSN(t *testing.T) {
	cfg, err := NewConfigFromDSN("user:passwd@ws(127.0.0.1:6041)/db?readTimeout=5s&writeTimeout=2s", "/ws")
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Endpoints) != 1 || cfg.Endpoints[0] != "ws://127.0.0.1:6041/ws" {
		t.Fatalf("unexpected endpoints: %+v", cfg.Endpoints)
	}
	if cfg.User != "user" || cfg.Passwd != "passwd" || cfg.DbName != "db" {
		t.Fatalf("unexpected auth/db: %+v", cfg)
	}
	if cfg.MessageTimeout != 5*time.Second || cfg.ReadTimeout != 5*time.Second || cfg.WriteTimeout != 2*time.Second {
		t.Fatalf("unexpected timeouts: msg=%v read=%v write=%v", cfg.MessageTimeout, cfg.ReadTimeout, cfg.WriteTimeout)
	}
}

// TestNewConfigFromDSNWithoutNetPrefix verifies the expected behavior for this scenario.
func TestNewConfigFromDSNWithoutNetPrefix(t *testing.T) {
	cfg, err := NewConfigFromDSN("user:passwd@(localhost:6041)/db", "/ws")
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Endpoints) != 1 || cfg.Endpoints[0] != "ws://localhost:6041/ws" {
		t.Fatalf("unexpected endpoints: %+v", cfg.Endpoints)
	}
	if cfg.User != "user" || cfg.Passwd != "passwd" || cfg.DbName != "db" {
		t.Fatalf("unexpected auth/db: %+v", cfg)
	}
}

// TestNewConfigFromDSNWriteTimeoutOnly verifies the expected behavior for this scenario.
func TestNewConfigFromDSNWriteTimeoutOnly(t *testing.T) {
	cfg, err := NewConfigFromDSN("user:passwd@ws(127.0.0.1:6041)/db?writeTimeout=2s", "/ws")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MessageTimeout != 5*time.Minute || cfg.ReadTimeout != 5*time.Minute || cfg.WriteTimeout != 2*time.Second {
		t.Fatalf("unexpected timeouts: msg=%v read=%v write=%v", cfg.MessageTimeout, cfg.ReadTimeout, cfg.WriteTimeout)
	}
}

// TestNewConfigFromDSNHostOmittedSingleNode verifies the expected behavior for this scenario.
func TestNewConfigFromDSNHostOmittedSingleNode(t *testing.T) {
	cfg, err := NewConfigFromDSN("user:passwd@ws(:6041)/db", "/ws")
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Endpoints) != 1 || cfg.Endpoints[0] != "ws://127.0.0.1:6041/ws" {
		t.Fatalf("unexpected endpoints: %+v", cfg.Endpoints)
	}
}

// TestNewConfigFromDSNMultiAddrList verifies the expected behavior for this scenario.
func TestNewConfigFromDSNMultiAddrList(t *testing.T) {
	cfg, err := NewConfigFromDSN("user:passwd@ws(a:6041,b:6042)/db", "/ws")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"ws://a:6041/ws", "ws://b:6042/ws"}
	if !reflect.DeepEqual(want, cfg.Endpoints) {
		t.Fatalf("want %v, got %v", want, cfg.Endpoints)
	}
}

// TestTryUnescape verifies the expected behavior for this scenario.
func TestTryUnescape(t *testing.T) {
	if got := TryUnescape("%3F"); got != "?" {
		t.Fatalf("unexpected unescape result: %s", got)
	}
	if got := TryUnescape("%"); got != "%" {
		t.Fatalf("unexpected unescape result: %s", got)
	}
}
