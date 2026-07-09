package unified

import (
	"reflect"
	"sync/atomic"
	"testing"
)

func withFreshClusterRegistry(t *testing.T) *clusterRegistry {
	t.Helper()
	old := globalClusterRegistry
	registry := newClusterRegistry()
	globalClusterRegistry = registry
	t.Cleanup(func() {
		globalClusterRegistry = old
	})
	return registry
}

func TestNewClientAdapterHAExpandsEndpointsFromGlobalRegistry(t *testing.T) {
	registry := withFreshClusterRegistry(t)
	registry.update([]string{"seed:6041", "peer:6041"})

	cfg := NewConfig([]string{"wss://seed:6041/ws?token=abc"})
	cfg.AdapterHA = true
	c, err := NewClient(cfg, "/ws")
	if err != nil {
		t.Fatal(err)
	}

	got := c.failover.endpointsCopy()
	want := []string{
		"wss://seed:6041/ws?token=abc",
		"wss://peer:6041/ws?token=abc",
	}
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("want %v, got %v", want, got)
	}
}

func TestNewClientAdapterHADisabledDoesNotReadRegistry(t *testing.T) {
	registry := withFreshClusterRegistry(t)
	registry.update([]string{"seed:6041", "peer:6041"})

	cfg := NewConfig([]string{"wss://seed:6041/ws?token=abc"})
	c, err := NewClient(cfg, "/ws")
	if err != nil {
		t.Fatal(err)
	}

	got := c.failover.endpointsCopy()
	want := []string{"wss://seed:6041/ws?token=abc"}
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("want %v, got %v", want, got)
	}
}

func TestClientMergeInstancesRegistersSeedsAndAppendsDiscoveredEndpoints(t *testing.T) {
	registry := withFreshClusterRegistry(t)
	failover, err := newFailoverState([]string{"wss://seed:6041/ws?token=abc"})
	if err != nil {
		t.Fatal(err)
	}
	c := &Client{failover: failover}

	c.mergeInstances("wss://seed:6041/ws?token=abc", []string{"peer:6041", "peer:6041", "bad"})

	gotEndpoints := failover.endpointsCopy()
	wantEndpoints := []string{
		"wss://seed:6041/ws?token=abc",
		"wss://peer:6041/ws?token=abc",
	}
	if !reflect.DeepEqual(wantEndpoints, gotEndpoints) {
		t.Fatalf("want endpoints %v, got %v", wantEndpoints, gotEndpoints)
	}

	gotRegistry := registry.expand([]string{"seed:6041"})
	wantRegistry := []string{"seed:6041", "peer:6041"}
	if !reflect.DeepEqual(wantRegistry, gotRegistry) {
		t.Fatalf("want registry expansion %v, got %v", wantRegistry, gotRegistry)
	}
}

func TestClientMergeAdapterHAInstancesOnceUsesFlag(t *testing.T) {
	registry := withFreshClusterRegistry(t)
	failover, err := newFailoverState([]string{"wss://seed:6041/ws?token=abc"})
	if err != nil {
		t.Fatal(err)
	}
	c := &Client{
		config:   Config{AdapterHA: true},
		failover: failover,
	}
	var fetched uint32
	firstInstances := []string{"peer:6041"}
	secondInstances := []string{"other:6041"}

	c.mergeAdapterHAInstancesOnce(&fetched, "wss://seed:6041/ws?token=abc", &firstInstances)
	c.mergeAdapterHAInstancesOnce(&fetched, "wss://seed:6041/ws?token=abc", &secondInstances)

	if got := atomic.LoadUint32(&fetched); got != 1 {
		t.Fatalf("want fetched flag 1, got %d", got)
	}
	gotEndpoints := failover.endpointsCopy()
	wantEndpoints := []string{
		"wss://seed:6041/ws?token=abc",
		"wss://peer:6041/ws?token=abc",
	}
	if !reflect.DeepEqual(wantEndpoints, gotEndpoints) {
		t.Fatalf("want endpoints %v, got %v", wantEndpoints, gotEndpoints)
	}
	gotRegistry := registry.expand([]string{"seed:6041"})
	wantRegistry := []string{"seed:6041", "peer:6041"}
	if !reflect.DeepEqual(wantRegistry, gotRegistry) {
		t.Fatalf("want registry expansion %v, got %v", wantRegistry, gotRegistry)
	}
}
