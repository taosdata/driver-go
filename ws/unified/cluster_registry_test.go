package unified

import (
	"reflect"
	"sync"
	"testing"
)

func TestClusterRegistryExpandDoesNotCreateCluster(t *testing.T) {
	registry := newClusterRegistry()
	seeds := []string{"a:1", "b:2"}

	got := registry.expand(seeds)

	if !reflect.DeepEqual(seeds, got) {
		t.Fatalf("want seeds unchanged, got %v", got)
	}
	if len(registry.endpointToCluster) != 0 {
		t.Fatalf("expand must not create clusters, got %d mappings", len(registry.endpointToCluster))
	}
}

func TestClusterRegistryUpdateLetsSeedInheritDiscoveredInstances(t *testing.T) {
	registry := newClusterRegistry()

	registry.update([]string{"seed:6041", "peer:6041"})

	got := registry.expand([]string{"seed:6041"})
	want := []string{"seed:6041", "peer:6041"}
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("want %v, got %v", want, got)
	}
}

func TestClusterRegistryUpdateConnectsClustersTransitively(t *testing.T) {
	registry := newClusterRegistry()

	registry.update([]string{"a:1", "b:2"})
	registry.update([]string{"b:2", "c:3"})

	got := registry.expand([]string{"a:1"})
	want := []string{"a:1", "b:2", "c:3"}
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("want %v, got %v", want, got)
	}
}

func TestClusterRegistryProtectsAgainstMultiClusterSeeds(t *testing.T) {
	registry := newClusterRegistry()
	registry.update([]string{"a:1", "b:2"})
	registry.update([]string{"c:3", "d:4"})

	seeds := []string{"a:1", "c:3"}
	got := registry.expand(seeds)
	if !reflect.DeepEqual(seeds, got) {
		t.Fatalf("multi-cluster expand must return seeds unchanged, got %v", got)
	}

	registry.update([]string{"b:2", "c:3", "e:5"})
	got = registry.expand([]string{"a:1"})
	want := []string{"a:1", "b:2"}
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("multi-cluster update must not merge clusters, want %v, got %v", want, got)
	}
}

func TestValidUniqueHostPortsFiltersInvalidAndDeduplicates(t *testing.T) {
	got := validUniqueHostPorts([]string{
		"a:1",
		"a:1",
		"missing-port",
		"b:0",
		"b:65536",
		":6041",
		"[::1]:6041",
		"c:6041",
	})
	want := []string{"a:1", "[::1]:6041", "c:6041"}
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("want %v, got %v", want, got)
	}
}

func TestClusterRegistryConcurrentUpdateConvergesWithoutDuplicates(t *testing.T) {
	registry := newClusterRegistry()

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			registry.update([]string{"seed:6041", "peer:6041"})
		}()
	}
	wg.Wait()

	got := registry.expand([]string{"seed:6041"})
	want := []string{"seed:6041", "peer:6041"}
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("want %v, got %v", want, got)
	}
}
