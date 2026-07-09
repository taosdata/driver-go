package unified

import (
	"reflect"
	"sync"
	"testing"
)

// TestFailoverStateInitialCandidates verifies the expected behavior for this scenario.
func TestFailoverStateInitialCandidates(t *testing.T) {
	resetGlobalConnCounterForTest(t)
	state, err := newFailoverState([]string{"ws://a:1/ws", "ws://b:2/ws", "ws://c:3/ws"})
	if err != nil {
		t.Fatal(err)
	}
	addEndpointConnCountForTest(t, "ws://a:1/ws", 2)
	addEndpointConnCountForTest(t, "ws://b:2/ws", 1)

	got := state.initialCandidates()
	want := []endpointCandidate{
		{Index: 2, URL: "ws://c:3/ws"},
		{Index: 1, URL: "ws://b:2/ws"},
		{Index: 0, URL: "ws://a:1/ws"},
	}
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("want %v, got %v", want, got)
	}
}

// TestFailoverStateReconnectCandidates verifies the expected behavior for this scenario.
func TestFailoverStateReconnectCandidates(t *testing.T) {
	resetGlobalConnCounterForTest(t)
	state, err := newFailoverState([]string{"ws://a:1/ws", "ws://b:2/ws", "ws://c:3/ws"})
	if err != nil {
		t.Fatal(err)
	}
	if err = state.markActive(1); err != nil {
		t.Fatal(err)
	}

	got := state.reconnectCandidates()
	want := []endpointCandidate{
		{Index: 1, URL: "ws://b:2/ws"},
		{Index: 0, URL: "ws://a:1/ws"},
		{Index: 2, URL: "ws://c:3/ws"},
	}
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("want %v, got %v", want, got)
	}
}

// TestFailoverStateReconnectCandidatesActiveFirstRegardlessOfConnectionCount verifies the expected behavior for this scenario.
func TestFailoverStateReconnectCandidatesActiveFirstRegardlessOfConnectionCount(t *testing.T) {
	resetGlobalConnCounterForTest(t)
	state, err := newFailoverState([]string{"ws://a:1/ws", "ws://b:2/ws", "ws://c:3/ws"})
	if err != nil {
		t.Fatal(err)
	}
	if err = state.markActive(1); err != nil {
		t.Fatal(err)
	}

	addEndpointConnCountForTest(t, "ws://a:1/ws", 1)
	addEndpointConnCountForTest(t, "ws://b:2/ws", 10)
	addEndpointConnCountForTest(t, "ws://c:3/ws", 2)

	got := state.reconnectCandidates()
	want := []endpointCandidate{
		{Index: 1, URL: "ws://b:2/ws"},
		{Index: 0, URL: "ws://a:1/ws"},
		{Index: 2, URL: "ws://c:3/ws"},
	}
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("want %v, got %v", want, got)
	}
}

// TestFailoverStateDoesNotCrossClientEndpointSet verifies the expected behavior for this scenario.
func TestFailoverStateDoesNotCrossClientEndpointSet(t *testing.T) {
	resetGlobalConnCounterForTest(t)
	state1, err := newFailoverState([]string{"ws://a:1/ws", "ws://b:2/ws"})
	if err != nil {
		t.Fatal(err)
	}
	state2, err := newFailoverState([]string{"ws://a:1/ws", "ws://c:3/ws"})
	if err != nil {
		t.Fatal(err)
	}
	addEndpointConnCountForTest(t, "ws://a:1/ws", 2)
	addEndpointConnCountForTest(t, "ws://b:2/ws", 1)

	got1 := state1.initialCandidates()
	want1 := []endpointCandidate{
		{Index: 1, URL: "ws://b:2/ws"},
		{Index: 0, URL: "ws://a:1/ws"},
	}
	if !reflect.DeepEqual(want1, got1) {
		t.Fatalf("state1 want %v, got %v", want1, got1)
	}

	got2 := state2.initialCandidates()
	want2 := []endpointCandidate{
		{Index: 1, URL: "ws://c:3/ws"},
		{Index: 0, URL: "ws://a:1/ws"},
	}
	if !reflect.DeepEqual(want2, got2) {
		t.Fatalf("state2 want %v, got %v", want2, got2)
	}
}

// TestGlobalHostPortConnCounterConcurrentIncDec verifies the expected behavior for this scenario.
func TestGlobalHostPortConnCounterConcurrentIncDec(t *testing.T) {
	resetGlobalConnCounterForTest(t)
	key := hostPortKeyForEndpointForTest(t, "ws://a:1/ws")

	var wg sync.WaitGroup
	const workers = 8
	const loops = 1000
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < loops; j++ {
				globalHostPortConnCounts.inc(key)
			}
			for j := 0; j < loops; j++ {
				globalHostPortConnCounts.dec(key)
			}
		}()
	}
	wg.Wait()

	if got := globalHostPortConnCounts.get(key); got != 0 {
		t.Fatalf("want count 0, got %d", got)
	}
	for i := 0; i < 10; i++ {
		globalHostPortConnCounts.dec(key)
	}
	if got := globalHostPortConnCounts.get(key); got != 0 {
		t.Fatalf("count should not go below zero, got %d", got)
	}
}

// TestFailoverStateMarkActiveAndActive verifies the expected behavior for this scenario.
func TestFailoverStateMarkActiveAndActive(t *testing.T) {
	state, err := newFailoverState([]string{"ws://a:1/ws", "ws://b:2/ws", "ws://c:3/ws"})
	if err != nil {
		t.Fatal(err)
	}
	if err = state.markActive(2); err != nil {
		t.Fatal(err)
	}
	active := state.active()
	if active.Index != 2 || active.URL != "ws://c:3/ws" {
		t.Fatalf("unexpected active: %+v", active)
	}
}

// TestFailoverStateMarkActiveInvalidIndex verifies the expected behavior for this scenario.
func TestFailoverStateMarkActiveInvalidIndex(t *testing.T) {
	state, err := newFailoverState([]string{"ws://a:1/ws"})
	if err != nil {
		t.Fatal(err)
	}
	if err = state.markActive(2); err == nil {
		t.Fatal("expect invalid endpoint index error")
	}
}

// TestFailoverStateMergeEndpointsAppendsNewHostPorts verifies dynamic endpoint
// discovery keeps existing indexes stable and only appends new host:port values.
func TestFailoverStateMergeEndpointsAppendsNewHostPorts(t *testing.T) {
	state, err := newFailoverState([]string{"ws://a:1/ws?token=seed"})
	if err != nil {
		t.Fatal(err)
	}
	added := state.mergeEndpoints([]string{
		"ws://a:1/other?token=dup",
		"wss://b:2/rest/tmq?token=next",
		"not-a-url",
	})

	if added != 1 {
		t.Fatalf("want one added endpoint, got %d", added)
	}
	got := state.endpointsCopy()
	want := []string{"ws://a:1/ws?token=seed", "wss://b:2/rest/tmq?token=next"}
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("want endpoints %v, got %v", want, got)
	}
	hostPort0, err := state.hostPortByIndex(0)
	if err != nil {
		t.Fatal(err)
	}
	hostPort1, err := state.hostPortByIndex(1)
	if err != nil {
		t.Fatal(err)
	}
	if hostPort0 != "a:1" || hostPort1 != "b:2" {
		t.Fatalf("unexpected host ports: %s, %s", hostPort0, hostPort1)
	}
}

func TestFormatHostPortsToURLsPreservesTemplate(t *testing.T) {
	got := formatHostPortsToURLs([]string{"b:2", "[::1]:6041"}, "wss://a:1/rest/tmq?token=abc&x=1")
	want := []string{
		"wss://b:2/rest/tmq?token=abc&x=1",
		"wss://[::1]:6041/rest/tmq?token=abc&x=1",
	}
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("want %v, got %v", want, got)
	}
}

func TestHostPortsOfEndpointsUsesEndpointDefaults(t *testing.T) {
	got := hostPortsOf([]string{"ws://a:1/ws", "wss://b/ws", "ws://[::1]:6041/ws", "bad"})
	want := []string{"a:1", "b:443", "[::1]:6041"}
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("want %v, got %v", want, got)
	}
}
