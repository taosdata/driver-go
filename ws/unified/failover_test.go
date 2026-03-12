package unified

import (
	"reflect"
	"testing"
)

func TestFailoverStateInitialCandidates(t *testing.T) {
	state, err := NewFailoverState([]string{"a", "b", "c"})
	if err != nil {
		t.Fatal(err)
	}
	got := state.InitialCandidates()

	// Should return all 3 endpoints
	if len(got) != 3 {
		t.Fatalf("want 3 candidates, got %d", len(got))
	}

	// Should contain all endpoints (order may vary due to randomization)
	urls := make(map[string]bool)
	indices := make(map[int]bool)
	for _, c := range got {
		urls[c.URL] = true
		indices[c.Index] = true
	}

	expectedURLs := map[string]bool{"a": true, "b": true, "c": true}
	expectedIndices := map[int]bool{0: true, 1: true, 2: true}

	if !reflect.DeepEqual(expectedURLs, urls) {
		t.Fatalf("want URLs %v, got %v", expectedURLs, urls)
	}
	if !reflect.DeepEqual(expectedIndices, indices) {
		t.Fatalf("want indices %v, got %v", expectedIndices, indices)
	}

	// Verify round-robin order: each candidate should be followed by the next in sequence
	for i := 0; i < len(got)-1; i++ {
		expectedNextIndex := (got[i].Index + 1) % 3
		if got[i+1].Index != expectedNextIndex {
			t.Fatalf("candidates not in round-robin order: %v", got)
		}
	}
}

func TestFailoverStateReconnectCandidates(t *testing.T) {
	state, err := NewFailoverState([]string{"a", "b", "c"})
	if err != nil {
		t.Fatal(err)
	}
	if err = state.MarkActive(1); err != nil {
		t.Fatal(err)
	}
	got := state.ReconnectCandidates()
	want := []EndpointCandidate{
		{Index: 2, URL: "c"},
		{Index: 0, URL: "a"},
		{Index: 1, URL: "b"},
	}
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("want %v, got %v", want, got)
	}
}

func TestFailoverStateMarkActiveAndActive(t *testing.T) {
	state, err := NewFailoverState([]string{"a", "b", "c"})
	if err != nil {
		t.Fatal(err)
	}
	if err = state.MarkActive(2); err != nil {
		t.Fatal(err)
	}
	active := state.Active()
	if active.Index != 2 || active.URL != "c" {
		t.Fatalf("unexpected active: %+v", active)
	}
}

func TestFailoverStateMarkActiveInvalidIndex(t *testing.T) {
	state, err := NewFailoverState([]string{"a"})
	if err != nil {
		t.Fatal(err)
	}
	if err = state.MarkActive(2); err == nil {
		t.Fatal("expect invalid endpoint index error")
	}
}
