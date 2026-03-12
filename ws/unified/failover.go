package unified

import (
	"math/rand"
	"sync"
)

type EndpointCandidate struct {
	Index int
	URL   string
}

// FailoverState stores active endpoint and candidate order for initial connect/reconnect.
type FailoverState struct {
	endpoints   []string
	activeIndex int
	lock        sync.RWMutex
}

// NewFailoverState initializes failover state with a copied endpoint list.
func NewFailoverState(endpoints []string) (*FailoverState, error) {
	if len(endpoints) == 0 {
		return nil, ErrNoEndpoints
	}
	copyEndpoints := make([]string, len(endpoints))
	copy(copyEndpoints, endpoints)
	return &FailoverState{
		endpoints:   copyEndpoints,
		activeIndex: 0,
	}, nil
}

// Endpoints returns a copy of configured endpoints.
func (s *FailoverState) Endpoints() []string {
	s.lock.RLock()
	defer s.lock.RUnlock()
	out := make([]string, len(s.endpoints))
	copy(out, s.endpoints)
	return out
}

// Active returns the currently selected endpoint candidate.
func (s *FailoverState) Active() EndpointCandidate {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return EndpointCandidate{
		Index: s.activeIndex,
		URL:   s.endpoints[s.activeIndex],
	}
}

// MarkActive updates the active endpoint index.
func (s *FailoverState) MarkActive(index int) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if index < 0 || index >= len(s.endpoints) {
		return ErrInvalidEndpointIndex
	}
	s.activeIndex = index
	return nil
}

// InitialCandidates returns endpoints from random start index for initial connection attempt.
func (s *FailoverState) InitialCandidates() []EndpointCandidate {
	s.lock.RLock()
	defer s.lock.RUnlock()
	startIndex := rand.Intn(len(s.endpoints))
	return s.orderedCandidatesFrom(startIndex)
}

// ReconnectCandidates returns endpoints starting from next index after active endpoint.
func (s *FailoverState) ReconnectCandidates() []EndpointCandidate {
	s.lock.RLock()
	defer s.lock.RUnlock()
	start := (s.activeIndex + 1) % len(s.endpoints)
	return s.orderedCandidatesFrom(start)
}

// orderedCandidatesFrom returns all endpoints in round-robin order from start.
func (s *FailoverState) orderedCandidatesFrom(start int) []EndpointCandidate {
	size := len(s.endpoints)
	candidates := make([]EndpointCandidate, 0, size)
	for i := 0; i < size; i++ {
		idx := (start + i) % size
		candidates = append(candidates, EndpointCandidate{
			Index: idx,
			URL:   s.endpoints[idx],
		})
	}
	return candidates
}
