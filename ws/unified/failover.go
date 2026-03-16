package unified

import (
	"math/rand"
	"sync"
	"time"
)

type endpointCandidate struct {
	Index int
	URL   string
}

// failoverState stores active endpoint and candidate order for initial connect/reconnect.
type failoverState struct {
	endpoints   []string
	activeIndex int
	rng         *rand.Rand
	lock        sync.RWMutex
}

// newFailoverState initializes failover state with a copied endpoint list.
func newFailoverState(endpoints []string) (*failoverState, error) {
	if len(endpoints) == 0 {
		return nil, ErrNoEndpoints
	}
	copyEndpoints := make([]string, len(endpoints))
	copy(copyEndpoints, endpoints)
	return &failoverState{
		endpoints:   copyEndpoints,
		activeIndex: 0,
		rng:         rand.New(rand.NewSource(time.Now().UnixNano())),
	}, nil
}

// Endpoints returns a copy of configured endpoints.
func (s *failoverState) Endpoints() []string {
	s.lock.RLock()
	defer s.lock.RUnlock()
	out := make([]string, len(s.endpoints))
	copy(out, s.endpoints)
	return out
}

// Active returns the currently selected endpoint candidate.
func (s *failoverState) Active() endpointCandidate {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return endpointCandidate{
		Index: s.activeIndex,
		URL:   s.endpoints[s.activeIndex],
	}
}

// MarkActive updates the active endpoint index.
func (s *failoverState) MarkActive(index int) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if index < 0 || index >= len(s.endpoints) {
		return ErrInvalidEndpointIndex
	}
	s.activeIndex = index
	return nil
}

// InitialCandidates returns endpoints from random start index for initial connection attempt.
func (s *failoverState) InitialCandidates() []endpointCandidate {
	s.lock.Lock()
	defer s.lock.Unlock()
	startIndex := s.rng.Intn(len(s.endpoints))
	return s.orderedCandidatesFrom(startIndex)
}

// ReconnectCandidates returns endpoints starting from next index after active endpoint.
func (s *failoverState) ReconnectCandidates() []endpointCandidate {
	s.lock.RLock()
	defer s.lock.RUnlock()
	start := (s.activeIndex + 1) % len(s.endpoints)
	return s.orderedCandidatesFrom(start)
}

// orderedCandidatesFrom returns all endpoints in round-robin order from start.
func (s *failoverState) orderedCandidatesFrom(start int) []endpointCandidate {
	size := len(s.endpoints)
	candidates := make([]endpointCandidate, 0, size)
	for i := 0; i < size; i++ {
		idx := (start + i) % size
		candidates = append(candidates, endpointCandidate{
			Index: idx,
			URL:   s.endpoints[idx],
		})
	}
	return candidates
}
