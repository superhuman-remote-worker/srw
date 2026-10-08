package main

import "sync"

const (
	// Calls and streams open across the whole pod.
	globalInFlight = 32
	// Sessions the front remembers, in all and per lease. A lease that
	// opens more forgets its own oldest, never another lease's.
	maxSessions         = 4096
	maxSessionsPerLease = 32
)

// inflight caps the calls and streams one binding (and the whole pod) has
// open.
type inflight struct {
	mu       sync.Mutex
	perLease map[string]int
	total    int
}

func (c *inflight) acquire(leaseID string, limit int) (func(), bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.perLease[leaseID] >= limit || c.total >= globalInFlight {
		return nil, false
	}
	c.perLease[leaseID]++
	c.total++
	return func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.perLease[leaseID]--
		if c.perLease[leaseID] <= 0 {
			delete(c.perLease, leaseID)
		}
		c.total--
	}, true
}

// sessionOwners keeps each server session to the lease that opened it. A
// session the front does not know (opened before it started, forgotten,
// or never answered to an initialize) is no session of anyone's: its
// requests get 404 and the client initializes again.
type sessionOwners struct {
	mu      sync.Mutex
	owners  map[string]string
	order   []string
	byLease map[string][]string
}

func newSessionOwners() *sessionOwners {
	return &sessionOwners{owners: map[string]string{}, byLease: map[string][]string{}}
}

func (s *sessionOwners) bind(session, leaseID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if owner, known := s.owners[session]; known {
		if owner == leaseID {
			return
		}
		s.forgetLocked(session)
	}
	if mine := s.byLease[leaseID]; len(mine) >= maxSessionsPerLease {
		s.forgetLocked(mine[0])
	}
	for len(s.order) >= maxSessions {
		s.forgetLocked(s.order[0])
	}
	s.owners[session] = leaseID
	s.order = append(s.order, session)
	s.byLease[leaseID] = append(s.byLease[leaseID], session)
}

func (s *sessionOwners) forget(session string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.forgetLocked(session)
}

func (s *sessionOwners) forgetLocked(session string) {
	owner, known := s.owners[session]
	if !known {
		return
	}
	delete(s.owners, session)
	s.order = without(s.order, session)
	if rest := without(s.byLease[owner], session); len(rest) > 0 {
		s.byLease[owner] = rest
	} else {
		delete(s.byLease, owner)
	}
}

func without(list []string, item string) []string {
	for i, value := range list {
		if value == item {
			return append(list[:i:i], list[i+1:]...)
		}
	}
	return list
}

// mayUse: only the lease that opened a session.
func (s *sessionOwners) mayUse(session, leaseID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	owner, known := s.owners[session]
	return known && owner == leaseID
}
