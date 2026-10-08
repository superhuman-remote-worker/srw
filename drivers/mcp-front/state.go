package main

import (
	"sync"
	"time"
)

const (
	// Calls and streams open across the whole pod.
	globalInFlight = 32
	// Sessions the front remembers, in all and per lease.
	maxSessions         = 4096
	maxSessionsPerLease = 32
	// A session unused this long may make room for another lease's.
	sessionIdle = 30 * time.Minute
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
//
// A lease holds at most maxSessionsPerLease sessions; one more forgets its
// own oldest. When the front holds maxSessions in all, a lease that has
// sessions makes room from its own; a lease that has none takes the least
// recently used session of a lease at its own cap, or of one unused for
// sessionIdle, and otherwise opens none (no lease under its cap loses a
// session it is using to another lease). What is forgotten is returned, so
// the front can close it on the server.
type sessionOwners struct {
	mu      sync.Mutex
	now     func() time.Time
	owners  map[string]*sessionEntry
	byLease map[string][]string // oldest first
}

type sessionEntry struct {
	lease string
	used  time.Time
}

func newSessionOwners(now func() time.Time) *sessionOwners {
	return &sessionOwners{now: now, owners: map[string]*sessionEntry{}, byLease: map[string][]string{}}
}

// room: whether leaseID may open a session now.
func (s *sessionOwners) room(leaseID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.victimLocked(leaseID)
	return ok
}

// victimLocked: the session to forget so leaseID may open one more ("" when
// none need be), or false when there is no room.
func (s *sessionOwners) victimLocked(leaseID string) (string, bool) {
	mine := s.byLease[leaseID]
	if len(mine) >= maxSessionsPerLease {
		return mine[0], true
	}
	if len(s.owners) < maxSessions {
		return "", true
	}
	if len(mine) > 0 {
		return mine[0], true
	}
	now := s.now()
	victim := ""
	var oldest time.Time
	for session, entry := range s.owners {
		atCap := len(s.byLease[entry.lease]) >= maxSessionsPerLease
		if !atCap && now.Sub(entry.used) < sessionIdle {
			continue
		}
		if victim == "" || entry.used.Before(oldest) {
			victim, oldest = session, entry.used
		}
	}
	return victim, victim != ""
}

// bind records that leaseID opened session. It returns the sessions it
// forgot to make room, and false (binding nothing) when there is none.
func (s *sessionOwners) bind(session, leaseID string) ([]string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if entry, known := s.owners[session]; known {
		if entry.lease == leaseID {
			entry.used = s.now()
			return nil, true
		}
		s.forgetLocked(session)
	}
	victim, ok := s.victimLocked(leaseID)
	if !ok {
		return nil, false
	}
	var forgotten []string
	if victim != "" {
		s.forgetLocked(victim)
		forgotten = append(forgotten, victim)
	}
	s.owners[session] = &sessionEntry{lease: leaseID, used: s.now()}
	s.byLease[leaseID] = append(s.byLease[leaseID], session)
	return forgotten, true
}

func (s *sessionOwners) forget(session string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.forgetLocked(session)
}

func (s *sessionOwners) forgetLocked(session string) {
	entry, known := s.owners[session]
	if !known {
		return
	}
	delete(s.owners, session)
	if rest := without(s.byLease[entry.lease], session); len(rest) > 0 {
		s.byLease[entry.lease] = rest
	} else {
		delete(s.byLease, entry.lease)
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

// mayUse: only the lease that opened a session; a use keeps it recent.
func (s *sessionOwners) mayUse(session, leaseID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, known := s.owners[session]
	if !known || entry.lease != leaseID {
		return false
	}
	entry.used = s.now()
	return true
}
