package main

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"
)

type driver struct {
	dialWS     func(context.Context, string, http.Header) (*websocket, error)
	cfg        *config
	clock      clock
	x          *exchange
	client     *http.Client
	log        func(string, map[string]any)
	ctx        context.Context
	cancel     context.CancelFunc
	mu         sync.Mutex
	sessions   map[string]*session
	stopping   bool
	admissions sync.WaitGroup
	background sync.WaitGroup
}

func newDriver(cfg *config, c clock, log func(string, map[string]any)) *driver {
	ctx, cancel := context.WithCancel(context.Background())
	d := &driver{cfg: cfg, clock: c, client: httpClient(), log: log, ctx: ctx, cancel: cancel, sessions: map[string]*session{}}
	d.dialWS = dialWebsocket
	d.x = newExchange(cfg, c, log)
	return d
}
func (d *driver) start() {
	d.background.Add(2)
	go func() { defer d.background.Done(); d.x.refreshLoop(d.ctx) }()
	go func() {
		defer d.background.Done()
		for {
			d.adopt()
			d.forget()
			if !pause(d.ctx, d.clock, 15*time.Second) {
				return
			}
		}
	}()
}
func (d *driver) forget() {
	d.mu.Lock()
	defer d.mu.Unlock()
	now := d.clock.Now()
	for id, s := range d.sessions {
		s.mu.Lock()
		at := s.view.EndedAt
		if at == nil {
			at = s.retiredAt
		}
		s.mu.Unlock()
		if at != nil && now.Sub(*at) >= 15*time.Minute {
			select {
			case <-s.done:
				delete(d.sessions, id)
			default:
			}
		}
	}
}
func (d *driver) shutdown(ctx context.Context) {
	d.mu.Lock()
	d.stopping = true
	d.mu.Unlock()
	d.cancel()
	done := make(chan struct{})
	go func() {
		d.background.Wait()
		d.admissions.Wait()
		d.mu.Lock()
		ss := make([]*session, 0, len(d.sessions))
		for _, s := range d.sessions {
			ss = append(ss, s)
		}
		d.mu.Unlock()
		for _, s := range ss {
			<-s.done
		}
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		d.mu.Lock()
		for _, s := range d.sessions {
			s.mu.Lock()
			if s.journal != nil {
				s.journal.cancel()
			}
			s.mu.Unlock()
		}
		d.mu.Unlock()
	}
}
func (d *driver) admit(h handoff) (*session, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if s := d.sessions[h.SessionID]; s != nil {
		return s, false
	}
	if d.stopping || d.x.isRevoked() {
		return nil, false
	}
	s := newSession(d, h)
	d.sessions[h.SessionID] = s
	d.admissions.Add(1)
	return s, true
}
func (d *driver) remove(s *session) {
	d.mu.Lock()
	if d.sessions[s.h.SessionID] == s {
		delete(d.sessions, s.h.SessionID)
	}
	d.mu.Unlock()
}
func (d *driver) accept(s *session) {
	defer d.admissions.Done()
	defer close(s.initialized)
	// Admission, including exchange calls, shares one 12-second budget.
	ctx, cancel := context.WithTimeout(d.ctx, 12*time.Second)
	defer cancel()
	owned, until, err := d.x.claim(ctx, s.h.SessionID, false)
	if err != nil || !owned {
		s.initHTTP = 409
		s.initCode = "not_owner"
		if err != nil {
			s.initHTTP = 503
			s.initCode = "exchange_unavailable"
		}
		d.remove(s)
		s.cancel()
		s.leaseCancel()
		close(s.done)
		return
	}
	s.mu.Lock()
	s.owned = true
	s.leaseUntil = until
	s.journal = newJournal(d.x, s.h.SessionID)
	s.mu.Unlock()
	c, err := d.x.getCredential(ctx)
	if err != nil {
		s.initHTTP = 503
		s.initCode = "credential_unavailable"
		s.cancel()
		go s.run()
		return
	}
	body := map[string]any{"session": map[string]any{"type": "live", "model": s.h.Model, "instructions": s.h.Accept.Instructions, "audio": map[string]any{"output": map[string]any{"voice": s.h.Accept.Voice}}, "delegation": map[string]any{"type": "client"}}}
	failure := enginePost(ctx, d.client, c, s.h.ProviderID, "accept", body)
	now := d.clock.Now()
	s.mu.Lock()
	if failure != nil {
		s.failure = failure
		s.view.Status = "failed"
		s.view.EndedAt = &now
		s.journal.state(map[string]any{"status": "failed", "error": fmt.Sprintf("accept failed: HTTP %d %s", failure.Status, failure.Code)})
		d.log("accept_failed", map[string]any{"session_id": s.h.SessionID, "status": failure.Status})
		s.cancel()
	} else {
		s.view.Status = "active"
		s.view.AcceptedAt = &now
		s.journal.state(map[string]any{"status": "active", "accepted_at": now})
		d.log("accepted", map[string]any{"session_id": s.h.SessionID, "status": "active"})
	}
	s.mu.Unlock()
	go s.run()
}
func (d *driver) adopt() {
	var answer struct {
		Sessions []struct {
			SessionID  string     `json:"session_id"`
			ProviderID string     `json:"provider_session_id"`
			JobID      string     `json:"job_id"`
			AcceptedAt time.Time  `json:"accepted_at"`
			AttachedAt *time.Time `json:"attached_at"`
			Handoff    handoff    `json:"handoff"`
			SentIDs    []string   `json:"sent_event_ids"`
		} `json:"sessions"`
	}
	if d.ctx.Err() != nil || d.x.isRevoked() {
		return
	}
	status, err := d.x.call(d.ctx, "/v1/live/sessions/adopt", map[string]any{}, &answer)
	if err != nil || status != 200 {
		return
	}
	for _, a := range answer.Sessions {
		h := a.Handoff
		if h.SessionID != "" && h.SessionID != a.SessionID || h.ProviderID != "" && h.ProviderID != a.ProviderID || h.JobID != "" && h.JobID != a.JobID {
			continue
		}
		h.SessionID = a.SessionID
		h.ProviderID = a.ProviderID
		h.JobID = a.JobID
		if !h.normalize(d.cfg) || a.AcceptedAt.IsZero() {
			continue
		}
		d.dropUnowned(h.SessionID)
		s, fresh := d.admit(h)
		if !fresh {
			continue
		}
		func() {
			defer d.admissions.Done()
			defer close(s.initialized)
			owned, until, err := d.x.claim(d.ctx, h.SessionID, false)
			if err != nil || !owned {
				d.remove(s)
				s.cancel()
				s.leaseCancel()
				close(s.done)
				return
			}
			s.mu.Lock()
			s.owned = true
			s.leaseUntil = until
			s.view.Status = "active"
			s.view.AcceptedAt = &a.AcceptedAt
			s.view.AttachedAt = a.AttachedAt
			for _, id := range a.SentIDs {
				s.sent[id] = true
			}
			s.journal = newJournal(d.x, h.SessionID)
			s.mu.Unlock()
			go s.run()
			d.log("adopted", map[string]any{"session_id": h.SessionID})
		}()
	}
}

// Only adoption replaces a completed recorder that lost ownership. A handoff
// replay must never accept an already accepted provider session again.
func (d *driver) dropUnowned(id string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	s := d.sessions[id]
	if s == nil {
		return
	}
	select {
	case <-s.done:
		s.mu.Lock()
		owned := s.owned
		s.mu.Unlock()
		if !owned {
			delete(d.sessions, id)
		}
	default:
	}
}
