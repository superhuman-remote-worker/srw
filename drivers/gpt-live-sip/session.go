package main

import (
	"context"
	"encoding/json"
	"net/url"
	"strings"
	"sync"
	"time"
)

type handoff struct {
	SessionID  string `json:"session_id"`
	ProviderID string `json:"provider_session_id"`
	JobID      string `json:"job_id"`
	Model      string `json:"model"`
	Accept     struct {
		Instructions string `json:"instructions"`
		Voice        string `json:"voice"`
	} `json:"accept"`
	Greeting string `json:"greeting_instruction"`
	Texts    struct {
		Fallback string `json:"fallback"`
	} `json:"texts"`
	Limits struct {
		MaxDuration *int `json:"max_duration_s"`
		Fallback    *int `json:"no_job_fallback_s"`
	} `json:"limits"`
	TransferTarget string `json:"transfer_target"`
}

func (h *handoff) normalize(cfg *config) bool {
	if strings.TrimSpace(h.SessionID) == "" || strings.TrimSpace(h.ProviderID) == "" || strings.TrimSpace(h.JobID) == "" || strings.TrimSpace(h.Model) == "" || strings.TrimSpace(h.Accept.Instructions) == "" {
		return false
	}
	if h.Limits.MaxDuration == nil {
		n := cfg.limits.MaxDuration
		h.Limits.MaxDuration = &n
	}
	if h.Limits.Fallback == nil {
		n := cfg.limits.Fallback
		h.Limits.Fallback = &n
	}
	if !(limits{*h.Limits.MaxDuration, *h.Limits.Fallback}).valid() {
		return false
	}
	if h.TransferTarget == "" {
		h.TransferTarget = cfg.transferTarget
	}
	if h.Accept.Voice == "" {
		h.Accept.Voice = "marin"
	}
	return true
}

type statusView struct {
	SessionID    string     `json:"session_id"`
	ProviderID   string     `json:"provider_session_id"`
	JobID        string     `json:"job_id"`
	Status       string     `json:"status"`
	AcceptedAt   *time.Time `json:"accepted_at"`
	AttachedAt   *time.Time `json:"attached_at"`
	EndedAt      *time.Time `json:"ended_at"`
	CloseReason  *string    `json:"close_reason"`
	UsageSeconds *float64   `json:"usage_seconds"`
}
type bufferedEvent struct {
	at    time.Time
	event event
}
type pendingAppend struct {
	result chan event
	ws     *websocket
}
type session struct {
	d               *driver
	h               handoff
	mu              sync.Mutex
	actionMu        sync.Mutex
	view            statusView
	failure         *engineError
	initHTTP        int
	initCode        string
	initialized     chan struct{}
	done            chan struct{}
	ctx             context.Context
	leaseCtx        context.Context
	leaseCancel     context.CancelFunc
	cancel          context.CancelFunc
	owned           bool
	leaseUntil      time.Time
	ws              *websocket
	journal         *journal
	seen            dedupe
	sent            map[string]bool
	delegations     map[string]time.Time
	delegationOrder []string
	retiredAt       *time.Time
	pending         map[string]pendingAppend
	replay          []bufferedEvent
	closed          event
	subscribers     map[chan event]bool
	// The greeter's inputs: whether the engine's timeline runs (session.started
	// or any transcript seen), how many assistant transcript deltas arrived,
	// and a wake-up for a new sideband or the start.
	started         bool
	assistantSpeech int
	greetWake       chan struct{}
}

func newSession(d *driver, h handoff) *session {
	ctx, cancel := context.WithCancel(d.ctx)
	leaseCtx, leaseCancel := context.WithCancel(d.ctx)
	return &session{d: d, h: h, ctx: ctx, cancel: cancel, leaseCtx: leaseCtx, leaseCancel: leaseCancel, initialized: make(chan struct{}), done: make(chan struct{}), view: statusView{SessionID: h.SessionID, ProviderID: h.ProviderID, JobID: h.JobID, Status: "initializing"}, sent: map[string]bool{}, delegations: map[string]time.Time{}, pending: map[string]pendingAppend{}, subscribers: map[chan event]bool{}, greetWake: make(chan struct{}, 1)}
}
func (s *session) wakeGreeter() {
	select {
	case s.greetWake <- struct{}{}:
	default:
	}
}
func (s *session) snapshot() statusView { s.mu.Lock(); defer s.mu.Unlock(); return s.view }
func (s *session) active() bool {
	return s.owned && s.view.Status == "active" && s.ctx.Err() == nil && s.d.clock.Now().Before(s.leaseUntil)
}
func (s *session) setWS(w *websocket) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.active() {
		return false
	}
	s.ws = w
	s.wakeGreeter()
	return true
}
func (s *session) clearWS(w *websocket) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ws == w {
		s.ws = nil
	}
	for id, p := range s.pending {
		if p.ws == w {
			p.result <- event{"type": "error", "code": "not_active", "message": "sideband disconnected"}
			delete(s.pending, id)
		}
	}
}
func (s *session) stopStreams() {
	for ch := range s.subscribers {
		close(ch)
		delete(s.subscribers, ch)
	}
}
func (s *session) loseOwnership() {
	s.mu.Lock()
	s.owned = false
	if s.retiredAt == nil {
		now := s.d.clock.Now()
		s.retiredAt = &now
	}
	w := s.ws
	s.stopStreams()
	s.mu.Unlock()
	s.journal.cancel()
	s.cancel()
	if w != nil {
		w.close()
	}
}
func (s *session) run() {
	defer func() {
		if s.initHTTP != 0 {
			s.d.remove(s)
		}
		close(s.done)
	}()
	s.journal.guard = func() bool {
		s.mu.Lock()
		valid := s.owned && s.d.clock.Now().Before(s.leaseUntil)
		s.mu.Unlock()
		if !valid {
			s.loseOwnership()
		}
		return valid
	}
	go s.journal.run()
	renewDone := make(chan struct{})
	go func() { defer close(renewDone); s.renew() }()
	var wg sync.WaitGroup
	for _, fn := range []func(){s.readLoop, s.monitor, s.greet} {
		wg.Add(1)
		go func(fn func()) { defer wg.Done(); fn() }(fn)
	}
	<-s.ctx.Done()
	s.mu.Lock()
	w := s.ws
	s.mu.Unlock()
	if w != nil {
		w.close()
	}
	wg.Wait()
	s.mu.Lock()
	s.stopStreams()
	owned := s.owned
	v := s.view
	s.mu.Unlock()
	if owned {
		if v.Status == "ended" {
			s.journal.state(map[string]any{"status": "ended", "ended_at": v.EndedAt, "close_reason": v.CloseReason, "usage_seconds": v.UsageSeconds})
		}
		// A normal close retries indefinitely under its renewed lease. Shutdown
		// supplies a bounded drain interval, leaving the call available for adoption.
		flushed := s.journal.flush(s.d.ctx)
		if !flushed && s.d.ctx.Err() != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			flushed = s.journal.flush(ctx)
			cancel()
		}
		s.leaseCancel()
		<-renewDone
		s.mu.Lock()
		owned = s.owned
		s.mu.Unlock()
		if owned {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			if flushed {
				s.d.x.retry(ctx, sessionPath(s.h.SessionID)+"/lease", map[string]any{"release": true})
			} else {
				s.d.log("journal_drain_incomplete", map[string]any{"session_id": s.h.SessionID})
			}
			cancel()
		}
	} else {
		s.leaseCancel()
		<-renewDone
	}
	s.journal.cancel()
	<-s.journal.done
}
func (s *session) readLoop() {
	delay := 250 * time.Millisecond
	var outage *time.Time
	for s.ctx.Err() == nil {
		c, err := s.d.x.getCredential(s.ctx)
		var w *websocket
		if err == nil {
			w, err = s.d.dialWS(s.ctx, strings.TrimRight(c.WSBase, "/")+"/live/sessions/"+url.PathEscape(s.h.ProviderID)+"/attach", c.headers())
		}
		if err == nil {
			if !s.setWS(w) {
				w.close()
				return
			}
			outage = nil
			delay = 250 * time.Millisecond
			stop := context.AfterFunc(s.ctx, w.close)
			hbCtx, hbCancel := context.WithCancel(s.ctx)
			hbDone := make(chan struct{})
			go func() {
				defer close(hbDone)
				for pause(hbCtx, s.d.clock, 20*time.Second) {
					if w.write(9, nil) != nil {
						w.close()
						return
					}
				}
			}()
			// The greeting is the greeter's: it waits for the engine's start.
			for s.ctx.Err() == nil {
				b, readErr := w.readText()
				if readErr != nil {
					err = readErr
					break
				}
				var raw map[string]any
				if json.Unmarshal(b, &raw) != nil || raw == nil {
					continue
				}
				s.handleEvent(raw)
			}
			stop()
			hbCancel()
			w.close()
			<-hbDone
			s.clearWS(w)
		}
		if s.ctx.Err() != nil {
			return
		}
		if e, ok := err.(*wsHTTPError); ok && (e.status == 404 || e.status == 409) {
			now := s.d.clock.Now()
			if outage == nil {
				outage = &now
			}
			if now.Sub(*outage) >= 30*time.Second {
				s.recorderLost()
				return
			}
		} else {
			outage = nil
		}
		if !pause(s.ctx, s.d.clock, delay) {
			return
		}
		delay = nextBackoff(delay)
	}
}
func (s *session) markStarted() {
	if !s.started {
		s.started = true
		s.wakeGreeter()
	}
}

// greet sends the greeting append once the engine's audio timeline runs:
// after session.started (live or replayed; a transcript also counts) and the
// settle delay after accept, or after the start wait if session.started was
// missed. GPT-Live takes an earlier append as context only and stays silent
// until the caller speaks. If no assistant speech follows within the retry
// window, one re-prompt goes out on the same sideband. A greeting sent before
// (by this process, or listed in sent_event_ids at adoption) is never sent
// again, and a sideband change during the window cancels the re-prompt,
// because speech in the gap may have gone unseen (duplicate greeting, I6).
func (s *session) greet() {
	greetID, retryID := "srw_greeting_"+s.h.SessionID, "srw_greeting_retry_"+s.h.SessionID
	cfg := s.d.cfg
	var greetedOn, failedOn *websocket
	var greetedAt time.Time
	spoken := 0
	for {
		s.mu.Lock()
		if !s.active() || s.view.AcceptedAt == nil || strings.TrimSpace(s.h.Greeting) == "" {
			s.mu.Unlock()
			return
		}
		if greetedOn == nil && s.sent[greetID] {
			// Only adoption restores a sent greeting (sent_event_ids).
			s.mu.Unlock()
			s.d.log("greeting_skipped", map[string]any{"session_id": s.h.SessionID, "reason": "already_sent"})
			return
		}
		now, w, started, mark := s.d.clock.Now(), s.ws, s.started, s.assistantSpeech
		accepted := *s.view.AcceptedAt
		id, due := greetID, accepted.Add(cfg.greetingDelay)
		if greetedOn == nil && !started {
			if limit := accepted.Add(cfg.greetingStartWait); limit.After(due) {
				due = limit
			}
		}
		if greetedOn != nil {
			id, due = retryID, greetedAt.Add(cfg.greetingRetry)
			skip := ""
			if mark != spoken {
				skip = "assistant_spoke"
			} else if w != greetedOn {
				skip = "sideband_changed"
			}
			if skip != "" {
				s.mu.Unlock()
				s.d.log("greeting_retry_skipped", map[string]any{"session_id": s.h.SessionID, "reason": skip})
				return
			}
		}
		s.mu.Unlock()
		if now.Before(due) || w == nil || w == failedOn {
			var timer <-chan time.Time
			if now.Before(due) {
				timer = s.d.clock.After(due.Sub(now))
			}
			select {
			case <-s.ctx.Done():
				return
			case <-s.greetWake:
			case <-timer:
			}
			continue
		}
		if err := s.sendRecorder(w, "instructions", s.h.Greeting, "", id); err != nil {
			if err != errUnavailable {
				failedOn = w
				w.close()
			}
			continue
		}
		fields := map[string]any{"session_id": s.h.SessionID, "after_accept_ms": now.Sub(accepted).Milliseconds()}
		if id == retryID {
			s.d.log("greeting_retry_sent", fields)
			return
		}
		fields["started"] = started
		s.d.log("greeting_sent", fields)
		if cfg.greetingRetry <= 0 {
			return
		}
		greetedOn, greetedAt, spoken = w, now, mark
	}
}
func (s *session) sendRecorder(w *websocket, kind, content, delegation, id string) error {
	s.actionMu.Lock()
	defer s.actionMu.Unlock()
	s.mu.Lock()
	if !s.active() || s.ws != w {
		s.mu.Unlock()
		return errUnavailable
	}
	if s.sent[id] || strings.TrimSpace(content) == "" || kind == "commentary" && s.view.AttachedAt != nil {
		s.mu.Unlock()
		return nil
	}
	s.mu.Unlock()
	var did any
	if delegation != "" {
		did = delegation
	}
	b, _ := json.Marshal(map[string]any{"type": "session." + kind + ".append", "event_id": id, "delegation_id": did, "content": clipAppendContent(content, 1500)})
	if err := w.write(1, b); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.active() || s.ws != w {
		return errUnavailable
	}
	s.sent[id] = true
	rowKind := "instruction"
	if kind == "commentary" {
		rowKind = "fallback"
	}
	s.journal.row(map[string]any{"event_id": id, "kind": rowKind, "speaker": "recorder", "text": content, "delegation_id": did, "payload": map[string]any{}})
	return nil
}
func (s *session) handleEvent(raw map[string]any) {
	e := parseEvent(raw)
	if e == nil {
		return
	}
	if e["type"] == "closed" {
		s.actionMu.Lock()
		defer s.actionMu.Unlock()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.active() || !s.seen.first(str(e["event_id"])) {
		return
	}
	switch e["type"] {
	case "append_ack", "error":
		if id := str(e["client_event_id"]); id != "" {
			if p, ok := s.pending[id]; ok {
				p.result <- e
				delete(s.pending, id)
			}
		}
	case "delegation":
		id := str(e["delegation_id"])
		if _, ok := s.delegations[id]; !ok {
			s.delegations[id] = s.d.clock.Now()
			s.delegationOrder = append(s.delegationOrder, id)
		}
	case "transcript":
		// Speech of either side means the audio timeline runs.
		s.markStarted()
		if e["speaker"] == "assistant" && strings.TrimSpace(str(e["text"])) != "" {
			s.assistantSpeech++
		}
	case "other":
		if e["wire_type"] == "session.started" {
			s.markStarted()
		}
	case "usage":
		n, _ := e["seconds"].(float64)
		s.view.UsageSeconds = &n
	case "closed":
		if n, ok := e["usage_seconds"].(float64); ok {
			s.view.UsageSeconds = &n
		}
		reason := str(e["reason"])
		now := s.d.clock.Now()
		s.view.Status = "ended"
		s.view.EndedAt = &now
		s.view.CloseReason = &reason
		s.closed = e
	}
	s.journal.row(journalEvent(e, raw))
	s.broadcastLocked(e)
	if e["type"] == "closed" {
		s.stopStreams()
		s.cancel()
	}
}
func (s *session) broadcastLocked(e event) {
	now := s.d.clock.Now()
	cut := now.Add(-3 * time.Second)
	i := 0
	for i < len(s.replay) && s.replay[i].at.Before(cut) {
		i++
	}
	s.replay = append(s.replay[i:], bufferedEvent{now, e})
	for ch := range s.subscribers {
		select {
		case ch <- e:
		default:
			close(ch)
			delete(s.subscribers, ch)
		}
	}
}
func (s *session) subscribe() ([]event, chan event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cut := s.d.clock.Now().Add(-3 * time.Second)
	replay := []event{}
	hasClosed := false
	for _, b := range s.replay {
		if !b.at.Before(cut) {
			replay = append(replay, b.event)
			if b.event["type"] == "closed" {
				hasClosed = true
			}
		}
	}
	if s.closed != nil && !hasClosed {
		replay = append(replay, s.closed)
	}
	ch := make(chan event, 1024)
	if s.active() {
		s.subscribers[ch] = true
	} else {
		close(ch)
	}
	return replay, ch
}
func (s *session) unsubscribe(ch chan event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.subscribers[ch] {
		delete(s.subscribers, ch)
		close(ch)
	}
}
func (s *session) recorderLost() {
	raw := map[string]any{"type": "session.closed", "event_id": "srw_lost_" + s.h.SessionID}
	// recorder_lost is a driver reason, outside the engine's CloseReason enum.
	s.actionMu.Lock()
	s.mu.Lock()
	if !s.active() {
		s.mu.Unlock()
		s.actionMu.Unlock()
		return
	}
	e := event{"type": "closed", "event_id": raw["event_id"], "reason": "recorder_lost", "raw_reason": nil, "usage_seconds": s.view.UsageSeconds}
	now := s.d.clock.Now()
	reason := "recorder_lost"
	s.view.Status = "ended"
	s.view.EndedAt = &now
	s.view.CloseReason = &reason
	s.closed = e
	s.journal.row(map[string]any{"kind": "closed", "event_id": raw["event_id"], "payload": map[string]any{"reason": "recorder_lost"}})
	s.broadcastLocked(e)
	s.stopStreams()
	s.cancel()
	s.mu.Unlock()
	s.actionMu.Unlock()
}
func (s *session) monitor() {
	for pause(s.ctx, s.d.clock, 500*time.Millisecond) {
		s.mu.Lock()
		if !s.active() {
			s.mu.Unlock()
			return
		}
		now := s.d.clock.Now()
		deadline := s.view.AcceptedAt.Add(time.Duration(*s.h.Limits.MaxDuration) * time.Second)
		maxID := "srw_max_duration_" + s.h.SessionID
		hangup := !now.Before(deadline) && !s.sent[maxID]
		w := s.ws
		pending := []string{}
		if s.view.AttachedAt == nil && w != nil {
			for _, id := range s.delegationOrder {
				at := s.delegations[id]
				if now.Sub(at) >= time.Duration(*s.h.Limits.Fallback)*time.Second {
					pending = append(pending, id)
				}
			}
		}
		s.mu.Unlock()
		if hangup {
			s.actionMu.Lock()
			s.mu.Lock()
			active := s.active()
			s.mu.Unlock()
			if active {
				c, err := s.d.x.getCredential(s.ctx)
				if err == nil && enginePost(s.ctx, s.d.client, c, s.h.ProviderID, "hangup", nil) == nil {
					s.mu.Lock()
					s.sent[maxID] = true
					s.journal.row(map[string]any{"event_id": maxID, "kind": "hangup", "speaker": "recorder", "payload": map[string]any{"reason": "max_duration"}})
					s.mu.Unlock()
				}
			}
			s.actionMu.Unlock()
		}
		for _, id := range pending {
			if s.sendRecorder(w, "commentary", s.h.Texts.Fallback, id, "srw_fallback_"+s.h.SessionID+"_"+id) != nil {
				w.close()
				break
			}
		}
	}
}
func (s *session) renew() {
	for {
		s.mu.Lock()
		until := s.leaseUntil
		s.mu.Unlock()
		remaining := until.Sub(s.d.clock.Now())
		if remaining <= 0 {
			s.loseOwnership()
			return
		}
		delay := 10 * time.Second
		if remaining < delay {
			delay = remaining
		}
		if !pause(s.leaseCtx, s.d.clock, delay) {
			return
		}
		remaining = until.Sub(s.d.clock.Now())
		if remaining <= 0 {
			s.loseOwnership()
			return
		}
		budget := 5 * time.Second
		if remaining < budget {
			budget = remaining
		}
		ctx, cancel := context.WithTimeout(s.leaseCtx, budget)
		owned, newUntil, err := s.d.x.claim(ctx, s.h.SessionID, false)
		cancel()
		if s.leaseCtx.Err() != nil {
			return
		}
		if err == nil && (!owned || !s.d.clock.Now().Before(newUntil)) || err != nil && !s.d.clock.Now().Before(until) {
			s.loseOwnership()
			return
		}
		if err == nil {
			s.mu.Lock()
			s.leaseUntil = newUntil
			s.mu.Unlock()
		}
	}
}
func (s *session) append(ctx context.Context, kind, content string, delegation *string, id string) (int, map[string]any) {
	notActive := func() (int, map[string]any) {
		return 409, map[string]any{"ok": false, "error": map[string]any{"code": "not_active"}}
	}
	s.mu.Lock()
	w := s.ws
	if !s.active() || w == nil {
		s.mu.Unlock()
		return notActive()
	}
	if _, ok := s.pending[id]; ok {
		s.mu.Unlock()
		return 400, map[string]any{"error": map[string]any{"code": "duplicate_event_id"}}
	}
	p := pendingAppend{make(chan event, 1), w}
	s.pending[id] = p
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		if current, ok := s.pending[id]; ok && current.result == p.result {
			delete(s.pending, id)
		}
		s.mu.Unlock()
	}()
	b, _ := json.Marshal(map[string]any{"type": "session." + kind + ".append", "event_id": id, "delegation_id": delegation, "content": content})
	if w.write(1, b) != nil {
		w.close()
		return notActive()
	}
	select {
	case e := <-p.result:
		if e["type"] == "append_ack" {
			return 200, map[string]any{"ok": true}
		}
		if e["code"] == "not_active" {
			return notActive()
		}
		return 200, map[string]any{"ok": false, "error": map[string]any{"code": e["code"], "message": e["message"]}}
	case <-s.ctx.Done():
		return notActive()
	case <-ctx.Done():
		return 200, map[string]any{"ok": false, "error": map[string]any{"code": "timeout", "message": "append acknowledgement timed out"}}
	case <-s.d.clock.After(5 * time.Second):
		return 200, map[string]any{"ok": false, "error": map[string]any{"code": "timeout", "message": "append acknowledgement timed out"}}
	}
}
