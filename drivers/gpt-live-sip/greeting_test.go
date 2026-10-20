package main

import (
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"
)

// The shared fixture greets at once (zero timing). These tests run the
// production timing and observe the greeter through its log events.
type greetingLog struct {
	mu     sync.Mutex
	events []map[string]any
}

func newGreetingFixture(t *testing.T) (*fixture, *greetingLog) {
	t.Helper()
	f := newFixture(t)
	f.d.cfg.greetingDelay = defaultGreetingDelay
	f.d.cfg.greetingRetry = defaultGreetingRetry
	f.d.cfg.greetingStartWait = defaultGreetingStartWait
	l := &greetingLog{}
	f.d.log = func(name string, fields map[string]any) {
		if !strings.HasPrefix(name, "greeting") {
			return
		}
		l.mu.Lock()
		defer l.mu.Unlock()
		e := map[string]any{"event": name}
		for k, v := range fields {
			e[k] = v
		}
		l.events = append(l.events, e)
	}
	return f, l
}
func (l *greetingLog) all(name, session string) []map[string]any {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := []map[string]any{}
	for _, e := range l.events {
		if e["event"] == name && e["session_id"] == session {
			out = append(out, e)
		}
	}
	return out
}
func (l *greetingLog) wait(t *testing.T, name, session string) map[string]any {
	t.Helper()
	eventually(t, func() bool { return len(l.all(name, session)) > 0 })
	return l.all(name, session)[0]
}

// begin admits a session and waits for its first sideband, without the
// greeting acknowledgement fixture.handoff waits for.
func (f *fixture) begin(id string) *session {
	f.t.Helper()
	status, out := f.request("POST", "/v1/sessions", f.d.cfg.controlToken, testHandoff(id))
	if status != 200 {
		f.t.Fatalf("handoff %d %v", status, out)
	}
	f.d.mu.Lock()
	s := f.d.sessions[id]
	f.d.mu.Unlock()
	f.waitAttached(s, 1)
	return s
}
func (f *fixture) waitAttached(s *session, sockets int) {
	f.t.Helper()
	eventually(f.t, func() bool {
		f.mu.Lock()
		n := len(f.sockets)
		f.mu.Unlock()
		s.mu.Lock()
		defer s.mu.Unlock()
		return n >= sockets && s.ws != nil
	})
}
func (f *fixture) greetingCommands(id string) []map[string]any {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := []map[string]any{}
	for _, c := range f.commands {
		if c["event_id"] == id {
			out = append(out, c)
		}
	}
	return out
}
func (f *fixture) journalRows(id string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, r := range f.records {
		if strings.HasSuffix(r.path, "/events") {
			for _, v := range r.body["events"].([]any) {
				if object(v)["event_id"] == id {
					n++
				}
			}
		}
	}
	return n
}

// end closes the call and waits for the session, so its greeter has exited.
func endCall(t *testing.T, f *fixture, s *session) {
	t.Helper()
	f.emit(map[string]any{"type": "session.closed", "event_id": "closed", "reason": "remote_hangup"})
	eventually(t, func() bool {
		select {
		case <-s.done:
			return true
		default:
			return false
		}
	})
}

var startedEvent = map[string]any{"type": "session.started", "event_id": "started"}

func TestGreetingDefaults(t *testing.T) {
	cfg, err := parseConfig(validRequest(), "sdi_"+strings.Repeat("A", 49))
	if err != nil || cfg.greetingDelay != 1500*time.Millisecond || cfg.greetingRetry != 2500*time.Millisecond || cfg.greetingStartWait != 4*time.Second {
		t.Fatal(cfg, err)
	}
	r := validRequest()
	delay, retry := 0, 0
	r.Connector.Config.GreetingDelay, r.Connector.Config.GreetingRetry = &delay, &retry
	if cfg, err = parseConfig(r, "sdi_"+strings.Repeat("A", 49)); err != nil || cfg.greetingDelay != 0 || cfg.greetingRetry != 0 {
		t.Fatal(cfg, err)
	}
	for _, bad := range [][2]int{{-1, 2500}, {10001, 2500}, {1500, -1}, {1500, 30001}} {
		r := validRequest()
		delay, retry := bad[0], bad[1]
		r.Connector.Config.GreetingDelay, r.Connector.Config.GreetingRetry = &delay, &retry
		if _, err := parseConfig(r, "sdi_"+strings.Repeat("A", 49)); err == nil {
			t.Fatal("accepted greeting timing", bad)
		}
	}
}

func TestGreetingWaitsForSessionStarted(t *testing.T) {
	f, l := newGreetingFixture(t)
	s := f.begin("s")
	// No session.started: only the start wait would release the greeting.
	waitTimer(t, f.clock, defaultGreetingStartWait, "greet")
	f.clock.advance(defaultGreetingDelay)
	if len(l.all("greeting_sent", "s")) != 0 {
		t.Fatal("greeted before session.started")
	}
	f.emit(startedEvent)
	e := l.wait(t, "greeting_sent", "s")
	if e["after_accept_ms"] != int64(1500) || e["started"] != true {
		t.Fatal(e)
	}
	eventually(t, func() bool { return len(f.greetingCommands("srw_greeting_s")) == 1 })
	c := f.greetingCommands("srw_greeting_s")[0]
	if c["type"] != "session.instructions.append" || c["content"] != "Greet now" || c["delegation_id"] != nil {
		t.Fatal(c)
	}
	endCall(t, f, s)
	if len(f.greetingCommands("srw_greeting_s")) != 1 || f.journalRows("srw_greeting_s") != 1 || len(f.greetingCommands("srw_greeting_retry_s")) != 0 {
		t.Fatal("greeting not sent exactly once")
	}
}

func TestGreetingWaitsForSettleDelayAfterStart(t *testing.T) {
	f, l := newGreetingFixture(t)
	s := f.begin("s")
	f.emit(startedEvent) // live, right after accept
	waitTimer(t, f.clock, defaultGreetingDelay, "greet")
	if len(l.all("greeting_sent", "s")) != 0 || len(f.greetingCommands("srw_greeting_s")) != 0 {
		t.Fatal("greeted before the settle delay")
	}
	f.clock.advance(defaultGreetingDelay)
	if e := l.wait(t, "greeting_sent", "s"); e["after_accept_ms"] != int64(1500) || e["started"] != true {
		t.Fatal(e)
	}
	endCall(t, f, s)
	if len(f.greetingCommands("srw_greeting_s")) != 1 {
		t.Fatal("greeting not sent exactly once")
	}
}

func TestGreetingAfterReplayedStartOrStartWait(t *testing.T) {
	t.Run("replayed", func(t *testing.T) {
		f, l := newGreetingFixture(t)
		f.replay = []map[string]any{startedEvent}
		f.begin("s")
		advanceFrom(t, f.clock, defaultGreetingDelay, "greet")
		if e := l.wait(t, "greeting_sent", "s"); e["after_accept_ms"] != int64(1500) || e["started"] != true {
			t.Fatal(e)
		}
	})
	t.Run("missed", func(t *testing.T) {
		f, l := newGreetingFixture(t)
		f.begin("s")
		advanceFrom(t, f.clock, defaultGreetingStartWait, "greet")
		if e := l.wait(t, "greeting_sent", "s"); e["after_accept_ms"] != int64(4000) || e["started"] != false {
			t.Fatal(e)
		}
	})
}

func TestGreetingRetriesOnceWhenTheVoiceStaysSilent(t *testing.T) {
	f, l := newGreetingFixture(t)
	f.replay = []map[string]any{startedEvent}
	s := f.begin("s")
	advanceFrom(t, f.clock, defaultGreetingDelay, "greet")
	l.wait(t, "greeting_sent", "s")
	// The caller's speech is no answer from the voice.
	f.emit(map[string]any{"type": "session.input_transcript.delta", "event_id": "caller", "delta": "Hallo?"})
	waitTimer(t, f.clock, defaultGreetingRetry, "greet")
	if len(f.greetingCommands("srw_greeting_retry_s")) != 0 {
		t.Fatal("re-prompted early")
	}
	f.clock.advance(defaultGreetingRetry)
	if e := l.wait(t, "greeting_retry_sent", "s"); e["after_accept_ms"] != int64(4000) {
		t.Fatal(e)
	}
	eventually(t, func() bool { return len(f.greetingCommands("srw_greeting_retry_s")) == 1 })
	first, retry := f.greetingCommands("srw_greeting_s")[0], f.greetingCommands("srw_greeting_retry_s")[0]
	if retry["type"] != first["type"] || retry["content"] != first["content"] {
		t.Fatal(first, retry)
	}
	f.clock.advance(2 * defaultGreetingRetry)
	endCall(t, f, s)
	if len(f.greetingCommands("srw_greeting_s")) != 1 || len(f.greetingCommands("srw_greeting_retry_s")) != 1 || f.journalRows("srw_greeting_retry_s") != 1 || len(l.all("greeting_retry_sent", "s")) != 1 {
		t.Fatal("more than one greeting or re-prompt")
	}
}

func TestGreetingNoRetryAfterTheVoiceSpoke(t *testing.T) {
	f, l := newGreetingFixture(t)
	f.replay = []map[string]any{startedEvent}
	s := f.begin("s")
	advanceFrom(t, f.clock, defaultGreetingDelay, "greet")
	l.wait(t, "greeting_sent", "s")
	f.emit(map[string]any{"type": "session.output_transcript.delta", "event_id": "voice", "delta": "Guten Tag"})
	eventually(t, func() bool { s.mu.Lock(); defer s.mu.Unlock(); return s.assistantSpeech == 1 })
	advanceFrom(t, f.clock, defaultGreetingRetry, "greet")
	if e := l.wait(t, "greeting_retry_skipped", "s"); e["reason"] != "assistant_spoke" {
		t.Fatal(e)
	}
	endCall(t, f, s)
	if len(f.greetingCommands("srw_greeting_retry_s")) != 0 || len(f.greetingCommands("srw_greeting_s")) != 1 {
		t.Fatal("re-prompted a speaking voice")
	}
}

func TestGreetingNotRepeatedAfterReconnect(t *testing.T) {
	f, l := newGreetingFixture(t)
	f.replay = []map[string]any{startedEvent}
	s := f.begin("s")
	advanceFrom(t, f.clock, defaultGreetingDelay, "greet")
	l.wait(t, "greeting_sent", "s")
	f.mu.Lock()
	first := f.sockets[0]
	f.mu.Unlock()
	first.conn.Close()
	eventually(t, func() bool { s.mu.Lock(); defer s.mu.Unlock(); return s.ws == nil })
	advanceFrom(t, f.clock, 250*time.Millisecond, "readLoop")
	f.waitAttached(s, 2)
	// The replayed session.started is deduplicated; speech in the gap may be
	// unseen, so the new sideband cancels the re-prompt.
	if e := l.wait(t, "greeting_retry_skipped", "s"); e["reason"] != "sideband_changed" {
		t.Fatal(e)
	}
	f.clock.advance(defaultGreetingStartWait + defaultGreetingRetry)
	endCall(t, f, s)
	if len(f.greetingCommands("srw_greeting_s")) != 1 || len(f.greetingCommands("srw_greeting_retry_s")) != 0 || f.journalRows("srw_greeting_s") != 1 {
		t.Fatal("greeting repeated after reconnect")
	}
}

func TestGreetingAfterAdoption(t *testing.T) {
	f, l := newGreetingFixture(t)
	f.replay = []map[string]any{startedEvent}
	adopted := func(id string, sent ...string) map[string]any {
		b, _ := json.Marshal(testHandoff(id))
		var h map[string]any
		_ = json.Unmarshal(b, &h)
		return map[string]any{"session_id": id, "provider_session_id": "rtc_" + id, "job_id": "job", "accepted_at": f.clock.Now().Add(-time.Minute), "attached_at": nil, "handoff": h, "sent_event_ids": sent}
	}
	f.mu.Lock()
	f.adoptSessions = []map[string]any{adopted("greeted", "srw_greeting_greeted"), adopted("silent")}
	f.mu.Unlock()
	f.d.adopt()
	f.d.mu.Lock()
	greeted, silent := f.d.sessions["greeted"], f.d.sessions["silent"]
	f.d.mu.Unlock()
	// A greeting sent before the pod restart is never repeated or re-prompted.
	if e := l.wait(t, "greeting_skipped", "greeted"); e["reason"] != "already_sent" {
		t.Fatal(e)
	}
	// One that was never sent goes out at once: the call started long ago.
	l.wait(t, "greeting_sent", "silent")
	f.d.adopt()
	f.clock.advance(defaultGreetingStartWait + defaultGreetingRetry)
	endCall(t, f, greeted)
	eventually(t, func() bool {
		select {
		case <-silent.done:
			return true
		default:
			return false
		}
	})
	if len(f.greetingCommands("srw_greeting_greeted")) != 0 || len(f.greetingCommands("srw_greeting_retry_greeted")) != 0 || len(f.greetingCommands("srw_greeting_silent")) != 1 {
		t.Fatal("adoption repeated or lost the greeting")
	}
	if len(l.all("greeting_sent", "silent")) != 1 || len(l.all("greeting_sent", "greeted")) != 0 {
		t.Fatal(l.events)
	}
}
