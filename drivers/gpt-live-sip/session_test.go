package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestGreetingReconnectDedupeAndAudioDrop(t *testing.T) {
	f := newFixture(t)
	s := f.handoff("s")
	eventually(t, func() bool { return f.countCommands("srw_greeting_s") == 1 })
	flushSession(t, s)
	_, ch := s.subscribe()
	defer s.unsubscribe(ch)
	raw := map[string]any{"type": "session.input_transcript.delta", "event_id": "duplicate", "delta": "hello"}
	f.emit(raw)
	f.emit(raw)
	f.emit(map[string]any{"type": "session.input_audio.append", "event_id": "audio", "audio": "ignored"})
	eventually(t, func() bool { return len(ch) == 1 })
	f.mu.Lock()
	first := f.sockets[0]
	f.mu.Unlock()
	first.conn.Close()
	eventually(t, func() bool { s.mu.Lock(); defer s.mu.Unlock(); return s.ws == nil })
	advanceFrom(t, f.clock, 250*time.Millisecond, "readLoop")
	eventually(t, func() bool { f.mu.Lock(); defer f.mu.Unlock(); return f.attaches == 2 })
	eventually(t, func() bool { s.mu.Lock(); defer s.mu.Unlock(); return s.ws != nil })
	f.emit(raw)
	flushSession(t, s)
	if f.countCommands("srw_greeting_s") != 1 {
		t.Fatal("greeting resent")
	}
	if len(ch) != 1 {
		t.Fatal("replay not deduped")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	greetings, transcripts := 0, 0
	for _, r := range f.records {
		if strings.HasSuffix(r.path, "/events") {
			for _, v := range r.body["events"].([]any) {
				row := object(v)
				if row["event_id"] == "srw_greeting_s" {
					greetings++
				}
				if row["event_id"] == "duplicate" {
					transcripts++
				}
				if row["event_id"] == "audio" {
					t.Fatal("audio journaled")
				}
			}
		}
	}
	if greetings != 1 || transcripts != 1 {
		t.Fatal(greetings, transcripts)
	}
}
func TestFallbackTimerAndStopOnAttach(t *testing.T) {
	f := newFixture(t)
	h := testHandoff("s")
	n := 1
	h.Limits.Fallback = &n
	status, _ := f.request("POST", "/v1/sessions", f.d.cfg.controlToken, h)
	if status != 200 {
		t.Fatal(status)
	}
	f.d.mu.Lock()
	s := f.d.sessions["s"]
	f.d.mu.Unlock()
	eventually(t, func() bool { return f.countCommands("srw_greeting_s") == 1 })
	f.emit(map[string]any{"type": "session.delegation.created", "event_id": "d1", "delegation": map[string]any{"id": "delegation1"}})
	eventually(t, func() bool { s.mu.Lock(); defer s.mu.Unlock(); return len(s.delegations) == 1 })
	advanceFrom(t, f.clock, 500*time.Millisecond, "monitor")
	if f.countCommands("srw_fallback_s_delegation1") != 0 {
		t.Fatal("fallback too early")
	}
	advanceFrom(t, f.clock, 500*time.Millisecond, "monitor")
	eventually(t, func() bool { return f.countCommands("srw_fallback_s_delegation1") == 1 })
	status, view := f.request("POST", "/v1/sessions/s/attach", testToken, map[string]any{})
	if status != 200 || view["attached_at"] == nil {
		t.Fatal(status, view)
	}
	attached := view["attached_at"]
	f.emit(map[string]any{"type": "session.delegation.created", "event_id": "d2", "delegation": map[string]any{"id": "delegation2"}})
	eventually(t, func() bool { s.mu.Lock(); defer s.mu.Unlock(); return len(s.delegations) == 2 })
	advanceFrom(t, f.clock, 500*time.Millisecond, "monitor")
	advanceFrom(t, f.clock, 500*time.Millisecond, "monitor")
	status, view = f.request("POST", "/v1/sessions/s/attach", testToken, map[string]any{})
	if status != 200 || view["attached_at"] != attached {
		t.Fatal("attach not idempotent")
	}
	flushSession(t, s)
	if f.countCommands("srw_fallback_s_delegation1") != 1 || f.countCommands("srw_fallback_s_delegation2") != 0 {
		t.Fatal("fallback continued after attach")
	}
}
func TestMaxDurationRetryFlushAndUsage(t *testing.T) {
	f := newFixture(t)
	s := f.handoff("s")
	f.mu.Lock()
	f.hangupFailures = 1
	f.closeOnHangup = true
	f.mu.Unlock()
	f.emit(map[string]any{"type": "session.usage.updated", "event_id": "usage", "usage": map[string]any{"seconds": 21.5}})
	eventually(t, func() bool { return s.snapshot().UsageSeconds != nil })
	for i := 0; i < 89; i++ {
		advanceFrom(t, f.clock, 10*time.Second, "renew")
		eventually(t, func() bool {
			s.mu.Lock()
			defer s.mu.Unlock()
			return s.leaseUntil.After(f.clock.Now().Add(20 * time.Second))
		})
	}
	f.clock.advance(9 * time.Second)
	eventually(t, func() bool { return f.clock.waiting(500 * time.Millisecond) })
	f.clock.advance(time.Second)
	eventually(t, func() bool { return f.pathCount("/v1/live/sessions/rtc_s/hangup") == 1 })
	advanceFrom(t, f.clock, 500*time.Millisecond, "monitor")
	eventually(t, func() bool {
		select {
		case <-s.done:
			return true
		default:
			return false
		}
	})
	v := s.snapshot()
	if v.Status != "ended" || v.UsageSeconds == nil || *v.UsageSeconds != 42 || *v.CloseReason != "close_requested" {
		t.Fatal(v)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	hangup, closed, ended := -1, -1, -1
	for i, r := range f.records {
		if strings.HasSuffix(r.path, "/events") {
			for _, v := range r.body["events"].([]any) {
				row := object(v)
				switch row["kind"] {
				case "hangup":
					hangup = i
				case "closed":
					closed = i
				case "usage":
					t.Fatal("usage journaled")
				}
			}
		}
		if strings.HasSuffix(r.path, "/state") && r.body["status"] == "ended" {
			ended = i
			if r.body["usage_seconds"] != float64(42) {
				t.Fatal(r)
			}
		}
	}
	if hangup < 0 || closed < hangup || ended <= closed {
		t.Fatal("journal/state order", hangup, closed, ended)
	}
}
func TestReconnectBackoffAndRecorderLost(t *testing.T) {
	for _, status := range []int{404, 409} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			f := newFixture(t)
			f.mu.Lock()
			f.attachStatus = status
			f.mu.Unlock()
			code, _ := f.request("POST", "/v1/sessions", f.d.cfg.controlToken, testHandoff("lost"))
			if code != 200 {
				t.Fatal(code)
			}
			f.d.mu.Lock()
			s := f.d.sessions["lost"]
			f.d.mu.Unlock()
			eventually(t, func() bool { f.mu.Lock(); defer f.mu.Unlock(); return f.attaches == 1 })
			delays := []time.Duration{250 * time.Millisecond, 500 * time.Millisecond, time.Second, 2 * time.Second, 4 * time.Second, 5 * time.Second, 5 * time.Second, 5 * time.Second, 5 * time.Second, 5 * time.Second}
			for i, delay := range delays {
				advanceFrom(t, f.clock, delay, "readLoop")
				eventually(t, func() bool { f.mu.Lock(); defer f.mu.Unlock(); return f.attaches == i+2 })
			}
			eventually(t, func() bool {
				select {
				case <-s.done:
					return true
				default:
					return false
				}
			})
			v := s.snapshot()
			if v.Status != "ended" || v.CloseReason == nil || *v.CloseReason != "recorder_lost" {
				t.Fatal(v)
			}
			f.mu.Lock()
			defer f.mu.Unlock()
			for i, d := range delays {
				if got := f.attachTimes[i+1].Sub(f.attachTimes[i]); got != d {
					t.Fatal("backoff", i, got, d)
				}
			}
			rows := 0
			for _, r := range f.records {
				if strings.HasSuffix(r.path, "/events") {
					for _, v := range r.body["events"].([]any) {
						row := object(v)
						if row["event_id"] == "srw_lost_lost" {
							rows++
							if object(row["payload"])["reason"] != "recorder_lost" {
								t.Fatal(row)
							}
						}
					}
				}
			}
			if rows != 1 {
				t.Fatal(rows)
			}
		})
	}
}
func TestLostRequiresConsecutive404409(t *testing.T) {
	f := newFixture(t)
	f.mu.Lock()
	f.attachStatus = 404
	f.mu.Unlock()
	status, _ := f.request("POST", "/v1/sessions", f.d.cfg.controlToken, testHandoff("s"))
	if status != 200 {
		t.Fatal(status)
	}
	eventually(t, func() bool { f.mu.Lock(); defer f.mu.Unlock(); return f.attaches == 1 })
	advanceFrom(t, f.clock, 250*time.Millisecond, "readLoop")
	eventually(t, func() bool { f.mu.Lock(); defer f.mu.Unlock(); return f.attaches == 2 })
	f.mu.Lock()
	f.attachStatus = 500
	f.mu.Unlock()
	advanceFrom(t, f.clock, 500*time.Millisecond, "readLoop")
	eventually(t, func() bool { f.mu.Lock(); defer f.mu.Unlock(); return f.attaches == 3 })
	f.mu.Lock()
	f.attachStatus = 404
	f.mu.Unlock()
	advanceFrom(t, f.clock, time.Second, "readLoop")
	eventually(t, func() bool { f.mu.Lock(); defer f.mu.Unlock(); return f.attaches == 4 })
	f.d.mu.Lock()
	s := f.d.sessions["s"]
	f.d.mu.Unlock()
	if s.snapshot().Status != "active" {
		t.Fatal("lost too early")
	}
}
func TestAdoptStartupSkipsSentGreeting(t *testing.T) {
	f := newFixture(t)
	h := testHandoff("adopt")
	b, _ := json.Marshal(h)
	var body map[string]any
	_ = json.Unmarshal(b, &body)
	now := f.clock.Now().Add(-time.Minute)
	f.mu.Lock()
	f.adoptSessions = []map[string]any{{"session_id": "adopt", "provider_session_id": "rtc_adopt", "job_id": "job", "accepted_at": now, "attached_at": now, "handoff": body, "sent_event_ids": []string{"srw_greeting_adopt", "srw_fallback_adopt_d"}}}
	f.mu.Unlock()
	f.d.start()
	eventually(t, func() bool { f.mu.Lock(); defer f.mu.Unlock(); return f.attaches == 1 })
	f.d.mu.Lock()
	s := f.d.sessions["adopt"]
	f.d.mu.Unlock()
	eventually(t, func() bool { s.mu.Lock(); defer s.mu.Unlock(); return s.ws != nil })
	flushSession(t, s)
	f.mu.Lock()
	accepts := f.accepts
	f.mu.Unlock()
	if accepts != 0 || f.countCommands("srw_greeting_adopt") != 0 || s.snapshot().AttachedAt == nil {
		t.Fatal("adopt reaccepted or regreeted")
	}
	advanceTick(t, f.clock, 15*time.Second)
	eventually(t, func() bool { f.mu.Lock(); defer f.mu.Unlock(); return f.adoptions == 2 })
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.attaches != 1 {
		t.Fatal("duplicate adoption")
	}
}
func TestLeaseLossStopsWithoutState(t *testing.T) {
	f := newFixture(t)
	s := f.handoff("s")
	flushSession(t, s)
	_, ch := s.subscribe()
	f.mu.Lock()
	f.owned = false
	f.mu.Unlock()
	advanceFrom(t, f.clock, 10*time.Second, "renew")
	eventually(t, func() bool {
		select {
		case <-s.done:
			return true
		default:
			return false
		}
	})
	if _, ok := <-ch; ok {
		t.Fatal("stream still open")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, r := range f.records {
		if strings.HasSuffix(r.path, "/state") && r.body["status"] == "ended" {
			t.Fatal("reported ended after losing lease")
		}
		if strings.HasSuffix(r.path, "/lease") && r.body["release"] == true {
			t.Fatal("released another owner's lease")
		}
	}
}
func TestShutdownFlushesReleasesWithoutHangup(t *testing.T) {
	f := newFixture(t)
	s := f.handoff("s")
	s.handleEvent(map[string]any{"type": "session.input_transcript.delta", "event_id": "last", "delta": "last"})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	f.d.shutdown(ctx)
	select {
	case <-s.done:
	default:
		t.Fatal("shutdown not finished")
	}
	status, _ := f.request("POST", "/v1/sessions", f.d.cfg.controlToken, testHandoff("new"))
	if status != 503 {
		t.Fatal(status)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	released, persisted := false, false
	for _, r := range f.records {
		if strings.HasSuffix(r.path, "/hangup") {
			t.Fatal("shutdown hung up call")
		}
		if strings.HasSuffix(r.path, "/lease") && r.body["release"] == true {
			released = true
		}
		if strings.HasSuffix(r.path, "/events") {
			for _, v := range r.body["events"].([]any) {
				if object(v)["event_id"] == "last" {
					persisted = true
				}
			}
		}
	}
	if !released || !persisted {
		t.Fatal(released, persisted)
	}
}
func TestSSEReplayFanoutCloseAndRetention(t *testing.T) {
	f := newFixture(t)
	s := f.handoff("s")
	s.handleEvent(map[string]any{"type": "transport.dtmf.received", "event_id": "old", "event": "0"})
	f.clock.advance(4 * time.Second)
	s.handleEvent(map[string]any{"type": "transport.dtmf.received", "event_id": "recent", "event": "1"})
	open := func() (*http.Response, *bufio.Reader) {
		req, _ := http.NewRequest("GET", f.pod.URL+"/v1/sessions/s/events", nil)
		req.Header.Set("Authorization", "Bearer "+testToken)
		resp, err := f.pod.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		if resp.Header.Get("Content-Type") != "text/event-stream" {
			t.Fatal(resp.Header)
		}
		return resp, bufio.NewReader(resp.Body)
	}
	r1, b1 := open()
	defer r1.Body.Close()
	r2, b2 := open()
	defer r2.Body.Close()
	readEvent := func(b *bufio.Reader) string {
		t.Helper()
		out := ""
		for {
			line, err := b.ReadString('\n')
			if err != nil {
				t.Fatal(err)
			}
			out += line
			if line == "\n" {
				return out
			}
		}
	}
	// Greeting acknowledgement may be replayed too; read through the recent event.
	for _, b := range []*bufio.Reader{b1, b2} {
		for {
			v := readEvent(b)
			if strings.Contains(v, "id: old") {
				t.Fatal("old replay")
			}
			if strings.Contains(v, "id: recent") {
				break
			}
		}
	}
	s.handleEvent(map[string]any{"type": "transport.dtmf.received", "event_id": "live", "event": "2"})
	for _, b := range []*bufio.Reader{b1, b2} {
		if v := readEvent(b); !strings.Contains(v, "id: live\nevent: dtmf\n") || !strings.Contains(v, `"digit":"2"`) {
			t.Fatal(v)
		}
	}
	s.handleEvent(map[string]any{"type": "session.closed", "event_id": "closed", "reason": "remote_hangup"})
	for _, b := range []*bufio.Reader{b1, b2} {
		if v := readEvent(b); !strings.Contains(v, "event: closed") {
			t.Fatal(v)
		}
		if _, err := b.ReadByte(); err != io.EOF {
			t.Fatal("stream did not end", err)
		}
	}
	f.clock.advance(20 * time.Second)
	r3, b3 := open()
	defer r3.Body.Close()
	if v := readEvent(b3); !strings.Contains(v, "id: closed") {
		t.Fatal("old closed not replayed", v)
	}
	if _, err := b3.ReadByte(); err != io.EOF {
		t.Fatal(err)
	}
	status, view := f.request("POST", "/v1/sessions/s/attach", testToken, map[string]any{})
	if status != 200 || view["status"] != "ended" {
		t.Fatal(status, view)
	}
	eventually(t, func() bool {
		select {
		case <-s.done:
			return true
		default:
			return false
		}
	})
	f.clock.advance(15 * time.Minute)
	f.d.forget()
	status, _ = f.request("GET", "/v1/sessions/s", testToken, nil)
	if status != 404 {
		t.Fatal("ended session retained too long")
	}
}
func TestSSEOverflowOnlyClosesSlowReader(t *testing.T) {
	f := newFixture(t)
	s := f.handoff("s")
	_, slow := s.subscribe()
	_, fast := s.subscribe()
	defer s.unsubscribe(fast)
	for i := 0; i < 1100; i++ {
		s.handleEvent(map[string]any{"type": "transport.dtmf.received", "event_id": fmt.Sprint("e", i), "event": "1"})
		select {
		case <-fast:
		default:
			t.Fatal("fast missed event")
		}
	}
	s.mu.Lock()
	slowPresent := s.subscribers[slow]
	fastPresent := s.subscribers[fast]
	s.mu.Unlock()
	if slowPresent || !fastPresent {
		t.Fatal("wrong subscriber closed")
	}
	for range slow {
	} // drains the buffered events and then observes EOF
}

func TestLatestUsageWhenCloseOmitsUsage(t *testing.T) {
	f := newFixture(t)
	s := f.handoff("s")
	s.handleEvent(map[string]any{"type": "session.usage.updated", "usage": map[string]any{"seconds": 12.5}})
	s.handleEvent(map[string]any{"type": "session.closed", "reason": "remote_hangup"})
	eventually(t, func() bool {
		select {
		case <-s.done:
			return true
		default:
			return false
		}
	})
	v := s.snapshot()
	if v.UsageSeconds == nil || *v.UsageSeconds != 12.5 {
		t.Fatal(v)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, r := range f.records {
		if strings.HasSuffix(r.path, "/state") && r.body["status"] == "ended" && r.body["usage_seconds"] != 12.5 {
			t.Fatal(r)
		}
	}
}
