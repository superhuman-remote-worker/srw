package main

import (
	"strings"
	"testing"
	"time"
)

// The quiet-line nudge (receptionist_expert T9): the voice said it was
// checking and never delegated, and the line stayed silent for 28 s.

func newIdleFixture(t *testing.T) (*fixture, *session) {
	t.Helper()
	f := newFixture(t)
	f.d.cfg.idleNudge = defaultIdleNudge
	f.d.cfg.idleNudgeMax = defaultIdleNudgeMax
	h := testHandoff("s")
	h.Idle = "Nudge"
	status, out := f.request("POST", "/v1/sessions", f.d.cfg.controlToken, h)
	if status != 200 {
		t.Fatalf("handoff %d %v", status, out)
	}
	f.d.mu.Lock()
	s := f.d.sessions["s"]
	f.d.mu.Unlock()
	eventually(t, func() bool { return f.countCommands("srw_greeting_s") == 1 })
	return f, s
}

// ticks advances the monitor's clock n half-second ticks and waits until the
// monitor has looked at the line after the last one.
func ticks(t *testing.T, f *fixture, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		advanceFrom(t, f.clock, 500*time.Millisecond, "monitor")
	}
	waitTimer(t, f.clock, 500*time.Millisecond, "monitor")
}

func say(t *testing.T, f *fixture, s *session, speaker, id, text string) {
	t.Helper()
	wire := "session.output_transcript.delta"
	if speaker == "caller" {
		wire = "session.input_transcript.delta"
	}
	f.emit(map[string]any{"type": wire, "event_id": id, "delta": text})
	eventually(t, func() bool { s.mu.Lock(); defer s.mu.Unlock(); return s.seen.seen[id] })
}

func TestIdleNudgeDefaultsAndBounds(t *testing.T) {
	identity := "sdi_" + strings.Repeat("A", 49)
	cfg, err := parseConfig(validRequest(), identity)
	if err != nil || cfg.idleNudge != 8*time.Second || cfg.idleNudgeMax != 2 {
		t.Fatal(cfg, err)
	}
	r := validRequest()
	off, none := 0, 0
	r.Connector.Config.IdleNudge, r.Connector.Config.IdleNudgeMax = &off, &none
	if cfg, err = parseConfig(r, identity); err != nil || cfg.idleNudge != 0 || cfg.idleNudgeMax != 0 {
		t.Fatal(cfg, err)
	}
	for _, bad := range [][2]int{{-1, 2}, {30001, 2}, {8000, -1}, {8000, 6}} {
		r := validRequest()
		ms, max := bad[0], bad[1]
		r.Connector.Config.IdleNudge, r.Connector.Config.IdleNudgeMax = &ms, &max
		if _, err := parseConfig(r, identity); err == nil {
			t.Fatal("accepted idle nudge settings", bad)
		}
	}
}

func TestIdleNudgeAfterTheVoiceWentQuiet(t *testing.T) {
	f, s := newIdleFixture(t)
	say(t, f, s, "assistant", "v1", "Einen Moment, ich schaue nach.")
	ticks(t, f, 15)
	if f.countCommands("srw_idle_s_1") != 0 {
		t.Fatal("nudged before idle_nudge_ms")
	}
	ticks(t, f, 1)
	eventually(t, func() bool { return f.countCommands("srw_idle_s_1") == 1 })
	f.mu.Lock()
	var c map[string]any
	for _, cmd := range f.commands {
		if cmd["event_id"] == "srw_idle_s_1" {
			c = cmd
		}
	}
	f.mu.Unlock()
	if c["type"] != "session.instructions.append" || c["content"] != "Nudge" || c["delegation_id"] != nil {
		t.Fatal(c)
	}
	// The nudge itself restarts the quiet: the second needs another spell.
	ticks(t, f, 15)
	if f.countCommands("srw_idle_s_2") != 0 {
		t.Fatal("second nudge too early")
	}
	ticks(t, f, 1)
	eventually(t, func() bool { return f.countCommands("srw_idle_s_2") == 1 })
	// idle_nudge_max: no third while the caller stays silent.
	ticks(t, f, 40)
	if f.countCommands("srw_idle_s_3") != 0 {
		t.Fatal("nudged past idle_nudge_max")
	}
	// The caller's words reset the count.
	say(t, f, s, "caller", "c1", "Hallo?")
	ticks(t, f, 16)
	eventually(t, func() bool { return f.countCommands("srw_idle_s_3") == 1 })
	flushSession(t, s)
	if f.journalRows("srw_idle_s_1") != 1 || f.journalRows("srw_idle_s_3") != 1 {
		t.Fatal("nudges not journaled once each")
	}
}

func TestNoIdleNudgeWhileADelegationIsOpen(t *testing.T) {
	f, s := newIdleFixture(t)
	// The job is attached, so no no-job fallback settles the delegation.
	if status, view := f.request("POST", "/v1/sessions/s/attach", testToken, map[string]any{}); status != 200 || view["attached_at"] == nil {
		t.Fatal(status, view)
	}
	say(t, f, s, "assistant", "v1", "Einen Moment, ich schaue nach.")
	f.emit(map[string]any{"type": "session.delegation.created", "event_id": "d1", "delegation": map[string]any{"id": "del1"}})
	eventually(t, func() bool { s.mu.Lock(); defer s.mu.Unlock(); return len(s.delegations) == 1 })
	ticks(t, f, 40)
	if f.countCommands("srw_idle_s_1") != 0 {
		t.Fatal("nudged while the back office was working")
	}
	// A back-office line without the delegation (Say exactly …) settles nothing.
	if status, out := f.request("POST", "/v1/sessions/s/append", testToken, map[string]any{"kind": "instructions", "text": "Say exactly: one moment", "event_id": "line"}); status != 200 || out["ok"] != true {
		t.Fatal(status, out)
	}
	ticks(t, f, 40)
	if f.countCommands("srw_idle_s_1") != 0 {
		t.Fatal("an instruction settled the delegation")
	}
	// The answer settles it; the quiet starts at the answer.
	if status, out := f.request("POST", "/v1/sessions/s/append", testToken, map[string]any{"kind": "commentary", "text": "Open until 22:00.", "delegation_id": "del1", "event_id": "answer"}); status != 200 || out["ok"] != true {
		t.Fatal(status, out)
	}
	ticks(t, f, 15)
	if f.countCommands("srw_idle_s_1") != 0 {
		t.Fatal("nudged right after the answer")
	}
	ticks(t, f, 1)
	eventually(t, func() bool { return f.countCommands("srw_idle_s_1") == 1 })
}

func TestNoIdleNudgeBeforeTheVoiceSpokeOrWhenOff(t *testing.T) {
	t.Run("silent voice", func(t *testing.T) {
		// The greeting re-prompt owns the start of the call.
		f, s := newIdleFixture(t)
		ticks(t, f, 40)
		flushSession(t, s)
		if f.countCommands("srw_idle_s_1") != 0 {
			t.Fatal("nudged before the voice spoke")
		}
	})
	t.Run("off", func(t *testing.T) {
		f, s := newIdleFixture(t)
		f.d.cfg.idleNudge = 0
		say(t, f, s, "assistant", "v1", "Guten Tag.")
		ticks(t, f, 40)
		if f.countCommands("srw_idle_s_1") != 0 {
			t.Fatal("nudged with idle_nudge_ms 0")
		}
	})
	t.Run("no instruction", func(t *testing.T) {
		f, s := newIdleFixture(t)
		s.mu.Lock()
		s.h.Idle = " "
		s.mu.Unlock()
		say(t, f, s, "assistant", "v1", "Guten Tag.")
		ticks(t, f, 40)
		if f.countCommands("srw_idle_s_1") != 0 {
			t.Fatal("nudged without an idle instruction")
		}
	})
}
