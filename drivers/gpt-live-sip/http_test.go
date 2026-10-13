package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestControlAuth(t *testing.T) {
	f := newFixture(t)
	for _, token := range []string{"", "wrong", testToken} {
		status, b := f.request("POST", "/v1/sessions", token, testHandoff("s"))
		if status != 401 || object(b["error"])["code"] != "unauthorized" {
			t.Fatal(status, b)
		}
	}
	status, _ := f.request("GET", "/livez", "", nil)
	if status != 200 {
		t.Fatal(status)
	}
}
func TestLeaseAuthAndCaching(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		active               bool
		connector, job       string
		exchangeStatus, want int
	}{
		{"active", true, "connector", "job", 200, 200}, {"inactive", false, "connector", "job", 200, 401}, {"connector", true, "other", "job", 200, 403}, {"job", true, "connector", "other", 200, 403}, {"down", true, "connector", "job", 500, 503}, {"refused", false, "connector", "job", 401, 401}, {"forbidden", true, "connector", "job", 403, 403},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			f.handoff("s")
			f.mu.Lock()
			f.active = tc.active
			f.connector = tc.connector
			f.job = tc.job
			f.authStatus = tc.exchangeStatus
			f.mu.Unlock()
			for i := 0; i < 2; i++ {
				status, _ := f.request("GET", "/v1/sessions/s", testToken, nil)
				if status != tc.want {
					t.Fatal(status)
				}
			}
			f.mu.Lock()
			calls := f.introspects
			f.mu.Unlock()
			want := 1
			if tc.want == 503 {
				want = 2
			}
			if calls != want {
				t.Fatal(calls)
			}
		})
	}
	f := newFixture(t)
	f.handoff("s")
	status, _ := f.request("GET", "/v1/sessions/s", "", nil)
	if status != 401 {
		t.Fatal(status)
	}
	status, _ = f.request("GET", "/v1/sessions/missing", testToken, nil)
	if status != 404 {
		t.Fatal(status)
	}
	for i := 0; i < 2; i++ {
		status, _ = f.request("GET", "/v1/sessions/s", testToken, nil)
		if status != 200 {
			t.Fatal(status)
		}
	}
	f.mu.Lock()
	if f.introspects != 1 {
		t.Fatal(f.introspects)
	}
	f.active = false
	f.mu.Unlock()
	f.clock.advance(30 * time.Second)
	status, _ = f.request("GET", "/v1/sessions/s", testToken, nil)
	if status != 401 {
		t.Fatal(status)
	}
	f.mu.Lock()
	f.active = true
	f.mu.Unlock()
	f.clock.advance(4 * time.Second)
	status, _ = f.request("GET", "/v1/sessions/s", testToken, nil)
	if status != 401 {
		t.Fatal(status)
	}
	f.clock.advance(time.Second)
	status, _ = f.request("GET", "/v1/sessions/s", testToken, nil)
	if status != 200 {
		t.Fatal(status)
	}
}
func TestAcceptExactIdempotentConcurrent(t *testing.T) {
	f := newFixture(t)
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			status, b := f.request("POST", "/v1/sessions", f.d.cfg.controlToken, testHandoff("s"))
			if status != 200 || b["status"] != "active" {
				t.Errorf("%d %v", status, b)
			}
		}()
	}
	wg.Wait()
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.accepts != 1 {
		t.Fatal(f.accepts)
	}
	want := `{"session":{"audio":{"output":{"voice":"marin"}},"delegation":{"type":"client"},"instructions":"Instructions","model":"gpt-live-1","type":"live"}}`
	for _, r := range f.records {
		if strings.HasSuffix(r.path, "/accept") {
			b, _ := json.Marshal(r.body)
			if string(b) != want {
				t.Fatal(string(b))
			}
		}
	}
}
func TestAcceptFailureAndOwnership(t *testing.T) {
	f := newFixture(t)
	f.mu.Lock()
	f.engineStatus = 409
	f.mu.Unlock()
	for i := 0; i < 2; i++ {
		status, b := f.request("POST", "/v1/sessions", f.d.cfg.controlToken, testHandoff("failed"))
		if status != 502 || object(b["error"])["status"] != float64(409) || object(b["error"])["code"] != "decision_already_made" {
			t.Fatal(status, b)
		}
		raw, _ := json.Marshal(b)
		if strings.Contains(string(raw), "secret") {
			t.Fatal("secret echoed")
		}
	}
	eventually(t, func() bool { return f.pathCount(sessionPath("failed")+"/state") == 1 })
	f.mu.Lock()
	if f.accepts != 1 {
		t.Fatal(f.accepts)
	}
	for _, r := range f.records {
		if r.path == sessionPath("failed")+"/state" && r.body["status"] != "failed" {
			t.Fatal(r)
		}
	}
	f.owned = false
	f.mu.Unlock()
	status, b := f.request("POST", "/v1/sessions", f.d.cfg.controlToken, testHandoff("unowned"))
	if status != 409 || object(b["error"])["code"] != "not_owner" {
		t.Fatal(status, b)
	}
	f.d.mu.Lock()
	_, ok := f.d.sessions["unowned"]
	f.d.mu.Unlock()
	if ok {
		t.Fatal("unowned session retained")
	}
}
func TestRejectTransferHangup(t *testing.T) {
	f := newFixture(t)
	status, _ := f.request("POST", "/v1/sessions/new/reject", f.d.cfg.controlToken, map[string]any{"provider_session_id": "rtc_new", "status_code": 480})
	if status != 200 {
		t.Fatal(status)
	}
	f.d.mu.Lock()
	n := len(f.d.sessions)
	f.d.mu.Unlock()
	if n != 0 {
		t.Fatal("reject created state")
	}
	for _, code := range []int{299, 700} {
		status, _ = f.request("POST", "/v1/sessions/new/reject", f.d.cfg.controlToken, map[string]any{"provider_session_id": "rtc_new", "status_code": code})
		if status != 400 {
			t.Fatal(status)
		}
	}
	f.handoff("s")
	status, _ = f.request("POST", "/v1/sessions/s/transfer", testToken, map[string]any{})
	if status != 200 {
		t.Fatal(status)
	}
	status, _ = f.request("POST", "/v1/sessions/s/hangup", testToken, nil)
	if status != 200 {
		t.Fatal(status)
	}
	f.mu.Lock()
	for _, r := range f.records {
		if strings.HasSuffix(r.path, "/refer") && r.body["target_uri"] != f.d.cfg.transferTarget {
			t.Fatal(r)
		}
		if strings.HasSuffix(r.path, "/hangup") && r.body != nil {
			t.Fatal("hangup body")
		}
	}
	f.engineStatus = 400
	f.mu.Unlock()
	status, _ = f.request("POST", "/v1/sessions/s/transfer", testToken, map[string]string{"target": "sip:override"})
	if status != 502 {
		t.Fatal(status)
	}
}
func TestAppendAckErrorTimeoutAndNoJournal(t *testing.T) {
	f := newFixture(t)
	s := f.handoff("s")
	eventually(t, func() bool { return f.countCommands("srw_greeting_s") == 1 })
	for _, mode := range []string{"ack", "error", "silent"} {
		t.Run(mode, func(t *testing.T) {
			f.mu.Lock()
			f.ackMode = mode
			f.mu.Unlock()
			result := make(chan map[string]any, 1)
			go func() {
				status, b := f.request("POST", "/v1/sessions/s/append", testToken, map[string]any{"kind": "thinking", "text": "hello", "delegation_id": nil, "event_id": mode})
				if status != 200 {
					t.Errorf("status %d", status)
				}
				result <- b
			}()
			eventually(t, func() bool { return f.countCommands(mode) == 1 })
			if mode == "silent" {
				advanceTick(t, f.clock, 5*time.Second)
			}
			b := <-result
			if mode == "ack" && b["ok"] != true || mode != "ack" && b["ok"] != false {
				t.Fatal(b)
			}
			if mode == "error" && object(b["error"])["code"] != "refused" || mode == "silent" && object(b["error"])["code"] != "timeout" {
				t.Fatal(b)
			}
		})
	}
	flushSession(t, s)
	f.mu.Lock()
	for _, r := range f.records {
		if strings.HasSuffix(r.path, "/events") {
			events, _ := r.body["events"].([]any)
			for _, v := range events {
				row := object(v)
				if row["kind"] == "instruction" && row["event_id"] == "ack" {
					t.Fatal("agent append journaled")
				}
			}
		}
	}
	f.mu.Unlock()
	status, _ := f.request("POST", "/v1/sessions/s/append", testToken, map[string]string{"kind": "thinking", "text": "  ", "event_id": "blank"})
	if status != 400 {
		t.Fatal(status)
	}
	s.mu.Lock()
	w := s.ws
	s.ws = nil
	s.mu.Unlock()
	w.close()
	status, b := f.request("POST", "/v1/sessions/s/append", testToken, map[string]string{"kind": "thinking", "text": "hi", "event_id": "down"})
	if status != 409 || object(b["error"])["code"] != "not_active" {
		t.Fatal(status, b)
	}
}
func TestReadyCredentialCacheRevocation(t *testing.T) {
	f := newFixture(t)
	status, _ := f.request("GET", "/readyz", "", nil)
	if status != 503 {
		t.Fatal(status)
	}
	for i := 0; i < 2; i++ {
		if _, err := f.d.x.getCredential(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	f.mu.Lock()
	if f.credentials != 1 {
		t.Fatal(f.credentials)
	}
	f.credentialStatus = 500
	f.mu.Unlock()
	status, _ = f.request("GET", "/readyz", "", nil)
	if status != 200 {
		t.Fatal(status)
	}
	f.clock.advance(301 * time.Second)
	_, _ = f.d.x.getCredential(context.Background())
	if !f.d.x.ready() {
		t.Fatal("readiness failed too early")
	}
	f.clock.advance(300 * time.Second)
	_, _ = f.d.x.getCredential(context.Background())
	if f.d.x.ready() {
		t.Fatal("stale readiness")
	}
	f.mu.Lock()
	f.credentialStatus = 401
	f.mu.Unlock()
	_, _ = f.d.x.getCredential(context.Background())
	status, _ = f.request("POST", "/v1/sessions", f.d.cfg.controlToken, testHandoff("revoked"))
	if status != 503 {
		t.Fatal(status)
	}
}
func TestAppendWireHasNullDelegation(t *testing.T) {
	f := newFixture(t)
	f.handoff("s")
	status, _ := f.request("POST", "/v1/sessions/s/append", testToken, map[string]any{"kind": "commentary", "text": "hello", "event_id": "agent"})
	if status != 200 {
		t.Fatal(status)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, cmd := range f.commands {
		if cmd["event_id"] == "agent" {
			v, present := cmd["delegation_id"]
			if !present || v != nil || cmd["content"] != "hello" {
				t.Fatal(cmd)
			}
			return
		}
	}
	t.Fatal("command missing")
}
func TestInvalidBodiesAndNoTransferTarget(t *testing.T) {
	f := newFixture(t)
	f.d.cfg.transferTarget = ""
	f.handoff("s")
	status, _ := f.request("POST", "/v1/sessions/s/transfer", testToken, map[string]any{})
	if status != 400 {
		t.Fatal(status)
	}
	req, _ := http.NewRequest("POST", f.pod.URL+"/v1/sessions", strings.NewReader(strings.Repeat("x", maxJSON+1)))
	req.Header.Set("Authorization", "Bearer "+f.d.cfg.controlToken)
	resp, err := f.pod.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 400 {
		t.Fatal(resp.StatusCode)
	}
}
