package main

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

func reply(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
func fail(w http.ResponseWriter, status int, code string) {
	reply(w, status, map[string]any{"error": map[string]any{"code": code}})
}
func bearer(r *http.Request) string {
	h := r.Header.Get("Authorization")
	if !strings.HasPrefix(h, "Bearer ") {
		return ""
	}
	return strings.TrimPrefix(h, "Bearer ")
}
func decode(w http.ResponseWriter, r *http.Request, body any) bool {
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxJSON))
	raw = bytes.TrimSpace(raw)
	if err != nil || len(raw) == 0 || raw[0] != '{' || json.Unmarshal(raw, body) != nil {
		fail(w, 400, "invalid_body")
		return false
	}
	return true
}
func (d *driver) control(w http.ResponseWriter, r *http.Request) bool {
	if subtle.ConstantTimeCompare([]byte(bearer(r)), []byte(d.cfg.controlToken)) != 1 {
		fail(w, 401, "unauthorized")
		return false
	}
	return true
}
func (d *driver) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" && r.URL.Path == "/livez" {
		reply(w, 200, map[string]bool{"ok": true})
		return
	}
	if r.Method == "GET" && r.URL.Path == "/readyz" {
		if d.x.ready() {
			reply(w, 200, map[string]bool{"ok": true})
		} else {
			fail(w, 503, "not_ready")
		}
		return
	}
	if r.Method == "POST" && r.URL.Path == "/v1/sessions" {
		if !d.control(w, r) {
			return
		}
		var h handoff
		if !decode(w, r, &h) {
			return
		}
		if !h.normalize(d.cfg) {
			fail(w, 400, "invalid_handoff")
			return
		}
		s, fresh := d.admit(h)
		if s == nil {
			fail(w, 503, "not_ready")
			return
		}
		if fresh {
			go d.accept(s)
		}
		select {
		case <-s.initialized:
		case <-r.Context().Done():
			return
		case <-time.After(13 * time.Second):
			fail(w, 503, "admission_pending")
			return
		}
		if s.initHTTP != 0 {
			fail(w, s.initHTTP, s.initCode)
			return
		}
		if s.failure != nil {
			reply(w, 502, map[string]any{"error": s.failure})
			return
		}
		reply(w, 200, map[string]any{"status": s.snapshot().Status})
		return
	}
	if !strings.HasPrefix(r.URL.Path, "/v1/sessions/") {
		fail(w, 404, "not_found")
		return
	}
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/v1/sessions/"), "/")
	if len(parts) > 2 || parts[0] == "" {
		fail(w, 404, "not_found")
		return
	}
	id := parts[0]
	action := ""
	if len(parts) == 2 {
		action = parts[1]
	}
	if action == "reject" && r.Method == "POST" {
		if !d.control(w, r) {
			return
		}
		var body struct {
			Provider string `json:"provider_session_id"`
			Status   int    `json:"status_code"`
		}
		if !decode(w, r, &body) {
			return
		}
		if strings.TrimSpace(body.Provider) == "" || body.Status < 300 || body.Status > 699 {
			fail(w, 400, "invalid_reject")
			return
		}
		d.engineControl(w, r, body.Provider, "reject", map[string]any{"status_code": body.Status})
		return
	}
	token := bearer(r)
	if !leaseShape.MatchString(token) {
		fail(w, 401, "unauthorized")
		return
	}
	d.mu.Lock()
	s := d.sessions[id]
	d.mu.Unlock()
	if s == nil {
		fail(w, 404, "unknown_session")
		return
	}
	select {
	case <-s.initialized:
	default:
		fail(w, 409, "not_active")
		return
	}
	auth := d.x.authorize(r.Context(), token, s.h.JobID)
	if auth != 200 {
		code := "unauthorized"
		if auth == 403 {
			code = "forbidden"
		}
		if auth == 503 {
			code = "exchange_unavailable"
		}
		fail(w, auth, code)
		return
	}
	switch {
	case r.Method == "GET" && action == "":
		reply(w, 200, s.snapshot())
	case r.Method == "POST" && action == "attach":
		var body map[string]any
		if !decode(w, r, &body) {
			return
		}
		s.actionMu.Lock()
		s.mu.Lock()
		if s.view.AttachedAt == nil && s.active() {
			now := d.clock.Now()
			s.view.AttachedAt = &now
			s.journal.state(map[string]any{"status": "active", "attached_at": now})
		}
		v := s.view
		s.mu.Unlock()
		s.actionMu.Unlock()
		reply(w, 200, v)
	case r.Method == "GET" && action == "events":
		d.events(w, r, s, token)
	case r.Method == "POST" && action == "append":
		var body struct {
			Kind       string  `json:"kind"`
			Text       string  `json:"text"`
			Delegation *string `json:"delegation_id"`
			ID         string  `json:"event_id"`
		}
		if !decode(w, r, &body) {
			return
		}
		content := clipAppendContent(body.Text, 1500)
		if body.Kind != "instructions" && body.Kind != "commentary" && body.Kind != "thinking" || content == "" || body.ID == "" {
			fail(w, 400, "invalid_append")
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		status, answer := s.append(ctx, body.Kind, content, body.Delegation, body.ID)
		reply(w, status, answer)
	case r.Method == "POST" && (action == "transfer" || action == "hangup"):
		s.mu.Lock()
		active := s.active()
		s.mu.Unlock()
		if !active {
			fail(w, 409, "not_active")
			return
		}
		var body any
		op := "hangup"
		if action == "transfer" {
			var t struct {
				Target string `json:"target"`
			}
			if !decode(w, r, &t) {
				return
			}
			if t.Target == "" {
				t.Target = s.h.TransferTarget
			}
			if strings.TrimSpace(t.Target) == "" {
				fail(w, 400, "missing_target")
				return
			}
			body = map[string]any{"target_uri": t.Target}
			op = "refer"
		}
		ctx, cancel := context.WithCancel(r.Context())
		stop := context.AfterFunc(s.ctx, cancel)
		defer stop()
		defer cancel()
		d.engineControl(w, r.WithContext(ctx), s.h.ProviderID, op, body)
	default:
		fail(w, 404, "not_found")
	}
}
func (d *driver) engineControl(w http.ResponseWriter, r *http.Request, provider, action string, body any) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	c, err := d.x.getCredential(ctx)
	if err != nil {
		fail(w, 503, "credential_unavailable")
		return
	}
	e := enginePost(ctx, d.client, c, provider, action, body)
	if e != nil {
		reply(w, 502, map[string]any{"error": e})
		return
	}
	reply(w, 200, map[string]any{"ok": true})
}
func (d *driver) events(w http.ResponseWriter, r *http.Request, s *session, token string) {
	if _, ok := w.(http.Flusher); !ok {
		fail(w, 500, "stream_unsupported")
		return
	}
	replay, ch := s.subscribe()
	defer s.unsubscribe(ch)
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(200)
	rc := http.NewResponseController(w)
	write := func(e event) bool {
		b, _ := json.Marshal(e)
		_ = rc.SetWriteDeadline(time.Now().Add(10 * time.Second))
		if id := str(e["event_id"]); id != "" && !strings.ContainsAny(id, "\r\n\x00") {
			if _, err := fmt.Fprintf(w, "id: %s\n", id); err != nil {
				return false
			}
		}
		if _, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", e["type"], b); err != nil {
			return false
		}
		return rc.Flush() == nil
	}
	for _, e := range replay {
		if !write(e) || e["type"] == "closed" {
			return
		}
	}
	_ = rc.SetWriteDeadline(time.Now().Add(10 * time.Second))
	if rc.Flush() != nil {
		return
	}
	ping := d.clock.After(15 * time.Second)
	auth := d.clock.After(30 * time.Second)
	for {
		select {
		case <-r.Context().Done():
			return
		case e, ok := <-ch:
			if !ok || !write(e) || e["type"] == "closed" {
				return
			}
		case <-ping:
			_ = rc.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if _, err := fmt.Fprint(w, ": ping\n\n"); err != nil || rc.Flush() != nil {
				return
			}
			ping = d.clock.After(15 * time.Second)
		case <-auth:
			if d.x.authorize(r.Context(), token, s.h.JobID) != 200 {
				return
			}
			auth = d.clock.After(30 * time.Second)
		}
	}
}
