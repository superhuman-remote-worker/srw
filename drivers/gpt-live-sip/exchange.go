package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

type clock interface {
	Now() time.Time
	After(time.Duration) <-chan time.Time
}
type realClock struct{}

func (realClock) Now() time.Time                         { return time.Now().UTC() }
func (realClock) After(d time.Duration) <-chan time.Time { return time.After(d) }
func pause(ctx context.Context, c clock, delay time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-c.After(delay):
		return true
	}
}
func nextBackoff(d time.Duration) time.Duration {
	d *= 2
	if d > 5*time.Second {
		d = 5 * time.Second
	}
	return d
}
func httpClient() *http.Client {
	return &http.Client{Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
}

var errUnavailable = errors.New("exchange unavailable")
var errRevoked = errors.New("driver identity revoked")

type credential struct {
	APIKey       string            `json:"api_key"`
	APIBase      string            `json:"api_base"`
	WSBase       string            `json:"ws_base"`
	Headers      map[string]string `json:"headers"`
	CacheSeconds int               `json:"max_cache_seconds"`
}

func (c credential) headers() http.Header {
	h := http.Header{}
	h.Set("Authorization", "Bearer "+c.APIKey)
	for k, v := range c.Headers {
		h.Set(k, v)
	}
	return h
}

type leaseDecision struct {
	Active      bool      `json:"active"`
	ConnectorID string    `json:"connector_id"`
	JobID       *string   `json:"job_id"`
	ThreadID    *string   `json:"thread_id"`
	Expires     time.Time `json:"expires_at"`
}
type cachedLease struct {
	decision leaseDecision
	status   int
	until    time.Time
}
type exchange struct {
	cfg                               *config
	client                            *http.Client
	clock                             clock
	log                               func(string, map[string]any)
	mu                                sync.Mutex
	fetchGate                         chan struct{}
	cred                              credential
	expires, lastAttempt, lastSuccess time.Time
	lastFailed, revoked               bool
	cache                             map[[32]byte]cachedLease
}

func newExchange(cfg *config, c clock, log func(string, map[string]any)) *exchange {
	return &exchange{cfg: cfg, client: httpClient(), clock: c, log: log, cache: map[[32]byte]cachedLease{}, fetchGate: make(chan struct{}, 1)}
}
func (x *exchange) post(ctx context.Context, path string, body any, answer any) (int, error) {
	b, err := json.Marshal(body)
	if err != nil || len(b) > maxJSON {
		return 0, errors.New("exchange body exceeds 1 MiB")
	}
	req, err := http.NewRequestWithContext(ctx, "POST", x.cfg.exchangeURL+path, bytes.NewReader(b))
	if err != nil {
		return 0, errUnavailable
	}
	req.Header.Set("Authorization", "Bearer "+x.cfg.identity)
	req.Header.Set("Content-Type", "application/json")
	resp, err := x.client.Do(req)
	if err != nil {
		return 0, errUnavailable
	}
	defer resp.Body.Close()
	if answer != nil && resp.StatusCode >= 200 && resp.StatusCode < 300 {
		raw, err := io.ReadAll(io.LimitReader(resp.Body, maxJSON+1))
		if err != nil || len(raw) > maxJSON || json.Unmarshal(raw, answer) != nil {
			return resp.StatusCode, errUnavailable
		}
	}
	return resp.StatusCode, nil
}
func (x *exchange) isRevoked() bool { x.mu.Lock(); defer x.mu.Unlock(); return x.revoked }
func (x *exchange) getCredential(ctx context.Context) (credential, error) {
	if err := x.lockFetch(ctx); err != nil {
		return credential{}, err
	}
	defer x.unlockFetch()
	x.mu.Lock()
	c, valid, revoked := x.cred, x.clock.Now().Before(x.expires), x.revoked
	x.mu.Unlock()
	if revoked {
		return credential{}, errRevoked
	}
	if valid {
		return c, nil
	}
	return x.fetchCredential(ctx)
}

// Called under fetchGate; refreshes never log credential values or upstream messages.
func (x *exchange) fetchCredential(ctx context.Context) (credential, error) {
	var c credential
	status, err := x.post(ctx, "/v1/live/credential", map[string]any{}, &c)
	if err == nil && (status != 200 || !validCredential(c)) {
		err = errUnavailable
	}
	x.mu.Lock()
	defer x.mu.Unlock()
	x.lastAttempt = x.clock.Now()
	x.lastFailed = err != nil
	if status == 401 && !x.revoked {
		x.revoked = true
		x.log("identity_revoked", map[string]any{"status": 401})
	}
	if x.revoked {
		return credential{}, errRevoked
	}
	if err != nil {
		return credential{}, err
	}
	ttl := c.CacheSeconds
	if ttl > 300 {
		ttl = 300
	}
	if ttl < 0 {
		ttl = 0
	}
	x.cred = c
	x.lastSuccess = x.clock.Now()
	x.expires = x.clock.Now().Add(time.Duration(ttl) * time.Second)
	return c, nil
}
func validCredential(c credential) bool {
	if c.APIKey == "" {
		return false
	}
	for i, s := range []string{c.APIBase, c.WSBase} {
		u, err := url.Parse(s)
		if err != nil || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
			return false
		}
		if i == 0 && u.Scheme != "https" && u.Scheme != "http" || i == 1 && u.Scheme != "wss" && u.Scheme != "ws" {
			return false
		}
	}
	for k, v := range c.Headers {
		if strings.ContainsAny(k+v, "\r\n") {
			return false
		}
	}
	return true
}
func (x *exchange) ready() bool {
	x.mu.Lock()
	defer x.mu.Unlock()
	return !x.revoked && !x.lastSuccess.IsZero() && (!x.lastFailed || x.clock.Now().Sub(x.lastSuccess) <= 10*time.Minute)
}
func (x *exchange) refreshLoop(ctx context.Context) {
	delay := 250 * time.Millisecond
	for ctx.Err() == nil {
		if x.lockFetch(ctx) != nil {
			return
		}
		x.mu.Lock()
		revoked := x.revoked
		remaining := x.expires.Sub(x.clock.Now())
		x.mu.Unlock()
		if revoked {
			x.unlockFetch()
			return
		}
		if remaining <= 30*time.Second {
			_, err := x.fetchCredential(ctx)
			x.unlockFetch()
			if err != nil {
				if !pause(ctx, x.clock, delay) {
					return
				}
				delay = nextBackoff(delay)
				continue
			}
			delay = 250 * time.Millisecond
			remaining = time.Second
		} else {
			x.unlockFetch()
			remaining -= 30 * time.Second
		}
		if remaining < time.Second {
			remaining = time.Second
		}
		if !pause(ctx, x.clock, remaining) {
			return
		}
	}
}
func (x *exchange) authorize(ctx context.Context, token string, job string) int {
	if !leaseShape.MatchString(token) {
		return 401
	}
	key := sha256.Sum256([]byte(token))
	now := x.clock.Now()
	x.mu.Lock()
	item, ok := x.cache[key]
	x.mu.Unlock()
	if !ok || !now.Before(item.until) {
		var d leaseDecision
		status, err := x.post(ctx, "/v1/leases/introspect", map[string]any{"lease_token": token}, &d)
		if err != nil || status >= 500 {
			return 503
		}
		result := 0
		switch status {
		case 200:
			if !d.Active {
				result = 401
			}
		case 400, 401:
			result = 401
		case 403:
			result = 403
		default:
			return 503
		}
		ttl := 5 * time.Second
		if d.Active && result == 0 {
			ttl = 30 * time.Second
		}
		until := now.Add(ttl)
		if !d.Expires.IsZero() && d.Expires.Before(until) {
			until = d.Expires
		}
		item = cachedLease{d, result, until}
		x.mu.Lock()
		if len(x.cache) >= 4096 {
			for k, v := range x.cache {
				if !now.Before(v.until) {
					delete(x.cache, k)
				}
			}
			if len(x.cache) >= 4096 {
				x.cache = map[[32]byte]cachedLease{}
			}
		}
		x.cache[key] = item
		x.mu.Unlock()
	}
	if item.status != 0 {
		return item.status
	}
	now = x.clock.Now()
	if !item.decision.Active || !item.decision.Expires.IsZero() && !now.Before(item.decision.Expires) {
		return 401
	}
	if item.decision.ConnectorID != x.cfg.connectorID || item.decision.JobID == nil || *item.decision.JobID != job {
		return 403
	}
	return 200
}
func (x *exchange) claim(ctx context.Context, id string, release bool) (bool, time.Time, error) {
	var answer struct {
		Owned bool      `json:"owned"`
		Until time.Time `json:"lease_until"`
	}
	status, err := x.call(ctx, sessionPath(id)+"/lease", map[string]any{"release": release}, &answer)
	if err != nil || status >= 500 {
		return false, time.Time{}, errUnavailable
	}
	if status != 200 {
		return false, time.Time{}, nil
	}
	if answer.Until.IsZero() {
		answer.Until = x.clock.Now().Add(30 * time.Second)
	}
	return answer.Owned && x.clock.Now().Before(answer.Until), answer.Until, nil
}
func sessionPath(id string) string { return "/v1/live/sessions/" + url.PathEscape(id) }
func (x *exchange) retry(ctx context.Context, path string, body any) bool {
	delay := 250 * time.Millisecond
	for ctx.Err() == nil {
		status, err := x.post(ctx, path, body, nil)
		if err == nil && status < 500 {
			if status >= 400 {
				x.log("exchange_refused", map[string]any{"status": status})
			}
			return true
		}
		if !pause(ctx, x.clock, delay) {
			return false
		}
		delay = nextBackoff(delay)
	}
	return false
}

type engineError struct {
	Status  int    `json:"status"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

func enginePost(ctx context.Context, client *http.Client, c credential, provider, action string, body any) *engineError {
	var b []byte
	if body != nil {
		b, _ = json.Marshal(body)
	}
	req, err := http.NewRequestWithContext(ctx, "POST", strings.TrimRight(c.APIBase, "/")+"/live/sessions/"+url.PathEscape(provider)+"/"+action, bytes.NewReader(b))
	if err != nil {
		return &engineError{0, "transport_error", "engine request failed"}
	}
	req.Header = c.headers()
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := client.Do(req)
	if err != nil {
		return &engineError{0, "transport_error", "engine request failed"}
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	var answer struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	_ = json.NewDecoder(io.LimitReader(resp.Body, 64*1024)).Decode(&answer)
	code := answer.Error.Code
	if strings.Contains(code, c.APIKey) {
		code = ""
	}
	for _, v := range c.Headers {
		if v != "" && strings.Contains(code, v) {
			code = ""
		}
	}
	if !regexpCode(code) {
		code = "engine_error"
	}
	// Error messages can echo keys, headers, instructions or caller data. Return a fixed message.
	return &engineError{resp.StatusCode, code, "engine request refused"}
}
func regexpCode(s string) bool {
	if len(s) == 0 || len(s) > 100 {
		return false
	}
	for _, r := range s {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_') {
			return false
		}
	}
	return true
}

// Control-plane writes retry only ambiguous transport failures and 5xx.
// Introspection deliberately uses a single attempt so exchange failure is 503.
func (x *exchange) call(ctx context.Context, path string, body, answer any) (int, error) {
	delay := 250 * time.Millisecond
	for {
		status, err := x.post(ctx, path, body, answer)
		if err == nil && status < 500 {
			return status, nil
		}
		if !pause(ctx, x.clock, delay) {
			return status, errUnavailable
		}
		delay = nextBackoff(delay)
	}
}

func (x *exchange) lockFetch(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case x.fetchGate <- struct{}{}:
		if ctx.Err() != nil {
			x.unlockFetch()
			return ctx.Err()
		}
		return nil
	}
}
func (x *exchange) unlockFetch() { <-x.fetchGate }
