package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"time"
)

const (
	frontPath      = "/mcp"
	maxRequestBody = 4 << 20
	// How long a caller may take to send its request body.
	bodyReadDeadline = 30 * time.Second
	// A stream lives at most this long; the client opens a new one.
	maxStreamLife  = 15 * time.Minute
	leaseRefused   = "the lease is revoked or expired: this execution no longer holds this connector"
	leaseRequired  = "a live lease of this connector is required"
	pinningRefused = "the server's tool list changed under this image; it is held until its pod is replaced"
	// The JSON-RPC error a call in flight gets when its lease ends: an
	// implementation-defined server error no MCP SDK uses. SRW's client
	// reads it (with the message prefix) as the lease ending, not a fault.
	leaseEndedCode    = -32091
	leaseEndedMessage = "lease revoked: " + leaseRefused
)

// A stream re-checks its lease this often (a variable for the tests).
var streamRecheck = 30 * time.Second

// Request headers the server may see; everything else (Authorization,
// Cookie, Origin, forwarding and fetch-metadata headers) stays here.
var forwardRequestHeaders = []string{
	"Accept",
	"Content-Type",
	"Last-Event-ID",
	"Mcp-Method",
	"Mcp-Name",
	"Mcp-Protocol-Version",
	"Mcp-Session-Id",
}

// Response headers the caller may see: no cookie, and no WWW-Authenticate
// that would start an OAuth flow against the server.
var forwardResponseHeaders = []string{
	"Cache-Control",
	"Content-Type",
	"Mcp-Protocol-Version",
	"Mcp-Session-Id",
}

type front struct {
	cfg      *config
	auth     *authCache
	upstream *http.Client
	logf     func(string, ...any)
	now      func() time.Time
	inflight *inflight
	sessions *sessionOwners
	probe    *prober
	buffers  *budget
}

func newFront(cfg *config, a authority, upstream *http.Client, logf func(string, ...any), now func() time.Time) *front {
	return &front{
		cfg:      cfg,
		auth:     newAuthCache(a, now),
		upstream: upstream,
		logf:     logf,
		now:      now,
		inflight: &inflight{perLease: map[string]int{}},
		sessions: newSessionOwners(now),
		probe:    &prober{cfg: cfg, client: upstream, logf: logf, now: now},
		buffers:  newBudget(bufferBudgetUnits),
	}
}

// newUpstreamClient reaches the server beside the front: no redirect is
// followed, no compression is asked for (responses are filtered and
// scrubbed), and a stream may stay open as long as the caller holds it.
func newUpstreamClient() *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			DialContext:           (&net.Dialer{Timeout: 5 * time.Second}).DialContext,
			ResponseHeaderTimeout: 2 * time.Minute,
			DisableCompression:    true,
			MaxIdleConnsPerHost:   16,
			IdleConnTimeout:       90 * time.Second,
		},
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(body)
}

func (f *front) unauthorized(w http.ResponseWriter, code, detail string) {
	w.Header().Set("WWW-Authenticate", `Bearer realm="srw-managed-mcp"`)
	writeJSON(w, http.StatusUnauthorized, map[string]string{"error": code, "detail": detail})
}

func bearer(r *http.Request) (string, bool) {
	scheme, token, found := strings.Cut(r.Header.Get("Authorization"), " ")
	if !found || !strings.EqualFold(scheme, "Bearer") {
		return "", false
	}
	token = strings.TrimSpace(token)
	return token, leaseShape.MatchString(token)
}

// rpcError answers a request with a JSON-RPC error, as the server would.
func rpcError(w http.ResponseWriter, message *rpcMessage, code int, text string) {
	writeJSON(w, http.StatusOK, map[string]any{
		"jsonrpc": "2.0",
		"id":      message.ID,
		"error":   map[string]any{"code": code, "message": text},
	})
}

func (f *front) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/livez":
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
		return
	case "/readyz":
		f.serveReady(w, r)
		return
	case frontPath:
	default:
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "not found"})
		return
	}
	if r.Method != http.MethodPost && r.Method != http.MethodGet && r.Method != http.MethodDelete {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "POST, GET or DELETE"})
		return
	}
	if r.Header.Get("Origin") != "" {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "a request with an Origin header is refused"})
		return
	}
	if f.probe.blocked() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": pinningRefused})
		return
	}
	token, ok := bearer(r)
	if !ok {
		f.unauthorized(w, "lease_required", leaseRequired)
		return
	}
	found, err := f.auth.lease(r.Context(), token)
	switch {
	case errors.Is(err, errFrontRevoked):
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "this server is stopping"})
		return
	case err != nil:
		f.logf("lease check failed: %v", err)
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "the lease exchange is unavailable"})
		return
	case !found.active || !strings.EqualFold(found.connectorID, f.cfg.connectorID):
		f.logf("refused a request without a live lease of this connector")
		f.unauthorized(w, "lease_inactive", leaseRefused)
		return
	}
	if session := r.Header.Get("Mcp-Session-Id"); session != "" && !f.sessions.mayUse(session, found.id) {
		// Another binding's session, or one this front does not know: the
		// client initializes again.
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "session not found"})
		return
	}
	// The caps come before the body is read.
	release, admitted := f.inflight.acquire(found.id, f.cfg.maxInFlight)
	if !admitted {
		writeJSON(w, http.StatusTooManyRequests, map[string]string{"error": "too many calls in flight for this binding"})
		return
	}
	defer release()
	var message *rpcMessage
	var body []byte
	if r.Method == http.MethodPost {
		_ = http.NewResponseController(w).SetReadDeadline(f.now().Add(bodyReadDeadline))
		raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxRequestBody))
		if err != nil {
			writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{"error": "the request is too large or too slow"})
			return
		}
		message, body, err = parseMessage(raw)
		if err != nil {
			f.logf("refused a malformed message lease=%s: %v", found.id, err)
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		if err := checkRoutingHeaders(r.Header, message); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		if !message.response && !allowedMethods[message.Method] {
			f.logf("refused method lease=%s method=%q", found.id, message.Method)
			if message.notification() {
				w.WriteHeader(http.StatusAccepted)
			} else {
				rpcError(w, message, -32601, "Method not found: "+message.Method)
			}
			return
		}
		if message.Method == "tools/call" && !f.cfg.allowed(message.tool, found.access) {
			f.logf("refused call lease=%s tool=%q class=%s access=%s", found.id, message.tool, f.cfg.toolClass(message.tool), found.access)
			if message.notification() {
				// A notification is never answered.
				w.WriteHeader(http.StatusAccepted)
			} else {
				rpcError(w, message, -32602, "Unknown tool: "+message.tool)
			}
			return
		}
		if message.Method == "initialize" && !f.sessions.room(found.id) {
			f.logf("refused a session lease=%s: the front holds its most sessions", found.id)
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "too many sessions on this server; try again later"})
			return
		}
	}
	operation := classRead
	if message != nil && message.Method == "tools/call" && f.cfg.toolClass(message.tool) == classWrite {
		operation = classWrite
	}
	credential := ""
	if f.cfg.credential != nil {
		issued, err := f.auth.credential(r.Context(), token, operation)
		switch {
		case errors.Is(err, errFrontRevoked):
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "this server is stopping"})
			return
		case err != nil:
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "the lease exchange is unavailable"})
			return
		case issued.status == http.StatusUnauthorized:
			f.logf("lease=%s refused by the exchange (%s)", found.id, issued.reason)
			f.unauthorized(w, "lease_inactive", leaseRefused)
			return
		case issued.status != http.StatusOK:
			f.logf("lease=%s refused %s by the exchange (%s)", found.id, operation, issued.reason)
			writeJSON(w, http.StatusForbidden, map[string]string{"error": "the lease does not allow this"})
			return
		}
		credential = issued.credential
	}
	started := f.now()
	status, size := f.forward(w, r, message, body, found, token, credential)
	if message != nil && message.Method == "tools/call" {
		// The audit line: never an argument, a token or a credential.
		f.logf("call lease=%s tool=%q class=%s status=%d bytes=%d duration=%s", found.id, message.tool, operation, status, size, f.now().Sub(started).Round(time.Millisecond))
	}
}

// Why the front ended a request's stream before the server did.
const (
	endNone        int32 = iota
	endLease             // the lease is no longer live
	endUnconfirmed       // the exchange could not confirm it for maxCache
)

// watchLease ends ctx when the lease stops being live, or when the
// exchange cannot confirm it for longer than a cached decision lasts, and
// records why in reason. Every streamRecheck it asks the exchange past the
// cache, so a revocation reaches an open stream within that interval.
func (f *front) watchLease(ctx context.Context, cancel context.CancelFunc, token string, reason *atomic.Int32) {
	ticker := time.NewTicker(streamRecheck)
	defer ticker.Stop()
	var unconfirmedSince time.Time
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			found, err := f.auth.leaseFresh(ctx, token)
			switch {
			case errors.Is(err, errFrontRevoked), err == nil && (!found.active || !strings.EqualFold(found.connectorID, f.cfg.connectorID)):
				f.logf("lease=%s ended: closing its stream", found.id)
				reason.Store(endLease)
				cancel()
				return
			case err != nil:
				if ctx.Err() != nil {
					return
				}
				now := f.now()
				if unconfirmedSince.IsZero() {
					unconfirmedSince = now
				}
				if now.Sub(unconfirmedSince) >= maxCache {
					f.logf("a lease could not be confirmed for %s (%v): closing its stream", maxCache, err)
					reason.Store(endUnconfirmed)
					cancel()
					return
				}
			default:
				unconfirmedSince = time.Time{}
			}
		}
	}
}

// streamEnded tells the caller why the front ended its request: a request
// in flight on a stream gets a JSON-RPC error for its id (the client fails
// that call with the reason instead of waiting for an answer that will not
// come); a JSON answer not yet written becomes what a new request would
// get (401 for an ended lease, 503 while the exchange cannot confirm it).
func (f *front) streamEnded(w http.ResponseWriter, message *rpcMessage, streaming bool, why int32) int {
	if !streaming {
		if why == endLease {
			f.unauthorized(w, "lease_inactive", leaseRefused)
			return http.StatusUnauthorized
		}
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "the lease exchange is unavailable"})
		return http.StatusServiceUnavailable
	}
	if message == nil || message.notification() || message.response {
		return 0
	}
	failure := map[string]any{"code": leaseEndedCode, "message": leaseEndedMessage}
	if why != endLease {
		failure = map[string]any{"code": -32603, "message": "the lease could not be confirmed: the lease exchange is unavailable"}
	}
	event, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": message.ID, "error": failure})
	fmt.Fprintf(w, "event: message\ndata: %s\n\n", event)
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}
	return 0
}

// closeSessions ends sessions the front forgot on the server (a stateful
// server keeps each until told), in the background and best effort: the
// DELETE carries the session id only.
func (f *front) closeSessions(sessions []string) {
	for _, session := range sessions {
		go func(session string) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			request, err := http.NewRequestWithContext(ctx, http.MethodDelete, f.cfg.upstream.String(), http.NoBody)
			if err != nil {
				return
			}
			request.Host = f.cfg.upstream.Host
			request.Header.Set("Mcp-Session-Id", session)
			response, err := f.upstream.Do(request)
			if err != nil {
				f.logf("a forgotten session was not closed on the server (%v)", err)
				return
			}
			io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
			response.Body.Close()
		}(session)
	}
}

// forward sends the checked message (re-encoded; never the caller's
// bytes) to the server with the credential in the header the spec names,
// and relays the answer filtered and scrubbed. It returns the status and
// the number of bytes relayed.
func (f *front) forward(w http.ResponseWriter, r *http.Request, message *rpcMessage, body []byte, found lease, token, credential string) (int, int) {
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	if r.Method == http.MethodGet {
		ctx, cancel = context.WithTimeout(ctx, maxStreamLife)
		defer cancel()
	}
	var ended atomic.Int32
	go f.watchLease(ctx, cancel, token, &ended)
	var payload io.Reader = http.NoBody
	if r.Method == http.MethodPost {
		payload = bytes.NewReader(body)
	}
	out, err := http.NewRequestWithContext(ctx, r.Method, f.cfg.upstream.String(), payload)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "the server is unavailable"})
		return http.StatusBadGateway, 0
	}
	out.Host = f.cfg.upstream.Host
	for _, name := range forwardRequestHeaders {
		if value := r.Header.Get(name); value != "" {
			out.Header.Set(name, value)
		}
	}
	if credential != "" {
		value := credential
		if f.cfg.credential.Scheme != "" {
			value = f.cfg.credential.Scheme + " " + credential
		}
		out.Header.Set(f.cfg.credential.Header, value)
	}
	response, err := f.upstream.Do(out)
	if why := ended.Load(); err != nil && why != endNone {
		return f.streamEnded(w, message, false, why), 0
	}
	if err != nil {
		if r.Context().Err() == nil {
			f.logf("lease=%s: the server did not answer (%s)", found.id, scrubText(err.Error(), credential))
		}
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "the server is unavailable"})
		return http.StatusBadGateway, 0
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusForbidden {
		f.logf("lease=%s: the server refused the connector's credential (HTTP %d)", found.id, response.StatusCode)
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "the server refused the connector's credential"})
		return http.StatusBadGateway, 0
	}
	session := r.Header.Get("Mcp-Session-Id")
	if response.StatusCode == http.StatusNotFound && session != "" {
		f.sessions.forget(session)
	}
	for _, name := range forwardResponseHeaders {
		if value := response.Header.Get(name); value != "" {
			w.Header().Set(name, scrubText(value, credential))
		}
	}
	if response.StatusCode < 300 {
		if message != nil && message.Method == "initialize" {
			if issued := response.Header.Get("Mcp-Session-Id"); issued != "" {
				forgotten, bound := f.sessions.bind(issued, found.id)
				if !bound {
					// No room after all (another lease took it since the
					// check): the client's next request gets 404.
					forgotten = append(forgotten, issued)
				}
				f.closeSessions(forgotten)
			}
		}
		if r.Method == http.MethodDelete && session != "" {
			f.sessions.forget(session)
		}
	}
	scrub := newScrubber(credential)
	allowed := func(name string) bool { return f.cfg.allowed(name, found.access) }
	if strings.HasPrefix(strings.ToLower(response.Header.Get("Content-Type")), "text/event-stream") {
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("X-Accel-Buffering", "no")
		w.WriteHeader(response.StatusCode)
		dropped := func() {
			f.logf("lease=%s: dropped a stream event the front cannot read as JSON", found.id)
		}
		size, err := relayEvents(ctx, w, response.Body, f.buffers, allowed, scrub, dropped)
		if err != nil && r.Context().Err() == nil && ctx.Err() == nil {
			f.logf("lease=%s: stream ended (%s)", found.id, scrubText(err.Error(), credential))
		}
		if why := ended.Load(); why != endNone && r.Context().Err() == nil {
			f.streamEnded(w, message, true, why)
		}
		return response.StatusCode, size
	}
	size, err := relayJSON(ctx, w, response, f.buffers, allowed, scrub)
	switch {
	case (errors.Is(err, errBusy) || errors.Is(err, errUnreadable)) && ended.Load() != endNone:
		// Nothing written yet: the lease ended while the answer was read.
		return f.streamEnded(w, message, false, ended.Load()), 0
	case errors.Is(err, errBusy):
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "the front is busy; try again"})
		return http.StatusServiceUnavailable, 0
	case errors.Is(err, errUnreadable), errors.Is(err, errTooLarge):
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
		return http.StatusBadGateway, 0
	}
	return response.StatusCode, size
}

// checkRoutingHeaders refuses mirrored routing headers that disagree with
// the body (the 2026-07-28 Mcp-Method and Mcp-Name): the front decides on
// the body, and a server routing on the header must see the same.
func checkRoutingHeaders(header http.Header, message *rpcMessage) error {
	if method := header.Get("Mcp-Method"); method != "" && method != message.Method {
		return errors.New("Mcp-Method does not match the body")
	}
	if name := header.Get("Mcp-Name"); name != "" && (message.Method != "tools/call" || name != message.tool) {
		return errors.New("Mcp-Name does not match the body")
	}
	return nil
}
