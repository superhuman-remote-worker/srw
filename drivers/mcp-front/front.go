package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	frontPath       = "/mcp"
	maxRequestBody  = 4 << 20
	maxResponseBody = 32 << 20
	maxEventBytes   = 16 << 20
	globalInFlight  = 64
	maxSessions     = 4096
	redacted        = "[redacted]"
)

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
}

func newFront(cfg *config, a authority, upstream *http.Client, logf func(string, ...any), now func() time.Time) *front {
	return &front{
		cfg:      cfg,
		auth:     newAuthCache(a, now),
		upstream: upstream,
		logf:     logf,
		now:      now,
		inflight: &inflight{perLease: map[string]int{}},
		sessions: &sessionOwners{owners: map[string]string{}},
		probe:    &prober{cfg: cfg, client: upstream, logf: logf, now: now},
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

func (f *front) unauthorized(w http.ResponseWriter) {
	w.Header().Set("WWW-Authenticate", `Bearer realm="srw-managed-mcp"`)
	writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "a live lease of this connector is required"})
}

func bearer(r *http.Request) (string, bool) {
	scheme, token, found := strings.Cut(r.Header.Get("Authorization"), " ")
	if !found || !strings.EqualFold(scheme, "Bearer") {
		return "", false
	}
	token = strings.TrimSpace(token)
	return token, leaseShape.MatchString(token)
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
	token, ok := bearer(r)
	if !ok {
		f.unauthorized(w)
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
		f.unauthorized(w)
		return
	}
	if session := r.Header.Get("Mcp-Session-Id"); session != "" && !f.sessions.mayUse(session, found.id) {
		// Another binding's session: as unknown to this caller as a stale one.
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "session not found"})
		return
	}
	var message *rpcMessage
	var body []byte
	if r.Method == http.MethodPost {
		body, err = io.ReadAll(http.MaxBytesReader(w, r.Body, maxRequestBody))
		if err != nil {
			writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{"error": "the request is too large"})
			return
		}
		message, err = parseMessage(body)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		if err := checkRoutingHeaders(r.Header, message); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		release, admitted := f.inflight.acquire(found.id, f.cfg.maxInFlight)
		if !admitted {
			writeJSON(w, http.StatusTooManyRequests, map[string]string{"error": "too many calls in flight for this binding"})
			return
		}
		defer release()
		if message.Method == "tools/call" && (message.tool == "" || !f.cfg.allowed(message.tool, found.access)) {
			f.logf("refused call lease=%s tool=%q class=%s access=%s", found.id, message.tool, f.cfg.toolClass(message.tool), found.access)
			refuseCall(w, message)
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
			f.unauthorized(w)
			return
		case issued.status != http.StatusOK:
			f.logf("lease=%s refused %s by the exchange (%s)", found.id, operation, issued.reason)
			writeJSON(w, http.StatusForbidden, map[string]string{"error": "the lease does not allow this"})
			return
		}
		credential = issued.credential
	}
	started := f.now()
	status, size := f.forward(w, r, message, body, found, credential)
	if message != nil && message.Method == "tools/call" {
		// The audit line: never an argument, a token or a credential.
		f.logf("call lease=%s tool=%q class=%s status=%d bytes=%d duration=%s", found.id, message.tool, operation, status, size, f.now().Sub(started).Round(time.Millisecond))
	}
}

// forward sends the request to the server with the credential in the
// header the spec names, and relays the answer filtered and scrubbed.
// It returns the status and the number of bytes relayed.
func (f *front) forward(w http.ResponseWriter, r *http.Request, message *rpcMessage, body []byte, found lease, credential string) (int, int) {
	out, err := http.NewRequestWithContext(r.Context(), r.Method, f.cfg.upstream.String(), bytes.NewReader(body))
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "the server is unavailable"})
		return http.StatusBadGateway, 0
	}
	if r.Method != http.MethodPost {
		out.Body = http.NoBody
		out.ContentLength = 0
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
	for _, name := range forwardResponseHeaders {
		if value := response.Header.Get(name); value != "" {
			w.Header().Set(name, scrubText(value, credential))
		}
	}
	if response.StatusCode < 300 {
		if message != nil && message.Method == "initialize" {
			if session := response.Header.Get("Mcp-Session-Id"); session != "" {
				f.sessions.bind(session, found.id)
			}
		}
		if r.Method == http.MethodDelete {
			f.sessions.forget(r.Header.Get("Mcp-Session-Id"))
		}
	}
	scrub := newScrubber(credential)
	var filter func([]byte) []byte
	if message != nil && message.Method == "tools/list" {
		filter = func(data []byte) []byte { return f.filterToolsList(data, message.ID, found.access) }
	}
	if strings.HasPrefix(strings.ToLower(response.Header.Get("Content-Type")), "text/event-stream") {
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("X-Accel-Buffering", "no")
		w.WriteHeader(response.StatusCode)
		size, err := relayEvents(w, response.Body, filter, scrub)
		if err != nil && r.Context().Err() == nil {
			f.logf("lease=%s: stream ended (%s)", found.id, scrubText(err.Error(), credential))
		}
		return response.StatusCode, size
	}
	raw, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBody+1))
	if err != nil || len(raw) > maxResponseBody {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "the server's answer is unreadable or too large"})
		return http.StatusBadGateway, 0
	}
	if filter != nil {
		raw = filter(raw)
	}
	raw = scrub.apply(raw)
	w.Header().Set("Content-Length", strconv.Itoa(len(raw)))
	w.WriteHeader(response.StatusCode)
	w.Write(raw)
	return response.StatusCode, len(raw)
}

// filterToolsList hides the tools the access level may not call from a
// tools/list result answering request id; anything else passes unchanged.
func (f *front) filterToolsList(data []byte, id json.RawMessage, access string) []byte {
	var answer map[string]json.RawMessage
	if err := json.Unmarshal(data, &answer); err != nil {
		return data
	}
	if !bytes.Equal(bytes.TrimSpace(answer["id"]), bytes.TrimSpace(id)) {
		return data
	}
	var result map[string]json.RawMessage
	if err := json.Unmarshal(answer["result"], &result); err != nil {
		return data
	}
	var tools []json.RawMessage
	if err := json.Unmarshal(result["tools"], &tools); err != nil {
		return data
	}
	kept := make([]json.RawMessage, 0, len(tools))
	for _, tool := range tools {
		var named struct {
			Name string `json:"name"`
		}
		if json.Unmarshal(tool, &named) == nil && f.cfg.allowed(named.Name, access) {
			kept = append(kept, tool)
		}
	}
	result["tools"] = marshal(kept)
	answer["result"] = marshal(result)
	return marshal(answer)
}

func marshal(value any) []byte {
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	encoder.Encode(value)
	return bytes.TrimRight(buffer.Bytes(), "\n")
}

// rpcMessage is the part of one JSON-RPC message the front decides on.
type rpcMessage struct {
	ID     json.RawMessage `json:"id"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
	tool   string
}

func parseMessage(body []byte) (*rpcMessage, error) {
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) > 0 && trimmed[0] == '[' {
		return nil, errors.New("JSON-RPC batches are not supported")
	}
	var message rpcMessage
	if err := json.Unmarshal(trimmed, &message); err != nil {
		return nil, errors.New("the body is not a JSON-RPC message")
	}
	if message.Method == "tools/call" {
		var params struct {
			Name string `json:"name"`
		}
		if json.Unmarshal(message.Params, &params) != nil || params.Name == "" {
			// A call the front cannot classify is never forwarded.
			params.Name = ""
		}
		message.tool = params.Name
	}
	return &message, nil
}

// checkRoutingHeaders refuses mirrored routing headers that disagree with
// the body (the 2026-07-28 Mcp-Method and Mcp-Name): the front decides on
// the body, and a server routing on the header must see the same.
func checkRoutingHeaders(header http.Header, message *rpcMessage) error {
	if method := header.Get("Mcp-Method"); method != "" && method != message.Method {
		return errors.New("Mcp-Method does not match the body")
	}
	if name := header.Get("Mcp-Name"); name != "" && message.Method == "tools/call" && name != message.tool {
		return errors.New("Mcp-Name does not match the body")
	}
	return nil
}

// refuseCall answers a tools/call the binding may not make as the server
// answers an unknown tool, without forwarding it.
func refuseCall(w http.ResponseWriter, message *rpcMessage) {
	id := message.ID
	if len(id) == 0 {
		id = json.RawMessage("null")
	}
	name := message.tool
	if name == "" {
		name = "(unnamed)"
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"jsonrpc": "2.0",
		"id":      id,
		"error": map[string]any{
			"code":    -32602,
			"message": "Unknown tool: " + name,
		},
	})
}

// scrubber replaces exact occurrences of the upstream credential, as sent
// and as JSON escapes it, in everything relayed to the caller.
type scrubber struct {
	secrets [][]byte
}

func newScrubber(credential string) *scrubber {
	s := &scrubber{}
	if credential == "" {
		return s
	}
	s.secrets = append(s.secrets, []byte(credential))
	if quoted, err := json.Marshal(credential); err == nil {
		escaped := quoted[1 : len(quoted)-1]
		if !bytes.Equal(escaped, []byte(credential)) {
			s.secrets = append(s.secrets, escaped)
		}
	}
	return s
}

func (s *scrubber) apply(data []byte) []byte {
	for _, secret := range s.secrets {
		data = bytes.ReplaceAll(data, secret, []byte(redacted))
	}
	return data
}

func scrubText(text, credential string) string {
	return string(newScrubber(credential).apply([]byte(text)))
}

// relayEvents copies a server-sent event stream event by event, each
// scrubbed and the data of each passed through filter (a tools/list
// answer), flushing after every event. It returns the bytes written.
func relayEvents(w http.ResponseWriter, body io.Reader, filter func([]byte) []byte, scrub *scrubber) (int, error) {
	flusher, _ := w.(http.Flusher)
	reader := bufio.NewReaderSize(body, 64*1024)
	written := 0
	var event [][]byte
	size := 0
	emit := func() error {
		if len(event) == 0 {
			return nil
		}
		var lines [][]byte
		if filter != nil {
			var data [][]byte
			for _, line := range event {
				if value, ok := bytes.CutPrefix(line, []byte("data:")); ok {
					data = append(data, bytes.TrimPrefix(value, []byte(" ")))
				} else {
					lines = append(lines, line)
				}
			}
			if len(data) > 0 {
				lines = append(lines, append([]byte("data: "), filter(bytes.Join(data, []byte("\n")))...))
			}
		} else {
			lines = event
		}
		var out bytes.Buffer
		for _, line := range lines {
			out.Write(scrub.apply(line))
			out.WriteByte('\n')
		}
		out.WriteByte('\n')
		n, err := w.Write(out.Bytes())
		written += n
		if flusher != nil {
			flusher.Flush()
		}
		event = nil
		size = 0
		return err
	}
	for {
		line, err := reader.ReadBytes('\n')
		if len(line) > 0 {
			line = bytes.TrimRight(line, "\r\n")
			if len(line) == 0 {
				if werr := emit(); werr != nil {
					return written, werr
				}
			} else {
				size += len(line)
				if size > maxEventBytes {
					return written, errors.New("an event exceeds the size cap")
				}
				event = append(event, append([]byte(nil), line...))
			}
		}
		if err != nil {
			if werr := emit(); werr != nil {
				return written, werr
			}
			if errors.Is(err, io.EOF) {
				return written, nil
			}
			return written, err
		}
	}
}

// inflight caps the calls one binding (and the whole pod) has open.
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

// sessionOwners keeps each server session to the lease that opened it, so
// one binding cannot speak in another's session on a shared server.
type sessionOwners struct {
	mu     sync.Mutex
	owners map[string]string
	order  []string
}

func (s *sessionOwners) bind(session, leaseID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, known := s.owners[session]; !known {
		s.order = append(s.order, session)
	}
	s.owners[session] = leaseID
	for len(s.order) > maxSessions {
		delete(s.owners, s.order[0])
		s.order = s.order[1:]
	}
}

func (s *sessionOwners) forget(session string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.owners, session)
}

// mayUse: a session the front does not know (opened before it started, or
// forgotten) is the server's to refuse; a known one only by its owner.
func (s *sessionOwners) mayUse(session, leaseID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	owner, known := s.owners[session]
	return !known || owner == leaseID
}
