package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// probeEvery bounds how often /readyz reaches the server: the kubelet asks
// every few seconds, and a probe opens (and closes) an MCP session.
const probeEvery = 5 * time.Second

// Behind the stdio bridge every probe starts a process of the server, too
// slow and too costly for the kubelet's every few seconds: the probe runs
// in the background instead, every bridgeProbeRetry until the server
// answered and every bridgeProbeEvery after, with a deadline that allows a
// process's start. /readyz answers from its last result and the bridge's
// own liveness (variables for the tests).
var (
	bridgeProbeEvery   = 5 * time.Minute
	bridgeProbeRetry   = 5 * time.Second
	bridgeProbeTimeout = 60 * time.Second
)

// prober answers /readyz from a real MCP probe of the server: initialize,
// notifications/initialized, tools/list and, when the server opened a
// session, DELETE. The probe carries no credential (the front has no lease
// of its own), so a server that refuses to list its tools without one is
// never ready. The tool list's hash is pinned on the first success; a
// change is logged, and refused with tool_pinning "block".
type prober struct {
	cfg    *config
	client *http.Client
	logf   func(string, ...any)
	now    func() time.Time
	// The stdio bridge's liveness (D5b).
	alive func(context.Context) bool

	mu     sync.Mutex
	at     time.Time
	ready  bool
	reason string
	tools  int
	pinned string
	// Set once the tool list changed under tool_pinning "block": every
	// request is refused, on new and kept-alive connections alike, until
	// the pod is replaced.
	held atomic.Bool
}

// blocked: the tool list changed under "block".
func (p *prober) blocked() bool {
	return p.held.Load()
}

func (f *front) serveReady(w http.ResponseWriter, r *http.Request) {
	ready, reason, tools := f.probe.check(r.Context())
	if !ready {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"ready": false, "reason": reason})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ready": true, "tools": tools})
}

func (p *prober) check(ctx context.Context) (bool, string, int) {
	if p.cfg.bridge {
		return p.bridgeCheck(ctx)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	now := p.now()
	if !p.at.IsZero() && now.Sub(p.at) < probeEvery {
		return p.ready, p.reason, p.tools
	}
	probeCtx, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()
	hash, tools, err := p.run(probeCtx)
	p.record(now, hash, tools, err)
	return p.ready, p.reason, p.tools
}

// bridgeCheck answers /readyz for a stdio server: the background probe's
// last result, while the bridge answers.
func (p *prober) bridgeCheck(ctx context.Context) (bool, string, int) {
	p.mu.Lock()
	probed, ready, reason, tools := !p.at.IsZero(), p.ready, p.reason, p.tools
	p.mu.Unlock()
	switch {
	case !probed:
		return false, "the server was not probed yet", 0
	case !ready:
		return false, reason, 0
	case !p.alive(ctx):
		return false, "the stdio bridge does not answer", 0
	}
	return true, "", tools
}

// loop probes a stdio server in the background until ctx ends.
func (p *prober) loop(ctx context.Context) {
	for {
		ready := p.probeOnce(ctx)
		wait := bridgeProbeRetry
		if ready {
			wait = bridgeProbeEvery
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
	}
}

// probeOnce runs one probe and records it; it reports readiness.
func (p *prober) probeOnce(ctx context.Context) bool {
	probeCtx, cancel := context.WithTimeout(ctx, bridgeProbeTimeout)
	defer cancel()
	hash, tools, err := p.run(probeCtx)
	p.mu.Lock()
	defer p.mu.Unlock()
	p.record(p.now(), hash, tools, err)
	return p.ready
}

// record keeps a probe's result and pins the tool list's hash on the first
// success; the caller holds p.mu.
func (p *prober) record(now time.Time, hash string, tools int, err error) {
	p.at = now
	switch {
	case err != nil:
		p.ready, p.reason, p.tools = false, err.Error(), 0
	case p.pinned == "":
		p.pinned = hash
		p.ready, p.reason, p.tools = true, "", tools
		p.logf("server ready: %d tools, tool list %s", tools, hash[:16])
	case hash != p.pinned:
		p.logf("the server's tool list changed under this image (%s, pinned %s)", hash[:16], p.pinned[:16])
		if p.cfg.toolPinning == "block" {
			p.held.Store(true)
			p.ready, p.reason, p.tools = false, "the tool list changed under this image", 0
		} else {
			p.ready, p.reason, p.tools = true, "", tools
		}
	default:
		p.ready, p.reason, p.tools = true, "", tools
	}
}

// run opens one probe session and returns the tool list's hash and size.
func (p *prober) run(ctx context.Context) (string, int, error) {
	initialize := map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "initialize",
		"params": map[string]any{
			"protocolVersion": "2025-06-18",
			"capabilities":    map[string]any{},
			"clientInfo":      map[string]any{"name": "srw-mcp-front", "version": version},
		},
	}
	answer, session, err := p.call(ctx, http.MethodPost, initialize, "")
	if err != nil {
		return "", 0, fmt.Errorf("initialize: %w", err)
	}
	if _, ok := answer["result"]; !ok {
		return "", 0, errors.New("initialize: the server answered without a result")
	}
	if session != "" {
		defer func() {
			closing, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			p.call(closing, http.MethodDelete, nil, session)
		}()
	}
	if _, _, err := p.call(ctx, http.MethodPost, map[string]any{"jsonrpc": "2.0", "method": "notifications/initialized"}, session); err != nil {
		return "", 0, fmt.Errorf("initialized: %w", err)
	}
	answer, _, err = p.call(ctx, http.MethodPost, map[string]any{"jsonrpc": "2.0", "id": 2, "method": "tools/list"}, session)
	if err != nil {
		return "", 0, fmt.Errorf("tools/list: %w", err)
	}
	var result struct {
		Tools []any `json:"tools"`
	}
	if raw, ok := answer["result"]; !ok || json.Unmarshal(raw, &result) != nil {
		return "", 0, errors.New("tools/list: the server answered without tools")
	}
	digest := sha256.Sum256(marshal(result.Tools))
	return hex.EncodeToString(digest[:]), len(result.Tools), nil
}

// call sends one message to the server and reads the JSON-RPC answer from a
// JSON body or the first event of a stream. A notification's 202 answers
// nothing; DELETE's answer is ignored.
func (p *prober) call(ctx context.Context, method string, message any, session string) (map[string]json.RawMessage, string, error) {
	var body io.Reader = http.NoBody
	if message != nil {
		body = bytes.NewReader(marshal(message))
	}
	request, err := http.NewRequestWithContext(ctx, method, p.cfg.upstream.String(), body)
	if err != nil {
		return nil, "", err
	}
	request.Host = p.cfg.upstream.Host
	request.Header.Set("Accept", "application/json, text/event-stream")
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Mcp-Protocol-Version", "2025-06-18")
	if session != "" {
		request.Header.Set("Mcp-Session-Id", session)
	}
	response, err := p.client.Do(request)
	if err != nil {
		return nil, "", errors.New("the server does not answer")
	}
	defer response.Body.Close()
	if method == http.MethodDelete || response.StatusCode == http.StatusAccepted {
		return nil, session, nil
	}
	if response.StatusCode != http.StatusOK {
		return nil, "", fmt.Errorf("HTTP %d", response.StatusCode)
	}
	if found := response.Header.Get("Mcp-Session-Id"); found != "" {
		session = found
	}
	reader := io.LimitReader(response.Body, maxResponseBody)
	var data []byte
	if strings.HasPrefix(strings.ToLower(response.Header.Get("Content-Type")), "text/event-stream") {
		data, err = firstEventData(reader)
		if err != nil {
			return nil, "", err
		}
	} else if data, err = io.ReadAll(reader); err != nil {
		return nil, "", err
	}
	var answer map[string]json.RawMessage
	if err := json.Unmarshal(data, &answer); err != nil {
		return nil, "", errors.New("the answer is not JSON-RPC")
	}
	if raw, failed := answer["error"]; failed {
		var detail struct {
			Code int `json:"code"`
		}
		json.Unmarshal(raw, &detail)
		return nil, "", fmt.Errorf("JSON-RPC error %d", detail.Code)
	}
	return answer, session, nil
}

// firstEventData returns the data of the first event of a stream that
// carries any.
func firstEventData(body io.Reader) ([]byte, error) {
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 64*1024), maxEventBytes)
	var data [][]byte
	for scanner.Scan() {
		line := bytes.TrimRight(scanner.Bytes(), "\r")
		if len(line) == 0 {
			if len(data) > 0 {
				return bytes.Join(data, []byte("\n")), nil
			}
			continue
		}
		if value, ok := bytes.CutPrefix(line, []byte("data:")); ok {
			data = append(data, append([]byte(nil), bytes.TrimPrefix(value, []byte(" "))...))
		}
	}
	if len(data) > 0 {
		return bytes.Join(data, []byte("\n")), nil
	}
	return nil, errors.New("the stream ended without an answer")
}
