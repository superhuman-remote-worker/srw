package main

import (
	"context"
	"encoding/base64"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// A stdio server (D5b) runs behind SRW's stdio bridge, one process per
// binding (drivers/mcp-bridge). The front stays the authorization boundary
// and tells the bridge three things: which binding (lease) a request is
// for, the binding's credential (which the bridge puts in its process's
// environment, never the pod's Secret), and when a binding ended, so its
// process stops with it. A caller can never set the two headers: the
// front forwards only the headers it lists.
const (
	bridgeBindingHeader    = "Srw-Bridge-Binding"
	bridgeCredentialHeader = "Srw-Bridge-Credential"
	bridgeBindingsPath     = "/srw/bindings/"
	bridgeLivenessPath     = "/srw/livez"
	maxBridgeBindings      = 4096
)

// A binding with a process is re-checked this often, so its process stops
// within this plus the revocation lag of its lease's end even when no
// request or stream of it is open (a variable for the tests).
var bindingSweep = 30 * time.Second

// bridgeBindings are the bindings whose process the bridge started (an
// initialize answered for them), with the lease token the sweep re-checks.
// The token stays in the front's memory, as on every request.
type bridgeBindings struct {
	mu     sync.Mutex
	tokens map[string]string
	order  []string
}

func newBridgeBindings() *bridgeBindings {
	return &bridgeBindings{tokens: map[string]string{}}
}

func (b *bridgeBindings) track(leaseID, token string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, known := b.tokens[leaseID]; !known {
		// Past the bound the oldest is no longer swept: its process still
		// stops when idle.
		for len(b.order) >= maxBridgeBindings {
			delete(b.tokens, b.order[0])
			b.order = b.order[1:]
		}
		b.order = append(b.order, leaseID)
	}
	b.tokens[leaseID] = token
}

func (b *bridgeBindings) forget(leaseID string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, known := b.tokens[leaseID]; known {
		delete(b.tokens, leaseID)
		b.order = without(b.order, leaseID)
	}
}

func (b *bridgeBindings) snapshot() map[string]string {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make(map[string]string, len(b.tokens))
	for leaseID, token := range b.tokens {
		out[leaseID] = token
	}
	return out
}

// setBridgeHeaders names the request's binding and hands over its
// credential (base64: any credential fits a header).
func setBridgeHeaders(header http.Header, leaseID, credential string) {
	header.Set(bridgeBindingHeader, leaseID)
	if credential != "" {
		header.Set(bridgeCredentialHeader, base64.StdEncoding.EncodeToString([]byte(credential)))
	}
}

// bridgeURL is one of the bridge's own routes, beside the MCP path.
func (f *front) bridgeURL(path string) string {
	return (&url.URL{Scheme: f.cfg.upstream.Scheme, Host: f.cfg.upstream.Host, Path: path}).String()
}

// endBinding tells the bridge a binding ended: it stops the binding's
// process.
func (f *front) endBinding(leaseID string) {
	f.bindings.forget(leaseID)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodDelete, f.bridgeURL(bridgeBindingsPath+url.PathEscape(leaseID)), http.NoBody)
	if err != nil {
		return
	}
	response, err := f.upstream.Do(request)
	if err != nil {
		f.logf("lease=%s ended; the bridge did not answer (its process stops when idle)", leaseID)
		return
	}
	response.Body.Close()
	f.logf("lease=%s ended: its process stops (bridge HTTP %d)", leaseID, response.StatusCode)
}

// sweepBindings ends the bindings whose lease is no longer live until ctx
// ends.
func (f *front) sweepBindings(ctx context.Context) {
	ticker := time.NewTicker(bindingSweep)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			f.sweepOnce(ctx)
		}
	}
}

// sweepOnce re-checks each binding with a process (from the cache: at most
// the revocation lag old) and ends those whose lease ended.
func (f *front) sweepOnce(ctx context.Context) {
	for leaseID, token := range f.bindings.snapshot() {
		found, err := f.auth.lease(ctx, token)
		switch {
		case errors.Is(err, errFrontRevoked):
			return // this pod is stopping, and every process with it
		case err != nil:
			continue // the exchange is unavailable: ask again next time
		case !found.active || !strings.EqualFold(found.connectorID, f.cfg.connectorID):
			f.endBinding(leaseID)
		}
	}
}

// bridgeAlive reports whether the bridge answers its liveness route.
func (f *front) bridgeAlive(ctx context.Context) bool {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, f.bridgeURL(bridgeLivenessPath), http.NoBody)
	if err != nil {
		return false
	}
	response, err := f.upstream.Do(request)
	if err != nil {
		return false
	}
	response.Body.Close()
	return response.StatusCode == http.StatusOK
}
