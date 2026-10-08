package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"sync"
	"time"
)

const (
	introspectPath = "/v1/leases/introspect"
	exchangePath   = "/v1/leases/exchange"
	// The longest a lease decision or a credential is reused: the
	// revocation lag (shared/connectors/leases.py MAX_CACHE_SECONDS).
	maxCache = 30 * time.Second
	// How long a refusal is remembered, so a client retrying a dead lease
	// does not reach the exchange on every request.
	negativeCache = 5 * time.Second
	maxCacheItems = 4096
	// The exchange's answers are a few hundred bytes.
	maxExchangeAnswer = 64 * 1024
)

var (
	// errUnavailable: the exchange did not answer; nothing is cached.
	errUnavailable = errors.New("the lease exchange is unavailable")
	// errDriverRevoked: the exchange refused this pod's own identity (the
	// pod is being stopped).
	errDriverRevoked = errors.New("this driver's identity was revoked")
)

// lease is what introspection says about a lease token; never the token.
type lease struct {
	active      bool
	id          string
	connectorID string
	access      string
	expires     time.Time
}

// grant is an exchanged credential, or why the exchange refused it.
type grant struct {
	credential string
	// The username the credential is presented upstream with, as the
	// exchange names it (defaultUpstreamUsername when it names none).
	username string
	allowed  []string
	status   int    // 200, or the HTTP status to answer the client with
	reason   string // the exchange's refusal, for the log
	cache    time.Duration
}

// usernameShape is what a username the exchange names may be: it goes into
// a Basic credential, so never a colon, a space or a control character.
var usernameShape = regexp.MustCompile(`\A[A-Za-z0-9._-]{1,64}\z`)

// upstreamUsername is the username of an exchange answer: the one it
// names, the default when it names none; false for one that may not be used.
func upstreamUsername(answer map[string]any) (string, bool) {
	named, present := answer["username"]
	if !present || named == nil {
		return defaultUpstreamUsername, true
	}
	text, ok := named.(string)
	if !ok || !usernameShape.MatchString(text) {
		return "", false
	}
	return text, true
}

// authority is the lease exchange as the driver calls it.
type authority interface {
	introspect(ctx context.Context, token string) (lease, error)
	exchange(ctx context.Context, token, operation string) (grant, error)
}

type httpAuthority struct {
	base     string
	identity string
	client   *http.Client
}

func newHTTPAuthority(base, identity string) *httpAuthority {
	return &httpAuthority{
		base:     base,
		identity: identity,
		client: &http.Client{
			Timeout:   10 * time.Second,
			Transport: &http.Transport{Proxy: nil, DisableCompression: true},
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}
}

func (a *httpAuthority) post(ctx context.Context, path string, body map[string]string) (int, map[string]any, error) {
	payload, _ := json.Marshal(body)
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, a.base+path, bytes.NewReader(payload))
	if err != nil {
		return 0, nil, errUnavailable
	}
	request.Header.Set("Authorization", "Bearer "+a.identity)
	request.Header.Set("Content-Type", "application/json")
	response, err := a.client.Do(request)
	if err != nil {
		return 0, nil, errUnavailable
	}
	defer response.Body.Close()
	var answer map[string]any
	if err := json.NewDecoder(io.LimitReader(response.Body, maxExchangeAnswer)).Decode(&answer); err != nil {
		answer = map[string]any{}
	}
	return response.StatusCode, answer, nil
}

func (a *httpAuthority) introspect(ctx context.Context, token string) (lease, error) {
	status, answer, err := a.post(ctx, introspectPath, map[string]string{"lease_token": token})
	if err != nil {
		return lease{}, err
	}
	switch {
	case status == http.StatusUnauthorized:
		return lease{}, errDriverRevoked
	case status == http.StatusForbidden:
		// A lease of another connector: never this pod's.
		return lease{}, nil
	case status != http.StatusOK:
		return lease{}, errUnavailable
	}
	active, _ := answer["active"].(bool)
	if !active {
		return lease{}, nil
	}
	found := lease{active: true}
	found.id, _ = answer["lease_id"].(string)
	found.connectorID, _ = answer["connector_id"].(string)
	found.access, _ = answer["access"].(string)
	if text, ok := answer["expires_at"].(string); ok {
		found.expires, _ = time.Parse(time.RFC3339Nano, text)
	}
	if found.id == "" || found.connectorID == "" {
		return lease{}, errUnavailable
	}
	return found, nil
}

func (a *httpAuthority) exchange(ctx context.Context, token, operation string) (grant, error) {
	status, answer, err := a.post(ctx, exchangePath, map[string]string{"lease_token": token, "operation": operation})
	if err != nil {
		return grant{}, err
	}
	reason, _ := answer["error"].(string)
	switch status {
	case http.StatusOK:
		credential, _ := answer["credential"].(string)
		seconds, _ := answer["max_cache_seconds"].(float64)
		username, ok := upstreamUsername(answer)
		if !ok {
			// Not a name a Basic credential may carry: an orchestrator bug,
			// never forwarded.
			return grant{}, errUnavailable
		}
		found := grant{credential: credential, username: username, status: http.StatusOK, cache: time.Duration(seconds) * time.Second}
		if items, ok := answer["allowed_upstream"].([]any); ok {
			for _, item := range items {
				if text, ok := item.(string); ok {
					found.allowed = append(found.allowed, text)
				}
			}
		}
		return found, nil
	case http.StatusUnauthorized:
		return grant{}, errDriverRevoked
	case http.StatusForbidden:
		if reason == "operation_not_allowed" {
			return grant{status: http.StatusForbidden, reason: reason}, nil
		}
		// Revoked, expired or another connector's: the client's lease is dead.
		return grant{status: http.StatusUnauthorized, reason: reason}, nil
	}
	return grant{}, errUnavailable
}

// authCache keeps lease decisions and credentials for the revocation lag at
// most, keyed by a digest of the lease token (never the token).
type authCache struct {
	authority authority
	now       func() time.Time
	mu        sync.Mutex
	leases    map[[32]byte]cachedLease
	grants    map[string]cachedGrant
}

type cachedLease struct {
	lease lease
	until time.Time
}

type cachedGrant struct {
	grant grant
	until time.Time
}

func newAuthCache(a authority, now func() time.Time) *authCache {
	return &authCache{authority: a, now: now, leases: map[[32]byte]cachedLease{}, grants: map[string]cachedGrant{}}
}

func tokenKey(token string) [32]byte {
	return sha256.Sum256([]byte(token))
}

// lease returns what the exchange says about the token, from the cache when
// the decision is fresh: a live lease for at most maxCache (and never past
// its expiry), a refusal for negativeCache.
func (c *authCache) lease(ctx context.Context, token string) (lease, error) {
	key := tokenKey(token)
	now := c.now()
	c.mu.Lock()
	cached, ok := c.leases[key]
	c.mu.Unlock()
	if ok && now.Before(cached.until) {
		return cached.lease, nil
	}
	found, err := c.authority.introspect(ctx, token)
	if err != nil {
		return lease{}, err
	}
	until := now.Add(negativeCache)
	if found.active {
		until = now.Add(maxCache)
		if !found.expires.IsZero() && found.expires.Before(until) {
			until = found.expires
		}
	}
	c.mu.Lock()
	c.prune(now)
	c.leases[key] = cachedLease{lease: found, until: until}
	c.mu.Unlock()
	return found, nil
}

// credential exchanges the token for the upstream credential of one
// operation, reusing it for the exchange's max_cache_seconds (at most
// maxCache).
func (c *authCache) credential(ctx context.Context, token, operation string) (grant, error) {
	digest := tokenKey(token)
	key := fmt.Sprintf("%x:%s", digest[:], operation)
	now := c.now()
	c.mu.Lock()
	cached, ok := c.grants[key]
	c.mu.Unlock()
	if ok && now.Before(cached.until) {
		return cached.grant, nil
	}
	found, err := c.authority.exchange(ctx, token, operation)
	if err != nil {
		return grant{}, err
	}
	keep := negativeCache
	if found.status == http.StatusOK {
		keep = found.cache
		if keep > maxCache {
			keep = maxCache
		}
	}
	if keep > 0 {
		c.mu.Lock()
		c.prune(now)
		c.grants[key] = cachedGrant{grant: found, until: now.Add(keep)}
		c.mu.Unlock()
	}
	return found, nil
}

// prune drops expired entries, and every entry when the cache is still
// full; the caller holds the lock.
func (c *authCache) prune(now time.Time) {
	if len(c.leases)+len(c.grants) < maxCacheItems {
		return
	}
	for key, item := range c.leases {
		if !now.Before(item.until) {
			delete(c.leases, key)
		}
	}
	for key, item := range c.grants {
		if !now.Before(item.until) {
			delete(c.grants, key)
		}
	}
	if len(c.leases)+len(c.grants) >= maxCacheItems {
		c.leases = map[[32]byte]cachedLease{}
		c.grants = map[string]cachedGrant{}
	}
}
