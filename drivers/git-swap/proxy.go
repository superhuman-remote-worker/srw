package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// The username SRW's clone URL always used with a forge token
	// (https://oauth2:<token>@host/...): GitHub, GitLab and Gitea accept it.
	upstreamUsername = "oauth2"
	// A transfer may sit this long with no byte moving either way (GitLab's
	// git ingress waits an hour; a large repository counts objects for
	// minutes before its first progress line).
	defaultIdleTimeout  = 15 * time.Minute
	maxInFlightPerLease = 32
	maxInFlight         = 512
	copyBuffer          = 32 * 1024
	leaseRequired       = "a live lease of this connector is required: SRW's credential helper supplies it"
	leaseRefused        = "the lease is revoked or expired: this execution no longer holds this connector"
	readOnlyRefused     = "this binding is read-only: SRW's git swap driver refuses every push (git-receive-pack)"
	lfsRefused          = "Git LFS is not supported by SRW's git swap driver (v1): LFS objects cannot be fetched or pushed through this connector"
)

var (
	gitProtocolShape = regexp.MustCompile(`\A[A-Za-z0-9=:._ -]{1,256}\z`)
	userAgentShape   = regexp.MustCompile(`\Agit/[\x20-\x7e]{1,200}\z`)
)

type driver struct {
	cfg      *config
	auth     *authCache
	upstream *http.Client
	logf     func(string, ...any)
	now      func() time.Time
	idle     time.Duration
	inflight *inflight
}

func newDriver(cfg *config, a authority, upstream *http.Client, logf func(string, ...any), now func() time.Time) *driver {
	return &driver{
		cfg:      cfg,
		auth:     newAuthCache(a, now),
		upstream: upstream,
		logf:     logf,
		now:      now,
		idle:     defaultIdleTimeout,
		inflight: &inflight{perLease: map[string]int{}},
	}
}

// newUpstreamClient reaches the connector's upstream: HTTPS with the system
// roots, no proxy from the environment, no compression asked for (answers
// are filtered and scrubbed), and no redirect followed (go-git
// CVE-2026-41506: a followed redirect carries the credential elsewhere).
func newUpstreamClient() *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			Proxy:                 nil,
			DialContext:           (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
			TLSClientConfig:       &tls.Config{MinVersion: tls.VersionTLS12},
			TLSHandshakeTimeout:   15 * time.Second,
			ResponseHeaderTimeout: defaultIdleTimeout,
			DisableCompression:    true,
			MaxIdleConnsPerHost:   16,
			IdleConnTimeout:       90 * time.Second,
		},
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

// inflight caps the requests one lease has open, and the driver's total.
type inflight struct {
	mu       sync.Mutex
	total    int
	perLease map[string]int
}

func (i *inflight) acquire(id string) (func(), bool) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.total >= maxInFlight || i.perLease[id] >= maxInFlightPerLease {
		return nil, false
	}
	i.total++
	i.perLease[id]++
	return func() {
		i.mu.Lock()
		defer i.mu.Unlock()
		i.total--
		if i.perLease[id]--; i.perLease[id] <= 0 {
			delete(i.perLease, id)
		}
	}, true
}

// fail answers with a plain-text message, which git shows its user as
// "remote: <message>" for a discovery request.
func fail(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	io.WriteString(w, message+"\n")
}

// unauthorized asks git for credentials: it calls its credential helper,
// which answers with the binding's lease token.
func unauthorized(w http.ResponseWriter, message string) {
	w.Header().Set("WWW-Authenticate", `Basic realm="SRW git swap driver", charset="UTF-8"`)
	fail(w, http.StatusUnauthorized, message)
}

// clientLease is the lease token the client presents: Basic auth's password
// (what git's credential helper supplies), or a Bearer token.
func clientLease(r *http.Request) (string, bool) {
	header := r.Header.Get("Authorization")
	scheme, value, found := strings.Cut(header, " ")
	if !found {
		return "", false
	}
	value = strings.TrimSpace(value)
	switch {
	case strings.EqualFold(scheme, "Bearer"):
		return value, leaseShape.MatchString(value)
	case strings.EqualFold(scheme, "Basic"):
		raw, err := base64.StdEncoding.DecodeString(value)
		if err != nil {
			return "", false
		}
		_, password, ok := strings.Cut(string(raw), ":")
		return password, ok && leaseShape.MatchString(password)
	}
	return "", false
}

func (d *driver) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	started := d.now()
	rt, refused := parseRoute(r, d.cfg)
	if refused != nil {
		fail(w, refused.status, refused.message)
		return
	}
	if rt.endpoint == endpointLFS {
		// git-lfs shows a batch answer's "message"; a pass-through proxy
		// could hand the client the injected credential (GitLab copies
		// Authorization into its LFS batch answers).
		w.Header().Set("Content-Type", "application/vnd.git-lfs+json")
		w.Header().Set("Cache-Control", "no-store")
		w.WriteHeader(http.StatusNotImplemented)
		json.NewEncoder(w).Encode(map[string]string{"message": lfsRefused})
		return
	}
	token, ok := clientLease(r)
	if !ok {
		unauthorized(w, leaseRequired)
		return
	}
	found, err := d.auth.lease(r.Context(), token)
	switch {
	case errors.Is(err, errDriverRevoked):
		fail(w, http.StatusServiceUnavailable, "this driver is stopping; retry in a moment")
		return
	case err != nil:
		d.logf("lease check failed: %v", err)
		fail(w, http.StatusServiceUnavailable, "the lease exchange is unavailable; retry in a moment")
		return
	case !found.active || !strings.EqualFold(found.connectorID, d.cfg.connectorID):
		d.logf("refused a request without a live lease of this connector")
		unauthorized(w, leaseRefused)
		return
	}
	// The access level decides by path: both receive-pack forms are pushes.
	operation := "read"
	switch {
	case rt.writes() && found.access != "ReadWrite":
		d.logf("refused a push lease=%s access=%s", found.id, found.access)
		fail(w, http.StatusForbidden, readOnlyRefused)
		return
	case rt.writes():
		operation = "write"
	case found.access != "ReadOnly" && found.access != "ReadWrite":
		fail(w, http.StatusForbidden, "the lease's access level allows nothing here")
		return
	}
	if rt.query {
		fail(w, http.StatusBadRequest, "a git-upload-pack or git-receive-pack POST carries no query")
		return
	}
	if rt.endpoint == endpointRPC {
		if message := checkBody(r, rt.service); message != "" {
			fail(w, http.StatusUnsupportedMediaType, message)
			return
		}
	}
	release, admitted := d.inflight.acquire(found.id)
	if !admitted {
		fail(w, http.StatusTooManyRequests, "too many requests in flight for this binding")
		return
	}
	defer release()

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	controller := http.NewResponseController(w)
	watch := startIdle(d.idle, func() {
		cancel()
		past := time.Unix(1, 0)
		_ = controller.SetReadDeadline(past)
		_ = controller.SetWriteDeadline(past)
	})
	defer watch.stop()

	var body io.Reader = http.NoBody
	if rt.endpoint == endpointRPC {
		input := bufio.NewReaderSize(&touchReader{r: r.Body, watch: watch}, copyBuffer)
		body = input
		if rt.service == receivePack {
			head, err := parsePushHead(input)
			if err != nil {
				d.logf("refused a malformed push lease=%s: %v", found.id, err)
				fail(w, http.StatusBadRequest, "the push request is malformed or too large: "+err.Error())
				return
			}
			if d.refusePush(w, found.id, head, input) {
				return
			}
			// What was checked, canonically encoded, then the pack as it comes.
			body = io.MultiReader(bytes.NewReader(head.encoded), input)
		}
	}

	issued, err := d.auth.credential(ctx, token, operation)
	switch {
	case errors.Is(err, errDriverRevoked):
		fail(w, http.StatusServiceUnavailable, "this driver is stopping; retry in a moment")
		return
	case err != nil:
		fail(w, http.StatusServiceUnavailable, "the lease exchange is unavailable; retry in a moment")
		return
	case issued.status == http.StatusUnauthorized:
		d.logf("lease=%s refused by the exchange (%s)", found.id, issued.reason)
		unauthorized(w, leaseRefused)
		return
	case issued.status != http.StatusOK:
		d.logf("lease=%s refused %s by the exchange (%s)", found.id, operation, issued.reason)
		if operation == "write" {
			fail(w, http.StatusForbidden, readOnlyRefused)
		} else {
			fail(w, http.StatusForbidden, "the lease does not allow this")
		}
		return
	case !allowedUpstream(issued.allowed, d.cfg.upstream):
		d.logf("lease=%s: the exchange no longer allows %s", found.id, d.cfg.upstream.url)
		fail(w, http.StatusForbidden, "the connector's repository changed; this driver pod is being replaced, retry in a moment")
		return
	}
	status, size := d.forward(ctx, w, r, rt, body, issued.credential, found.id, watch)
	d.logf("lease=%s %s %s status=%d bytes=%d duration=%s", found.id, r.Method, rt.label(), status, size, d.now().Sub(started).Round(time.Millisecond))
}

func (r route) label() string {
	if r.endpoint == endpointRefs {
		return "info/refs?service=" + r.service
	}
	return r.service
}

// checkBody refuses an RPC body whose type is not the service's request,
// and an encoded push (the driver reads a push's commands; git never
// compresses one).
func checkBody(r *http.Request, service string) string {
	if got := r.Header.Get("Content-Type"); got != "application/x-"+service+"-request" {
		return "the body must be application/x-" + service + "-request"
	}
	switch encoding := strings.ToLower(strings.TrimSpace(r.Header.Get("Content-Encoding"))); encoding {
	case "", "identity":
	case "gzip":
		if service == receivePack {
			return "a push body is never compressed"
		}
	default:
		return "unsupported Content-Encoding " + encoding
	}
	return ""
}

func allowedUpstream(allowed []string, served upstream) bool {
	for _, item := range allowed {
		if other, err := parseUpstream(item); err == nil && other == served {
			return true
		}
	}
	return false
}

// refusePush answers a push that updates a ref the driver does not allow,
// after reading the client's body to its end (git sends the whole pack
// before it reads an answer). It reports whether it answered.
func (d *driver) refusePush(w http.ResponseWriter, leaseID string, head *pushHead, rest io.Reader) bool {
	reasons := make([]string, len(head.commands))
	refused := false
	for i, c := range head.commands {
		reasons[i] = refusal(c)
		verdict := "allowed"
		if reasons[i] != "" {
			refused = true
			verdict = "refused (" + reasons[i] + ")"
		}
		// The push audit: ref updates, never pack contents.
		d.logf("push lease=%s ref=%q old=%.12s new=%.12s %s", leaseID, c.ref, c.old, c.new, verdict)
	}
	if !refused {
		return false
	}
	if _, err := io.Copy(io.Discard, rest); err != nil {
		return true
	}
	w.Header().Set("Content-Type", "application/x-git-receive-pack-result")
	w.Header().Set("Cache-Control", "no-cache")
	var report bytes.Buffer
	if wrote, _ := writeReport(&report, head, reasons); !wrote {
		fail(w, http.StatusForbidden, "this push was refused: "+firstReason(reasons))
		return true
	}
	w.WriteHeader(http.StatusOK)
	w.Write(report.Bytes())
	return true
}

func firstReason(reasons []string) string {
	for _, reason := range reasons {
		if reason != "" {
			return reason
		}
	}
	return ""
}

// forward sends the request to the connector's configured upstream (never
// a host or path from the request) with the credential, and streams the
// answer back filtered and scrubbed. It returns the status and the bytes
// relayed.
func (d *driver) forward(ctx context.Context, w http.ResponseWriter, r *http.Request, rt route, body io.Reader, credential, leaseID string, watch *idleWatch) (int, int64) {
	target := d.cfg.upstream.url
	method := http.MethodPost
	accept := "application/x-" + rt.service + "-result"
	if rt.endpoint == endpointRefs {
		target += "/info/refs?service=" + url.QueryEscape(rt.service)
		method = http.MethodGet
		accept = "*/*"
	} else {
		target += "/" + rt.service
	}
	out, err := http.NewRequestWithContext(ctx, method, target, body)
	if err != nil {
		fail(w, http.StatusBadGateway, "the upstream request could not be built")
		return http.StatusBadGateway, 0
	}
	if method == http.MethodPost {
		// The same length: the checked head is re-encoded byte for byte.
		out.ContentLength = r.ContentLength
		out.Header.Set("Content-Type", "application/x-"+rt.service+"-request")
		if encoding := strings.ToLower(strings.TrimSpace(r.Header.Get("Content-Encoding"))); encoding == "gzip" {
			out.Header.Set("Content-Encoding", "gzip")
		}
	}
	out.Header.Set("Accept", accept)
	out.Header.Set("Pragma", "no-cache")
	out.Header.Set("Authorization", "Basic "+basicValue(credential))
	if protocol := r.Header.Get("Git-Protocol"); gitProtocolShape.MatchString(protocol) {
		out.Header.Set("Git-Protocol", protocol)
	}
	agent := r.Header.Get("User-Agent")
	if !userAgentShape.MatchString(agent) {
		agent = "git/srw-git-swap"
	}
	out.Header.Set("User-Agent", agent)

	response, err := d.upstream.Do(out)
	if err != nil {
		if ctx.Err() != nil && r.Context().Err() == nil {
			fail(w, http.StatusGatewayTimeout, "the upstream sent nothing for too long")
			return http.StatusGatewayTimeout, 0
		}
		d.logf("lease=%s: the upstream did not answer (%s)", leaseID, scrubText(err.Error(), credential))
		fail(w, http.StatusBadGateway, "the upstream did not answer")
		return http.StatusBadGateway, 0
	}
	defer response.Body.Close()
	if status, message := upstreamRefusal(response, credential); status != 0 {
		d.logf("lease=%s: %s", leaseID, message)
		fail(w, status, message)
		return status, 0
	}
	kind := "result"
	if rt.endpoint == endpointRefs {
		kind = "advertisement"
	}
	want := "application/x-" + rt.service + "-" + kind
	if got := response.Header.Get("Content-Type"); !strings.EqualFold(strings.TrimSpace(strings.Split(got, ";")[0]), want) {
		fail(w, http.StatusBadGateway, "the upstream did not answer as a smart HTTP git server")
		return http.StatusBadGateway, 0
	}
	encoding := strings.ToLower(strings.TrimSpace(response.Header.Get("Content-Encoding")))
	if encoding != "" && encoding != "identity" && rt.endpoint == endpointRefs {
		// A compressed advertisement cannot be filtered.
		fail(w, http.StatusBadGateway, "the upstream compressed its ref advertisement")
		return http.StatusBadGateway, 0
	}
	header := w.Header()
	header.Set("Content-Type", want)
	header.Set("Cache-Control", "no-cache, max-age=0, must-revalidate")
	header.Set("Pragma", "no-cache")
	header.Set("Expires", "Fri, 01 Jan 1980 00:00:00 GMT")
	if encoding != "" && encoding != "identity" {
		header.Set("Content-Encoding", encoding)
	}
	w.WriteHeader(http.StatusOK)
	sink := &flushWriter{w: w, controller: http.NewResponseController(w), watch: watch}
	scrub := newScrubber(sink, credential)
	source := &touchReader{r: response.Body, watch: watch}
	if rt.endpoint == endpointRefs && rt.service == uploadPack {
		err = filterAdvertisement(scrub, bufio.NewReaderSize(source, copyBuffer))
	} else {
		_, err = io.CopyBuffer(scrub, source, make([]byte, copyBuffer))
	}
	if finishErr := scrub.finish(); err == nil {
		err = finishErr
	}
	if err != nil && r.Context().Err() == nil {
		d.logf("lease=%s: the transfer ended early (%s)", leaseID, scrubText(err.Error(), credential))
		// The status is sent; only the connection can say it failed.
		panic(http.ErrAbortHandler)
	}
	return http.StatusOK, sink.written.Load()
}

// upstreamRefusal maps an upstream answer that is no success to the
// driver's own: never the upstream's body or its challenge, never a
// redirect (the driver follows none and passes none on).
func upstreamRefusal(response *http.Response, credential string) (int, string) {
	switch status := response.StatusCode; {
	case status == http.StatusOK:
		return 0, ""
	case status >= 300 && status < 400:
		host := "elsewhere"
		if location, err := url.Parse(response.Header.Get("Location")); err == nil && location.Hostname() != "" {
			host = scrubText(location.Hostname(), credential)
		}
		return http.StatusBadGateway, fmt.Sprintf("the upstream redirected to %s; SRW's git swap driver never follows a redirect with the connector's credential", host)
	case status == http.StatusUnauthorized:
		return http.StatusBadGateway, "the upstream refused the connector's credential (HTTP 401): its token is wrong or expired"
	case status == http.StatusForbidden:
		return http.StatusForbidden, "the upstream refused this with the connector's credential (HTTP 403)"
	case status == http.StatusNotFound:
		return http.StatusNotFound, "the upstream has no such repository, or the connector's credential cannot see it"
	default:
		return http.StatusBadGateway, fmt.Sprintf("the upstream failed (HTTP %d)", status)
	}
}

// flushWriter sends each chunk on at once (no buffering) and counts it.
type flushWriter struct {
	w          http.ResponseWriter
	controller *http.ResponseController
	watch      *idleWatch
	written    atomic.Int64
}

func (f *flushWriter) Write(p []byte) (int, error) {
	n, err := f.w.Write(p)
	f.written.Add(int64(n))
	if err == nil {
		err = f.controller.Flush()
	}
	f.watch.touch()
	return n, err
}

// touchReader resets the idle watch whenever bytes arrive.
type touchReader struct {
	r     io.Reader
	watch *idleWatch
}

func (t *touchReader) Read(p []byte) (int, error) {
	n, err := t.r.Read(p)
	if n > 0 {
		t.watch.touch()
	}
	return n, err
}

// idleWatch runs fire once no byte has moved for d. The request body is
// read on the upstream transport's goroutine and the answer written on the
// handler's, so both touch it.
type idleWatch struct {
	d       time.Duration
	mu      sync.Mutex
	timer   *time.Timer
	stopped bool
}

func startIdle(d time.Duration, fire func()) *idleWatch {
	return &idleWatch{d: d, timer: time.AfterFunc(d, fire)}
}

func (i *idleWatch) touch() {
	i.mu.Lock()
	defer i.mu.Unlock()
	if !i.stopped {
		i.timer.Reset(i.d)
	}
}

func (i *idleWatch) stop() {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.stopped = true
	i.timer.Stop()
}
