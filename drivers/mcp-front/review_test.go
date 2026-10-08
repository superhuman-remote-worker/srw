package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// The D5a review's probes, kept as tests: a JSON-parsing differential must
// never let a read-only lease run a write tool, every answer carrying a
// tool list is filtered, unknown sessions are nobody's, only allowed
// methods pass, answers are bounded, and the credential is scrubbed in
// every form a server may echo it.

// views are the ways a server may read a forwarded body: exact keys with
// the last of a duplicate (encoding/json into a map, Python's json), exact
// keys with the first (a streaming parser), and Go's case-insensitive
// struct decoding. The front forwards a body all of them agree on.
func views(t *testing.T, body string) []string {
	t.Helper()
	var names []string
	var last map[string]any
	if err := json.Unmarshal([]byte(body), &last); err != nil {
		t.Fatalf("the forwarded body is not JSON: %s", body)
	}
	params, _ := last["params"].(map[string]any)
	name, _ := params["name"].(string)
	method, _ := last["method"].(string)
	names = append(names, method+"/"+name)
	members, err := objectMembers([]byte(body))
	if err != nil {
		t.Fatalf("the forwarded body has duplicate keys: %s", body)
	}
	firstMethod, _ := jsonString(func() json.RawMessage { v, _ := lookup(members, "method"); return v }())
	rawParams, _ := lookup(members, "params")
	firstName := ""
	if isObject(rawParams) {
		nested, err := objectMembers(rawParams)
		if err != nil {
			t.Fatalf("the forwarded params have duplicate keys: %s", body)
		}
		value, _ := lookup(nested, "name")
		firstName, _ = jsonString(value)
	}
	names = append(names, firstMethod+"/"+firstName)
	var folded struct {
		Method string `json:"method"`
		Params struct {
			Name string `json:"name"`
		} `json:"params"`
	}
	json.Unmarshal([]byte(body), &folded)
	names = append(names, folded.Method+"/"+folded.Params.Name)
	return names
}

func TestTheReviewersFourBypassBodiesAreRefused(t *testing.T) {
	bodies := []string{
		`{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"notes_write","Name":"whoami","arguments":{}}}`,
		`{"jsonrpc":"2.0","id":7,"method":"tools/call","Method":"ping","params":{"name":"notes_write","arguments":{}}}`,
		`{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"notes_write"},"Params":{"name":"whoami"}}`,
		`{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"notes_write"},"paramſ":{"name":"whoami"}}`,
		// And the reviewer's go-sdk differential corpus.
		`{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"notes_write","name":"whoami","arguments":{}}}`,
		`{"jsonrpc":"2.0","id":7,"method":"tools/call","method":"ping","params":{"name":"notes_write"}}`,
	}
	for _, body := range bodies {
		h := newHarness(t)
		response := h.do(t, http.MethodPost, tokenRO, body, nil)
		if response.Code != http.StatusBadRequest {
			t.Errorf("%s: %d %s", body, response.Code, response.Body)
		}
		if h.server.count() != 0 || strings.Contains(response.Body.String(), "called") {
			t.Errorf("%s reached the server", body)
		}
	}
}

// variants of one key: case changes, Unicode case folding (ſ folds to s,
// the Kelvin sign to k) and a JSON escape of the exact key.
func variants(key string) []string {
	out := []string{strings.ToUpper(key), strings.ToUpper(key[:1]) + key[1:], key + " "}
	if strings.Contains(key, "s") {
		out = append(out, strings.Replace(key, "s", "ſ", 1))
	}
	if strings.Contains(key, "k") {
		out = append(out, strings.Replace(key, "k", "K", 1))
	}
	return out
}

func TestNoKeyVariantLetsAReadOnlyLeaseRunAWriteTool(t *testing.T) {
	write, read := "notes_write", "whoami"
	var bodies []string
	for _, key := range []string{"jsonrpc", "id", "method", "params"} {
		for _, variant := range variants(key) {
			bodies = append(bodies,
				fmt.Sprintf(`{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":%q},%q:{"name":%q}}`, write, variant, read),
				fmt.Sprintf(`{%q:"tools/call","jsonrpc":"2.0","id":7,"method":"ping","params":{"name":%q}}`, variant, write),
			)
		}
	}
	for _, variant := range append(variants("name"), "namé") {
		bodies = append(bodies,
			fmt.Sprintf(`{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":%q,%q:%q}}`, write, variant, read),
			fmt.Sprintf(`{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{%q:%q,"name":%q}}`, variant, write, read),
		)
	}
	// Duplicates in both orders, escapes, whitespace, nesting and garbage.
	bodies = append(bodies,
		`{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"whoami","name":"notes_write"}}`,
		`{"jsonrpc":"2.0","id":7,"params":{"name":"whoami"},"method":"tools/call","params":{"name":"notes_write"}}`,
		`{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"notes_write"}}`,
		`{"jsonrpc":"2.0","id":7,"method":"tools\/call","params":{"name":"notes_write"}}`,
		` { "jsonrpc" : "2.0" , "id" : 7 , "method" : "tools/call" , "params" : { "name" : "notes_write" } } `,
		`{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"whoami"}}{"method":"tools/call","params":{"name":"notes_write"}}`,
		`{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"whoami","arguments":{"name":"notes_write"}}}`,
		`{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":["notes_write"]}}`,
		`{"jsonrpc":"2.0","id":7,"method":"tools/call","params":"notes_write"}`,
		"{\"jsonrpc\":\"2.0\",\"id\":7,\"method\":\"tools/call\",\"params\":{\"name\":\"whoami\xff\"}}",
		`{"jsonrpc":"2.0","id":null,"method":"tools/call","params":{"name":"notes_write"}}`,
		`{"jsonrpc":"1.0","id":7,"method":"tools/call","params":{"name":"notes_write"}}`,
		`{"id":7,"method":"tools/call","params":{"name":"notes_write"}}`,
	)
	for _, body := range bodies {
		h := newHarness(t)
		response := h.do(t, http.MethodPost, tokenRO, body, nil)
		answer := response.Body.String()
		if strings.Contains(answer, "called notes_write") {
			t.Fatalf("BYPASS: %s ran notes_write: %s", body, answer)
		}
		seen, forwarded := h.server.last()
		if seen == nil {
			continue // refused here
		}
		for _, view := range views(t, forwarded) {
			if view != "tools/call/whoami" {
				t.Fatalf("%s was forwarded as %s (views %v)", body, forwarded, views(t, forwarded))
			}
		}
	}
}

func TestTheServerGetsTheBodyTheFrontChecked(t *testing.T) {
	h := newHarness(t)
	body := ` { "jsonrpc" : "2.0", "id" : 7, "method" : "tools\/call", "params" : {"name":"whoami", "arguments" : { "x" : [ 1 , 2 ] }} } `
	if code := h.do(t, http.MethodPost, tokenRO, body, nil).Code; code != http.StatusOK {
		t.Fatalf("status %d", code)
	}
	_, forwarded := h.server.last()
	want := `{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"whoami","arguments":{"x":[1,2]}}}`
	if forwarded != want {
		t.Fatalf("forwarded %s, want %s", forwarded, want)
	}
}

func TestOnlyTheAllowedMethodsReachTheServer(t *testing.T) {
	h := newHarness(t)
	for _, method := range []string{"resources/list", "resources/read", "prompts/get", "completion/complete", "tasks/get", "logging/setLevel", "vendor/anything", "tools/call2"} {
		response := h.do(t, http.MethodPost, tokenA, rpc(4, method, map[string]any{}), nil)
		if !strings.Contains(response.Body.String(), `"code":-32601`) {
			t.Fatalf("%s: %d %s", method, response.Code, response.Body)
		}
	}
	// A notification nobody may answer is accepted and dropped.
	notification := `{"jsonrpc":"2.0","method":"notifications/roots/list_changed"}`
	if code := h.do(t, http.MethodPost, tokenA, notification, nil).Code; code != http.StatusAccepted {
		t.Fatalf("notification: %d", code)
	}
	if h.server.count() != 0 {
		t.Fatal("a refused method reached the server")
	}
	for _, body := range []string{
		rpc(5, "ping", nil),
		rpc(6, "tools/list", nil),
		`{"jsonrpc":"2.0","method":"notifications/initialized"}`,
		`{"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":6}}`,
		// A client's answer to a server request.
		`{"jsonrpc":"2.0","id":"srv-1","result":{}}`,
	} {
		before := h.server.count()
		if code := h.do(t, http.MethodPost, tokenA, body, nil).Code; code >= 400 {
			t.Fatalf("%s: %d", body, code)
		}
		if h.server.count() != before+1 {
			t.Fatalf("%s did not reach the server", body)
		}
	}
	for _, body := range []string{
		`{"jsonrpc":"2.0","id":"srv-1","result":{},"error":{}}`,
		`{"jsonrpc":"2.0","result":{}}`,
		`{"jsonrpc":"2.0","id":"srv-1","result":{},"params":{}}`,
	} {
		if code := h.do(t, http.MethodPost, tokenA, body, nil).Code; code != http.StatusBadRequest {
			t.Fatalf("%s: %d", body, code)
		}
	}
}

func TestEveryAnswerCarryingAToolListIsFiltered(t *testing.T) {
	// An id the server re-encodes, or no id the front asked for at all.
	for _, id := range []string{`1.0`, `"a"`, `1e0`} {
		h := newHarness(t)
		names := listed(t, h.do(t, http.MethodPost, tokenRO, `{"jsonrpc":"2.0","id":`+id+`,"method":"tools/list"}`, nil))
		if strings.Join(names, ",") != "whoami,notes_read,leak_credential" {
			t.Fatalf("id %s: %v", id, names)
		}
	}
	// A stream replaying answers (GET with Last-Event-ID), and a batch.
	h := newHarness(t)
	tools := `{"tools":[{"name":"whoami"},{"name":"notes_write"},{"name":"delete_everything"}]}`
	h.server.getEvents = []string{
		`id: 9` + "\n" + `data: {"jsonrpc":"2.0","id":99,"result":` + tools + `}`,
		`data: [{"jsonrpc":"2.0","id":100,"result":` + tools + `}]`,
	}
	stream := h.do(t, http.MethodGet, tokenRO, "", map[string]string{"Last-Event-ID": "8"}).Body.String()
	if strings.Contains(stream, "notes_write") || strings.Contains(stream, "delete_everything") || !strings.Contains(stream, "whoami") {
		t.Fatalf("the replay leaked: %s", stream)
	}
	if !strings.Contains(stream, "id: 9") {
		t.Fatalf("the event id was lost: %s", stream)
	}
}

func TestAnUnknownSessionIsNobodys(t *testing.T) {
	h := newHarness(t)
	// Never seen by this front (it restarted, or forgot it).
	response := h.do(t, http.MethodPost, tokenB, call("whoami"), map[string]string{"Mcp-Session-Id": "session-x"})
	if response.Code != http.StatusNotFound || h.server.count() != 0 {
		t.Fatalf("unknown session: %d, server reached %d", response.Code, h.server.count())
	}
}

func TestOneLeaseCannotEvictAnothersSessions(t *testing.T) {
	owners := newSessionOwners(time.Now)
	owners.bind("b-1", "lease-b")
	for i := 0; i < maxSessionsPerLease+5; i++ {
		owners.bind(fmt.Sprintf("a-%d", i), "lease-a")
	}
	if !owners.mayUse("b-1", "lease-b") {
		t.Fatal("lease a evicted lease b's session")
	}
	if owners.mayUse("a-0", "lease-a") || !owners.mayUse(fmt.Sprintf("a-%d", maxSessionsPerLease+4), "lease-a") {
		t.Fatal("lease a's own oldest session was not the one forgotten")
	}
	if len(owners.byLease["lease-a"]) != maxSessionsPerLease {
		t.Fatalf("lease a holds %d sessions", len(owners.byLease["lease-a"]))
	}
	// A session id reused by another lease belongs to that lease only.
	owners.bind("b-1", "lease-a")
	if owners.mayUse("b-1", "lease-b") {
		t.Fatal("a rebound session stayed with its old lease")
	}
}

type discardWriter struct {
	header http.Header
	n      int
	status int
}

func (d *discardWriter) Header() http.Header         { return d.header }
func (d *discardWriter) Write(b []byte) (int, error) { d.n += len(b); return len(b), nil }
func (d *discardWriter) WriteHeader(status int)      { d.status = status }

// peakHeap relays one answer of “size“ bytes and returns the status, the
// bytes relayed and the heap in use above the baseline at its peak.
func peakHeap(t *testing.T, size int) (int, int, uint64) {
	t.Helper()
	body := []byte(`{"jsonrpc":"2.0","id":7,"result":{"content":[{"type":"text","text":"` + strings.Repeat("a", size) + `"}]}}`)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(body)
	}))
	defer upstream.Close()
	f := newFront(testConfig(t, upstream.URL+"/mcp"), newFakeAuthority(), newUpstreamClient(), func(string, ...any) {}, time.Now)
	runtime.GC()
	var base runtime.MemStats
	runtime.ReadMemStats(&base)
	var peak atomic.Uint64
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		var m runtime.MemStats
		for {
			select {
			case <-stop:
				return
			default:
			}
			runtime.ReadMemStats(&m)
			if m.HeapInuse > peak.Load() {
				peak.Store(m.HeapInuse)
			}
			time.Sleep(time.Millisecond)
		}
	}()
	request := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(call("whoami")))
	request.Header.Set("Authorization", "Bearer "+tokenA)
	w := &discardWriter{header: http.Header{}}
	f.ServeHTTP(w, request)
	close(stop)
	<-done
	above := uint64(0)
	if peak.Load() > base.HeapInuse {
		above = peak.Load() - base.HeapInuse
	}
	return w.status, w.n, above
}

func TestALargeAnswerCannotExhaustTheFrontsMemory(t *testing.T) {
	status, relayed, above := peakHeap(t, 24<<20)
	t.Logf("24 MiB answer: status %d, %d bytes relayed, peak heap +%d MiB", status, relayed, above>>20)
	if status != http.StatusBadGateway || above > 24<<20 {
		t.Fatalf("a 24 MiB answer: status %d, peak heap +%d MiB", status, above>>20)
	}
	status, relayed, above = peakHeap(t, 3<<20)
	t.Logf("3 MiB answer: status %d, %d bytes relayed, peak heap +%d MiB", status, relayed, above>>20)
	if status != http.StatusOK || relayed < 3<<20 || above > 24<<20 {
		t.Fatalf("a 3 MiB answer: status %d, peak heap +%d MiB", status, above>>20)
	}
}

func TestTheBufferBudgetBoundsWhatIsHeldAtOnce(t *testing.T) {
	buffers := newBudget(5)
	first, ok := buffers.acquire(context.Background(), 4)
	if !ok {
		t.Fatal("4 of 5 units")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, ok := buffers.acquire(ctx, 4); ok {
		t.Fatal("8 units out of 5")
	}
	if len(buffers.tokens) != 1 {
		t.Fatalf("a failed acquire kept %d units", 1-len(buffers.tokens))
	}
	first()
	second, ok := buffers.acquire(context.Background(), 4)
	if !ok {
		t.Fatal("units were not returned")
	}
	second()
	if unitsFor(-1) != 4 || unitsFor(1) != 1 || unitsFor(maxResponseBody*2) != 4 {
		t.Fatal("unitsFor")
	}
}

func TestEveryEncodedFormOfTheCredentialIsScrubbed(t *testing.T) {
	secret := "upstream-token-0123456789/+=?&"
	scrub := newScrubber(secret)
	forms := []string{
		secret,
		url.QueryEscape(secret),
		url.PathEscape(secret),
		base64.StdEncoding.EncodeToString([]byte(secret)),
		base64.URLEncoding.EncodeToString([]byte(secret)),
		base64.RawStdEncoding.EncodeToString([]byte(secret)),
	}
	// Inside a longer base64 value, at each alignment.
	for _, prefix := range []string{"user:", "user1:", "u:"} {
		forms = append(forms, base64.StdEncoding.EncodeToString([]byte(prefix+secret+"!")))
	}
	for _, form := range forms {
		out := string(scrub.apply([]byte("before " + form + " after")))
		if strings.Contains(out, form) || !strings.Contains(out, redacted) {
			t.Errorf("%q survived: %s", form, out)
		}
	}
	// A short credential keeps its plain forms only (no false positives).
	if string(newScrubber("abc").apply([]byte("YWJj abc"))) != "YWJj "+redacted {
		t.Fatal("a short credential")
	}
}

func TestACredentialSplitAcrossEventLinesIsScrubbed(t *testing.T) {
	h := newHarness(t)
	half := len(credential) / 2
	h.server.getEvents = []string{
		"data: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/message\",\"params\":{\"data\":\"" + credential[:half] + "\ndata: " + credential[half:] + "\"}}",
	}
	stream := h.do(t, http.MethodGet, tokenRO, "", nil).Body.String()
	flat := strings.ReplaceAll(stream, "\ndata: ", "")
	if strings.Contains(flat, credential) || !strings.Contains(stream, redacted) {
		t.Fatalf("the split credential survived: %q", stream)
	}
}

func TestAStreamEndsWhenItsLeaseIsRevoked(t *testing.T) {
	previous := streamRecheck
	streamRecheck = 20 * time.Millisecond
	defer func() { streamRecheck = previous }()
	h := newHarness(t)
	h.server.holdGet = true
	h.server.getEvents = []string{`data: {"jsonrpc":"2.0","method":"notifications/message","params":{}}`}
	finished := make(chan int)
	go func() {
		finished <- h.do(t, http.MethodGet, tokenB, "", nil).Code
	}()
	time.Sleep(100 * time.Millisecond)
	h.authority.mu.Lock()
	delete(h.authority.leases, tokenB)
	h.authority.mu.Unlock()
	h.advance(31 * time.Second) // past the cached decision
	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("the stream outlived its lease")
	}
	if !strings.Contains(h.logText(), "closing its stream") {
		t.Fatal("the close was not logged")
	}
}

func TestACallInFlightIsToldItsLeaseEnded(t *testing.T) {
	previous := streamRecheck
	streamRecheck = 20 * time.Millisecond
	defer func() { streamRecheck = previous }()
	h := newHarness(t)
	h.server.stream = true
	h.server.holdCall = true
	finished := make(chan *httptest.ResponseRecorder)
	go func() {
		finished <- h.do(t, http.MethodPost, tokenB, `{"jsonrpc":"2.0","id":"call-7","method":"tools/call","params":{"name":"whoami"}}`, nil)
	}()
	time.Sleep(100 * time.Millisecond)
	h.authority.mu.Lock()
	delete(h.authority.leases, tokenB)
	h.authority.mu.Unlock()
	h.advance(31 * time.Second) // past the cached decision
	var recorder *httptest.ResponseRecorder
	select {
	case recorder = <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("the call outlived its lease")
	}
	body := recorder.Body.String()
	// The progress event was relayed, then the call's own error.
	if !strings.Contains(body, "notifications/progress") {
		t.Fatalf("the stream so far was lost: %s", body)
	}
	want := `data: {"error":{"code":-32091,"message":"lease revoked: ` + leaseRefused + `"},"id":"call-7","jsonrpc":"2.0"}`
	if !strings.Contains(body, want) {
		t.Fatalf("the call was not told its lease ended: %s", body)
	}
}

func TestStreamsCountTowardTheCaps(t *testing.T) {
	h := newHarness(t)
	release1, _ := h.front.inflight.acquire("lease-a", 2)
	release2, _ := h.front.inflight.acquire("lease-a", 2)
	defer release1()
	defer release2()
	if code := h.do(t, http.MethodGet, tokenA, "", nil).Code; code != http.StatusTooManyRequests {
		t.Fatalf("a stream over the cap: %d", code)
	}
}

// A body that fails the test when read: the caps come first.
type unreadable struct{ t *testing.T }

func (u unreadable) Read([]byte) (int, error) {
	u.t.Error("the body was read before the cap")
	return 0, io.EOF
}

func TestTheCapComesBeforeTheBody(t *testing.T) {
	h := newHarness(t)
	release1, _ := h.front.inflight.acquire("lease-a", 2)
	release2, _ := h.front.inflight.acquire("lease-a", 2)
	defer release1()
	defer release2()
	request := httptest.NewRequest(http.MethodPost, "/mcp", unreadable{t})
	request.Header.Set("Authorization", "Bearer "+tokenA)
	recorder := httptest.NewRecorder()
	h.front.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusTooManyRequests {
		t.Fatalf("status %d", recorder.Code)
	}
}

func TestAPinningHoldRefusesEveryRequest(t *testing.T) {
	h := newHarness(t)
	h.front.probe.held.Store(true)
	response := h.do(t, http.MethodPost, tokenA, call("whoami"), nil)
	if response.Code != http.StatusServiceUnavailable || h.server.count() != 0 {
		t.Fatalf("status %d, server reached %d", response.Code, h.server.count())
	}
}

func TestARevokedLeaseIsSaidSoInTheRefusal(t *testing.T) {
	h := newHarness(t)
	response := h.do(t, http.MethodPost, tokenDead, call("whoami"), nil)
	var answer map[string]string
	json.Unmarshal(response.Body.Bytes(), &answer)
	if response.Code != http.StatusUnauthorized || answer["error"] != "lease_inactive" || !strings.Contains(answer["detail"], "revoked or expired") {
		t.Fatalf("%d %s", response.Code, response.Body)
	}
	response = h.do(t, http.MethodPost, "", call("whoami"), nil)
	json.Unmarshal(response.Body.Bytes(), &answer)
	if answer["error"] != "lease_required" {
		t.Fatalf("%s", response.Body)
	}
}

func TestTheCredentialHeaderCannotOverrideAForwardedHeader(t *testing.T) {
	for _, header := range []string{"Mcp-Session-Id", "mcp-session-id", "Content-Type", "Host", "Cookie", "Last-Event-ID", "Transfer-Encoding"} {
		request := requestFile{ProtocolVersion: "1.0", Plane: "service"}
		request.Connector.ID = connectorID
		request.Service.Port = 8080
		request.Exchange.URL = "http://exchange:8088"
		request.MCP = &mcpBlock{
			Upstream:   "http://127.0.0.1:8091/mcp",
			Access:     map[string][]string{"ReadWrite": {"read", "write"}},
			Credential: &credentialRule{Header: header},
		}
		if _, err := parseConfig(request, identity); err == nil {
			t.Errorf("%s was accepted as the credential header", header)
		}
	}
}

// testClock is the harness's clock, safe to read from the front's own
// goroutines (a stream's lease watch) while a test moves it.
type testClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *testClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func (h *harness) advance(d time.Duration) { h.clock.advance(d) }
