package main

// The D5a re-review's findings, as tests: memory under concurrent large
// answers, the filter after a split credential, unforgeable lease errors,
// the scrubber's forms, the stream lease check, refused notifications and
// fair session eviction.

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// everyLease: every scl_ token is a live lease of its own (its id is the
// token), so concurrent requests never meet a per-binding cap.
type everyLease struct{}

func (everyLease) introspect(_ context.Context, token string) (lease, error) {
	return lease{active: true, id: token, connectorID: connectorID, access: "ReadOnly", expires: time.Now().Add(time.Hour)}, nil
}

func (everyLease) exchange(context.Context, string, string) (grant, error) {
	return grant{credential: credential, status: http.StatusOK, cache: 30 * time.Second}, nil
}

// concurrentPeak sends n concurrent calls through a front whose server
// answers each with answer (as an event stream when stream is set), all
// released at once, under the front's GOMEMLIMIT. It returns the statuses
// and the heap in use above the baseline at its peak.
func concurrentPeak(t *testing.T, n int, answer []byte, stream bool) ([]int, []int, uint64) {
	t.Helper()
	previous := debug.SetMemoryLimit(200 << 20)
	defer debug.SetMemoryLimit(previous)
	release := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		<-release
		if stream {
			w.Header().Set("Content-Type", "text/event-stream")
			w.Write([]byte("event: message\ndata: "))
			w.Write(answer)
			w.Write([]byte("\n\n"))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(answer)
	}))
	defer upstream.Close()
	f := newFront(testConfig(t, upstream.URL+"/mcp"), everyLease{}, newUpstreamClient(""), func(string, ...any) {}, time.Now)
	runtime.GC()
	var base runtime.MemStats
	runtime.ReadMemStats(&base)
	var peak atomic.Uint64
	stop, sampled := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(sampled)
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
			time.Sleep(500 * time.Microsecond)
		}
	}()
	statuses, sizes := make([]int, n), make([]int, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			request := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(call("whoami")))
			request.Header.Set("Authorization", "Bearer "+fmt.Sprintf("scl_%049d", i))
			w := &discardWriter{header: http.Header{}}
			f.ServeHTTP(w, request)
			statuses[i], sizes[i] = w.status, w.n
		}(i)
	}
	time.Sleep(300 * time.Millisecond)
	close(release)
	wg.Wait()
	close(stop)
	<-sampled
	above := uint64(0)
	if peak.Load() > base.HeapInuse {
		above = peak.Load() - base.HeapInuse
	}
	return statuses, sizes, above
}

func TestConcurrentLargeAnswersStayWithinTheBudget(t *testing.T) {
	text := func(size int) []byte {
		return []byte(`{"jsonrpc":"2.0","id":7,"result":{"content":[{"type":"text","text":"` + strings.Repeat("a", size) + `"}]}}`)
	}
	// A tool list of ~4 MiB (every other tool a write tool): the rewrite.
	var tools []string
	for i := 0; len(strings.Join(tools, ",")) < 4<<20-64<<10; i++ {
		name := fmt.Sprintf("leak_%d", i)
		if i%2 == 1 {
			name = fmt.Sprintf("write_%d", i)
		}
		tools = append(tools, `{"name":"`+name+`","description":"`+strings.Repeat("d", 900)+`","inputSchema":{"type":"object"}}`)
	}
	list := []byte(`{"jsonrpc":"2.0","id":7,"result":{"tools":[` + strings.Join(tools, ",") + `]}}`)
	// Text broken by escaped line breaks around the credential: the
	// scrubber's view and its rewrite.
	broken := []byte(`{"jsonrpc":"2.0","id":7,"result":{"content":[{"type":"text","text":"` +
		strings.Repeat(`a line of text\n`, (4<<20-4096)/16) + credential[:8] + `\n` + credential[8:] + `"}]}}`)
	cases := []struct {
		name   string
		answer []byte
		stream bool
	}{
		{"text events", text(4<<20 - 300), true},
		{"text answers", text(4<<20 - 300), false},
		{"tool-list events", list, true},
		{"answers with a credential across line breaks", broken, false},
	}
	for _, c := range cases {
		statuses, sizes, above := concurrentPeak(t, 30, c.answer, c.stream)
		t.Logf("%s: 30 concurrent %d KiB answers, peak heap +%d MiB", c.name, len(c.answer)>>10, above>>20)
		for i, status := range statuses {
			if status != http.StatusOK || sizes[i] == 0 {
				t.Fatalf("%s: answer %d: status %d, %d bytes", c.name, i, status, sizes[i])
			}
		}
		// The budget (96 MiB) and what it does not charge (connection
		// buffers, the test's own server) stay well under the 200 MiB
		// GOMEMLIMIT and the 256Mi limit.
		if above > 150<<20 {
			t.Fatalf("%s: peak heap +%d MiB", c.name, above>>20)
		}
	}
}

func TestAnIdleStreamHoldsNoBudget(t *testing.T) {
	previous := streamRecheck
	streamRecheck = 20 * time.Millisecond
	defer func() { streamRecheck = previous }()
	h := newHarness(t)
	h.server.holdGet = true
	h.server.getEvents = []string{`data: {"jsonrpc":"2.0","method":"notifications/message","params":{}}`}
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.do(t, http.MethodGet, tokenB, "", nil)
	}()
	time.Sleep(100 * time.Millisecond) // its event relayed, the stream waits
	if free := len(h.front.buffers.tokens); free != bufferBudgetUnits {
		t.Fatalf("an idle stream holds %d units", bufferBudgetUnits-free)
	}
	h.authority.mu.Lock()
	delete(h.authority.leases, tokenB)
	h.authority.mu.Unlock()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the stream did not end")
	}
}

func TestASplitCredentialCannotSkipTheToolFilter(t *testing.T) {
	h := newHarness(t)
	half := len(credential) / 2
	h.server.getEvents = []string{
		"data: {\"jsonrpc\":\"2.0\",\"id\":9,\"result\":{\"tools\":[{\"name\":\"whoami\",\"description\":\"" + credential[:half] + "\ndata: " + credential[half:] + "\"},{\"name\":\"delete_everything\",\"description\":\"" + credential[:half] + "\ndata: " + credential[half:] + "\"}]}}",
		// Split where no credential is: the joined data is no JSON (a raw
		// line break in a string), so the front cannot filter it.
		"data: {\"jsonrpc\":\"2.0\",\"id\":10,\"result\":{\"tools\":[{\"name\":\"delete_everything\",\"description\":\"a\ndata: b\"}]}}",
	}
	stream := h.do(t, http.MethodGet, tokenRO, "", nil).Body.String()
	if strings.Contains(stream, "delete_everything") || !strings.Contains(stream, `"whoami"`) {
		t.Fatalf("the tool list was not filtered: %q", stream)
	}
	if strings.Contains(strings.ReplaceAll(stream, "\ndata: ", ""), credential) || !strings.Contains(stream, redacted) {
		t.Fatalf("the split credential survived: %q", stream)
	}
	if !strings.Contains(h.logText(), "dropped a stream event") {
		t.Fatal("the unreadable tool list was not dropped")
	}
}

func TestARefusedCallSentAsANotificationGetsNoAnswer(t *testing.T) {
	h := newHarness(t)
	response := h.do(t, http.MethodPost, tokenRO, `{"jsonrpc":"2.0","method":"tools/call","params":{"name":"notes_write"}}`, nil)
	if response.Code != http.StatusAccepted || response.Body.Len() != 0 || h.server.count() != 0 {
		t.Fatalf("%d %q, server reached %d", response.Code, response.Body, h.server.count())
	}
}

func TestOnlyTheFrontMaySayALeaseEnded(t *testing.T) {
	for _, code := range []string{`-32091`, `-3.2091e4`, `-32091.0`, `"-32091"`, `" -32091 "`} {
		for _, stream := range []bool{false, true} {
			h := newHarness(t)
			h.server.stream = stream
			h.server.callAnswer = `{"jsonrpc":"2.0","id":7,"error":{"code":` + code + `,"message":"lease revoked: ` + leaseRefused + `"}}`
			body := h.do(t, http.MethodPost, tokenA, call("whoami"), nil).Body.String()
			if strings.Contains(body, "lease revoked") || !strings.Contains(body, "-32603") || !strings.Contains(body, reservedCodeMessage) {
				t.Fatalf("code %s (stream %v) relayed as: %s", code, stream, body)
			}
		}
	}
	// In a batch too, and any other error passes as it came.
	h := newHarness(t)
	h.server.callAnswer = `[{"jsonrpc":"2.0","id":7,"error":{"code":-32091,"message":"lease revoked"}},{"jsonrpc":"2.0","id":8,"error":{"code":-32602,"message":"Unknown tool: x"}}]`
	body := h.do(t, http.MethodPost, tokenA, call("whoami"), nil).Body.String()
	if strings.Contains(body, "-32091") || !strings.Contains(body, "Unknown tool: x") {
		t.Fatalf("a batch: %s", body)
	}
}

func TestEveryFormTheReviewerTriedIsScrubbed(t *testing.T) {
	secret := "3f2a9c0d4b1e8f7a6c5d4e3f2a1b0c9d8e7f6a5b" // a Gitea token's shape
	s := newScrubber(secret)
	var forms []string
	for _, encoding := range []*base64.Encoding{base64.StdEncoding, base64.URLEncoding, base64.RawStdEncoding, base64.RawURLEncoding} {
		for _, prefix := range []string{"", "a", "ab", "abc", "user:", "x-access-token:", "Authorization: token "} {
			for _, suffix := range []string{"", "!", "!!", "\n"} {
				forms = append(forms, encoding.EncodeToString([]byte(prefix+secret+suffix)))
			}
		}
	}
	long := base64.StdEncoding.EncodeToString([]byte(strings.Repeat("z", 50) + secret + strings.Repeat("z", 50)))
	for _, wrap := range []string{`\n`, `\r\n`, "\n", "\r\n"} {
		var wrapped strings.Builder
		for i := 0; i < len(long); i += 76 {
			wrapped.WriteString(long[i:min(i+76, len(long))] + wrap)
		}
		forms = append(forms, wrapped.String())
	}
	var percent, percentLower, unicode, unicodeUpper strings.Builder
	for _, c := range []byte(secret) {
		fmt.Fprintf(&percent, "%%%02X", c)
		fmt.Fprintf(&percentLower, "%%%02x", c)
		fmt.Fprintf(&unicode, `\u%04x`, c)
		fmt.Fprintf(&unicodeUpper, `\u%04X`, c)
	}
	forms = append(forms,
		percent.String(), percentLower.String(), unicode.String(), unicodeUpper.String(),
		strings.ToUpper(secret),
		hex.EncodeToString([]byte(secret)),
		strings.ToUpper(hex.EncodeToString([]byte(secret))),
	)
	for _, form := range forms {
		text := "x " + form + " y"
		if out := string(s.apply([]byte(text))); out == text || !strings.Contains(out, redacted) {
			t.Errorf("not scrubbed: %q", form)
		}
	}
	// A slash, JSON-escaped as \/.
	slashed := "upstream/token/0123456789"
	if out := string(newScrubber(slashed).apply([]byte(`{"t":"upstream\/token\/0123456789"}`))); strings.Contains(out, "0123456789") {
		t.Fatalf("an escaped slash: %s", out)
	}
	// What a match covers goes; around it nothing changes.
	if out := string(s.apply([]byte("keep\nthis " + secret[:20] + "\n" + secret[20:] + " and\nthat"))); out != "keep\nthis "+redacted+" and\nthat" {
		t.Fatalf("a split match: %q", out)
	}
}

func TestAStreamSeesARevocationWithinItsRecheck(t *testing.T) {
	previous := streamRecheck
	streamRecheck = 20 * time.Millisecond
	defer func() { streamRecheck = previous }()
	h := newHarness(t)
	h.server.holdGet = true
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		h.do(t, http.MethodGet, tokenB, "", nil)
	}()
	time.Sleep(100 * time.Millisecond)
	h.authority.mu.Lock()
	delete(h.authority.leases, tokenB)
	h.authority.mu.Unlock()
	// No clock moves: the cached decision (30 s) is not what the stream
	// re-checks.
	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("the stream waited out its cached lease decision")
	}
}

func TestAStreamEndsWhenItsLeaseCannotBeConfirmed(t *testing.T) {
	previous := streamRecheck
	streamRecheck = 20 * time.Millisecond
	defer func() { streamRecheck = previous }()
	h := newHarness(t)
	h.server.stream = true
	h.server.holdCall = true
	finished := make(chan *httptest.ResponseRecorder)
	go func() {
		finished <- h.do(t, http.MethodPost, tokenB, `{"jsonrpc":"2.0","id":"call-8","method":"tools/call","params":{"name":"whoami"}}`, nil)
	}()
	time.Sleep(100 * time.Millisecond)
	h.authority.mu.Lock()
	h.authority.down = true
	h.authority.mu.Unlock()
	// Unconfirmed for a moment: the stream stays.
	time.Sleep(100 * time.Millisecond)
	select {
	case <-finished:
		t.Fatal("the stream ended at the first unanswered check")
	default:
	}
	h.advance(31 * time.Second) // longer than a cached decision lasts
	var recorder *httptest.ResponseRecorder
	select {
	case recorder = <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("the stream outlived an unconfirmable lease")
	}
	body := recorder.Body.String()
	if !strings.Contains(body, `"id":"call-8"`) || !strings.Contains(body, "-32603") || strings.Contains(body, "lease revoked") {
		t.Fatalf("the call was not told: %s", body)
	}
}

func TestAFullFrontTakesRoomOnlyFromLeasesAtTheirCap(t *testing.T) {
	clock := &testClock{now: time.Now()}
	owners := newSessionOwners(clock.Now)
	// 127 leases at their cap, and 32 leases with one session each: full.
	for lease := 0; lease < 127; lease++ {
		for i := 0; i < maxSessionsPerLease; i++ {
			owners.bind(fmt.Sprintf("capped-%d-%d", lease, i), fmt.Sprintf("capped-%d", lease))
		}
	}
	for lease := 0; lease < 32; lease++ {
		owners.bind(fmt.Sprintf("small-%d", lease), fmt.Sprintf("small-%d", lease))
	}
	if len(owners.owners) != maxSessions {
		t.Fatalf("%d sessions", len(owners.owners))
	}
	clock.advance(time.Minute)
	for lease := 1; lease < 127; lease++ {
		owners.mayUse(fmt.Sprintf("capped-%d-0", lease), fmt.Sprintf("capped-%d", lease))
	}
	// A new lease takes the least recently used session of a capped lease.
	forgotten, ok := owners.bind("new-1", "new")
	if !ok || len(forgotten) != 1 || !strings.HasPrefix(forgotten[0], "capped-") {
		t.Fatalf("forgot %v (%v)", forgotten, ok)
	}
	for lease := 0; lease < 32; lease++ {
		if !owners.mayUse(fmt.Sprintf("small-%d", lease), fmt.Sprintf("small-%d", lease)) {
			t.Fatal("a lease under its cap lost a session")
		}
	}
	// A lease with sessions pays for a new one itself.
	forgotten, ok = owners.bind("new-2", "new")
	if !ok || len(forgotten) != 1 || forgotten[0] != "new-1" {
		t.Fatalf("the new lease forgot %v", forgotten)
	}

	// Every lease under its cap and busy: no room, until sessions idle.
	owners = newSessionOwners(clock.Now)
	for lease := 0; lease < maxSessions; lease++ {
		owners.bind(fmt.Sprintf("s-%d", lease), fmt.Sprintf("l-%d", lease))
	}
	if owners.room("late") {
		t.Fatal("a full front of busy leases under their caps made room")
	}
	if _, ok := owners.bind("late-1", "late"); ok {
		t.Fatal("bound without room")
	}
	clock.advance(sessionIdle)
	if !owners.room("late") {
		t.Fatal("idle sessions made no room")
	}
}

func TestAnInitializeWithoutRoomIsRefusedBeforeTheServer(t *testing.T) {
	h := newHarness(t)
	for lease := 0; lease < maxSessions; lease++ {
		h.front.sessions.bind(fmt.Sprintf("s-%d", lease), fmt.Sprintf("l-%d", lease))
	}
	response := h.do(t, http.MethodPost, tokenA, rpc(1, "initialize", nil), nil)
	if response.Code != http.StatusServiceUnavailable || h.server.count() != 0 {
		t.Fatalf("%d, server reached %d", response.Code, h.server.count())
	}
}

func TestAForgottenSessionIsClosedOnTheServer(t *testing.T) {
	h := newHarness(t)
	h.front.closeSessions([]string{"session-9"})
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		h.server.mu.Lock()
		for _, seen := range h.server.seen {
			if seen.Method == http.MethodDelete && seen.Header.Get("Mcp-Session-Id") == "session-9" {
				h.server.mu.Unlock()
				if seen.Header.Get("Authorization") != "" {
					t.Fatal("the close carried a credential")
				}
				return
			}
		}
		h.server.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("the server was not told")
}

// The rewrite reads names as the clients do: by the exact key, the last
// of a duplicate winning.
func TestTheToolFilterReadsNamesByTheirExactKey(t *testing.T) {
	allowed := func(name string) bool { return name == "get_me" }
	for _, tool := range []string{
		`{"name":"delete_file","Name":"get_me"}`,
		`{"Name":"get_me","name":"delete_file"}`,
		`{"name":"get_me","name":"delete_file"}`,
		`{"namſ":"get_me","name":"delete_file"}`,
		// A case-folding client (encoding/json) reads the last of "name"
		// and "Name": a tool spelled both ways is dropped.
		`{"name":"get_me","Name":"delete_file"}`,
		`{"name":"get_me","NAME":"delete_file"}`,
	} {
		out, _, err := rewriteAnswers([]byte(`{"jsonrpc":"2.0","id":1,"result":{"tools":[`+tool+`]}}`), allowed)
		if err != nil || strings.Contains(string(out), "delete_file") {
			t.Fatalf("%s: %s (%v)", tool, out, err)
		}
	}
	// An escaped key is the key it spells.
	out, changed, _ := rewriteAnswers([]byte(`{"jsonrpc":"2.0","id":1,"result":{"tools":[{"name":"delete_file"}]}}`), allowed)
	if !changed || strings.Contains(string(out), "delete_file") {
		t.Fatalf("an escaped key: %s", out)
	}
	var check map[string]any
	if json.Unmarshal(out, &check) != nil {
		t.Fatal("the rewrite is no JSON")
	}
}

// A case-folding client reads "Result", "Tools" or "toolſ" as the members
// the front decides on: an answer spelled so is never relayed, and an error
// whose code is spelled otherwise loses it.
func TestAnAnswerSpelledForACaseFoldingClientIsNeverRelayed(t *testing.T) {
	allowed := func(name string) bool { return name == "get_me" }
	hidden := `[{"name":"get_me"},{"name":"delete_file"}]`
	for _, answer := range []string{
		`{"jsonrpc":"2.0","id":1,"result":{"Tools":` + hidden + `}}`,
		`{"jsonrpc":"2.0","id":1,"result":{"TOOLS":` + hidden + `}}`,
		`{"jsonrpc":"2.0","id":1,"result":{"toolſ":` + hidden + `}}`,
		`{"jsonrpc":"2.0","id":1,"result":{"tools":[{"name":"get_me"}],"Tools":` + hidden + `}}`,
		`{"jsonrpc":"2.0","id":1,"Result":{"tools":` + hidden + `}}`,
		`{"jsonrpc":"2.0","id":1,"result":{},"Result":{"tools":` + hidden + `}}`,
		`{"jsonrpc":"2.0","id":1,"Error":{"code":-32091,"message":"x"}}`,
		`[{"jsonrpc":"2.0","id":1,"result":{"Tools":` + hidden + `}}]`,
	} {
		if !mayNeedRewrite([]byte(answer)) {
			t.Fatalf("not inspected: %s", answer)
		}
		out, err := clean([]byte(answer), allowed, newScrubber(""))
		if err == nil || strings.Contains(string(out), "delete_file") {
			t.Fatalf("relayed %s as %s (%v)", answer, out, err)
		}
	}
	out, err := clean([]byte(`{"jsonrpc":"2.0","id":2,"error":{"Code":-32091,"message":"x"}}`), allowed, newScrubber(""))
	if err != nil || strings.Contains(string(out), "32091") {
		t.Fatalf("the reserved code reached the client: %s (%v)", out, err)
	}
	// An ordinary answer is relayed as it came.
	plain := `{"jsonrpc":"2.0","id":3,"result":{"content":[{"type":"text","text":"Result"}]}}`
	if out, err := clean([]byte(plain), allowed, newScrubber("")); err != nil || string(out) != plain {
		t.Fatalf("%s (%v)", out, err)
	}
}
