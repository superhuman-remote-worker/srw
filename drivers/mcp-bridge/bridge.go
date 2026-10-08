package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"os"
	"os/exec"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	// Set by the front on every request it forwards: the binding (its
	// lease's id) and that binding's credential (base64). The front never
	// forwards a caller's own header of these names, and only the front
	// reaches the bridge's socket.
	bindingHeader    = "Srw-Bridge-Binding"
	credentialHeader = "Srw-Bridge-Credential"
	sessionHeader    = "Mcp-Session-Id"
	// The bridge's own routes: liveness, status and a binding's end.
	controlPrefix = "/srw/"
	// The largest message the front forwards.
	maxBody = 4 << 20
	// The front's readiness probe ends its session at once; a probe process
	// left behind stops after this long.
	probeIdle = 30 * time.Second
	// How long a stopping process's group may take to be reaped.
	reapWithin = 5 * time.Second
	// The JSON-RPC error a call gets when its process stops before it
	// answered: the SDKs' "connection closed", which SRW's client reads as
	// a session that ended, not a tool that failed.
	connectionClosed = -32000
	// What the probe's process finds in the credential's variable: never a
	// real credential (the probe has no lease), only enough for a server
	// that exits without one to start and list its tools.
	probePlaceholder = "srw-probe-placeholder"
)

// testHookRead, when a test sets it, runs between reading a session's
// message and handing it to the process: a test holds a message there to
// order it against a takeover.
var testHookRead atomic.Pointer[func(jsonrpc.Message)]

// A binding id is the lease's id the front names (a UUID in SRW).
var bindingShape = regexp.MustCompile(`\A[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}\z`)

type options struct {
	socket        string
	socketGroup   int
	path          string
	credentialEnv string
	maxProcesses  int
	idle          time.Duration
	stopGrace     time.Duration
	// The first of the users the processes run as (0: every process runs
	// as the bridge itself, for tests only).
	uidBase int
	// Where each process's private directory is made.
	homeRoot string
	// The writable directories the processes share, swept of a user's
	// files before the user is handed out again.
	sweepDirs []string
	program   []string
}

// bridge serves the front and runs one process per binding.
type bridge struct {
	opts options
	env  []string
	logf func(string, ...any)
	now  func() time.Time

	mu        sync.Mutex
	closed    bool
	processes map[string]*process // by binding; "" is the front's probe
	sessions  map[string]*session // by Mcp-Session-Id
	// The processes the bridge started and has not waited for yet (live
	// ones and stopping ones): their own command waits for each, so the
	// orphan reaper never does.
	mains    map[int]bool
	stopping sync.WaitGroup
	// The users the processes run as (nil: as the bridge itself), and the
	// last private directory's number when they do not.
	users    *userPool
	sequence uint64
}

func newBridge(opts options, env []string, logf func(string, ...any), now func() time.Time) *bridge {
	b := &bridge{
		opts:      opts,
		env:       env,
		logf:      logf,
		now:       now,
		processes: map[string]*process{},
		sessions:  map[string]*session{},
		mains:     map[int]bool{},
	}
	if opts.uidBase > 0 {
		b.users = newUserPool(opts.uidBase, poolSize(opts.maxProcesses))
	}
	return b
}

// process is one binding's stdio process. It starts with the binding's
// first session and lives as long as the binding: until the front ends the
// binding, it is idle, or it exits. It serves the binding's sessions one at
// a time (SRW's agent opens one each time it attaches the connector): a new
// session takes over from the previous one. A stdio server serves one
// client, so the process is initialized once: a later session's initialize
// is answered with the process's own first answer, and that session's
// initialized notification is not forwarded. A process with a call still
// unanswered when a new session comes (a cancelled one included: a server
// may still answer it) is restarted instead, so a late answer can never
// reach another session's call of the same id.
type process struct {
	bridge     *bridge
	binding    string
	cmd        *exec.Cmd
	child      mcp.Connection
	stderr     *lineLogger
	credential bool
	started    time.Time
	// The user it runs as (0: the bridge's own) and its private directory.
	uid  int
	home string

	mu sync.Mutex
	// The session the process serves now; nil between sessions.
	current *session
	// The first initialize's id until its answer, then that answer.
	initID     jsonrpc.ID
	initDone   bool
	initResult json.RawMessage
	// The calls the process has not answered yet, by the session that made
	// each.
	pending  map[jsonrpc.ID]*session
	lastUsed time.Time
	// Requests and streams of its session open now: never idle while any.
	active int

	done     chan struct{}
	stopOnce sync.Once
	// Closed once the process and its group are gone.
	stopped chan struct{}
}

// session is one MCP session of a binding: the SDK's streamable server
// transport facing the front, and a pump copying its messages to the
// binding's process.
type session struct {
	id        string
	process   *process
	transport *mcp.StreamableServerTransport
	server    mcp.Connection
	// The process was initialized before this session took it over.
	reused    bool
	closeOnce sync.Once
	closed    chan struct{}
}

func labelOf(binding string) string {
	if binding == "" {
		return "probe"
	}
	return "binding=" + binding
}

func (p *process) label() string { return labelOf(p.binding) }

func (p *process) pid() int {
	if p.cmd.Process == nil {
		return 0
	}
	return p.cmd.Process.Pid
}

// enter and leave count the requests and streams open on the process's
// session; a process with one open is never idle.
func (p *process) enter() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.active++
	p.lastUsed = p.bridge.now()
}

func (p *process) leave() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.active--
	p.lastUsed = p.bridge.now()
}

func (p *process) touch() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.lastUsed = p.bridge.now()
}

func (p *process) idleFor(now time.Time) time.Duration {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.active > 0 {
		return 0
	}
	return now.Sub(p.lastUsed)
}

// reusableLocked: a new session may take the process over (it runs, it is
// initialized, and no earlier session's call is still unanswered). The
// caller holds p.mu, and switches the process's session under the same
// hold, so no call of the old session is registered in between.
func (p *process) reusableLocked() bool {
	select {
	case <-p.done:
		return false
	default:
	}
	return p.initDone && len(p.pending) == 0
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(body)
}

func refuse(w http.ResponseWriter, status int, text string) {
	writeJSON(w, status, map[string]string{"error": text})
}

func (b *bridge) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.URL.Path == b.opts.path:
		b.serveMCP(w, r)
	case r.URL.Path == controlPrefix+"livez" && r.Method == http.MethodGet:
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	case r.URL.Path == controlPrefix+"status" && r.Method == http.MethodGet:
		writeJSON(w, http.StatusOK, b.status())
	case strings.HasPrefix(r.URL.Path, controlPrefix+"bindings/") && r.Method == http.MethodDelete:
		binding := strings.TrimPrefix(r.URL.Path, controlPrefix+"bindings/")
		if !bindingShape.MatchString(binding) {
			refuse(w, http.StatusBadRequest, "no binding id")
			return
		}
		stopped := b.endBinding(binding, "its binding ended")
		writeJSON(w, http.StatusOK, map[string]bool{"stopped": stopped})
	default:
		refuse(w, http.StatusNotFound, "not found")
	}
}

func (b *bridge) serveMCP(w http.ResponseWriter, r *http.Request) {
	binding := r.Header.Get(bindingHeader)
	if binding != "" && !bindingShape.MatchString(binding) {
		refuse(w, http.StatusBadRequest, "the binding id is malformed")
		return
	}
	credential, err := decodeCredential(r.Header.Get(credentialHeader))
	if err != nil {
		refuse(w, http.StatusBadRequest, err.Error())
		return
	}
	// The SDK never sees SRW's own headers.
	r.Header.Del(bindingHeader)
	r.Header.Del(credentialHeader)
	sessionID := r.Header.Get(sessionHeader)
	switch r.Method {
	case http.MethodPost:
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBody))
		if err != nil {
			refuse(w, http.StatusRequestEntityTooLarge, "the message is too large")
			return
		}
		message, err := checkMessage(body)
		if err != nil {
			b.logf("refused a message for %s: %v", labelOf(binding), err)
			refuse(w, http.StatusBadRequest, err.Error())
			return
		}
		var s *session
		if sessionID == "" {
			if !initializeCall(message) {
				refuse(w, http.StatusBadRequest, "only an initialize request opens a session")
				return
			}
			var status int
			s, status, err = b.open(binding, credential)
			if err != nil {
				refuse(w, status, err.Error())
				return
			}
		} else if s = b.lookup(sessionID, binding); s == nil {
			refuse(w, http.StatusNotFound, "session not found")
			return
		}
		// The SDK reads the same bytes again: the message checkMessage
		// accepted is the one it decodes.
		r.Body = io.NopCloser(bytes.NewReader(body))
		r.ContentLength = int64(len(body))
		s.process.enter()
		defer s.process.leave()
		s.transport.ServeHTTP(w, r)
	case http.MethodGet:
		s := b.lookup(sessionID, binding)
		if s == nil {
			refuse(w, http.StatusNotFound, "session not found")
			return
		}
		s.process.enter()
		defer s.process.leave()
		s.transport.ServeHTTP(w, r)
	case http.MethodDelete:
		s := b.lookup(sessionID, binding)
		if s == nil {
			refuse(w, http.StatusNotFound, "session not found")
			return
		}
		s.close()
		if binding == "" {
			// The probe's process serves one probe.
			s.process.stop("the probe ended")
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		w.Header().Set("Allow", "GET, POST, DELETE")
		refuse(w, http.StatusMethodNotAllowed, "GET, POST or DELETE")
	}
}

// lookup finds the session a binding's process serves now: a session id
// another binding opened, or one a later session took over from, is no
// session of this one's.
func (b *bridge) lookup(sessionID, binding string) *session {
	if sessionID == "" {
		return nil
	}
	b.mu.Lock()
	s := b.sessions[sessionID]
	b.mu.Unlock()
	if s == nil || s.process.binding != binding {
		return nil
	}
	s.process.mu.Lock()
	defer s.process.mu.Unlock()
	if s.process.current != s {
		return nil
	}
	return s
}

// open starts a session of a binding on the binding's process: the one it
// has, or a new one.
func (b *bridge) open(binding, credential string) (*session, int, error) {
	switch {
	case b.opts.credentialEnv == "":
		// A server that takes no credential gets none.
		credential = ""
	case binding == "":
		// The probe has no lease, so no credential: its process gets a
		// placeholder, never a real credential, so a server that exits
		// without its variable still starts and lists its tools.
		credential = probePlaceholder
	case credential == "":
		return nil, http.StatusBadRequest, errors.New("the front named no credential for this binding")
	}
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil, http.StatusServiceUnavailable, errors.New("the bridge is stopping")
	}
	var replaced *process
	if p := b.processes[binding]; p != nil {
		s, previous, ok, err := b.attach(p, false)
		if ok || err != nil {
			b.mu.Unlock()
			return b.opened(s, previous, err)
		}
		replaced = p
	}
	if binding != "" && b.bindingCount(replaced) >= b.opts.maxProcesses {
		b.mu.Unlock()
		b.logf("refused %s: %d binding processes run, the most this pod runs", labelOf(binding), b.opts.maxProcesses)
		return nil, http.StatusServiceUnavailable, fmt.Errorf("this server's pod runs at most %d binding processes", b.opts.maxProcesses)
	}
	started, err := b.start(binding, credential)
	if errors.Is(err, errNoUser) {
		b.mu.Unlock()
		b.logf("refused %s: every user of the pod belongs to a process still stopping", labelOf(binding))
		return nil, http.StatusServiceUnavailable, errors.New("this server's pod is stopping processes; try again")
	}
	if err != nil {
		b.mu.Unlock()
		b.logf("%s: the server's program did not start: %v", labelOf(binding), err)
		return nil, http.StatusBadGateway, errors.New("the server's program did not start")
	}
	b.processes[binding] = started
	b.mains[started.pid()] = true
	s, previous, _, err := b.attach(started, true)
	b.mu.Unlock()
	if replaced != nil {
		replaced.stop("a new session of its binding came while it had a call unanswered")
	}
	return b.opened(s, previous, err)
}

// opened closes the session a new one took over from, and answers open.
func (b *bridge) opened(s, previous *session, err error) (*session, int, error) {
	if previous != nil {
		previous.close()
	}
	if err != nil {
		return nil, http.StatusBadGateway, errors.New("the session did not start")
	}
	return s, 0, nil
}

// bindingCount is the binding processes running (the probe's and one
// being replaced excluded); the caller holds b.mu.
func (b *bridge) bindingCount(except *process) int {
	count := 0
	for binding, p := range b.processes {
		if binding != "" && p != except {
			count++
		}
	}
	return count
}

// attach opens a session on a process and returns the session the process
// served until now, for the caller to close. A process just started takes
// any session; another is taken over only when it is reusable, decided
// under its lock together with the switch (ok is false when it is not).
// The caller holds b.mu.
func (b *bridge) attach(p *process, fresh bool) (s, previous *session, ok bool, err error) {
	transport := &mcp.StreamableServerTransport{SessionID: rand.Text()}
	server, err := transport.Connect(context.Background())
	if err != nil {
		return nil, nil, false, err
	}
	s = &session{
		id:        transport.SessionID,
		process:   p,
		transport: transport,
		server:    server,
		closed:    make(chan struct{}),
	}
	p.mu.Lock()
	if !fresh && !p.reusableLocked() {
		p.mu.Unlock()
		server.Close()
		return nil, nil, false, nil
	}
	previous = p.current
	p.current = s
	s.reused = p.initDone
	p.lastUsed = b.now()
	p.mu.Unlock()
	b.sessions[s.id] = s
	go s.toProcess()
	if s.reused {
		b.logf("%s: a new session took over process %d", p.label(), p.pid())
	}
	return s, previous, true, nil
}

// start runs the program for one binding, as a user of its own with a
// private directory, and connects to it. The caller holds b.mu.
func (b *bridge) start(binding, credential string) (*process, error) {
	uid := 0
	if b.users != nil {
		var ok bool
		if uid, ok = b.users.take(); !ok {
			return nil, errNoUser
		}
	}
	b.sequence++
	home, err := makeHome(b.opts.homeRoot, homeName(uid, b.sequence), uid)
	if err != nil {
		b.unused(uid, "")
		return nil, fmt.Errorf("its directory: %w", err)
	}
	program := homeArgs(b.opts.program, home)
	cmd := exec.Command(program[0], program[1:]...)
	cmd.Env = homeEnv(childEnv(b.env, b.opts.credentialEnv, credential), home)
	if binding == "" {
		credential = "" // the probe's placeholder is no credential
	}
	stderr := &lineLogger{logf: b.logf, label: labelOf(binding), scrub: newScrubber(credential)}
	cmd.Stderr = stderr
	// A process that leaves its stderr open in a child never holds Wait.
	cmd.WaitDelay = b.opts.stopGrace + time.Second
	ownGroup(cmd)
	if uid > 0 {
		runAs(cmd.SysProcAttr, uid)
	}
	var child mcp.Connection
	onSpawner(func() {
		child, err = (&mcp.CommandTransport{Command: cmd, TerminateDuration: b.opts.stopGrace}).Connect(context.Background())
	})
	if err != nil {
		b.unused(uid, home) // nothing started
		return nil, err
	}
	now := b.now()
	p := &process{
		bridge:     b,
		binding:    binding,
		cmd:        cmd,
		child:      child,
		stderr:     stderr,
		credential: credential != "",
		started:    now,
		uid:        uid,
		home:       home,
		lastUsed:   now,
		pending:    map[jsonrpc.ID]*session{},
		done:       make(chan struct{}),
		stopped:    make(chan struct{}),
	}
	go p.toFront()
	if uid > 0 {
		b.logf("%s: started process %d as user %d", p.label(), p.pid(), uid)
	} else {
		b.logf("%s: started process %d", p.label(), p.pid())
	}
	return p, nil
}

// unused returns a user and directory no process ever ran with.
func (b *bridge) unused(uid int, home string) {
	if home != "" {
		os.RemoveAll(home)
	}
	if uid > 0 {
		b.users.give(uid)
	}
}

// release returns a stopped process's user to the pool once none of its
// processes or files is left; a user with a process that survived SIGKILL
// is never handed out again.
func (b *bridge) release(uid int, home string) {
	gone := true
	if uid > 0 {
		b.mu.Lock()
		mains := maps.Clone(b.mains)
		b.mu.Unlock()
		gone = killUser(uid, mains, reapWithin)
	}
	if err := os.RemoveAll(home); err != nil {
		b.logf("the directory %s was not removed: %v", home, err)
	}
	if uid <= 0 {
		return
	}
	if !gone {
		b.logf("user %d keeps a process: it is not handed out again", uid)
		return
	}
	sweepUser(b.opts.sweepDirs, uid)
	b.users.give(uid)
}

// toProcess copies the session's messages to its process's stdin. Each was
// checked to encode to the bytes the front sent, so the process reads
// exactly those. A session that took an initialized process over gets the
// process's own initialize answer, and its initialized notification stays
// here.
func (s *session) toProcess() {
	ctx := context.Background()
	p := s.process
	for {
		message, err := s.server.Read(ctx)
		if err != nil {
			s.close()
			return
		}
		if s.reused {
			if request, ok := message.(*jsonrpc.Request); ok {
				switch {
				case request.IsCall() && request.Method == "initialize":
					p.mu.Lock()
					result := p.initResult
					p.mu.Unlock()
					s.server.Write(ctx, &jsonrpc.Response{ID: request.ID, Result: result})
					continue
				case !request.IsCall() && request.Method == "notifications/initialized":
					continue
				}
			}
		}
		if hook := testHookRead.Load(); hook != nil {
			(*hook)(message)
		}
		p.mu.Lock()
		if p.current != s {
			// A new session took the process over since this message was
			// read: a call of this one registered now could take the answer
			// of the new session's call of the same id. Dropped, with the
			// session.
			p.mu.Unlock()
			s.close()
			return
		}
		if request, ok := message.(*jsonrpc.Request); ok && request.IsCall() {
			if request.Method == "initialize" && !p.initDone && !p.initID.IsValid() {
				p.initID = request.ID
			}
			p.pending[request.ID] = s
		}
		p.mu.Unlock()
		if err := p.child.Write(ctx, message); err != nil {
			p.stop("its process stopped reading")
			return
		}
	}
}

// toFront copies the process's messages to its sessions: an answer to the
// session whose call it answers, if that session is still the process's;
// a request or notification of the server's to its session's open stream.
// A message nothing can take is dropped.
func (p *process) toFront() {
	ctx := context.Background()
	for {
		message, err := p.child.Read(ctx)
		if err != nil {
			p.stop("its process ended")
			return
		}
		p.touch()
		var target *session
		p.mu.Lock()
		switch m := message.(type) {
		case *jsonrpc.Response:
			owner := p.pending[m.ID]
			delete(p.pending, m.ID)
			if p.initID.IsValid() && m.ID == p.initID {
				p.initID = jsonrpc.ID{}
				if m.Error == nil {
					p.initDone, p.initResult = true, m.Result
				}
			}
			if owner != nil && owner == p.current {
				target = owner
			}
		default:
			target = p.current
		}
		p.mu.Unlock()
		if target != nil {
			// Undeliverable (no stream open for it): dropped.
			target.server.Write(ctx, message)
		}
	}
}

// close ends a session: its requests and streams end and its id is
// unknown from now on. Its process stays with its binding.
func (s *session) close() {
	s.closeOnce.Do(func() {
		close(s.closed)
		s.server.Close()
		p := s.process
		b := p.bridge
		b.mu.Lock()
		if b.sessions[s.id] == s {
			delete(b.sessions, s.id)
		}
		b.mu.Unlock()
		p.mu.Lock()
		if p.current == s {
			p.current = nil
		}
		p.mu.Unlock()
	})
}

// failPending answers each call of the process's session the process will
// no longer answer with a closed-connection error, so its caller learns at
// once that the session ended (SRW's client reconnects within its budget)
// instead of waiting for its call timeout.
func (p *process) failPending(reason string) {
	p.mu.Lock()
	current := p.current
	var ids []jsonrpc.ID
	for id, owner := range p.pending {
		if owner == current {
			ids = append(ids, id)
		}
	}
	p.pending = map[jsonrpc.ID]*session{}
	p.mu.Unlock()
	for _, id := range ids {
		current.server.Write(context.Background(), &jsonrpc.Response{
			ID:    id,
			Error: &jsonrpc.Error{Code: connectionClosed, Message: "the server's process stopped: " + reason},
		})
	}
}

// stop ends the process's session at once (each call in flight is answered
// with a closed-connection error) and the process within the grace: stdin
// closed, then SIGTERM, then SIGKILL, and its process group killed after
// it; then every other process of its user (one that left its group or
// session), its directory and the files its user left.
func (p *process) stop(reason string) {
	p.stopOnce.Do(func() {
		p.failPending(reason)
		close(p.done)
		p.mu.Lock()
		current := p.current
		p.mu.Unlock()
		if current != nil {
			current.close()
		}
		b := p.bridge
		b.mu.Lock()
		if b.processes[p.binding] == p {
			delete(b.processes, p.binding)
		}
		b.mu.Unlock()
		b.stopping.Add(1)
		go func() {
			defer b.stopping.Done()
			defer close(p.stopped)
			pid := p.pid()
			err := p.child.Close()
			killGroup(pid)
			reapGroup(pid, reapWithin)
			p.stderr.flush()
			b.mu.Lock()
			delete(b.mains, pid)
			b.mu.Unlock()
			b.release(p.uid, p.home)
			outcome := "exited"
			if err != nil {
				outcome = err.Error()
			}
			b.logf("%s: process %d stopped (%s): %s", p.label(), pid, reason, outcome)
		}()
	})
}

// endBinding stops a binding's process: the front saw its lease end.
func (b *bridge) endBinding(binding, reason string) bool {
	b.mu.Lock()
	p := b.processes[binding]
	b.mu.Unlock()
	if p == nil {
		return false
	}
	p.stop(reason)
	return true
}

// housekeep stops idle processes and reaps orphans until ctx ends.
func (b *bridge) housekeep(ctx context.Context) {
	every := b.opts.idle / 4
	if every < time.Second {
		every = time.Second
	}
	if every > 30*time.Second {
		every = 30 * time.Second
	}
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			b.stopIdle()
			b.reapOrphans()
		}
	}
}

// stopIdle stops each process that has had no request or open stream for
// the idle time (the probe's for probeIdle at most).
func (b *bridge) stopIdle() {
	now := b.now()
	b.mu.Lock()
	var idle []*process
	for binding, p := range b.processes {
		limit := b.opts.idle
		if binding == "" && probeIdle < limit {
			limit = probeIdle
		}
		if p.idleFor(now) >= limit {
			idle = append(idle, p)
		}
	}
	b.mu.Unlock()
	for _, p := range idle {
		p.stop("idle")
	}
}

func (b *bridge) reapOrphans() {
	b.mu.Lock()
	mains := make(map[int]bool, len(b.mains))
	for pid := range b.mains {
		mains[pid] = true
	}
	b.mu.Unlock()
	reapOrphans(mains)
}

// close stops every process and waits for them, within their grace.
func (b *bridge) close(reason string) {
	b.mu.Lock()
	b.closed = true
	var all []*process
	for _, p := range b.processes {
		all = append(all, p)
	}
	b.mu.Unlock()
	for _, p := range all {
		p.stop(reason)
	}
	b.stopping.Wait()
}

type processStatus struct {
	Binding    string    `json:"binding"`
	Probe      bool      `json:"probe,omitempty"`
	PID        int       `json:"pid"`
	Started    time.Time `json:"started"`
	LastUsed   time.Time `json:"last_used"`
	Active     int       `json:"active"`
	Session    bool      `json:"session"`
	Credential bool      `json:"credential"`
	UID        int       `json:"uid,omitempty"`
}

type bridgeStatus struct {
	Processes     []processStatus `json:"processes"`
	MaxProcesses  int             `json:"max_processes"`
	CredentialEnv string          `json:"credential_env"`
	IdleSeconds   int             `json:"idle_seconds"`
}

// status lists the processes: their binding, process id, whether a session
// holds each now and whether each got a credential; never a credential or
// a session id.
func (b *bridge) status() bridgeStatus {
	b.mu.Lock()
	processes := make([]*process, 0, len(b.processes))
	for _, p := range b.processes {
		processes = append(processes, p)
	}
	b.mu.Unlock()
	out := bridgeStatus{
		Processes:     []processStatus{},
		MaxProcesses:  b.opts.maxProcesses,
		CredentialEnv: b.opts.credentialEnv,
		IdleSeconds:   int(b.opts.idle / time.Second),
	}
	for _, p := range processes {
		p.mu.Lock()
		out.Processes = append(out.Processes, processStatus{
			Binding:    p.binding,
			Probe:      p.binding == "",
			PID:        p.pid(),
			Started:    p.started.UTC(),
			LastUsed:   p.lastUsed.UTC(),
			Active:     p.active,
			Session:    p.current != nil,
			Credential: p.credential,
			UID:        p.uid,
		})
		p.mu.Unlock()
	}
	sort.Slice(out.Processes, func(i, j int) bool { return out.Processes[i].Binding < out.Processes[j].Binding })
	return out
}
