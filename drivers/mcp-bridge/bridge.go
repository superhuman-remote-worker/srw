package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	// Set by the front on every request it forwards: the binding (its
	// lease's id) and that binding's credential (base64). The front never
	// forwards a caller's own header of these names.
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
)

// A binding id is the lease's id the front names (a UUID in SRW).
var bindingShape = regexp.MustCompile(`\A[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}\z`)

type options struct {
	listen        string
	path          string
	credentialEnv string
	maxProcesses  int
	idle          time.Duration
	stopGrace     time.Duration
	program       []string
}

// bridge serves the front and runs one process per binding.
type bridge struct {
	opts options
	env  []string
	logf func(string, ...any)
	now  func() time.Time

	mu       sync.Mutex
	closed   bool
	sessions map[string]*session // by Mcp-Session-Id
	bindings map[string]*session // by binding; "" is the front's probe
	// Process groups not yet reaped (live sessions and stopping ones): the
	// orphan reaper never waits for one of them.
	groups   map[int]bool
	stopping sync.WaitGroup
}

func newBridge(opts options, env []string, logf func(string, ...any), now func() time.Time) *bridge {
	return &bridge{
		opts:     opts,
		env:      env,
		logf:     logf,
		now:      now,
		sessions: map[string]*session{},
		bindings: map[string]*session{},
		groups:   map[int]bool{},
	}
}

// session is one binding's MCP session and the process serving it: the
// SDK's streamable server transport faces the front, its command transport
// the process, and two pumps copy messages between them.
type session struct {
	bridge     *bridge
	id         string
	binding    string
	transport  *mcp.StreamableServerTransport
	server     mcp.Connection
	child      mcp.Connection
	cmd        *exec.Cmd
	stderr     *lineLogger
	credential bool
	started    time.Time

	mu       sync.Mutex
	lastUsed time.Time
	active   int

	done     chan struct{}
	stopOnce sync.Once
	// Closed once the process and its group are gone.
	stopped chan struct{}
}

func (s *session) label() string {
	if s.binding == "" {
		return "probe"
	}
	return "binding=" + s.binding
}

func (s *session) pid() int {
	if s.cmd.Process == nil {
		return 0
	}
	return s.cmd.Process.Pid
}

// enter and leave count the requests and streams open on the session; a
// session with one open is never idle.
func (s *session) enter() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.active++
	s.lastUsed = s.bridge.now()
}

func (s *session) leave() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.active--
	s.lastUsed = s.bridge.now()
}

func (s *session) touch() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastUsed = s.bridge.now()
}

func (s *session) idleFor(now time.Time) time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.active > 0 {
		return 0
	}
	return now.Sub(s.lastUsed)
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
		s.enter()
		defer s.leave()
		s.transport.ServeHTTP(w, r)
	case http.MethodGet:
		s := b.lookup(sessionID, binding)
		if s == nil {
			refuse(w, http.StatusNotFound, "session not found")
			return
		}
		s.enter()
		defer s.leave()
		s.transport.ServeHTTP(w, r)
	case http.MethodDelete:
		s := b.lookup(sessionID, binding)
		if s == nil {
			refuse(w, http.StatusNotFound, "session not found")
			return
		}
		s.stop("its client ended the session")
		w.WriteHeader(http.StatusNoContent)
	default:
		w.Header().Set("Allow", "GET, POST, DELETE")
		refuse(w, http.StatusMethodNotAllowed, "GET, POST or DELETE")
	}
}

func labelOf(binding string) string {
	if binding == "" {
		return "probe"
	}
	return "binding=" + binding
}

// lookup finds a session of this binding: a session id another binding
// opened is no session of this one's.
func (b *bridge) lookup(sessionID, binding string) *session {
	if sessionID == "" {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	s := b.sessions[sessionID]
	if s == nil || s.binding != binding {
		return nil
	}
	return s
}

// open starts a session and its process for a binding, replacing the
// binding's previous one: one process per binding.
func (b *bridge) open(binding, credential string) (*session, int, error) {
	if b.opts.credentialEnv != "" && binding != "" && credential == "" {
		return nil, http.StatusBadRequest, errors.New("the front named no credential for this binding")
	}
	if b.opts.credentialEnv == "" || binding == "" {
		// The probe never runs with a credential, and a server that takes
		// none gets none.
		credential = ""
	}
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil, http.StatusServiceUnavailable, errors.New("the bridge is stopping")
	}
	previous := b.bindings[binding]
	if binding != "" && previous == nil && b.bindingCount() >= b.opts.maxProcesses {
		b.mu.Unlock()
		b.logf("refused %s: %d binding processes run, the most this pod runs", labelOf(binding), b.opts.maxProcesses)
		return nil, http.StatusServiceUnavailable, fmt.Errorf("this server's pod runs at most %d binding processes", b.opts.maxProcesses)
	}
	s, err := b.start(binding, credential)
	if err != nil {
		b.mu.Unlock()
		b.logf("%s: the server's program did not start: %v", labelOf(binding), err)
		return nil, http.StatusBadGateway, errors.New("the server's program did not start")
	}
	if previous != nil {
		delete(b.sessions, previous.id)
	}
	b.sessions[s.id] = s
	b.bindings[binding] = s
	b.groups[s.pid()] = true
	b.mu.Unlock()
	if previous != nil {
		previous.stop("a new session of its binding replaced it")
	}
	return s, 0, nil
}

// bindingCount is the binding processes running (the probe's excluded);
// the caller holds b.mu.
func (b *bridge) bindingCount() int {
	count := 0
	for binding := range b.bindings {
		if binding != "" {
			count++
		}
	}
	return count
}

// start runs the program for one binding and connects the two halves.
func (b *bridge) start(binding, credential string) (*session, error) {
	cmd := exec.Command(b.opts.program[0], b.opts.program[1:]...)
	cmd.Env = childEnv(b.env, b.opts.credentialEnv, credential)
	stderr := &lineLogger{logf: b.logf, label: labelOf(binding), scrub: newScrubber(credential)}
	cmd.Stderr = stderr
	// A process that leaves its stderr open in a child never holds Wait.
	cmd.WaitDelay = b.opts.stopGrace + time.Second
	ownGroup(cmd)
	child, err := (&mcp.CommandTransport{Command: cmd, TerminateDuration: b.opts.stopGrace}).Connect(context.Background())
	if err != nil {
		return nil, err
	}
	transport := &mcp.StreamableServerTransport{SessionID: rand.Text()}
	server, err := transport.Connect(context.Background())
	if err != nil {
		child.Close()
		return nil, err
	}
	now := b.now()
	s := &session{
		bridge:     b,
		id:         transport.SessionID,
		binding:    binding,
		transport:  transport,
		server:     server,
		child:      child,
		cmd:        cmd,
		stderr:     stderr,
		credential: credential != "",
		started:    now,
		lastUsed:   now,
		done:       make(chan struct{}),
		stopped:    make(chan struct{}),
	}
	go s.toProcess()
	go s.toFront()
	b.logf("%s: started process %d", s.label(), s.pid())
	return s, nil
}

// toProcess copies the front's messages to the process's stdin. Each was
// checked to encode to the bytes the front sent, so the process reads
// exactly those.
func (s *session) toProcess() {
	ctx := context.Background()
	for {
		message, err := s.server.Read(ctx)
		if err != nil {
			s.stop("its session closed")
			return
		}
		if err := s.child.Write(ctx, message); err != nil {
			s.stop("its process stopped reading")
			return
		}
	}
}

// toFront copies the process's messages to the session: an answer to the
// request that asked, anything else to the session's open stream. A
// message nothing can take (no stream open, an answer nobody waits for) is
// dropped.
func (s *session) toFront() {
	ctx := context.Background()
	for {
		message, err := s.child.Read(ctx)
		if err != nil {
			s.stop("its process ended")
			return
		}
		s.touch()
		if err := s.server.Write(ctx, message); err != nil {
			select {
			case <-s.done:
				return
			default:
			}
		}
	}
}

// stop ends the session at once (its requests and streams end; its id is
// unknown from now on) and its process within the grace: stdin closed,
// then SIGTERM, then SIGKILL, and its process group killed after it.
func (s *session) stop(reason string) {
	s.stopOnce.Do(func() {
		close(s.done)
		s.server.Close()
		b := s.bridge
		b.forget(s)
		b.stopping.Add(1)
		go func() {
			defer b.stopping.Done()
			defer close(s.stopped)
			pid := s.pid()
			err := s.child.Close()
			killGroup(pid)
			reapGroup(pid, reapWithin)
			s.stderr.flush()
			b.mu.Lock()
			delete(b.groups, pid)
			b.mu.Unlock()
			outcome := "exited"
			if err != nil {
				outcome = err.Error()
			}
			b.logf("%s: process %d stopped (%s): %s", s.label(), pid, reason, outcome)
		}()
	})
}

// forget removes the session from the registry, if it is still there.
func (b *bridge) forget(s *session) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.sessions[s.id] == s {
		delete(b.sessions, s.id)
	}
	if b.bindings[s.binding] == s {
		delete(b.bindings, s.binding)
	}
}

// endBinding stops a binding's process: the front saw its lease end.
func (b *bridge) endBinding(binding, reason string) bool {
	b.mu.Lock()
	s := b.bindings[binding]
	b.mu.Unlock()
	if s == nil {
		return false
	}
	s.stop(reason)
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

// stopIdle stops each process whose session has had no request or open
// stream for the idle time (the probe's for probeIdle at most).
func (b *bridge) stopIdle() {
	now := b.now()
	b.mu.Lock()
	var idle []*session
	for _, s := range b.sessions {
		limit := b.opts.idle
		if s.binding == "" && probeIdle < limit {
			limit = probeIdle
		}
		if s.idleFor(now) >= limit {
			idle = append(idle, s)
		}
	}
	b.mu.Unlock()
	for _, s := range idle {
		s.stop("idle")
	}
}

func (b *bridge) reapOrphans() {
	b.mu.Lock()
	live := make(map[int]bool, len(b.groups))
	for pid := range b.groups {
		live[pid] = true
	}
	b.mu.Unlock()
	reapOrphans(live)
}

// close stops every process and waits for them, within their grace.
func (b *bridge) close(reason string) {
	b.mu.Lock()
	b.closed = true
	var all []*session
	for _, s := range b.sessions {
		all = append(all, s)
	}
	b.mu.Unlock()
	for _, s := range all {
		s.stop(reason)
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
	Credential bool      `json:"credential"`
}

type bridgeStatus struct {
	Processes     []processStatus `json:"processes"`
	MaxProcesses  int             `json:"max_processes"`
	CredentialEnv string          `json:"credential_env"`
	IdleSeconds   int             `json:"idle_seconds"`
}

// status lists the processes: their binding, process id and whether each
// got a credential; never a credential or a session id.
func (b *bridge) status() bridgeStatus {
	b.mu.Lock()
	sessions := make([]*session, 0, len(b.bindings))
	for _, s := range b.bindings {
		sessions = append(sessions, s)
	}
	b.mu.Unlock()
	out := bridgeStatus{
		Processes:     []processStatus{},
		MaxProcesses:  b.opts.maxProcesses,
		CredentialEnv: b.opts.credentialEnv,
		IdleSeconds:   int(b.opts.idle / time.Second),
	}
	for _, s := range sessions {
		s.mu.Lock()
		out.Processes = append(out.Processes, processStatus{
			Binding:    s.binding,
			Probe:      s.binding == "",
			PID:        s.pid(),
			Started:    s.started.UTC(),
			LastUsed:   s.lastUsed.UTC(),
			Active:     s.active,
			Credential: s.credential,
		})
		s.mu.Unlock()
	}
	sort.Slice(out.Processes, func(i, j int) bool { return out.Processes[i].Binding < out.Processes[j].Binding })
	return out
}
