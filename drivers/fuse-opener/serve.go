package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

// mounter is the privileged part, faked in tests.
type mounter interface {
	// Stale says whether the top mount at target is FUSE and no longer
	// answers: its daemon died, and every stat of it fails with ENOTCONN.
	Stale(target string) bool
	// Detach lazily unmounts every FUSE mount stacked at target, never
	// following a symlink, and says how many. A non-FUSE mount at target is
	// refused, never removed.
	Detach(target string) (int, error)
	// Check refuses a target that is not a directory or is a symlink.
	Check(target string) error
	// Mount opens /dev/fuse, mounts it at target and returns the descriptor.
	Mount(target string, spec mountSpec, p policy, uid, gid int) (int, error)
}

type server struct {
	// targets are the mountpoints this opener owns, in the order given;
	// readOnly says which of them it mounts read-only whatever the client
	// asks. dirs are plain directories it only creates: a mountpoint the
	// workspace itself uses, such as the protected overlay's merged view.
	targets   []string
	readOnly  map[string]bool
	dirs      []string
	policy    policy // the base policy; its ReadOnly forces every target
	clientUID int
	mounter   mounter
	peer      func(*net.UnixConn) (uid, gid int, err error)
	logger    *log.Logger
	mu        sync.Mutex // one mount operation at a time
	stopping  bool       // under mu: the shutdown detach ran; mount nothing more
}

// targetFor names the target a request is for. A client that sends no
// mountpoint (the exec client) gets the only target, if there is one.
func (s *server) targetFor(mountpoint string) (string, error) {
	if mountpoint == "" {
		if len(s.targets) == 1 {
			return s.targets[0], nil
		}
		return "", errors.New("name the mountpoint: this opener owns several")
	}
	clean := filepath.Clean(mountpoint)
	for _, target := range s.targets {
		if clean == target {
			return target, nil
		}
	}
	if len(s.targets) == 1 {
		return "", fmt.Errorf("only %s may be mounted", s.targets[0])
	}
	return "", fmt.Errorf("%s is not one of this opener's mountpoints", clean)
}

// policyFor is the base policy, read-only where the target is.
func (s *server) policyFor(target string) policy {
	p := s.policy
	p.ReadOnly = p.ReadOnly || s.readOnly[target]
	return p
}

const (
	requestTimeout = 10 * time.Second
	// Connections served at once; one past this is closed unanswered. Only
	// the rclone sidecar (and the opener's own probe) can reach the socket.
	maxHandlers = 8
	retryEvery  = 2 * time.Second
)

// readRequest reads one JSON line with plain reads: without a control
// buffer the kernel discards any descriptor a client tried to pass.
func readRequest(conn *net.UnixConn, req *request) error {
	buf := make([]byte, 0, 512)
	chunk := make([]byte, 512)
	for {
		n, err := conn.Read(chunk)
		buf = append(buf, chunk[:n]...)
		if i := bytes.IndexByte(buf, '\n'); i >= 0 {
			if i != len(buf)-1 {
				return errors.New("one request per connection")
			}
			if err := json.Unmarshal(buf[:i], req); err != nil {
				return fmt.Errorf("malformed request: %w", err)
			}
			return nil
		}
		if len(buf) > maxMessage {
			return errors.New("request too long")
		}
		if err != nil {
			return fmt.Errorf("incomplete request: %w", err)
		}
	}
}

func (s *server) handle(conn *net.UnixConn) {
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(requestTimeout))
	answer := func(err error, fd int) {
		resp := response{OK: err == nil}
		if err != nil {
			resp.Error = err.Error()
			fd = -1
		}
		if werr := writeMessage(conn, resp, fd); werr != nil {
			s.logger.Printf("answer lost: %v", werr)
		}
	}
	// Who is asking, before anything they sent is parsed. Root is the
	// opener's own startup probe and may only ping.
	uid, gid, err := s.peer(conn)
	if err != nil {
		s.logger.Printf("refused: no peer credentials: %v", err)
		answer(errors.New("no peer credentials"), -1)
		return
	}
	if uid != s.clientUID && uid != 0 {
		s.logger.Printf("refused: peer uid %d is not the client uid %d", uid, s.clientUID)
		answer(fmt.Errorf("uid %d may not use this opener", uid), -1)
		return
	}
	var req request
	if err := readRequest(conn, &req); err != nil {
		answer(err, -1)
		return
	}
	if req.Op == "ping" {
		answer(nil, -1)
		return
	}
	if uid != s.clientUID {
		answer(fmt.Errorf("uid %d may only ping", uid), -1)
		return
	}
	target, err := s.targetFor(req.Mountpoint)
	if err != nil {
		s.logger.Printf("refused: %s for %q: %v", req.Op, req.Mountpoint, err)
		answer(err, -1)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopping {
		answer(errors.New("the opener is stopping"), -1)
		return
	}
	switch req.Op {
	case "unmount":
		detached, err := s.mounter.Detach(target)
		s.logger.Printf("unmount %s: %d detached, %s", target, detached, errString(err))
		answer(err, -1)
	case "mount":
		p := s.policyFor(target)
		spec, err := planMount(p, req.Options)
		if err != nil {
			s.logger.Printf("refused mount: %v", err)
			answer(err, -1)
			return
		}
		// A restarted daemon finds its predecessor's dead mount here.
		detached, err := s.mounter.Detach(target)
		if err != nil {
			s.logger.Printf("refused mount: stale mount: %v", err)
			answer(err, -1)
			return
		}
		if detached > 0 {
			s.logger.Printf("detached %d stale mount(s) at %s", detached, target)
		}
		fd, err := s.mounter.Mount(target, spec, p, uid, gid)
		if err != nil {
			s.logger.Printf("mount %s failed: %v", target, err)
			answer(err, -1)
			return
		}
		s.logger.Printf("mounted %s (%s, flags %#x) for uid %d", target, spec.FSType, spec.Flags, uid)
		answer(nil, fd)
		// The client holds its copy now; the opener keeps none.
		closeFD(fd)
	default:
		answer(fmt.Errorf("unknown op %q", req.Op), -1)
	}
}

func errString(err error) string {
	if err == nil {
		return "ok"
	}
	return err.Error()
}

// prepareTarget makes the target servable and says how many dead mounts it
// detached. A restarted opener can find its predecessor's mount there: a
// live one (rclone still serves it) stays, a dead one answers ENOTCONN to
// every stat, mkdir included, so it goes first.
func prepareTarget(m mounter, target string) (int, error) {
	detached := 0
	if m.Stale(target) {
		n, err := m.Detach(target)
		if err != nil {
			return n, fmt.Errorf("detach the dead mount: %w", err)
		}
		detached = n
	}
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return detached, err
	}
	if err := os.Mkdir(target, 0o755); err != nil && !errors.Is(err, fs.ErrExist) {
		return detached, err
	}
	return detached, m.Check(target)
}

// prepareDir creates a plain directory the opener never mounts on, and
// refuses one a symlink stands in for.
func prepareDir(m mounter, dir string) error {
	if err := os.MkdirAll(filepath.Dir(dir), 0o755); err != nil {
		return err
	}
	if err := os.Mkdir(dir, 0o755); err != nil && !errors.Is(err, fs.ErrExist) {
		return err
	}
	return m.Check(dir)
}

// prepare readies every target and directory and says how many dead mounts
// it detached.
func (s *server) prepare() (int, error) {
	detached := 0
	for _, target := range s.targets {
		n, err := prepareTarget(s.mounter, target)
		detached += n
		if err != nil {
			return detached, err
		}
	}
	for _, dir := range s.dirs {
		if err := prepareDir(s.mounter, dir); err != nil {
			return detached, err
		}
	}
	return detached, nil
}

// listen creates the socket for the client's uid alone. The client mounts
// the socket's directory read-only, so only the opener writes there; it
// still never follows a link: the socket is born 0600 under the umask and
// handed over with lchown.
func listen(path string, uid int) (*net.UnixListener, error) {
	if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}
	old := syscall.Umask(0o177)
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	syscall.Umask(old)
	if err != nil {
		return nil, err
	}
	if err := os.Lchown(path, uid, -1); err != nil {
		listener.Close()
		return nil, err
	}
	return listener, nil
}

// serve handles connections concurrently, each under its own deadline, so
// an idle client never stalls the accept loop or the shutdown detach.
func (s *server) serve(listener *net.UnixListener, stop <-chan os.Signal) {
	go func() {
		<-stop
		listener.Close()
	}()
	slots := make(chan struct{}, maxHandlers)
	for {
		conn, err := listener.AcceptUnix()
		if err != nil {
			return
		}
		select {
		case slots <- struct{}{}:
			go func() {
				defer func() { <-slots }()
				s.handle(conn)
			}()
		default:
			conn.Close()
		}
	}
}

// shutdown detaches what the daemon left mounted, inside the grace period,
// so the kubelet can tear the emptyDir down; nothing mounts after it.
func (s *server) shutdown() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stopping = true
	for _, target := range s.targets {
		detached, err := s.mounter.Detach(target)
		s.logger.Printf("stopping: unmount %s: %d detached, %s", target, detached, errString(err))
	}
}

// start prepares the target and the socket, retrying instead of exiting: a
// crash-looping opener would leave nobody to detach a dead mount at Pod
// deletion. It returns nil once stop arrives first.
func (s *server) start(socketPath string, stop <-chan os.Signal) *net.UnixListener {
	for {
		detached, err := s.prepare()
		if detached > 0 {
			s.logger.Printf("detached %d dead mount(s) on start", detached)
		}
		if err == nil {
			var listener *net.UnixListener
			if listener, err = listen(socketPath, s.clientUID); err == nil {
				return listener
			}
		}
		s.logger.Printf("not serving yet, retrying: %v", err)
		select {
		case <-stop:
			return nil
		case <-time.After(retryEvery):
		}
	}
}

// targetFlag collects repeated --target PATH[:ro] arguments.
type targetFlag struct {
	paths    []string
	readOnly map[string]bool
}

func (f *targetFlag) String() string { return strings.Join(f.paths, ",") }

func (f *targetFlag) Set(value string) error {
	path, ro := strings.CutSuffix(value, ":ro")
	if !cleanAbsolute(path) {
		return fmt.Errorf("target %q must be a clean absolute path", value)
	}
	if _, seen := f.readOnly[path]; seen {
		return fmt.Errorf("target %s given twice", path)
	}
	if f.readOnly == nil {
		f.readOnly = map[string]bool{}
	}
	f.paths = append(f.paths, path)
	f.readOnly[path] = ro
	return nil
}

// dirFlag collects repeated --dir PATH arguments.
type dirFlag []string

func (f *dirFlag) String() string { return strings.Join(*f, ",") }

func (f *dirFlag) Set(value string) error {
	if !cleanAbsolute(value) {
		return fmt.Errorf("dir %q must be a clean absolute path", value)
	}
	*f = append(*f, value)
	return nil
}

func cleanAbsolute(path string) bool {
	return filepath.IsAbs(path) && filepath.Clean(path) == path && path != "/"
}

func serveMain(args []string, stderr io.Writer) int {
	flags := flag.NewFlagSet("serve", flag.ContinueOnError)
	flags.SetOutput(stderr)
	socketPath := flags.String("socket", "", "unix socket the client reaches")
	var targets targetFlag
	flags.Var(&targets, "target", "a mountpoint this opener owns, PATH or PATH:ro (repeatable)")
	var dirs dirFlag
	flags.Var(&dirs, "dir", "a plain directory to create, never mounted on (repeatable)")
	clientUID := flags.Int("client-uid", -1, "the only uid that may ask")
	readOnly := flags.Bool("read-only", false, "force every mount read-only")
	allowOther := flags.Bool("allow-other", true, "let other users enter the mount")
	source := flags.String("source", "srw-cloud", "the mount's source")
	subtype := flags.String("subtype", "rclone", "the mount's type is fuse.<subtype>")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if *socketPath == "" || len(targets.paths) == 0 || *clientUID <= 0 {
		fmt.Fprintln(stderr, "serve needs --socket, at least one --target and a non-root --client-uid")
		return 2
	}
	for _, dir := range dirs {
		if _, isTarget := targets.readOnly[dir]; isTarget {
			fmt.Fprintf(stderr, "%s is both a --target and a --dir\n", dir)
			return 2
		}
	}
	logger := log.New(stderr, "srw-fuse-opener: ", log.LstdFlags|log.LUTC)
	s := &server{
		targets:   targets.paths,
		readOnly:  targets.readOnly,
		dirs:      dirs,
		policy:    policy{ReadOnly: *readOnly, AllowOther: *allowOther, Source: *source, Subtype: *subtype},
		clientUID: *clientUID,
		mounter:   systemMounter{},
		peer:      peerCredentials,
		logger:    logger,
	}
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)
	if listener := s.start(*socketPath, stop); listener != nil {
		for _, target := range s.targets {
			logger.Printf("serving %s for uid %d (read-only %v)", target, s.clientUID, s.policyFor(target).ReadOnly)
		}
		s.serve(listener, stop)
	}
	s.shutdown()
	return 0
}

// checkMain is the daemon sidecar's startup probe, so its image needs no
// shell: exit 0 once target is a live fuse.<subtype> mount.
func checkMain(args []string, stderr io.Writer) int {
	flags := flag.NewFlagSet("check", flag.ContinueOnError)
	flags.SetOutput(stderr)
	target := flags.String("target", "", "the mountpoint")
	subtype := flags.String("subtype", "rclone", "the expected fuse.<subtype>")
	if err := flags.Parse(args); err != nil || *target == "" {
		return 2
	}
	if err := checkMount(filepath.Clean(*target), *subtype); err != nil {
		fmt.Fprintf(stderr, "check: %v\n", err)
		return 1
	}
	return 0
}

func pingMain(args []string, stderr io.Writer) int {
	flags := flag.NewFlagSet("ping", flag.ContinueOnError)
	flags.SetOutput(stderr)
	socketPath := flags.String("socket", "", "the opener's socket")
	if err := flags.Parse(args); err != nil || *socketPath == "" {
		return 2
	}
	if _, err := call(*socketPath, request{Op: "ping"}, 2*time.Second); err != nil {
		fmt.Fprintf(stderr, "ping: %v\n", err)
		return 1
	}
	return 0
}
