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
	target    string
	policy    policy
	clientUID int
	mounter   mounter
	peer      func(*net.UnixConn) (uid, gid int, err error)
	logger    *log.Logger
	mu        sync.Mutex // one mount operation at a time
	stopping  bool       // under mu: the shutdown detach ran; mount nothing more
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
	if req.Mountpoint != "" && filepath.Clean(req.Mountpoint) != s.target {
		s.logger.Printf("refused: %s for %q, not the target", req.Op, req.Mountpoint)
		answer(fmt.Errorf("only %s may be mounted", s.target), -1)
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
		detached, err := s.mounter.Detach(s.target)
		s.logger.Printf("unmount %s: %d detached, %s", s.target, detached, errString(err))
		answer(err, -1)
	case "mount":
		spec, err := planMount(s.policy, req.Options)
		if err != nil {
			s.logger.Printf("refused mount: %v", err)
			answer(err, -1)
			return
		}
		// A restarted daemon finds its predecessor's dead mount here.
		detached, err := s.mounter.Detach(s.target)
		if err != nil {
			s.logger.Printf("refused mount: stale mount: %v", err)
			answer(err, -1)
			return
		}
		if detached > 0 {
			s.logger.Printf("detached %d stale mount(s) at %s", detached, s.target)
		}
		fd, err := s.mounter.Mount(s.target, spec, s.policy, uid, gid)
		if err != nil {
			s.logger.Printf("mount %s failed: %v", s.target, err)
			answer(err, -1)
			return
		}
		s.logger.Printf("mounted %s (%s, flags %#x) for uid %d", s.target, spec.FSType, spec.Flags, uid)
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
	s.stopping = true
	detached, err := s.mounter.Detach(s.target)
	s.mu.Unlock()
	s.logger.Printf("stopping: unmount %s: %d detached, %s", s.target, detached, errString(err))
}

// start prepares the target and the socket, retrying instead of exiting: a
// crash-looping opener would leave nobody to detach a dead mount at Pod
// deletion. It returns nil once stop arrives first.
func (s *server) start(socketPath string, stop <-chan os.Signal) *net.UnixListener {
	for {
		detached, err := prepareTarget(s.mounter, s.target)
		if detached > 0 {
			s.logger.Printf("detached %d dead mount(s) at %s on start", detached, s.target)
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

func serveMain(args []string, stderr io.Writer) int {
	flags := flag.NewFlagSet("serve", flag.ContinueOnError)
	flags.SetOutput(stderr)
	socketPath := flags.String("socket", "", "unix socket the client reaches")
	target := flags.String("target", "", "the one mountpoint this opener owns")
	clientUID := flags.Int("client-uid", -1, "the only uid that may ask")
	readOnly := flags.Bool("read-only", false, "force a read-only mount")
	allowOther := flags.Bool("allow-other", true, "let other users enter the mount")
	source := flags.String("source", "srw-cloud", "the mount's source")
	subtype := flags.String("subtype", "rclone", "the mount's type is fuse.<subtype>")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if *socketPath == "" || *target == "" || *clientUID <= 0 {
		fmt.Fprintln(stderr, "serve needs --socket, --target and a non-root --client-uid")
		return 2
	}
	logger := log.New(stderr, "srw-fuse-opener: ", log.LstdFlags|log.LUTC)
	s := &server{
		target:    filepath.Clean(*target),
		policy:    policy{ReadOnly: *readOnly, AllowOther: *allowOther, Source: *source, Subtype: *subtype},
		clientUID: *clientUID,
		mounter:   systemMounter{},
		peer:      peerCredentials,
		logger:    logger,
	}
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)
	if listener := s.start(*socketPath, stop); listener != nil {
		logger.Printf("serving %s for uid %d (read-only %v)", s.target, s.clientUID, s.policy.ReadOnly)
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
