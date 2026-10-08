package main

import (
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
	// Detach lazily unmounts every FUSE mount stacked at target and says
	// how many. A mount whose daemon died answers ENOTCONN until it is
	// detached. A non-FUSE mount at target is refused, never removed.
	Detach(target string) (int, error)
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
}

const requestTimeout = 10 * time.Second

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
	var req request
	if fd, err := readMessage(conn, &req); err != nil {
		answer(err, -1)
		return
	} else if fd >= 0 {
		closeFD(fd)
		answer(errors.New("a request carries no descriptor"), -1)
		return
	}
	if req.Op == "ping" {
		// The opener's own startup probe runs as root; a ping touches nothing.
		answer(nil, -1)
		return
	}
	uid, gid, err := s.peer(conn)
	if err != nil {
		s.logger.Printf("refused: no peer credentials: %v", err)
		answer(errors.New("no peer credentials"), -1)
		return
	}
	if uid != s.clientUID {
		s.logger.Printf("refused: peer uid %d is not the client uid %d", uid, s.clientUID)
		answer(fmt.Errorf("uid %d may not use this opener", uid), -1)
		return
	}
	if req.Mountpoint != "" && filepath.Clean(req.Mountpoint) != s.target {
		s.logger.Printf("refused: %s for %q, not the target", req.Op, req.Mountpoint)
		answer(fmt.Errorf("only %s may be mounted", s.target), -1)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
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

// listen creates the socket for the client's uid alone.
func listen(path string, uid int) (*net.UnixListener, error) {
	if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		return nil, err
	}
	if err := os.Chown(path, uid, -1); err != nil {
		listener.Close()
		return nil, err
	}
	if err := os.Chmod(path, 0o600); err != nil {
		listener.Close()
		return nil, err
	}
	return listener, nil
}

func (s *server) serve(listener *net.UnixListener, stop <-chan os.Signal) {
	go func() {
		<-stop
		listener.Close()
	}()
	for {
		conn, err := listener.AcceptUnix()
		if err != nil {
			return
		}
		s.handle(conn)
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
	if *socketPath == "" || *target == "" || *clientUID < 0 {
		fmt.Fprintln(stderr, "serve needs --socket, --target and --client-uid")
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
	if err := os.MkdirAll(s.target, 0o755); err != nil {
		logger.Printf("target: %v", err)
		return 1
	}
	listener, err := listen(*socketPath, *clientUID)
	if err != nil {
		logger.Printf("listen: %v", err)
		return 1
	}
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)
	logger.Printf("serving %s for uid %d (read-only %v)", s.target, s.clientUID, s.policy.ReadOnly)
	s.serve(listener, stop)
	// The kubelet stops sidecars after the workspace; whatever the daemon
	// left mounted goes now, inside the grace period, so the emptyDir can be
	// torn down.
	s.mu.Lock()
	detached, err := s.mounter.Detach(s.target)
	s.mu.Unlock()
	logger.Printf("stopping: unmount %s: %d detached, %s", s.target, detached, errString(err))
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
