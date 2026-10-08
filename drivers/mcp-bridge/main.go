// Command srw-mcp-bridge is SRW's stdio bridge for managed MCP servers
// (connector drivers D5b): the command of a managed MCP pod's server
// container when its image speaks MCP over stdio. An init container copies
// it into the pod from SRW's front image, so a stock image (Docker's mcp/*
// catalogue) runs unchanged, with its own program as the bridge's arguments.
//
//	srw-mcp-bridge install DIR                     copy itself into DIR
//	srw-mcp-bridge serve [flags] -- PROGRAM [ARG...]
//	srw-mcp-bridge status --socket PATH            print the processes it runs
//	srw-mcp-bridge version
//
// It serves streamable HTTP to SRW's front on a unix socket in a directory
// only the front's group may enter (--socket, --socket-group): never on a
// port, which every process of the pod could reach. The front stays the
// authorization boundary: it authenticates each request's lease, hides and
// refuses the tools the lease's access level may not call, exchanges the
// lease for the connector's credential and scrubs it from every answer.
// Behind it, the bridge runs one stdio process per binding (a stdio server
// serves one client at a time):
//
//   - the front names the binding (the lease) of every request in
//     Srw-Bridge-Binding and hands over the binding's credential in
//     Srw-Bridge-Credential (base64); a request without a binding is the
//     front's readiness probe, which gets a process of its own with a
//     placeholder in the credential's variable (srw-probe-placeholder,
//     never a credential), so a server that exits without it still starts;
//   - the binding's first initialize starts its process (the image's
//     program, with the container's environment and, in --credential-env,
//     the binding's credential; never the pod's Secret, which holds none),
//     and a session's requests reach only its binding's process;
//   - each process runs as a user of its own (--uid-base: the bridge is the
//     container's root, with SETUID and SETGID; the process keeps no
//     capability and can gain none), with a private directory (0700) as
//     HOME and TMPDIR (${binding.home} in its environment or arguments) and
//     private files (umask 077): a binding's process cannot read another's
//     environment or files, signal it, or reach the bridge's socket;
//   - the process lives with its binding and serves the binding's later
//     sessions too (SRW's agent opens one per attach), one at a time: a new
//     session takes over, its initialize is answered with the process's own
//     first answer, so the server is initialized once; a process with a
//     call still unanswered is restarted instead;
//   - it accepts a message only in the exact form the front forwards (the
//     MCP Go SDK decodes it and must encode it back to the same bytes), so
//     the process reads exactly the bytes the front checked;
//   - it stops a binding's process when the front says the binding ended
//     (DELETE /srw/bindings/{binding}), when the process exits, and when it
//     has had no request or open stream for --idle, answering its calls in
//     flight at once; it runs at most --max-processes binding processes at
//     once (503 past it);
//   - each process runs in its own process group, which is killed with it,
//     and then every other process of its user (one that left its group or
//     session); its directory and its user's files in /tmp and /dev/shm go
//     before the user is handed out again; the bridge adopts every orphan
//     a process leaves (it is a subreaper) and reaps it while the process
//     still runs; a process's stderr goes to the container log with the
//     credential scrubbed.
//
// It is built on the official MCP Go SDK: CommandTransport runs each process
// and StreamableServerTransport serves its session to the front. It is
// static (CGO off).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

const usage = `usage:
  srw-mcp-bridge install DIR
  srw-mcp-bridge serve --socket PATH [--socket-group GID] [--path /mcp]
      --home-root DIR [--uid-base UID] [--sweep-dir DIR ...]
      [--credential-env NAME] [--max-processes N] [--idle 10m] [--stop-grace 5s]
      -- PROGRAM [ARG...]
  srw-mcp-bridge status --socket PATH
  srw-mcp-bridge version
`

// The shared writable directories of a container whose root filesystem is
// read-only (the pod mounts /tmp; /dev/shm is the runtime's).
var defaultSweepDirs = []string{"/tmp", "/dev/shm"}

// The front's user, never one of the processes'.
const frontUser = 65532

// version is set at build time (-ldflags "-X main.version=...").
var version = "dev"

func main() {
	os.Exit(dispatch(os.Args[1:], os.Stdout, os.Stderr))
}

func dispatch(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, usage)
		return 2
	}
	switch args[0] {
	case "version", "--version":
		fmt.Fprintln(stdout, version)
		return 0
	case "install":
		if len(args) != 2 {
			fmt.Fprint(stderr, usage)
			return 2
		}
		if err := install(args[1]); err != nil {
			fmt.Fprintf(stderr, "srw-mcp-bridge: install: %v\n", err)
			return 1
		}
		return 0
	case "status":
		return status(args[1:], stdout, stderr)
	case "serve":
		opts, err := parseServe(args[1:])
		if err != nil {
			fmt.Fprintf(stderr, "srw-mcp-bridge: %v\n", err)
			fmt.Fprint(stderr, usage)
			return 2
		}
		return serve(opts)
	}
	fmt.Fprint(stderr, usage)
	return 2
}

// dirList is a repeatable flag.
type dirList []string

func (d *dirList) String() string { return strings.Join(*d, ",") }

func (d *dirList) Set(value string) error {
	*d = append(*d, value)
	return nil
}

// parseServe reads serve's flags and the program after "--".
func parseServe(args []string) (options, error) {
	split := -1
	for i, arg := range args {
		if arg == "--" {
			split = i
			break
		}
	}
	if split < 0 || split == len(args)-1 {
		return options{}, errors.New("serve needs the server's program after --")
	}
	set := flag.NewFlagSet("serve", flag.ContinueOnError)
	set.SetOutput(io.Discard)
	opts := options{}
	var sweep dirList
	set.StringVar(&opts.socket, "socket", "", "the unix socket to serve the front on")
	set.IntVar(&opts.socketGroup, "socket-group", -1, "the group (the front's) that may reach the socket")
	set.StringVar(&opts.path, "path", "/mcp", "the MCP path")
	set.StringVar(&opts.credentialEnv, "credential-env", "", "the environment variable a binding's credential is delivered in")
	set.IntVar(&opts.maxProcesses, "max-processes", 8, "binding processes at once")
	set.DurationVar(&opts.idle, "idle", 10*time.Minute, "stop a binding's process after this long without a request")
	set.DurationVar(&opts.stopGrace, "stop-grace", 5*time.Second, "how long a stopping process may take after its stdin closes")
	set.IntVar(&opts.uidBase, "uid-base", 0, "the first user the processes run as (0: as the bridge, for tests only)")
	set.StringVar(&opts.homeRoot, "home-root", "", "where each process's private directory is made")
	set.Var(&sweep, "sweep-dir", "a shared writable directory swept of a user's files (default /tmp and /dev/shm)")
	if err := set.Parse(args[:split]); err != nil {
		return options{}, err
	}
	if set.NArg() != 0 {
		return options{}, fmt.Errorf("unexpected arguments before --: %q", set.Args())
	}
	opts.sweepDirs = sweep
	if len(opts.sweepDirs) == 0 {
		opts.sweepDirs = append([]string(nil), defaultSweepDirs...)
	}
	opts.program = append([]string(nil), args[split+1:]...)
	return opts, opts.validate()
}

func absolute(path string) bool {
	return filepath.IsAbs(path) && filepath.Clean(path) == path
}

func (o options) validate() error {
	// sun_path holds 108 bytes, its NUL included.
	if !absolute(o.socket) || len(o.socket) > 100 || o.socket == "/" {
		return errors.New("--socket must be an absolute path of at most 100 bytes")
	}
	if o.socketGroup < -1 || o.socketGroup == 0 {
		return errors.New("--socket-group must be the front's group (not root's)")
	}
	if !strings.HasPrefix(o.path, "/") || strings.HasPrefix(o.path, controlPrefix) {
		return errors.New("--path must be an absolute path outside /srw/")
	}
	if o.credentialEnv != "" {
		if err := checkEnvName(o.credentialEnv); err != nil {
			return fmt.Errorf("--credential-env: %w", err)
		}
	}
	if o.maxProcesses < 1 || o.maxProcesses > 64 {
		return errors.New("--max-processes must be between 1 and 64")
	}
	if o.idle < time.Second || o.idle > 24*time.Hour {
		return errors.New("--idle must be between 1s and 24h")
	}
	if o.stopGrace < 0 || o.stopGrace > time.Minute {
		return errors.New("--stop-grace must be between 0 and 1m")
	}
	if !absolute(o.homeRoot) || o.homeRoot == "/" {
		return errors.New("--home-root must be an absolute directory")
	}
	for _, dir := range o.sweepDirs {
		if !absolute(dir) || dir == "/" {
			return fmt.Errorf("--sweep-dir %q must be an absolute directory", dir)
		}
	}
	if o.uidBase != 0 {
		last := o.uidBase + poolSize(o.maxProcesses) - 1
		switch {
		case o.uidBase < 1000 || last >= frontUser:
			return fmt.Errorf("--uid-base: the users %d to %d must lie between 1000 and %d", o.uidBase, last, frontUser-1)
		case o.socketGroup >= o.uidBase && o.socketGroup <= last:
			return errors.New("--socket-group is one of the processes' users")
		}
	}
	if o.program[0] == "" {
		return errors.New("the server's program is empty")
	}
	return nil
}

func serve(opts options) int {
	logger := log.New(os.Stderr, "srw-mcp-bridge: ", log.LstdFlags)
	if err := restrictProcess(); err != nil {
		logger.Printf("%v", err)
		return 1
	}
	if err := becomeSubreaper(); err != nil {
		logger.Printf("the bridge cannot adopt its processes' orphans (%v): only the container's first process reaps them", err)
	}
	if opts.uidBase > 0 {
		if err := prepareUsers(opts); err != nil {
			logger.Printf("%v", err)
			return 1
		}
	} else {
		if err := os.MkdirAll(opts.homeRoot, 0o700); err != nil {
			logger.Printf("%v", err)
			return 1
		}
		logger.Printf("every process runs as the bridge's own user (--uid-base 0): bindings are not isolated from each other")
	}
	b := newBridge(opts, os.Environ(), logger.Printf, time.Now)
	listener, err := listenSocket(opts.socket, opts.socketGroup)
	if err != nil {
		logger.Printf("%v", err)
		return 1
	}
	server := &http.Server{
		Handler:           b,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       2 * time.Minute,
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	go b.housekeep(ctx)
	go func() {
		<-ctx.Done()
		// Streams never finish on their own: close them, then stop every
		// process within its grace.
		closing, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		server.Shutdown(closing)
		server.Close()
	}()
	logger.Printf("serving %s on %s for %q (at most %d binding processes, idle %s)", opts.path, opts.socket, opts.program[0], opts.maxProcesses, opts.idle)
	err = server.Serve(listener)
	b.close("the bridge is stopping")
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Printf("%v", err)
		return 1
	}
	return 0
}

// status prints the bridge's processes (what GET /srw/status answers): the
// bindings, their process ids and users and whether each got a credential;
// never a credential or a session id.
func status(args []string, stdout, stderr io.Writer) int {
	set := flag.NewFlagSet("status", flag.ContinueOnError)
	set.SetOutput(io.Discard)
	socket := set.String("socket", "", "the bridge's unix socket")
	if err := set.Parse(args); err != nil || !absolute(*socket) || set.NArg() != 0 {
		fmt.Fprint(stderr, usage)
		return 2
	}
	response, err := socketClient(*socket, 5*time.Second).Get("http://srw-mcp-bridge" + controlPrefix + "status")
	if err != nil {
		fmt.Fprintf(stderr, "srw-mcp-bridge: %v\n", err)
		return 1
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		fmt.Fprintf(stderr, "srw-mcp-bridge: status answered HTTP %d\n", response.StatusCode)
		return 1
	}
	if _, err := io.Copy(stdout, io.LimitReader(response.Body, 1<<20)); err != nil {
		return 1
	}
	return 0
}
