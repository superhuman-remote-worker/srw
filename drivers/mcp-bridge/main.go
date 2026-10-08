// Command srw-mcp-bridge is SRW's stdio bridge for managed MCP servers
// (connector drivers D5b): the command of a managed MCP pod's server
// container when its image speaks MCP over stdio. An init container copies
// it into the pod from SRW's front image, so a stock image (Docker's mcp/*
// catalogue) runs unchanged, with its own program as the bridge's arguments.
//
//	srw-mcp-bridge install DIR                     copy itself into DIR
//	srw-mcp-bridge serve [flags] -- PROGRAM [ARG...]
//	srw-mcp-bridge status --listen HOST:PORT       print the processes it runs
//	srw-mcp-bridge version
//
// It serves streamable HTTP on the pod's loopback address to SRW's front,
// which stays the authorization boundary: the front authenticates each
// request's lease, hides and refuses the tools the lease's access level may
// not call, exchanges the lease for the connector's credential and scrubs it
// from every answer. Behind it, the bridge runs one stdio process per binding
// (a stdio server serves one client at a time):
//
//   - the front names the binding (the lease) of every request in
//     Srw-Bridge-Binding and hands over the binding's credential in
//     Srw-Bridge-Credential (base64); a request without a binding is the
//     front's readiness probe, which gets a process of its own;
//   - an initialize starts the binding's process (the image's program, with
//     the container's environment and, in --credential-env, the binding's
//     credential; never the pod's Secret, which holds none); a new
//     initialize of the binding replaces its process, and one session's
//     requests reach only its own process;
//   - it accepts a message only in the exact form the front forwards (the
//     MCP Go SDK decodes it and must encode it back to the same bytes), so
//     the process reads exactly the bytes the front checked;
//   - it stops a binding's process when the front says the binding ended
//     (DELETE /srw/bindings/{binding}), when its session ends, and when it
//     has had no request or open stream for --idle; it runs at most
//     --max-processes binding processes at once (503 past it);
//   - each process runs in its own process group, which is killed with it;
//     its stderr goes to the container log with the credential scrubbed.
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
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

const usage = `usage:
  srw-mcp-bridge install DIR
  srw-mcp-bridge serve --listen 127.0.0.1:PORT [--path /mcp]
      [--credential-env NAME] [--max-processes N] [--idle 10m] [--stop-grace 5s]
      -- PROGRAM [ARG...]
  srw-mcp-bridge status --listen 127.0.0.1:PORT
  srw-mcp-bridge version
`

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
	set.StringVar(&opts.listen, "listen", "", "the loopback address to serve the front on")
	set.StringVar(&opts.path, "path", "/mcp", "the MCP path")
	set.StringVar(&opts.credentialEnv, "credential-env", "", "the environment variable a binding's credential is delivered in")
	set.IntVar(&opts.maxProcesses, "max-processes", 8, "binding processes at once")
	set.DurationVar(&opts.idle, "idle", 10*time.Minute, "stop a binding's process after this long without a request")
	set.DurationVar(&opts.stopGrace, "stop-grace", 5*time.Second, "how long a stopping process may take after its stdin closes")
	if err := set.Parse(args[:split]); err != nil {
		return options{}, err
	}
	if set.NArg() != 0 {
		return options{}, fmt.Errorf("unexpected arguments before --: %q", set.Args())
	}
	opts.program = append([]string(nil), args[split+1:]...)
	return opts, opts.validate()
}

func (o options) validate() error {
	host, port, err := net.SplitHostPort(o.listen)
	if err != nil || port == "" || !loopback(host) {
		return errors.New("--listen must be a loopback host:port: only the front beside it may reach the bridge")
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
	if o.program[0] == "" {
		return errors.New("the server's program is empty")
	}
	return nil
}

func loopback(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func serve(opts options) int {
	logger := log.New(os.Stderr, "srw-mcp-bridge: ", log.LstdFlags)
	b := newBridge(opts, os.Environ(), logger.Printf, time.Now)
	listener, err := net.Listen("tcp", opts.listen)
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
	logger.Printf("serving %s on %s for %q (at most %d binding processes, idle %s)", opts.path, opts.listen, opts.program[0], opts.maxProcesses, opts.idle)
	err = server.Serve(listener)
	b.close("the bridge is stopping")
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Printf("%v", err)
		return 1
	}
	return 0
}

// status prints the bridge's processes (what GET /srw/status answers): the
// bindings, their process ids and whether each got a credential; never a
// credential or a session id.
func status(args []string, stdout, stderr io.Writer) int {
	set := flag.NewFlagSet("status", flag.ContinueOnError)
	set.SetOutput(io.Discard)
	listen := set.String("listen", "", "the bridge's loopback address")
	if err := set.Parse(args); err != nil || *listen == "" || set.NArg() != 0 {
		fmt.Fprint(stderr, usage)
		return 2
	}
	host, _, err := net.SplitHostPort(*listen)
	if err != nil || !loopback(host) {
		fmt.Fprintln(stderr, "srw-mcp-bridge: --listen must be a loopback host:port")
		return 2
	}
	client := &http.Client{Timeout: 5 * time.Second}
	response, err := client.Get("http://" + *listen + controlPrefix + "status")
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
