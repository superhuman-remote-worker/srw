// Command srw-git-swap is SRW's git swap driver (connector drivers C3): a
// git smart-HTTP reverse proxy, not a second Git server, in the service pod
// of one repository connector. A workspace's git reaches it through a
// per-binding insteadOf while its remote stays the clean upstream URL, and
// presents the binding's lease token through SRW's credential helper; the
// forge token never enters the workspace.
//
//	srw-git-swap serve      serve the srw-driver port over TLS
//	srw-git-swap version
//
// Before it serves, it checks that its upstream answers HTTPS with a
// certificate it trusts (public roots, or only the connector's upstream CA
// when it names one); an upstream it cannot reach or trust ends the
// process with exit code 78 and the reason as the container's termination
// message, which SRW records with the pod (deliveries then fall back).
//
// For every request it
//
//   - serves only GET info/refs?service=git-upload-pack|git-receive-pack,
//     POST git-upload-pack and POST git-receive-pack under
//     /<connector>/<repository>, the connector's configured repository; any
//     other path, method, escape or dot segment is a 404 (Git LFS gets a
//     message saying it is unsupported);
//   - answers a missing or invalid lease with 401 and WWW-Authenticate: Basic,
//     so git asks its credential helper; the lease must be live and this
//     pod's connector's (the exchange's introspection, cached for at most
//     the 30 s revocation lag);
//   - decides a push by the request path, never the "service" query: a
//     ReadOnly lease gets 403 for both receive-pack forms;
//   - for a ReadWrite push, parses only the command list (shallow lines and
//     push certificates included), refuses deletes and any ref outside
//     refs/heads/ with "ng <ref> <reason>", and forwards the head it checked,
//     re-encoded, then streams the pack through;
//   - exchanges the lease for the connector's forge token (read or write by
//     path), injects it upstream as Basic auth and forwards only to the
//     connector's configured upstream, never a host or path from the
//     request; it follows no redirect;
//   - strips packfile-uris and bundle-uri from a v2 capability
//     advertisement, so every object comes through the driver;
//   - passes Git-Protocol, Content-Encoding and Transfer-Encoding through,
//     buffers nothing but the heads it checks (1 MiB, 256 KiB), caps no body
//     and cuts a transfer only after 15 minutes without a byte;
//   - scrubs the forge token from every answer and maps every upstream
//     refusal to its own message (never the upstream's body or challenge);
//   - logs each request's lease, route, status and size, and each pushed
//     ref, never a token or a credential.
//
// It is static (CGO off) and uses only the Go standard library.
package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"
)

const usage = `usage:
  srw-git-swap serve
  srw-git-swap version
`

// version is set at build time (-ldflags "-X main.version=...").
var version = "dev"

func main() {
	os.Exit(dispatch(os.Args[1:]))
}

func dispatch(args []string) int {
	if len(args) != 1 {
		fmt.Fprint(os.Stderr, usage)
		return 2
	}
	switch args[0] {
	case "version", "--version":
		fmt.Println(version)
		return 0
	case "serve":
		cfg, err := loadConfig(os.Getenv)
		if errors.Is(err, errBadUpstreamCA) {
			// Reported like an upstream it cannot trust: SRW stops the pod
			// at once and token repositories fall back.
			fmt.Fprintf(os.Stderr, "srw-git-swap: %s\n", clean(err.Error()))
			writeTermination(terminationLog, badCAReport)
			return upstreamExitCode
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "srw-git-swap: %v\n", err)
			return 2
		}
		logf := func(format string, a ...any) {
			log.Printf("srw-git-swap: "+format, a...)
		}
		client := newUpstreamClient(cfg.upstreamRoots)
		// An upstream the driver cannot reach or trust is reported, not
		// served: SRW stops the pod and token repositories fall back. The
		// termination message names a fixed class; the upstream's own
		// words go to the log only, cleaned.
		if problem, detail := probeUpstream(context.Background(), client, cfg.upstream, probeAttempts, probePause); problem != "" {
			logf("%s (%s)", problem, detail)
			writeTermination(terminationLog, problem)
			return upstreamExitCode
		}
		handler := newDriver(cfg, newHTTPAuthority(cfg.exchangeURL, cfg.identity), client, logf, systemNow)
		server := newServer(":"+cfg.port, handler)
		logf("serving %s for connector %s on :%s (upstream %s)", cfg.driver, cfg.connectorID, cfg.port, cfg.upstream.url)
		if err := server.ListenAndServeTLS(cfg.certFile, cfg.keyFile); err != nil {
			logf("%v", err)
			return 1
		}
		return 0
	}
	fmt.Fprint(os.Stderr, usage)
	return 2
}

// newServer serves HTTP/1.1 over TLS only: git's smart HTTP is chunked
// HTTP/1.1, and a transfer's only time limit is the handler's idle watch.
func newServer(addr string, handler http.Handler) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 30 * time.Second,
		IdleTimeout:       defaultIdleTimeout,
		MaxHeaderBytes:    64 * 1024,
		TLSConfig:         &tls.Config{MinVersion: tls.VersionTLS12, NextProtos: []string{"http/1.1"}},
		// A non-nil, empty map turns HTTP/2 off.
		TLSNextProto: map[string]func(*http.Server, *tls.Conn, http.Handler){},
	}
}

func systemNow() time.Time { return time.Now() }
