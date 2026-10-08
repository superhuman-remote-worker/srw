// Command srw-mcp-front is SRW's front for managed MCP servers (connector
// drivers D5a): the only exposed port of a managed MCP driver pod, in front
// of a stock MCP server image that speaks streamable HTTP on 127.0.0.1.
//
//	srw-mcp-front serve      serve the srw-driver port in front of the server
//	srw-mcp-front version
//
// For every request it
//
//   - refuses one that carries an Origin header (no browser reaches it);
//   - authenticates the caller's lease token (Authorization: Bearer scl_...)
//     with the lease exchange's introspection route, with the pod's own sdi_
//     identity: the lease must be live and for this pod's connector, else 401;
//   - refuses a JSON-RPC batch, and a tools/call of a tool the lease's access
//     level does not allow (the spec's tool classes; a tool no class names is
//     a write tool); hides those tools from tools/list;
//   - caps the calls one binding has in flight (429);
//   - exchanges the lease for the connector's upstream credential and hands
//     it to the server in the header the spec names; the lease token never
//     reaches the server, and the credential never reaches the caller: exact
//     occurrences are scrubbed from every response;
//   - keeps a session to the lease that opened it;
//   - logs each call's tool, class, status and duration, never an argument,
//     a token or a credential.
//
// /readyz is a real MCP probe of the server (initialize and tools/list), and
// /livez answers while the process runs. It is static (CGO off) and uses
// only the Go standard library.
package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"time"
)

const usage = `usage:
  srw-mcp-front serve
  srw-mcp-front version
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
		if err != nil {
			fmt.Fprintf(os.Stderr, "srw-mcp-front: %v\n", err)
			return 2
		}
		logf := func(format string, a ...any) {
			log.Printf("srw-mcp-front: "+format, a...)
		}
		handler := newFront(cfg, newHTTPAuthority(cfg.exchangeURL, cfg.identity), newUpstreamClient(), logf, systemNow)
		server := &http.Server{
			Addr:              ":" + cfg.port,
			Handler:           handler,
			ReadHeaderTimeout: 10 * time.Second,
			IdleTimeout:       2 * time.Minute,
		}
		logf("serving %s for connector %s on :%s in front of %s", cfg.driver, cfg.connectorID, cfg.port, cfg.upstream)
		if err := server.ListenAndServe(); err != nil {
			logf("%v", err)
			return 1
		}
		return 0
	}
	fmt.Fprint(os.Stderr, usage)
	return 2
}

func systemNow() time.Time { return time.Now() }
