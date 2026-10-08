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
//   - caps the calls and streams one binding has open (429), before it
//     reads a body, which must arrive within 30 s;
//   - parses the message strictly (exact keys, no duplicate, no unknown key,
//     valid UTF-8, no batch) and forwards a body re-encoded from what it
//     checked, never the caller's bytes, so a server that reads JSON another
//     way decides on the same message;
//   - forwards only the methods it knows (initialize, ping, tools/list,
//     tools/call and their notifications), and a tools/call only of a tool
//     the lease's access level allows (the spec's tool classes; a tool no
//     class names is a write tool);
//   - hides the tools a lease may not call from every answer carrying a tool
//     list, whatever its id, on JSON answers and streams alike;
//   - exchanges the lease for the connector's upstream credential and hands
//     it to the server in the header the spec names; the lease token never
//     reaches the server, and the credential never reaches the caller: it is
//     scrubbed from every answer, plain, escaped, URL- or base64-encoded;
//   - keeps a session to the lease that opened it; an unknown session is
//     nobody's (404, the client initializes again);
//   - buffers each answer (4 MiB at most, 96 MiB in all) and ends a stream
//     when its lease ends (a call still waiting gets a "lease revoked"
//     JSON-RPC error, code -32091) or after 15 minutes;
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
			// The whole request, its body included (a stream's answer is
			// no read): a slow body never holds a call slot for long.
			ReadTimeout: bodyReadDeadline + 5*time.Second,
			IdleTimeout: 2 * time.Minute,
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
