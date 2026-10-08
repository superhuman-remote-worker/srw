// Command srw-mcp-test is a development MCP server (connector drivers D5a):
// the image behind srw.mcp-test/v1, which scripts/k3d-managed-mcp-gate.py
// runs behind SRW's front to prove a managed MCP pod. It is not a product
// driver.
//
// It speaks the 2025 streamable HTTP transport with sessions (initialize
// opens one; a request naming an unknown session gets 404, as after a pod
// replacement) on -listen (default 127.0.0.1:8091) at /mcp, and treats the
// Authorization bearer of each request as its upstream credential:
//
//	whoami           read: the SHA-256 of the credential it received, the
//	                 connector's message and this pod's name
//	notes_list       read: the note names
//	notes_read       read: one note
//	leak_credential  read: answers with the credential itself (the front
//	                 must scrub it)
//	notes_write      write: sets a note
//	notes_delete     write: deletes a note
//
// Listing tools needs no credential (the front's readiness probe has none);
// every call does. It never logs a credential.
//
// With -stdio (D5b) it serves one client on stdin and stdout instead, behind
// SRW's stdio bridge, with its credential in -credential-env, and lists the
// probe tools the stdio gate uses (stdio.go). That mode is the image behind
// srw.mcp-stdio-probe/v1, which scripts/k3d-managed-mcp-stdio-gate.py runs
// to prove one binding's process cannot reach another's.
package main

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

type server struct {
	message  string
	hostname string
	// Serving one client on stdin and stdout (D5b): the probe tools are
	// listed too.
	stdio    bool
	mu       sync.Mutex
	sessions map[string]bool
	notes    map[string]string
}

func newServer(message, hostname string) *server {
	return &server{message: message, hostname: hostname, sessions: map[string]bool{}, notes: map[string]string{}}
}

type tool struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	InputSchema map[string]any `json:"inputSchema"`
}

func schema(properties map[string]any, required ...string) map[string]any {
	out := map[string]any{"type": "object", "properties": properties}
	if len(required) > 0 {
		out["required"] = required
	}
	return out
}

var (
	nameProperty = map[string]any{"name": map[string]any{"type": "string"}}
	tools        = []tool{
		{"whoami", "Reports the SHA-256 of the credential this server received, the connector's message and the pod's name.", schema(map[string]any{})},
		{"notes_list", "Lists the names of the notes.", schema(map[string]any{})},
		{"notes_read", "Reads one note.", schema(nameProperty, "name")},
		{"leak_credential", "Answers with the credential this server received (a test of the front's scrubbing).", schema(map[string]any{})},
		{"notes_write", "Sets a note.", schema(map[string]any{"name": map[string]any{"type": "string"}, "text": map[string]any{"type": "string"}}, "name", "text")},
		{"notes_delete", "Deletes a note.", schema(nameProperty, "name")},
	}
)

type rpcRequest struct {
	ID     json.RawMessage `json:"id"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
}

func newSessionID() string {
	buffer := make([]byte, 16)
	rand.Read(buffer)
	return hex.EncodeToString(buffer)
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(body)
}

func result(id json.RawMessage, value any) map[string]any {
	return map[string]any{"jsonrpc": "2.0", "id": id, "result": value}
}

func rpcError(id json.RawMessage, code int, message string) map[string]any {
	return map[string]any{"jsonrpc": "2.0", "id": id, "error": map[string]any{"code": code, "message": message}}
}

func text(value string, failed bool) map[string]any {
	return map[string]any{"content": []any{map[string]any{"type": "text", "text": value}}, "isError": failed}
}

func credentialOf(r *http.Request) string {
	scheme, token, found := strings.Cut(r.Header.Get("Authorization"), " ")
	if !found || !strings.EqualFold(scheme, "Bearer") {
		return ""
	}
	return strings.TrimSpace(token)
}

func (s *server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/healthz" {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
		return
	}
	if r.URL.Path != "/mcp" {
		http.NotFound(w, r)
		return
	}
	session := r.Header.Get("Mcp-Session-Id")
	switch r.Method {
	case http.MethodGet:
		// No server-initiated stream.
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	case http.MethodDelete:
		s.mu.Lock()
		delete(s.sessions, session)
		s.mu.Unlock()
		w.WriteHeader(http.StatusOK)
		return
	case http.MethodPost:
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	var request rpcRequest
	if err := json.Unmarshal(body, &request); err != nil {
		writeJSON(w, http.StatusBadRequest, rpcError(nil, -32700, "parse error"))
		return
	}
	if request.Method == "initialize" {
		id := newSessionID()
		s.mu.Lock()
		s.sessions[id] = true
		s.mu.Unlock()
		w.Header().Set("Mcp-Session-Id", id)
		writeJSON(w, http.StatusOK, result(request.ID, map[string]any{
			"protocolVersion": "2025-06-18",
			"capabilities":    map[string]any{"tools": map[string]any{"listChanged": false}},
			"serverInfo":      map[string]any{"name": "srw-mcp-test", "version": "1"},
		}))
		return
	}
	s.mu.Lock()
	known := s.sessions[session]
	s.mu.Unlock()
	if session == "" {
		writeJSON(w, http.StatusBadRequest, rpcError(request.ID, -32600, "a session is required"))
		return
	}
	if !known {
		writeJSON(w, http.StatusNotFound, rpcError(request.ID, -32001, "session not found"))
		return
	}
	if len(request.ID) == 0 || string(request.ID) == "null" {
		// A notification.
		w.WriteHeader(http.StatusAccepted)
		return
	}
	writeJSON(w, http.StatusOK, s.answer(request, credentialOf(r)))
}

// answer is the JSON-RPC answer to a request that is no initialize.
func (s *server) answer(request rpcRequest, credential string) map[string]any {
	switch request.Method {
	case "ping":
		return result(request.ID, map[string]any{})
	case "tools/list":
		if s.stdio {
			return result(request.ID, map[string]any{"tools": append(append([]tool(nil), tools...), probeTools...)})
		}
		return result(request.ID, map[string]any{"tools": tools})
	case "tools/call":
		return result(request.ID, s.call(credential, request.Params))
	}
	return rpcError(request.ID, -32601, "method not found")
}

func (s *server) call(credential string, raw json.RawMessage) map[string]any {
	var params struct {
		Name      string            `json:"name"`
		Arguments map[string]string `json:"arguments"`
	}
	if err := json.Unmarshal(raw, &params); err != nil {
		return text("invalid arguments", true)
	}
	if s.stdio {
		if found, ok := probeTool(params.Name, params.Arguments); ok {
			return text(found, false)
		}
	}
	if credential == "" {
		return text("no upstream credential reached this server", true)
	}
	log.Printf("srw-mcp-test: call %q", params.Name)
	s.mu.Lock()
	defer s.mu.Unlock()
	name := params.Arguments["name"]
	switch params.Name {
	case "whoami":
		digest := sha256.Sum256([]byte(credential))
		answer, _ := json.Marshal(map[string]string{
			"credential_sha256": hex.EncodeToString(digest[:]),
			"message":           s.message,
			"pod":               s.hostname,
		})
		return text(string(answer), false)
	case "leak_credential":
		return text("credential: "+credential, false)
	case "notes_list":
		names := []string{}
		for note := range s.notes {
			names = append(names, note)
		}
		answer, _ := json.Marshal(names)
		return text(string(answer), false)
	case "notes_read":
		note, ok := s.notes[name]
		if !ok {
			return text("no such note", true)
		}
		return text(note, false)
	case "notes_write":
		s.notes[name] = params.Arguments["text"]
		return text("written", false)
	case "notes_delete":
		delete(s.notes, name)
		return text("deleted", false)
	}
	return text(fmt.Sprintf("unknown tool %q", params.Name), true)
}

func main() {
	listen := flag.String("listen", "127.0.0.1:8091", "address to serve MCP on")
	stdio := flag.Bool("stdio", false, "serve one client on stdin and stdout instead (D5b)")
	credentialEnv := flag.String("credential-env", "MCP_TEST_TOKEN", "with -stdio: the variable the credential is in")
	flag.Parse()
	hostname, _ := os.Hostname()
	s := newServer(os.Getenv("MCP_TEST_MESSAGE"), hostname)
	if *stdio {
		s.stdio = true
		log.SetOutput(os.Stderr)
		if err := s.serveStdio(os.Stdin, os.Stdout, os.Getenv(*credentialEnv)); err != nil {
			log.Fatal(err)
		}
		return
	}
	server := &http.Server{Addr: *listen, Handler: s, ReadHeaderTimeout: 10 * time.Second}
	log.Printf("srw-mcp-test: serving on %s", *listen)
	log.Fatal(server.ListenAndServe())
}
