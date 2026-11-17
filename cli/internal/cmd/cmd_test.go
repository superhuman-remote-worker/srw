package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/superhuman-remote-worker/srw/cli/internal/exit"
)

const goodToken = "ak_good_token_0123456789"

var now = time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)

// harness is one person's machine and one server.
type harness struct {
	t        *testing.T
	home     string
	env      map[string]string
	server   *httptest.Server
	terminal bool
	secret   string
	version  string

	mu       sync.Mutex
	requests []string
	// identity and info are what the server answers; a nil info is a
	// server that does not have /api/version yet.
	identity map[string]any
	info     map[string]any
	// routes answers further paths, keyed by "METHOD /path".
	routes map[string]http.HandlerFunc
	// stdoutTerminal says a person reads the output.
	stdoutTerminal bool
}

func newHarness(t *testing.T) *harness {
	h := &harness{
		t: t, home: t.TempDir(), env: map[string]string{}, version: "0.3.0",
		identity: map[string]any{
			"user": map[string]any{
				"id": "11111111-1111-1111-1111-111111111111", "display_name": "Alice",
				"email": "alice@example.com", "is_admin": false, "is_approved": true,
			},
			"credential": map[string]any{
				"method": "pat", "token_name": "laptop", "scopes": []string{"jobs:read", "resources:write"},
				"scope": nil, "expires_at": "2027-06-01T00:00:00Z",
			},
		},
		info: map[string]any{
			"release_version": "0.3.1", "source_revision": "abc1234",
			"manifest_api_versions": []string{"srw/v1alpha1"}, "cockpit_url": "https://cockpit.example.com",
		},
	}
	h.server = httptest.NewServer(http.HandlerFunc(h.serve))
	t.Cleanup(h.server.Close)
	return h
}

func (h *harness) serve(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	h.requests = append(h.requests, r.Method+" "+r.URL.RequestURI())
	identity, info := h.identity, h.info
	h.mu.Unlock()
	answer := func(status int, body any) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		json.NewEncoder(w).Encode(body)
	}
	if r.Header.Get("Authorization") != "Bearer "+goodToken {
		answer(401, map[string]string{"detail": "Invalid token"})
		return
	}
	h.mu.Lock()
	handler := h.routes[r.Method+" "+r.URL.Path]
	h.mu.Unlock()
	if handler != nil {
		handler(w, r)
		return
	}
	switch r.URL.Path {
	case "/api/auth/me":
		answer(200, identity)
	case "/api/version":
		if info == nil {
			answer(404, map[string]string{"detail": "Not Found"})
			return
		}
		answer(200, info)
	case "/api/echo":
		body, _ := io.ReadAll(r.Body)
		answer(200, map[string]string{"method": r.Method, "body": string(body), "type": r.Header.Get("Content-Type"), "custom": r.Header.Get("X-Custom")})
	case "/api/jobs/missing":
		answer(404, map[string]string{"detail": "Job 'missing' not found"})
	default:
		answer(404, map[string]string{"detail": "Not Found"})
	}
}

func (h *harness) run(stdin string, args ...string) (stdout, stderr string, code int) {
	h.t.Helper()
	var out, errOut bytes.Buffer
	app := &App{
		In: strings.NewReader(stdin), Out: &out, Err: &errOut,
		Getenv: func(name string) string { return h.env[name] },
		Home:   h.home, Now: func() time.Time { return now },
		StdinIsTerminal: h.terminal, StdoutIsTerminal: h.stdoutTerminal,
		ReadSecret: func(string) (string, error) { return h.secret, nil },
		Version:    h.version, Commit: "c0ffee1",
		Pauses: []time.Duration{0, 0},
	}
	code = app.Run(context.Background(), args)
	if strings.Contains(out.String()+errOut.String(), goodToken) {
		h.t.Errorf("the token was printed by `srw %s`:\n%s\n%s", strings.Join(args, " "), out.String(), errOut.String())
	}
	return out.String(), errOut.String(), code
}

func (h *harness) file(name string) string {
	h.t.Helper()
	data, err := os.ReadFile(filepath.Join(h.home, ".config", "srw", name))
	if err != nil {
		return ""
	}
	return string(data)
}

func (h *harness) login() string {
	h.t.Helper()
	out, errOut, code := h.run(goodToken+"\n", "login", "--server", h.server.URL, "--context", "dev", "--token-stdin")
	if code != 0 {
		h.t.Fatalf("login failed with %d: %s", code, errOut)
	}
	return out
}

func want(t *testing.T, text string, parts ...string) {
	t.Helper()
	for _, part := range parts {
		if !strings.Contains(text, part) {
			t.Errorf("missing %q in:\n%s", part, text)
		}
	}
}

func TestLoginSavesAContextAndWhoamiReadsIt(t *testing.T) {
	h := newHarness(t)
	out := h.login()
	want(t, out, "Logged in to "+h.server.URL+" as Alice <alice@example.com>", `Context "dev" is now the current one`,
		"https://cockpit.example.com/settings/api-keys")

	want(t, h.file("config.yaml"), "current-context: dev", "server: "+h.server.URL)
	if strings.Contains(h.file("config.yaml"), "ak_") {
		t.Error("the config file holds a token")
	}
	want(t, h.file("credentials.yaml"), "dev: "+goodToken)
	if runtime.GOOS != "windows" {
		info, _ := os.Stat(filepath.Join(h.home, ".config", "srw", "credentials.yaml"))
		if info.Mode().Perm() != 0o600 {
			t.Errorf("credentials.yaml has mode %o", info.Mode().Perm())
		}
	}

	out, errOut, code := h.run("", "whoami")
	if code != 0 {
		t.Fatalf("whoami: %d %s", code, errOut)
	}
	want(t, out, "User:        Alice <alice@example.com>", "Server:      "+h.server.URL, "Context:     dev",
		"Token name:  laptop", "Scopes:      jobs:read, resources:write", "Expires:     2027-06-01 (in 195 days)",
		"Admin:       no (this token has no admin scope)", "credentials.yaml")
	if errOut != "" {
		t.Errorf("stderr: %q", errOut)
	}
}

func TestLoginWithAWrongTokenWritesNothing(t *testing.T) {
	h := newHarness(t)
	_, errOut, code := h.run("ak_wrong\n", "login", "--server", h.server.URL, "--token-stdin")
	if code != exit.Auth {
		t.Fatalf("code %d: %s", code, errOut)
	}
	want(t, errOut, "the token is unknown, revoked or expired", "Settings → API Keys")
	if h.file("config.yaml") != "" || h.file("credentials.yaml") != "" {
		t.Error("a failed login wrote a file")
	}
}

func TestLoginRules(t *testing.T) {
	t.Run("a server in the environment would shadow the login", func(t *testing.T) {
		h := newHarness(t)
		h.env["SRW_API_URL"] = h.server.URL
		_, errOut, code := h.run(goodToken+"\n", "login", "--server", h.server.URL, "--token-stdin")
		if code != exit.Usage || !strings.Contains(errOut, "SRW_API_URL is set") {
			t.Errorf("%d %s", code, errOut)
		}
	})
	t.Run("a person is asked for the server and the token", func(t *testing.T) {
		h := newHarness(t)
		h.terminal, h.secret = true, goodToken
		out, errOut, code := h.run(h.server.URL+"\n", "login")
		if code != 0 {
			t.Fatalf("%d %s", code, errOut)
		}
		want(t, errOut, "Server (the API address", "Settings → API Keys")
		// Without --context the context is named after the host.
		want(t, out, `Context "127.0.0.1-`)
	})
	t.Run("without a terminal the token must come from standard input", func(t *testing.T) {
		h := newHarness(t)
		_, errOut, code := h.run("", "login", "--server", h.server.URL)
		if code != exit.Usage || !strings.Contains(errOut, "--token-stdin") {
			t.Errorf("%d %s", code, errOut)
		}
		_, errOut, code = h.run(goodToken+"\n", "login", "--token-stdin")
		if code != exit.Usage || !strings.Contains(errOut, "--server") {
			t.Errorf("no server: %d %s", code, errOut)
		}
	})
	t.Run("http is refused for another machine", func(t *testing.T) {
		h := newHarness(t)
		_, errOut, code := h.run(goodToken+"\n", "login", "--server", "http://api.example.com", "--token-stdin")
		if code != exit.Usage || !strings.Contains(errOut, "unencrypted") {
			t.Errorf("%d %s", code, errOut)
		}
	})
	t.Run("a context with a token-command is not overwritten", func(t *testing.T) {
		h := newHarness(t)
		h.login()
		path := filepath.Join(h.home, ".config", "srw", "config.yaml")
		edited := strings.Replace(h.file("config.yaml"), "server:", "token-command: [pass, show, x]\n    server:", 1)
		os.WriteFile(path, []byte(edited), 0o644)
		_, errOut, code := h.run(goodToken+"\n", "login", "--server", h.server.URL, "--context", "dev", "--token-stdin")
		if code != exit.Usage || !strings.Contains(errOut, "token-command") {
			t.Errorf("%d %s", code, errOut)
		}
	})
}

func TestLogoutRemovesTheTokenAndSaysItIsStillValid(t *testing.T) {
	h := newHarness(t)
	h.login()
	out, _, code := h.run("", "logout")
	if code != 0 {
		t.Fatal(code)
	}
	want(t, out, `Removed the token of context "dev"`, "still valid on the server", "Settings → API Keys")
	if strings.Contains(h.file("credentials.yaml"), "ak_") {
		t.Error("the token is still in credentials.yaml")
	}
	want(t, h.file("config.yaml"), "current-context: dev")
	_, errOut, code := h.run("", "whoami")
	if code != exit.Auth || !strings.Contains(errOut, "srw login --context dev") {
		t.Errorf("whoami after logout: %d %s", code, errOut)
	}
	out, _, code = h.run("", "logout")
	if code != 0 || !strings.Contains(out, "has no saved token") {
		t.Errorf("a second logout: %d %s", code, out)
	}
}

func TestEnvironmentModeIgnoresTheSavedLogin(t *testing.T) {
	h := newHarness(t)
	h.login()
	h.env["SRW_API_URL"], h.env["SRW_TOKEN"] = h.server.URL, goodToken
	out, errOut, code := h.run("", "whoami")
	if code != 0 {
		t.Fatalf("%d %s", code, errOut)
	}
	want(t, out, "Context:     none (server and token come from the environment)", "Token from:  SRW_TOKEN")
	_, errOut, code = h.run("", "whoami", "--context", "dev")
	if code != exit.Usage || !strings.Contains(errOut, "either the environment or a saved context") {
		t.Errorf("--context in environment mode: %d %s", code, errOut)
	}
}

func TestWhoamiFormatsAndWarnings(t *testing.T) {
	h := newHarness(t)
	h.login()
	out, _, code := h.run("", "whoami", "-o", "json")
	if code != 0 {
		t.Fatal(code)
	}
	var document map[string]any
	if err := json.Unmarshal([]byte(out), &document); err != nil || document["user"] == nil {
		t.Errorf("not the server's object: %s", out)
	}
	if out, _, _ := h.run("", "whoami", "-o", "name"); out != "user/11111111-1111-1111-1111-111111111111\n" {
		t.Errorf("name: %q", out)
	}
	h.env["SRW_OUTPUT"] = "yaml"
	if out, _, _ := h.run("", "whoami"); !strings.Contains(out, "display_name: Alice") {
		t.Errorf("SRW_OUTPUT=yaml: %s", out)
	}
	delete(h.env, "SRW_OUTPUT")

	h.identity["credential"].(map[string]any)["expires_at"] = "2026-11-20T12:00:00Z"
	out, errOut, _ := h.run("", "whoami")
	want(t, out, "Expires:     2026-11-20 (in 3 days)")
	want(t, errOut, "warning: this token expires in 3 days")

	// An admin's token with the admin scope; and a server that reports no
	// credential yet.
	h.identity["user"].(map[string]any)["is_admin"] = true
	delete(h.identity, "credential")
	out, errOut, _ = h.run("", "whoami")
	want(t, out, "Admin:       yes")
	if strings.Contains(out, "Scopes") || errOut != "" {
		t.Errorf("an older server: %s %s", out, errOut)
	}
}

func TestVersion(t *testing.T) {
	h := newHarness(t)
	out, errOut, code := h.run("", "version")
	if code != 0 || !strings.Contains(out, "Client:  0.3.0 (commit c0ffee1)") || !strings.Contains(out, "Server:  unknown (no server configured)") {
		t.Errorf("nothing configured: %d %q %q", code, out, errOut)
	}
	h.login()
	out, errOut, code = h.run("", "version")
	if code != 0 {
		t.Fatal(errOut)
	}
	want(t, out, "Server:  0.3.1 (commit abc1234), manifest API srw/v1alpha1")
	if errOut != "" {
		t.Errorf("the same minor version must not warn: %q", errOut)
	}
	h.info["release_version"] = "0.4.0"
	_, errOut, _ = h.run("", "version")
	want(t, errOut, "warning: this srw is 0.3 and the server is 0.4")
	// A build from a checkout, or a server from one, is unknown: no warning.
	h.info["release_version"] = "development"
	if _, errOut, _ := h.run("", "version"); errOut != "" {
		t.Errorf("a development server warned: %q", errOut)
	}
	h.info = nil
	out, _, code = h.run("", "version")
	if code != 0 || !strings.Contains(out, "Server:  unknown (older server)") {
		t.Errorf("an older server: %d %q", code, out)
	}
	out, _, _ = h.run("", "version", "--client", "-o", "json")
	if strings.Contains(out, "server") || !strings.Contains(out, `"version": "0.3.0"`) {
		t.Errorf("--client: %s", out)
	}
	h.requests = nil
	h.run("", "version", "--client")
	if len(h.requests) != 0 {
		t.Errorf("--client asked the server: %v", h.requests)
	}
}

func TestConfigCommands(t *testing.T) {
	h := newHarness(t)
	if out, _, _ := h.run("", "config", "get-contexts"); !strings.Contains(out, "srw login") {
		t.Errorf("no context: %q", out)
	}
	h.login()
	out, _, code := h.run("", "config", "set-context", "prod", "--server", "https://api.example.com", "--project", "website", "--ca-file", "/etc/ca.pem")
	if code != 0 || !strings.Contains(out, "has no token yet") {
		t.Fatalf("set-context: %d %s", code, out)
	}
	out, _, _ = h.run("", "config", "get-contexts")
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) != 3 || !strings.HasPrefix(lines[1], "*        dev") || !strings.Contains(lines[2], "prod  https://api.example.com  website") {
		t.Errorf("table:\n%s", out)
	}
	if out, _, _ := h.run("", "config", "get-contexts", "-o", "name"); out != "context/dev\ncontext/prod\n" {
		t.Errorf("names: %q", out)
	}
	if out, _, code := h.run("", "config", "use-context", "prod"); code != 0 || !strings.Contains(out, `"prod" is now the current one`) {
		t.Errorf("use-context: %d %s", code, out)
	}
	if _, _, code := h.run("", "config", "use-context", "nope"); code != exit.NotFound {
		t.Errorf("an unknown context: %d", code)
	}
	out, _, _ = h.run("", "config", "view")
	want(t, out, "current-context: prod", "ca-file: /etc/ca.pem", "project: website")
	if _, _, code := h.run("", "config", "set-context", "new"); code != exit.Usage {
		t.Errorf("a new context without --server: %d", code)
	}
	out, _, code = h.run("", "config", "delete-context", "dev")
	if code != 0 || strings.Contains(h.file("credentials.yaml"), "ak_") || strings.Contains(h.file("config.yaml"), "dev:") {
		t.Errorf("delete-context: %d %s", code, out)
	}
	out, _, _ = h.run("", "config", "delete-context", "prod")
	want(t, out, "There is no current context now")
}

func TestAPI(t *testing.T) {
	h := newHarness(t)
	h.login()
	out, errOut, code := h.run("", "api", "/api/auth/me")
	if code != 0 || !strings.Contains(out, `"display_name":"Alice"`) {
		t.Fatalf("%d %s %s", code, out, errOut)
	}
	body := filepath.Join(t.TempDir(), "body.json")
	os.WriteFile(body, []byte(`{"a": 1}`), 0o600)
	out, _, code = h.run("", "api", "-X", "post", "/api/echo?x=1", "--input", body, "-H", "X-Custom: yes")
	if code != 0 {
		t.Fatal(out)
	}
	want(t, out, `"method":"POST"`, `"body":"{\"a\": 1}"`, `"type":"application/json"`, `"custom":"yes"`)
	if last := h.requests[len(h.requests)-1]; last != "POST /api/echo?x=1" {
		t.Errorf("request: %s", last)
	}
	out, _, _ = h.run(`{"from": "stdin"}`, "api", "-X", "PUT", "/api/echo", "--input", "-", "-i")
	want(t, out, "HTTP 200 OK\n", "Content-Type: application/json", `from`)

	// A failed request prints the server's body and exits by the status;
	// the error is JSON because this command's output is.
	out, errOut, code = h.run("", "api", "/api/jobs/missing")
	if code != exit.NotFound || !strings.Contains(out, "Job 'missing' not found") {
		t.Errorf("%d %s", code, out)
	}
	if errOut != `{"error":{"message":"Job 'missing' not found","status":404}}`+"\n" {
		t.Errorf("stderr: %s", errOut)
	}
	for _, refused := range [][]string{
		{"api", "https://evil.example.com/api/jobs"},
		{"api", "//evil.example.com/api/jobs"},
		{"api", "api/jobs"},
		{"api", "/api/jobs", "-H", "Authorization: Bearer x"},
		{"api", "/api/jobs", "-H", "no-colon"},
		{"api", "/api/jobs", "--input", "-", "--token-stdin"},
		{"api"},
	} {
		before := len(h.requests)
		if _, _, code := h.run("", refused...); code != exit.Usage || len(h.requests) != before {
			t.Errorf("%v: code %d, requests sent %d", refused, code, len(h.requests)-before)
		}
	}
}

func TestWrongUsageIsExitTwo(t *testing.T) {
	h := newHarness(t)
	h.login()
	for _, args := range [][]string{
		{"nonsense"},
		{"whoami", "--no-such-flag"},
		{"whoami", "extra"},
		{"whoami", "-o", "table"},
		{"config", "use-context"},
		{"version", "-o", "name"},
	} {
		_, errOut, code := h.run("", args...)
		if code != exit.Usage || !strings.HasPrefix(errOut, "error: ") {
			t.Errorf("%v: code %d, stderr %q", args, code, errOut)
		}
	}
	h.env["SRW_OUTPUT"] = "table"
	if _, _, code := h.run("", "whoami"); code != exit.Usage {
		t.Errorf("a bad SRW_OUTPUT: %d", code)
	}
}

func TestAnInterruptedCommandExits130(t *testing.T) {
	h := newHarness(t)
	h.login()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var out, errOut bytes.Buffer
	app := &App{
		In: strings.NewReader(""), Out: &out, Err: &errOut,
		Getenv: func(name string) string { return h.env[name] }, Home: h.home, Now: time.Now,
		Version: "0.3.0", Commit: "c",
	}
	if code := app.Run(ctx, []string{"whoami"}); code != exit.Interrupted {
		t.Errorf("code %d, stderr %q", code, errOut.String())
	}
}

func TestHelpNeverDependsOnTheEnvironment(t *testing.T) {
	render := func(env map[string]string) string {
		var out bytes.Buffer
		app := &App{In: strings.NewReader(""), Out: &out, Err: &out, Getenv: func(name string) string { return env[name] }, Home: "/home/a", Now: time.Now}
		app.Run(context.Background(), []string{"--help"})
		app.Run(context.Background(), []string{"api", "--help"})
		return out.String()
	}
	plain := render(nil)
	if plain != render(map[string]string{"SRW_API_URL": "https://x.example.com", "SRW_OUTPUT": "json", "SRW_CONTEXT": "dev"}) {
		t.Error("the help text changes with the environment; the generated reference would differ between machines")
	}
	want(t, plain, "login", "logout", "whoami", "config", "version", "api", "completion")
}
