package remote

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/superhuman-remote-worker/srw/cli/internal/config"
	"github.com/superhuman-remote-worker/srw/cli/internal/exit"
)

type fixture struct {
	t      *testing.T
	env    map[string]string
	paths  config.Paths
	stderr bytes.Buffer
	stdin  string
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	paths, err := config.Locate(func(string) string { return "" }, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return &fixture{t: t, env: map[string]string{}, paths: paths}
}

func (f *fixture) saved(current string, contexts map[string]*config.Context, tokens map[string]string) {
	f.t.Helper()
	if err := (&config.File{CurrentContext: current, Contexts: contexts}).Save(f.paths); err != nil {
		f.t.Fatal(err)
	}
	if err := (&config.Credentials{Tokens: tokens}).Save(f.paths); err != nil {
		f.t.Fatal(err)
	}
}

func (f *fixture) resolve(options Options) (*Target, error) {
	host := Host{
		Getenv: func(name string) string { return f.env[name] },
		Stdin:  strings.NewReader(f.stdin),
		Stderr: &f.stderr,
	}
	return Resolve(options, host, f.paths, true)
}

func code(t *testing.T, err error) int {
	t.Helper()
	var coded *exit.Error
	if !errors.As(err, &coded) {
		t.Fatalf("not an exit error: %v", err)
	}
	return coded.Code
}

func TestEnvironmentModeTakesTheServerAndTheTokenFromTheEnvironment(t *testing.T) {
	f := newFixture(t)
	// A saved login exists and must not be used.
	f.saved("dev", map[string]*config.Context{"dev": {Server: "https://saved.example.com"}}, map[string]string{"dev": "ak_saved"})
	f.env["SRW_API_URL"] = "https://ci.example.com/"
	f.env["SRW_TOKEN"] = " ak_ci \n"
	got, err := f.resolve(Options{})
	if err != nil {
		t.Fatal(err)
	}
	if got.Mode != Environment || got.Server.String() != "https://ci.example.com" || got.Token != "ak_ci" || got.TokenSource != "SRW_TOKEN" || got.Context != "" {
		t.Errorf("%+v", got)
	}
}

func TestEnvironmentModeTokenOrder(t *testing.T) {
	f := newFixture(t)
	if err := os.MkdirAll(filepath.Dir(f.paths.EnvToken), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(f.paths.EnvToken, []byte("ak_file\nsecond line\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	f.env["COCKPIT_API_URL"] = "https://legacy.example.com"
	got, err := f.resolve(Options{})
	if err != nil || got.Token != "ak_file" || got.TokenSource != f.paths.EnvToken {
		t.Fatalf("the token file: %+v %v", got, err)
	}
	f.env["SRW_TOKEN"] = "ak_env"
	if got, _ := f.resolve(Options{}); got.Token != "ak_env" {
		t.Errorf("SRW_TOKEN must win over the file: %+v", got)
	}
	f.stdin = "ak_stdin\n"
	got, err = f.resolve(Options{Server: "https://flag.example.com", TokenStdin: true})
	if err != nil || got.Token != "ak_stdin" || got.Server.Host != "flag.example.com" {
		t.Errorf("--server and --token-stdin must win: %+v %v", got, err)
	}
}

func TestTheTwoModesAreNeverMixed(t *testing.T) {
	f := newFixture(t)
	f.saved("dev", map[string]*config.Context{"dev": {Server: "https://saved.example.com"}}, nil)
	f.env["SRW_TOKEN"] = "ak_env"
	if _, err := f.resolve(Options{Server: "https://x.example.com", Context: "dev"}); code(t, err) != exit.Usage {
		t.Errorf("--server with --context: %v", err)
	}
	f.env["SRW_API_URL"], f.env["SRW_CONTEXT"] = "https://x.example.com", "dev"
	if _, err := f.resolve(Options{}); code(t, err) != exit.Usage {
		t.Errorf("SRW_API_URL with SRW_CONTEXT: %v", err)
	}
}

func TestContextModeDoesNotUseAnExportedToken(t *testing.T) {
	f := newFixture(t)
	f.saved("dev", map[string]*config.Context{
		"dev":  {Server: "https://dev.example.com", Project: "p1", CAFile: "/ca.pem"},
		"prod": {Server: "https://prod.example.com"},
	}, map[string]string{"dev": "ak_dev", "prod": "ak_prod"})
	f.env["SRW_TOKEN"] = "ak_exported"
	got, err := f.resolve(Options{})
	if err != nil {
		t.Fatal(err)
	}
	if got.Mode != Context || got.Context != "dev" || got.Token != "ak_dev" || got.Project != "p1" || got.CAFile != "/ca.pem" {
		t.Errorf("%+v", got)
	}
	if note := f.stderr.String(); !strings.Contains(note, "SRW_TOKEN is set but not used") || strings.Contains(note, "ak_") {
		t.Errorf("stderr: %q", note)
	}
	f.env["SRW_CONTEXT"] = "prod"
	if got, _ := f.resolve(Options{}); got.Context != "prod" || got.Token != "ak_prod" {
		t.Errorf("SRW_CONTEXT: %+v", got)
	}
	if got, _ := f.resolve(Options{Context: "dev"}); got.Context != "dev" {
		t.Errorf("--context must win over SRW_CONTEXT: %+v", got)
	}
}

func TestNothingConfiguredSaysToLogIn(t *testing.T) {
	f := newFixture(t)
	_, err := f.resolve(Options{})
	var coded *exit.Error
	if !errors.As(err, &coded) || coded.Code != exit.Usage || !strings.Contains(coded.Hint, "srw login") {
		t.Errorf("%v", err)
	}
	f.saved("dev", map[string]*config.Context{"dev": {Server: "https://dev.example.com"}}, nil)
	if _, err := f.resolve(Options{}); code(t, err) != exit.Auth {
		t.Errorf("a context without a token is an authentication failure: %v", err)
	}
	if _, err := f.resolve(Options{Context: "nope"}); code(t, err) != exit.Usage {
		t.Errorf("an unknown context: %v", err)
	}
}

func TestPlainHTTPIsForThisMachineOnly(t *testing.T) {
	for _, server := range []string{"http://localhost:8085", "http://127.0.0.1:8085", "http://api.localhost", "http://[::1]:8085", "https://api.example.com"} {
		if _, err := ParseServer(server, false); err != nil {
			t.Errorf("%s was refused: %v", server, err)
		}
	}
	for _, server := range []string{"http://api.example.com", "http://10.0.0.5:8085", "ftp://x", "api.example.com", "https://user:pw@x.example.com", "https://x.example.com?a=b", ""} {
		if _, err := ParseServer(server, false); err == nil {
			t.Errorf("%q was accepted", server)
		}
	}
	if _, err := ParseServer("http://api.example.com", true); err != nil {
		t.Errorf("--insecure-http must allow it: %v", err)
	}
	f := newFixture(t)
	f.saved("lan", map[string]*config.Context{"lan": {Server: "http://10.0.0.5:8085", InsecureHTTP: true}}, map[string]string{"lan": "ak_lan"})
	if _, err := f.resolve(Options{}); err != nil {
		t.Errorf("a context's insecure-http: %v", err)
	}
}

func TestTokenCommand(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("uses sh")
	}
	cases := []struct {
		name    string
		command []string
		token   string
		fails   string
	}{
		{"first line, trimmed", []string{"sh", "-c", "echo '  ak_from_command  '; echo second"}, "ak_from_command", ""},
		{"non-zero exit", []string{"sh", "-c", "echo ak_leaked; exit 3"}, "", "failed"},
		{"no output", []string{"sh", "-c", "true"}, "", "printed no token"},
		{"not found", []string{"srw-no-such-command"}, "", "was not found"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			f := newFixture(t)
			// A stored token for the same context must lose to the command.
			f.saved("dev", map[string]*config.Context{"dev": {Server: "https://dev.example.com", TokenCommand: test.command}}, map[string]string{"dev": "ak_stored"})
			got, err := f.resolve(Options{})
			if test.fails == "" {
				if err != nil || got.Token != test.token {
					t.Fatalf("%+v %v", got, err)
				}
				return
			}
			var coded *exit.Error
			if !errors.As(err, &coded) || coded.Code != exit.Auth || !strings.Contains(coded.Message, test.fails) {
				t.Fatalf("%v", err)
			}
			// The command is named, its output never.
			if !strings.Contains(coded.Message, test.command[0]) || strings.Contains(coded.Message, "ak_leaked") {
				t.Errorf("message: %q", coded.Message)
			}
		})
	}
}

func TestTokenCommandStderrPassesThrough(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("uses sh")
	}
	f := newFixture(t)
	f.saved("dev", map[string]*config.Context{"dev": {Server: "https://dev.example.com", TokenCommand: []string{"sh", "-c", "echo unlock first >&2; echo ak_x"}}}, nil)
	if _, err := f.resolve(Options{}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(f.stderr.String(), "unlock first") {
		t.Errorf("stderr: %q", f.stderr.String())
	}
}
