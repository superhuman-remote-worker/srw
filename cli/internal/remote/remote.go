// Package remote decides which server a command talks to and with which
// token.
//
// There are two modes and they are never mixed. Environment mode (CI,
// containers) takes the server from a flag or a variable and the token from
// the environment. Context mode (a person's machine) takes both from a named
// context in the config file. An exported token must not travel to whatever
// server the current context names, so SRW_TOKEN is not used in context mode.
package remote

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/superhuman-remote-worker/srw/cli/internal/config"
	"github.com/superhuman-remote-worker/srw/cli/internal/exit"
)

// The two modes.
const (
	Environment = "environment"
	Context     = "context"
)

// Options are the global flags that steer the choice.
type Options struct {
	Server       string
	Context      string
	TokenStdin   bool
	CAFile       string
	InsecureHTTP bool
}

// Host is what the process sees of its surroundings. Tests supply their own.
type Host struct {
	Getenv func(string) string
	Home   string
	Stdin  io.Reader
	Stderr io.Writer
	// StdinIsTerminal says whether a child process may be given stdin.
	StdinIsTerminal bool
}

// Target is one server and how to authenticate to it.
type Target struct {
	Mode string
	// Server is the base URL, without a trailing slash.
	Server *url.URL
	// Context is the context's name; empty in environment mode.
	Context string
	Token   string
	// TokenSource says where the token came from, for whoami.
	TokenSource  string
	CAFile       string
	Project      string
	InsecureHTTP bool
}

const tokenCommandTimeout = 10 * time.Second

// Resolve picks the server and the token. A target without a token is
// returned without an error when wantToken is false (version, login).
func Resolve(options Options, host Host, paths config.Paths, wantToken bool) (*Target, error) {
	server, serverSource := options.Server, "--server"
	if server == "" {
		for _, name := range []string{"SRW_API_URL", "COCKPIT_API_URL"} {
			if value := host.Getenv(name); value != "" {
				server, serverSource = value, name
				break
			}
		}
	}
	if server != "" {
		return environment(options, host, paths, server, serverSource, wantToken)
	}
	return fromContext(options, host, paths, wantToken)
}

func environment(options Options, host Host, paths config.Paths, server, source string, wantToken bool) (*Target, error) {
	if options.Context != "" {
		return nil, exit.Usagef("--context cannot be combined with a server from %s: a command uses either the environment or a saved context", source)
	}
	if name := host.Getenv("SRW_CONTEXT"); name != "" {
		return nil, exit.Usagef("SRW_CONTEXT and %s are both set: a command uses either the environment or a saved context; unset one", source)
	}
	base, err := ParseServer(server, options.InsecureHTTP)
	if err != nil {
		return nil, err
	}
	target := &Target{Mode: Environment, Server: base, CAFile: options.CAFile, InsecureHTTP: options.InsecureHTTP}
	switch {
	case options.TokenStdin:
		target.Token, err = readLine(host.Stdin)
		if err != nil {
			return nil, err
		}
		target.TokenSource = "--token-stdin"
	case host.Getenv("SRW_TOKEN") != "":
		target.Token, target.TokenSource = strings.TrimSpace(host.Getenv("SRW_TOKEN")), "SRW_TOKEN"
	case paths.EnvToken != "":
		data, err := os.ReadFile(paths.EnvToken)
		if err == nil {
			target.Token, target.TokenSource = firstLine(data), paths.EnvToken
		} else if !errors.Is(err, fs.ErrNotExist) {
			return nil, exit.New(exit.Auth, "cannot read %s: %v", paths.EnvToken, err)
		}
	}
	if wantToken && target.Token == "" {
		return nil, exit.New(exit.Auth, "no token: set SRW_TOKEN or pass --token-stdin").
			WithHint("create a token in the Cockpit under Settings → API Keys")
	}
	return target, nil
}

func fromContext(options Options, host Host, paths config.Paths, wantToken bool) (*Target, error) {
	file, err := config.Load(paths)
	if err != nil {
		return nil, exit.Usagef("%v", err)
	}
	name, source := options.Context, "--context"
	if name == "" {
		name, source = host.Getenv("SRW_CONTEXT"), "SRW_CONTEXT"
	}
	if name == "" {
		name, source = file.CurrentContext, "current-context"
	}
	if name == "" {
		return nil, exit.Usagef("no server configured").
			WithHint("run `srw login`, or set SRW_API_URL and SRW_TOKEN")
	}
	entry, ok := file.Contexts[name]
	if !ok {
		return nil, exit.Usagef("context %q (from %s) is not in %s", name, source, paths.Config).
			WithHint("`srw config get-contexts` lists the contexts")
	}
	insecure := options.InsecureHTTP || entry.InsecureHTTP
	base, err := ParseServer(entry.Server, insecure)
	if err != nil {
		return nil, err
	}
	target := &Target{
		Mode: Context, Server: base, Context: name, CAFile: entry.CAFile,
		Project: entry.Project, InsecureHTTP: insecure,
	}
	if options.CAFile != "" {
		target.CAFile = options.CAFile
	}
	if host.Getenv("SRW_TOKEN") != "" && host.Stderr != nil {
		fmt.Fprintf(host.Stderr, "note: SRW_TOKEN is set but not used; the context %q supplies the token\n", name)
	}
	switch {
	case options.TokenStdin:
		target.Token, err = readLine(host.Stdin)
		if err != nil {
			return nil, err
		}
		target.TokenSource = "--token-stdin"
	case len(entry.TokenCommand) > 0:
		if !wantToken {
			break
		}
		target.Token, err = runTokenCommand(entry.TokenCommand, host)
		if err != nil {
			return nil, err
		}
		target.TokenSource = "token-command of context " + name
	default:
		credentials, warning, err := config.LoadCredentials(paths)
		if err != nil {
			return nil, exit.New(exit.Auth, "%v", err)
		}
		if warning != "" && host.Stderr != nil {
			fmt.Fprintln(host.Stderr, "warning: "+warning)
		}
		if token := credentials.Tokens[name]; token != "" {
			target.Token, target.TokenSource = token, paths.Credentials
		}
	}
	if wantToken && target.Token == "" {
		return nil, exit.New(exit.Auth, "the context %q has no token", name).
			WithHint("run `srw login --context %s`", name)
	}
	return target, nil
}

// ParseServer checks a server URL. TLS is always verified, and http:// is
// accepted for loopback hosts only unless insecure is set.
func ParseServer(raw string, insecure bool) (*url.URL, error) {
	trimmed := strings.TrimRight(strings.TrimSpace(raw), "/")
	parsed, err := url.Parse(trimmed)
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, exit.Usagef("%q is not a server URL; expected something like https://api.example.com", raw)
	}
	switch parsed.Scheme {
	case "https":
	case "http":
		if !insecure && !IsLoopback(parsed.Hostname()) {
			return nil, exit.Usagef("refusing http:// for %s: the token would travel unencrypted", parsed.Host).
				WithHint("use https://, or pass --insecure-http if this is deliberate")
		}
	default:
		return nil, exit.Usagef("%q is not a server URL; expected https://…", raw)
	}
	return parsed, nil
}

// IsLoopback says whether host is this machine.
func IsLoopback(host string) bool {
	host = strings.ToLower(strings.TrimSuffix(host, "."))
	if host == "localhost" || strings.HasSuffix(host, ".localhost") {
		return true
	}
	address := net.ParseIP(host)
	return address != nil && address.IsLoopback()
}

func readLine(stdin io.Reader) (string, error) {
	if stdin == nil {
		return "", exit.Usagef("--token-stdin needs a token on standard input")
	}
	line, err := bufio.NewReader(stdin).ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return "", exit.Usagef("cannot read the token from standard input: %v", err)
	}
	token := strings.TrimSpace(line)
	if token == "" {
		return "", exit.Usagef("--token-stdin read an empty line")
	}
	return token, nil
}

func firstLine(data []byte) string {
	line, _, _ := bytes.Cut(data, []byte("\n"))
	return strings.TrimSpace(string(line))
}

// runTokenCommand runs a context's token-command. The contract: an argument
// list run without a shell; the token is the first line of stdout, trimmed;
// stderr passes through; stdin is connected only when the CLI's own stdin is
// a terminal; a non-zero exit or empty output is an authentication failure
// that names the command, never its output; ten seconds at most. The result
// is used for this process and never written.
func runTokenCommand(argv []string, host Host) (string, error) {
	name := argv[0]
	deadline, cancel := context.WithTimeout(context.Background(), tokenCommandTimeout)
	defer cancel()
	command := exec.CommandContext(deadline, name, argv[1:]...)
	var stdout bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = host.Stderr
	if host.StdinIsTerminal {
		command.Stdin = os.Stdin
	}
	if err := command.Run(); err != nil {
		reason := "failed"
		if errors.Is(deadline.Err(), context.DeadlineExceeded) {
			reason = fmt.Sprintf("did not finish within %s", tokenCommandTimeout)
		} else if errors.Is(err, exec.ErrNotFound) {
			reason = "was not found"
		}
		return "", exit.New(exit.Auth, "the token-command %q %s", name, reason)
	}
	token := firstLine(stdout.Bytes())
	if token == "" {
		return "", exit.New(exit.Auth, "the token-command %q printed no token", name)
	}
	return token, nil
}
