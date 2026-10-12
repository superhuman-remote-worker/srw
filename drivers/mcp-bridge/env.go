package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"sync"
)

// The largest credential a binding may hand over (decoded).
const maxCredential = 64 << 10

var envName = regexp.MustCompile(`\A[A-Za-z_][A-Za-z0-9_]{0,127}\z`)

// codeEnv are the variables a runtime reads code, its search path, a
// command to run or a package index from, and codeEnvPrefixes whole
// families of them: a binding's credential is user data, so it may never
// land in one (shared/connectors/env_names.py CODE_ENV and CODE_ENV_PREFIXES hold
// the same lists). The process's HOME and TMPDIR are the bridge's.
var (
	codeEnv = map[string]bool{
		"BASH_ENV": true, "BASHOPTS": true, "BROWSER": true, "BUN_OPTIONS": true,
		"CLASSPATH": true, "DOTNET_STARTUP_HOOKS": true, "EDITOR": true,
		"ELECTRON_RUN_AS_NODE": true, "ENV": true, "GCONV_PATH": true,
		"GEM_HOME": true, "GEM_PATH": true, "GLIBC_TUNABLES": true, "HOME": true,
		"IFS": true, "JAVA_OPTS": true, "JAVA_TOOL_OPTIONS": true,
		"JDK_JAVA_OPTIONS": true, "_JAVA_OPTIONS": true, "LESSOPEN": true,
		"NODE_OPTIONS": true, "NODE_PATH": true, "NODE_REPL_EXTERNAL_MODULE": true,
		"OPENSSL_CONF": true, "OPENSSL_MODULES": true, "PAGER": true, "PATH": true,
		"PERL5DB": true, "PERL5LIB": true, "PERL5OPT": true, "PERLLIB": true,
		"PROMPT_COMMAND": true, "PS4": true, "PYTHONBREAKPOINT": true,
		"PYTHONHOME": true, "PYTHONINSPECT": true, "PYTHONPATH": true,
		"PYTHONSTARTUP": true, "PYTHONUSERBASE": true, "PYTHONWARNINGS": true,
		"RUBYGEMS_GEMDEPS": true, "RUBYLIB": true, "RUBYOPT": true,
		"SHELLOPTS": true, "SSH_ASKPASS": true, "SUDO_ASKPASS": true,
		"TMPDIR": true, "VISUAL": true, "ZDOTDIR": true,
	}
	codeEnvPrefixes = []string{"GIT_", "NPM_CONFIG_", "PIP_", "UV_"}
)

// checkEnvName refuses a name the credential may not be delivered in: SRW's
// own, the loader's, and the ones that load code.
func checkEnvName(name string) error {
	upper := strings.ToUpper(name)
	switch {
	case !envName.MatchString(name):
		return fmt.Errorf("%q is not an environment name", name)
	case strings.HasPrefix(upper, "SRW_"), strings.HasPrefix(upper, "LD_"), strings.HasPrefix(upper, "DYLD_"):
		return fmt.Errorf("%q is reserved", name)
	case codeEnv[upper]:
		return fmt.Errorf("%q loads code or a search path", name)
	}
	for _, prefix := range codeEnvPrefixes {
		if strings.HasPrefix(upper, prefix) {
			return fmt.Errorf("%q configures a command or a package index", name)
		}
	}
	return nil
}

// decodeCredential reads Srw-Bridge-Credential: base64, so any credential
// fits a header; it may not hold a NUL, which no environment can.
func decodeCredential(header string) (string, error) {
	if header == "" {
		return "", nil
	}
	raw, err := base64.StdEncoding.DecodeString(header)
	if err != nil || len(raw) == 0 {
		return "", errors.New("the credential is not base64")
	}
	if len(raw) > maxCredential || bytes.IndexByte(raw, 0) >= 0 {
		return "", errors.New("the credential is too long or holds a NUL")
	}
	return string(raw), nil
}

// childEnv is a binding's process environment: the container's (the
// image's and the spec's, which carry no secret) without SRW's own
// variables or another value of the credential's name, plus the binding's
// credential when it has one.
func childEnv(base []string, name, credential string) []string {
	out := make([]string, 0, len(base)+1)
	for _, entry := range base {
		key, _, ok := strings.Cut(entry, "=")
		if !ok || key == "" || strings.HasPrefix(strings.ToUpper(key), "SRW_") {
			continue
		}
		if name != "" && key == name {
			continue
		}
		out = append(out, entry)
	}
	if name != "" && credential != "" {
		out = append(out, name+"="+credential)
	}
	return out
}

// scrubber replaces a credential, as written and in the forms a program
// may print it (JSON-escaped, URL-encoded, base64), with [redacted].
type scrubber struct {
	secrets [][]byte
}

const redacted = "[redacted]"

func newScrubber(credential string) *scrubber {
	s := &scrubber{}
	if credential == "" {
		return s
	}
	add := func(form string) {
		if form == "" {
			return
		}
		for _, known := range s.secrets {
			if string(known) == form {
				return
			}
		}
		s.secrets = append(s.secrets, []byte(form))
	}
	add(credential)
	if quoted, err := json.Marshal(credential); err == nil {
		add(string(quoted[1 : len(quoted)-1]))
	}
	if len(credential) >= 8 {
		add(url.QueryEscape(credential))
		add(base64.StdEncoding.EncodeToString([]byte(credential)))
		add(base64.RawStdEncoding.EncodeToString([]byte(credential)))
		add(base64.URLEncoding.EncodeToString([]byte(credential)))
		add(base64.RawURLEncoding.EncodeToString([]byte(credential)))
	}
	return s
}

func (s *scrubber) apply(data []byte) []byte {
	for _, secret := range s.secrets {
		if bytes.Contains(data, secret) {
			data = bytes.ReplaceAll(data, secret, []byte(redacted))
		}
	}
	return data
}

// The longest stderr line logged; a longer one is cut. A line is kept up to
// maxLogLine plus twice the longest credential before the rest of it is
// dropped, so a credential that starts inside the logged part is always
// whole when it is scrubbed.
const (
	maxLogLine = 16 << 10
	maxKept    = maxLogLine + 2*maxCredential
)

// lineLogger writes a process's stderr to the bridge's log, a line at a
// time, prefixed with its binding and scrubbed of its credential (a server
// may print its environment).
type lineLogger struct {
	logf    func(string, ...any)
	label   string
	scrub   *scrubber
	mu      sync.Mutex
	pending []byte
	// Set once the line outgrew maxKept: its rest is dropped.
	dropping bool
}

func (l *lineLogger) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	written := len(p)
	for len(p) > 0 {
		index := bytes.IndexByte(p, '\n')
		chunk := p
		if index >= 0 {
			chunk = p[:index]
		}
		if !l.dropping {
			room := maxKept - len(l.pending)
			if len(chunk) > room {
				chunk, l.dropping = chunk[:room], true
			}
			l.pending = append(l.pending, chunk...)
		}
		if index < 0 {
			break
		}
		l.emit()
		p = p[index+1:]
	}
	return written, nil
}

// flush logs what is left without a newline.
func (l *lineLogger) flush() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.pending) > 0 {
		l.emit()
	}
}

// emit logs the pending line, scrubbed first and only then cut.
func (l *lineLogger) emit() {
	line := l.scrub.apply(bytes.TrimRight(l.pending, "\r"))
	if len(line) > maxLogLine || l.dropping {
		line = append(line[:min(len(line), maxLogLine):min(len(line), maxLogLine)], "..."...)
	}
	l.logf("%s stderr: %s", l.label, line)
	l.pending, l.dropping = nil, false
}
