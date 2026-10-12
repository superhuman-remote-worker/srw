package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"syscall"
)

const (
	defaultRequestFile  = "/run/srw/request.json"
	defaultIdentityFile = "/run/srw/identity"
	// Requests are capped at 512 KiB by the orchestrator.
	maxRequestBytes = 512 * 1024
)

// An sdi_ driver identity: the prefix, 43 base62 characters and a 6-character
// checksum (shared/connectors/leases.py).
var identityShape = regexp.MustCompile(`\Asdi_[0-9A-Za-z]{49}\z`)

type execFunc func(path string, argv []string, env []string) error

func systemExec(path string, argv []string, env []string) error {
	return syscall.Exec(path, argv, env)
}

// readRequest checks the pod's request file and returns it.
func readRequest(getenv func(string) string) (map[string]any, string, error) {
	path := getenv("SRW_REQUEST_FILE")
	if path == "" {
		path = defaultRequestFile
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, path, fmt.Errorf("the request file: %w", err)
	}
	if len(raw) > maxRequestBytes {
		return nil, path, fmt.Errorf("the request file exceeds %d bytes", maxRequestBytes)
	}
	var request map[string]any
	if err := json.Unmarshal(raw, &request); err != nil || request == nil {
		return nil, path, fmt.Errorf("the request file is not a JSON object")
	}
	version, _ := request["protocol_version"].(string)
	if !strings.HasPrefix(version, "1.") {
		return nil, path, fmt.Errorf("protocol_version %q is not one this shim speaks", version)
	}
	return request, path, nil
}

// readIdentity returns the pod's sdi_ identity, checked for shape.
func readIdentity(getenv func(string) string) (string, string, error) {
	path := getenv("SRW_DRIVER_IDENTITY_FILE")
	if path == "" {
		path = defaultIdentityFile
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", path, fmt.Errorf("the identity file: %w", err)
	}
	token := strings.TrimSpace(string(raw))
	if !identityShape.MatchString(token) {
		return "", path, fmt.Errorf("the identity file holds no sdi_ driver identity")
	}
	return token, path, nil
}

// serve replaces the shim with a service driver once its request and identity
// are in place. The driver reads both from the files the environment names;
// the shim adds nothing to argv and never reads stdin.
func serve(program []string, getenv func(string) string, execve execFunc) error {
	request, requestPath, err := readRequest(getenv)
	if err != nil {
		return err
	}
	if plane, _ := request["plane"].(string); plane != "service" {
		return fmt.Errorf("the request is for the %q plane, not a service", plane)
	}
	_, identityPath, err := readIdentity(getenv)
	if err != nil {
		return err
	}
	path, err := exec.LookPath(program[0])
	if err != nil {
		return fmt.Errorf("the driver program %q: %w", program[0], err)
	}
	env := withEnv(os.Environ(), map[string]string{
		"SRW_REQUEST_FILE":         requestPath,
		"SRW_DRIVER_IDENTITY_FILE": identityPath,
	})
	return execve(path, program, env)
}

func withEnv(environ []string, set map[string]string) []string {
	out := make([]string, 0, len(environ)+len(set))
	for _, entry := range environ {
		name, _, _ := strings.Cut(entry, "=")
		if _, replaced := set[name]; !replaced {
			out = append(out, entry)
		}
	}
	for name, value := range set {
		out = append(out, name+"="+value)
	}
	return out
}
