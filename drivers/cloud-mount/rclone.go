package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// preflightArgs asks the remote about its root folder. Building a WebDAV
// remote never contacts the server, so rclone mounts with a wrong password
// and every later read fails; this call is what turns a bad credential, a
// deleted folder or an unreachable server into a status before mounting.
func preflightArgs(configPath string, m Mount) []string {
	return []string{
		"lsjson", "--stat",
		"--config", configPath,
		"--contimeout", "10s", "--timeout", "20s",
		"--low-level-retries", "1", "--retries", "1",
		"--log-level", "NOTICE",
		m.Remote,
	}
}

// cloudignoreArgs reads the folder's own .cloudignore.
func cloudignoreArgs(configPath string, m Mount) []string {
	return []string{
		"cat",
		"--config", configPath,
		"--contimeout", "10s", "--timeout", "15s",
		"--low-level-retries", "1",
		"--log-level", "ERROR",
		remoteChild(m.Remote, ".cloudignore"),
	}
}

// remoteChild names a file inside a remote's folder.
func remoteChild(remote, name string) string {
	if strings.HasSuffix(remote, ":") || strings.HasSuffix(remote, "/") {
		return remote + name
	}
	return remote + "/" + name
}

// mountArgs is the long-running rclone. Its remote control listens on a
// unix socket in the sidecar's private /tmp only: a TCP port would be
// reachable from the workspace, which shares the Pod's network.
func mountArgs(plan *Plan, m Mount, configPath, cacheDir, rcSocket, filterPath string) []string {
	args := []string{
		"mount2", m.Remote, m.Target,
		"--config", configPath,
		"--cache-dir", cacheDir,
		"--allow-other",
		// After a restart the dead mount is still listed; the opener
		// detaches it before it mounts the new one.
		"--allow-non-empty",
		"--uid", strconv.Itoa(plan.UID),
		"--gid", strconv.Itoa(plan.GID),
		"--umask", "022",
		"--rc", "--rc-addr", "unix://" + rcSocket, "--rc-no-auth",
		"--log-level", "INFO",
	}
	if m.ReadOnly {
		args = append(args, "--read-only")
	}
	args = append(args, m.Flags...)
	if filterPath != "" {
		args = append(args, "--exclude-from", filterPath)
	}
	return args
}

var (
	// Nextcloud answers a disabled account with 503 "Account disabled"
	// (OC\User\DisabledUserException): the credential is refused, not the
	// server down.
	credentialRE  = regexp.MustCompile(`\b(401|403)\b|Unauthorized|Forbidden|Account disabled|DisabledUserException`)
	notFoundRE    = regexp.MustCompile(`directory not found|object not found|\b404\b|Not Found`)
	timeoutTextRE = regexp.MustCompile(`deadline exceeded|i/o timeout|Client\.Timeout|TLS handshake timeout`)
	// rclone could not build the remote from its config at all (no server
	// was asked): no retry cures that config.
	remoteConfigRE = regexp.MustCompile(`Failed to create file system`)
)

// classify maps a failed rclone call to a reason. rclone's exit code 3 is
// "directory not found" and 4 "file not found".
func classify(code int, stderr string, timedOut bool) string {
	switch {
	case timedOut:
		return reasonTimeout
	case credentialRE.MatchString(stderr):
		return reasonCredentialRejected
	case code == 3 || code == 4 || notFoundRE.MatchString(stderr):
		return reasonNotFound
	case timeoutTextRE.MatchString(stderr):
		return reasonTimeout
	case remoteConfigRE.MatchString(stderr):
		return reasonMountFailed
	default:
		return reasonUnreachable
	}
}

// compileCloudignore turns .cloudignore lines into rclone exclude rules,
// exactly as the workspace's mount manager does
// (shared/runtime/services/cloud_mount: compile_cloudignore): comments,
// negations and ".." segments are dropped, a leading "/" anchors nothing,
// a directory rule covers its whole tree, and a bare name matches at any
// depth.
func compileCloudignore(lines []string) []string {
	var out []string
	for _, raw := range lines {
		line := strings.Trim(strings.TrimSuffix(raw, "\r"), " \t")
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, "!") {
			continue
		}
		if dotdotRE.MatchString(line) {
			continue
		}
		line = strings.TrimLeft(line, "/")
		if line == "" {
			continue
		}
		if strings.HasSuffix(line, "/") {
			line = strings.TrimRight(line, "/")
			if line == "" {
				continue
			}
			out = append(out, line+"/**")
			if !strings.Contains(line, "/") {
				out = append(out, "**/"+line+"/**")
			}
			continue
		}
		out = append(out, line)
		if !strings.Contains(line, "/") && !strings.ContainsAny(line, "*?[") {
			out = append(out, "**/"+line)
		}
	}
	return out
}

var dotdotRE = regexp.MustCompile(`(^|/)\.\.($|/)`)

// filterRules is the default lines and the folder's own, compiled, sorted
// and de-duplicated (sort -u in the workspace manager).
func filterRules(defaults, folder []string) []string {
	rules := append(compileCloudignore(defaults), compileCloudignore(folder)...)
	seen := map[string]bool{}
	var out []string
	for _, rule := range rules {
		if !seen[rule] {
			seen[rule] = true
			out = append(out, rule)
		}
	}
	sort.Strings(out)
	return out
}

// rcClient calls one rclone's remote control on its unix socket.
type rcClient interface {
	call(ctx context.Context, socket, method string, params map[string]any) (map[string]any, error)
}

type unixRC struct{}

func (unixRC) call(ctx context.Context, socket, method string, params map[string]any) (map[string]any, error) {
	if params == nil {
		params = map[string]any{}
	}
	body, err := json.Marshal(params)
	if err != nil {
		return nil, err
	}
	client := &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var dialer net.Dialer
			return dialer.DialContext(ctx, "unix", socket)
		},
	}}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://rc/"+method, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s: HTTP %d", method, response.StatusCode)
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// pendingUploads reads vfs/stats: files uploading plus files queued.
func pendingUploads(ctx context.Context, rc rcClient, socket string) (int, error) {
	stats, err := rc.call(ctx, socket, "vfs/stats", nil)
	if err != nil {
		return -1, err
	}
	cache, ok := stats["diskCache"].(map[string]any)
	if !ok {
		// No disk cache (--vfs-cache-mode off): nothing waits to upload.
		return 0, nil
	}
	return int(number(cache["uploadsInProgress"]) + number(cache["uploadsQueued"])), nil
}

// expediteQueue makes every queued upload eligible now, instead of after
// rclone's write-back delay.
func expediteQueue(ctx context.Context, rc rcClient, socket string) error {
	queue, err := rc.call(ctx, socket, "vfs/queue", nil)
	if err != nil {
		return err
	}
	items, _ := queue["queue"].([]any)
	var failed error
	for _, raw := range items {
		item, ok := raw.(map[string]any)
		if !ok || item["uploading"] == true || number(item["expiry"]) <= 0 {
			continue
		}
		if _, err := rc.call(ctx, socket, "vfs/queue-set-expiry", map[string]any{
			"id": number(item["id"]), "expiry": -1e9,
		}); err != nil {
			failed = errors.Join(failed, err)
		}
	}
	return failed
}

func number(value any) float64 {
	switch v := value.(type) {
	case float64:
		return v
	case int:
		return float64(v)
	case json.Number:
		f, _ := v.Float64()
		return f
	}
	return 0
}

// drainMount waits until a mount has nothing left to upload, or until the
// deadline; it returns what is still pending (-1 when rclone does not
// answer).
func drainMount(ctx context.Context, rc rcClient, socket string, deadline time.Time, every time.Duration) int {
	for {
		callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		_ = expediteQueue(callCtx, rc, socket)
		pending, err := pendingUploads(callCtx, rc, socket)
		cancel()
		if err == nil && pending == 0 {
			return 0
		}
		if !time.Now().Before(deadline) || ctx.Err() != nil {
			if err != nil {
				return -1
			}
			return pending
		}
		select {
		case <-ctx.Done():
		case <-time.After(every):
		}
	}
}
