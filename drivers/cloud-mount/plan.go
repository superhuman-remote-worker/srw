package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// Plan is the non-secret half of a workspace Pod's cloud mounts, written by
// the orchestrator into the Pod's plan ConfigMap. The credentials are the
// other half: one rclone config file from the Pod's Secret, a section per
// mount, named by Remote.
type Plan struct {
	Version int `json:"version"`
	// UID and GID own what every mount shows: the workspace's agent-host.
	UID int `json:"uid"`
	GID int `json:"gid"`
	// DrainSeconds bounds waiting for uploads at a drain request and at stop.
	DrainSeconds int     `json:"drain_seconds"`
	Mounts       []Mount `json:"mounts"`
}

// Mount is one rclone remote mounted at Target.
type Mount struct {
	Index int    `json:"index"`
	Name  string `json:"name"`
	// Remote is the rclone remote and path, e.g. "m0:" or "m0:Projects/x".
	Remote   string `json:"remote"`
	Target   string `json:"target"`
	ReadOnly bool   `json:"read_only"`
	// Flags are the remote's cache and provider flags, as rclone arguments.
	Flags []string `json:"flags"`
	// Ignore holds default .cloudignore lines; Cloudignore also reads the
	// folder's own .cloudignore at each start.
	Ignore      []string `json:"ignore"`
	Cloudignore bool     `json:"cloudignore"`
}

const (
	planVersion = 1
	maxMounts   = 16
)

var (
	remoteRE = regexp.MustCompile(`^m[0-9]{1,2}:[^\x00-\x1f\x7f]*$`)
	// The flags a plan may carry. Everything that decides where rclone
	// listens, which config and cache it uses, who owns the mount and
	// whether it is read-only is the supervisor's, never the plan's.
	flagRE = regexp.MustCompile(`^--(vfs-[a-z-]+|dir-cache-time|poll-interval|buffer-size|attr-timeout|transfers|checkers|timeout|contimeout|low-level-retries|webdav-[a-z-]+|no-modtime|no-checksum)(=[^\x00-\x1f\x7f]*)?$`)
)

// loadPlan reads and checks a plan file.
func loadPlan(path string) (*Plan, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var plan Plan
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&plan); err != nil {
		return nil, fmt.Errorf("malformed plan: %w", err)
	}
	return &plan, plan.check()
}

func (p *Plan) check() error {
	if p.Version != planVersion {
		return fmt.Errorf("plan version %d is not %d", p.Version, planVersion)
	}
	if p.UID <= 0 || p.GID <= 0 {
		return errors.New("plan needs a non-root uid and gid")
	}
	if p.DrainSeconds < 0 || p.DrainSeconds > 3600 {
		return fmt.Errorf("drain_seconds %d is out of range", p.DrainSeconds)
	}
	if len(p.Mounts) == 0 || len(p.Mounts) > maxMounts {
		return fmt.Errorf("a plan has 1 to %d mounts, not %d", maxMounts, len(p.Mounts))
	}
	seen := map[string]bool{}
	for i, m := range p.Mounts {
		if m.Index != i {
			return fmt.Errorf("mount %d has index %d", i, m.Index)
		}
		if !validName(m.Name) {
			return fmt.Errorf("mount %d has a malformed name", i)
		}
		if len(m.Remote) > 1024 || !remoteRE.MatchString(m.Remote) {
			return fmt.Errorf("mount %s has a malformed remote", m.Name)
		}
		if !filepath.IsAbs(m.Target) || filepath.Clean(m.Target) != m.Target || m.Target == "/" {
			return fmt.Errorf("mount %s has a malformed target", m.Name)
		}
		for _, key := range []string{"name:" + m.Name, "target:" + m.Target} {
			if seen[key] {
				return fmt.Errorf("mount %s repeats a name or target", m.Name)
			}
			seen[key] = true
		}
		if err := checkFlags(m.Flags); err != nil {
			return fmt.Errorf("mount %s: %w", m.Name, err)
		}
	}
	return nil
}

// validName accepts the workspace's folder name: one path segment as the
// orchestrator slugs it (Unicode letters included), never hidden.
func validName(name string) bool {
	if name == "" || len(name) > 255 || strings.HasPrefix(name, ".") || strings.Contains(name, "/") {
		return false
	}
	for _, r := range name {
		if r < 0x20 || r == 0x7f {
			return false
		}
	}
	return true
}

// checkFlags allows a flag from flagRE, each optionally followed by one
// value that is not itself a flag.
func checkFlags(flags []string) error {
	valueAllowed := false
	for _, arg := range flags {
		if strings.HasPrefix(arg, "-") {
			if !flagRE.MatchString(arg) {
				return fmt.Errorf("flag %q is not allowed", arg)
			}
			valueAllowed = !strings.Contains(arg, "=")
			continue
		}
		if !valueAllowed || strings.ContainsAny(arg, "\x00\n\r") {
			return fmt.Errorf("value %q follows no flag", arg)
		}
		valueAllowed = false
	}
	return nil
}
