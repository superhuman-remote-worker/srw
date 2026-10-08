package main

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

// Bindings in one pod share its network, its /tmp and its image, so the
// bridge (the server container's only root process) starts each process as
// a user of its own (--uid-base): a process cannot read another binding's
// environment (/proc/PID/environ needs the same user), signal it, trace it
// or open its files, and it cannot reach the bridge, whose socket only the
// front's group may open. Each process gets a private directory (0700, its
// own) as HOME and TMPDIR, its files are private by default (umask 077),
// and it can gain no privilege (no capability, no_new_privs). A user goes
// back to the pool only once no process of it is left (every one is
// killed, whatever group or session it left for) and none of its files
// (its directory, and what it left in /tmp and /dev/shm), so the next
// process of that user finds nothing of the last one.

// homeToken in a value of the program's environment or arguments is the
// process's private directory (shared/connectors/mcp.py BINDING_HOME).
const homeToken = "${binding.home}"

// errNoUser: every user of the pool is taken (by processes still stopping).
var errNoUser = errors.New("every user of this pod is in use")

// poolSize is how many users a bridge needs: one per binding process and
// the probe's, twice over, since a process may still be stopping while the
// next one of its binding runs.
func poolSize(maxProcesses int) int { return 2*maxProcesses + 2 }

// userPool hands out the users base .. base+size-1, one per process.
type userPool struct {
	mu   sync.Mutex
	free []int
}

func newUserPool(base, size int) *userPool {
	pool := &userPool{}
	for uid := base; uid < base+size; uid++ {
		pool.free = append(pool.free, uid)
	}
	return pool
}

func (u *userPool) take() (int, bool) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if len(u.free) == 0 {
		return 0, false
	}
	uid := u.free[0]
	u.free = u.free[1:]
	return uid, true
}

func (u *userPool) give(uid int) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.free = append(u.free, uid)
}

// homeEnv is a process's environment with its private directory as HOME
// and TMPDIR, and as the value of homeToken wherever a value names it.
func homeEnv(env []string, home string) []string {
	out := make([]string, 0, len(env)+2)
	for _, entry := range env {
		key, _, _ := strings.Cut(entry, "=")
		if key == "HOME" || key == "TMPDIR" {
			continue
		}
		out = append(out, strings.ReplaceAll(entry, homeToken, home))
	}
	return append(out, "HOME="+home, "TMPDIR="+home)
}

// homeArgs is the program with homeToken replaced by the private directory.
func homeArgs(program []string, home string) []string {
	out := make([]string, len(program))
	for i, arg := range program {
		out[i] = strings.ReplaceAll(arg, homeToken, home)
	}
	return out
}

// homeName is a process's directory under the home root: its user's id
// when it runs as one, else a number of its own.
func homeName(uid int, sequence uint64) string {
	if uid > 0 {
		return strconv.Itoa(uid)
	}
	return "p" + strconv.FormatUint(sequence, 10)
}

// makeHome creates a process's private directory, owned by its user.
func makeHome(root, name string, uid int) (string, error) {
	home := filepath.Join(root, name)
	if err := os.RemoveAll(home); err != nil {
		return "", err
	}
	if err := os.Mkdir(home, 0o700); err != nil {
		return "", err
	}
	if uid > 0 {
		if err := os.Chown(home, uid, uid); err != nil {
			os.Remove(home)
			return "", err
		}
	}
	return home, nil
}

// clearDir removes everything in a directory, not the directory.
func clearDir(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	var first error
	for _, entry := range entries {
		if err := os.RemoveAll(filepath.Join(dir, entry.Name())); err != nil && first == nil {
			first = err
		}
	}
	return first
}
