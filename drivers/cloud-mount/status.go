package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

// The states a mount reports. A status file never carries rclone's own
// words: a reason is one of the closed codes below, so nothing the remote
// says (or a credential rclone might echo) reaches the workspace.
const (
	statePending     = "pending"
	stateMounted     = "mounted"
	stateUnavailable = "unavailable"

	reasonCredentialRejected = "credential_rejected" // 401 or 403
	reasonNotFound           = "not_found"           // the folder is gone
	reasonUnreachable        = "unreachable"         // DNS, connect, TLS, 5xx
	reasonTimeout            = "timeout"             // no answer in time
	reasonMountFailed        = "mount_failed"        // rclone or the opener could not mount
	reasonConfigMissing      = "config_missing"      // the credential file never arrived
)

// Ack records the answer to one control request (drain or refresh).
type Ack struct {
	Nonce string `json:"nonce"`
	// State is draining, drained or incomplete for a drain, and done or
	// failed for a refresh.
	State string `json:"state"`
	// Pending counts uploads still queued when a drain gave up (-1:
	// unknown, rclone did not answer).
	Pending int `json:"pending"`
}

// Status is what /srw/cloud-status/<index>.json holds. The workspace reads
// it (the view is read-only) and so does the agent, over SSH.
type Status struct {
	Version  int    `json:"version"`
	Index    int    `json:"index"`
	Name     string `json:"name"`
	State    string `json:"state"`
	Reason   string `json:"reason,omitempty"`
	Since    string `json:"since"`
	Attempts int    `json:"attempts"`
	Drain    *Ack   `json:"drain,omitempty"`
	Refresh  *Ack   `json:"refresh,omitempty"`
}

// statusBoard owns the status files of every mount.
type statusBoard struct {
	dir string
	now func() time.Time
	mu  sync.Mutex
	all map[int]*Status
}

func newStatusBoard(dir string, plan *Plan, now func() time.Time) *statusBoard {
	board := &statusBoard{dir: dir, now: now, all: map[int]*Status{}}
	for _, m := range plan.Mounts {
		board.all[m.Index] = &Status{Version: 1, Index: m.Index, Name: m.Name, State: statePending}
	}
	return board
}

// set records a mount's state; Since moves only when the state or reason
// changes.
func (b *statusBoard) set(index int, state, reason string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	status := b.all[index]
	if status.State != state || status.Reason != reason || status.Since == "" {
		status.Since = b.now().UTC().Format(time.RFC3339)
	}
	status.State, status.Reason = state, reason
	if state == statePending {
		status.Attempts++
	}
	b.write(status)
}

func (b *statusBoard) ack(index int, refresh bool, ack Ack) {
	b.mu.Lock()
	defer b.mu.Unlock()
	status := b.all[index]
	if refresh {
		status.Refresh = &ack
	} else {
		status.Drain = &ack
	}
	b.write(status)
}

func (b *statusBoard) get(index int) Status {
	b.mu.Lock()
	defer b.mu.Unlock()
	return *b.all[index]
}

// write replaces the file atomically, so a reader never sees half of it.
// Called under mu.
func (b *statusBoard) write(status *Status) {
	if status.Since == "" {
		status.Since = b.now().UTC().Format(time.RFC3339)
	}
	payload, err := json.Marshal(status)
	if err != nil {
		return
	}
	final := filepath.Join(b.dir, strconv.Itoa(status.Index)+".json")
	tmp := final + ".tmp"
	if err := os.WriteFile(tmp, append(payload, '\n'), 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "srw-cloud-mount: status %d: %v\n", status.Index, err)
		return
	}
	if err := os.Rename(tmp, final); err != nil {
		fmt.Fprintf(os.Stderr, "srw-cloud-mount: status %d: %v\n", status.Index, err)
	}
}

// publishAll writes every mount's current status once.
func (b *statusBoard) publishAll() {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, status := range b.all {
		b.write(status)
	}
}
