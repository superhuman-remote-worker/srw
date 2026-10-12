//go:build linux

package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestTheRealCheckRefusesASymlinkOrAFileAtTheTarget(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "root")
	if err := os.Mkdir(target, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := (systemMounter{}).Check(target); err != nil {
		t.Fatalf("a directory: %v", err)
	}
	link := filepath.Join(dir, "link")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(dir, "file")
	if err := os.WriteFile(file, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	for _, refused := range []string{link, file, filepath.Join(dir, "missing")} {
		if err := (systemMounter{}).Check(refused); err == nil {
			t.Fatalf("%s passed the check", refused)
		}
	}
}

func TestTheRealStaleSeesNoDeadMountOnAPlainDirectory(t *testing.T) {
	if (systemMounter{}).Stale(t.TempDir()) {
		t.Fatal("a plain directory is not a dead mount")
	}
}
