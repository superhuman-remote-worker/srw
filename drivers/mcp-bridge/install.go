package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// installName is the bridge's file name inside the shared emptyDir.
const installName = "srw-mcp-bridge"

// install copies the running executable into dir, readable and executable by
// every user (the server image may run as anyone, root included), written to
// a temporary name first so a half-written bridge is never executed.
func install(dir string) error {
	self, err := os.Executable()
	if err != nil {
		return err
	}
	return copyExecutable(self, dir)
}

func copyExecutable(source, dir string) error {
	info, err := os.Stat(dir)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("%s is not a directory", dir)
	}
	in, err := os.Open(source)
	if err != nil {
		return err
	}
	defer in.Close()
	target := filepath.Join(dir, installName)
	tmp, err := os.CreateTemp(dir, "."+installName+"-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := io.Copy(tmp, in); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Chmod(0o555); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), target)
}
