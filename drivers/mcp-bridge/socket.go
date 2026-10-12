package main

import (
	"context"
	"errors"
	"io/fs"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

// The bridge serves the front on a unix socket, never on a TCP port: every
// process of the pod shares its loopback address, but only the front's
// group may enter the socket's directory (root:GROUP 0750, an emptyDir the
// front mounts too), so no binding's process reaches the bridge, its
// binding header or its control routes.

// listenSocket listens on path, in a directory only the bridge and group
// (the front's) may enter; without a group, the bridge's user alone.
func listenSocket(path string, group int) (net.Listener, error) {
	dir := filepath.Dir(path)
	dirMode, socketMode := fs.FileMode(0o700), fs.FileMode(0o600)
	if group >= 0 {
		if err := os.Chown(dir, -1, group); err != nil {
			return nil, err
		}
		dirMode, socketMode = 0o750, 0o660
	}
	if err := os.Chmod(dir, dirMode); err != nil {
		return nil, err
	}
	// A socket left by the container's last run (an emptyDir outlives it).
	if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}
	listener, err := net.Listen("unix", path)
	if err != nil {
		return nil, err
	}
	if group >= 0 {
		if err := os.Chown(path, -1, group); err != nil {
			listener.Close()
			return nil, err
		}
	}
	if err := os.Chmod(path, socketMode); err != nil {
		listener.Close()
		return nil, err
	}
	return listener, nil
}

// socketClient is an HTTP client that reaches the bridge's socket (the
// URL's host is ignored).
func socketClient(path string, timeout time.Duration) *http.Client {
	return &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, "unix", path)
			},
		},
	}
}
