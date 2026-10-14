// Command srw-gpt-live-sip holds SIP call sidebands for one phone connector.
// It uses only the standard library and detaches without hanging up on SIGTERM.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

var version = "dev"

func main() { os.Exit(dispatch(os.Args[1:])) }
func dispatch(args []string) int {
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr, "usage: srw-gpt-live-sip serve|version")
		return 2
	}
	switch args[0] {
	case "version", "--version":
		fmt.Println(version)
		return 0
	case "serve":
	default:
		fmt.Fprintln(os.Stderr, "usage: srw-gpt-live-sip serve|version")
		return 2
	}
	cfg, err := loadConfig(os.Getenv)
	if err != nil {
		logEvent("config_error", map[string]any{"message": err.Error()})
		return 2
	}
	d := newDriver(cfg, realClock{}, logEvent)
	d.start()
	server := &http.Server{Addr: ":" + cfg.port, Handler: d, ReadHeaderTimeout: 10 * time.Second, ReadTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 64 * 1024}
	signalCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.ListenAndServe() }()
	result := 0
	select {
	case <-signalCtx.Done():
	case err := <-serveErr:
		if err != http.ErrServerClosed {
			logEvent("serve_failed", map[string]any{})
			result = 1
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	d.shutdown(ctx)
	_ = server.Shutdown(ctx)
	return result
}

var logMu sync.Mutex

func logEvent(name string, fields map[string]any) {
	fields["event"] = name
	fields["time"] = time.Now().UTC().Format(time.RFC3339Nano)
	logMu.Lock()
	defer logMu.Unlock()
	_ = json.NewEncoder(os.Stderr).Encode(fields)
}
