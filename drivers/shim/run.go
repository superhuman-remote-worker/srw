package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"time"
)

// A bind-time driver (D6) reads its request file and writes one JSON object
// per line to stdout (shared/connectors/envelope.py). The shim runs it, keeps
// stderr in the pod log, checks each stdout line's type, and posts the lines
// with the driver's exit code to SRW_RESULT_URL, authenticated by the pod's
// sdi_ identity. SRW reads the outcome (read_output); the shim only refuses
// what it cannot carry.

// maxOutputBytes caps a driver's whole stdout (envelope.MAX_OUTPUT_BYTES).
const maxOutputBytes = 1024 * 1024

var lineTypes = map[string]bool{"result": true, "log": true, "error": true, "update": true}

// outcome is the body posted to SRW.
type outcome struct {
	ProtocolVersion string            `json:"protocol_version"`
	Operation       string            `json:"operation"`
	ExitCode        int               `json:"exit_code"`
	Lines           []json.RawMessage `json:"lines"`
	ProtocolError   string            `json:"protocol_error,omitempty"`
}

// readLines collects stdout as typed lines. A line that is not a JSON object
// of a known type, or output past the cap, becomes a protocol error; the
// lines before it are kept for the operator log.
func readLines(stdout io.Reader) ([]json.RawMessage, string) {
	var lines []json.RawMessage
	reader := bufio.NewReaderSize(io.LimitReader(stdout, maxOutputBytes+1), 64*1024)
	total := 0
	number := 0
	for {
		raw, err := reader.ReadBytes('\n')
		total += len(raw)
		if total > maxOutputBytes {
			io.Copy(io.Discard, stdout)
			return lines, fmt.Sprintf("output exceeds %d bytes", maxOutputBytes)
		}
		if text := bytes.TrimSpace(raw); len(text) > 0 {
			number++
			var line struct {
				Type string `json:"type"`
			}
			if json.Unmarshal(text, &line) != nil || !lineTypes[line.Type] {
				io.Copy(io.Discard, stdout)
				return lines, fmt.Sprintf("line %d is not a typed JSON object", number)
			}
			lines = append(lines, json.RawMessage(append([]byte(nil), text...)))
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return lines, ""
			}
			return lines, fmt.Sprintf("reading output: %v", err)
		}
	}
}

func post(url, identity string, body []byte, client *http.Client) error {
	var last error
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(attempt) * time.Second)
		}
		request, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
		if err != nil {
			return err
		}
		request.Header.Set("Authorization", "Bearer "+identity)
		request.Header.Set("Content-Type", "application/json")
		response, err := client.Do(request)
		if err != nil {
			last = err
			continue
		}
		io.Copy(io.Discard, io.LimitReader(response.Body, 64*1024))
		response.Body.Close()
		if response.StatusCode < 300 {
			return nil
		}
		last = fmt.Errorf("SRW answered %d", response.StatusCode)
		if response.StatusCode < 500 {
			return last
		}
	}
	return last
}

func run(program []string, getenv func(string) string, logf func(string, ...any)) int {
	return runWith(program, getenv, logf, &http.Client{Timeout: 10 * time.Second})
}

func runWith(program []string, getenv func(string) string, logf func(string, ...any), client *http.Client) int {
	request, _, err := readRequest(getenv)
	if err != nil {
		logf("run: %v", err)
		return exitUsage
	}
	identity, _, err := readIdentity(getenv)
	if err != nil {
		logf("run: %v", err)
		return exitUsage
	}
	url := getenv("SRW_RESULT_URL")
	if !strings.HasPrefix(url, "http://") && !strings.HasPrefix(url, "https://") {
		logf("run: SRW_RESULT_URL is not set")
		return exitUsage
	}
	command := exec.Command(program[0], program[1:]...)
	command.Stdin = nil
	command.Stderr = os.Stderr
	stdout, err := command.StdoutPipe()
	if err != nil {
		logf("run: %v", err)
		return exitSoftware
	}
	if err := command.Start(); err != nil {
		logf("run: the driver program: %v", err)
		return exitSoftware
	}
	lines, protocolError := readLines(stdout)
	exitCode := 0
	if err := command.Wait(); err != nil {
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) {
			logf("run: %v", err)
			return exitSoftware
		}
		exitCode = exitErr.ExitCode()
	}
	operation, _ := request["operation"].(string)
	version, _ := request["protocol_version"].(string)
	if lines == nil {
		lines = []json.RawMessage{}
	}
	body, err := json.Marshal(outcome{
		ProtocolVersion: version,
		Operation:       operation,
		ExitCode:        exitCode,
		Lines:           lines,
		ProtocolError:   protocolError,
	})
	if err != nil {
		logf("run: %v", err)
		return exitSoftware
	}
	if err := post(url, identity, body, client); err != nil {
		logf("run: posting the outcome: %v", err)
		return exitSoftware
	}
	if protocolError != "" {
		logf("run: %s", protocolError)
		return exitSoftware
	}
	return exitCode
}
