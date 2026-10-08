package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"
)

const (
	// The largest answer, and the largest stream event, the front relays.
	// Each is buffered whole (to filter and scrub it), so it is bounded,
	// and all buffers together are bounded by the budget below.
	maxResponseBody = 4 << 20
	maxEventBytes   = 4 << 20
	bufferUnit      = 1 << 20
	// Buffered bytes in flight across every request, in units: with the
	// copies a filter makes, the front's heap stays under its limit.
	bufferBudgetUnits = 96
	budgetWait        = 10 * time.Second
	redacted          = "[redacted]"
	// The shortest encoded form of a credential the scrubber looks for.
	minScrubbed = 8
)

// budget bounds the bytes the front buffers at once. One acquirer takes
// units at a time, so two never hold part of what each needs.
type budget struct {
	mu     sync.Mutex
	tokens chan struct{}
}

func newBudget(units int) *budget {
	b := &budget{tokens: make(chan struct{}, units)}
	for i := 0; i < units; i++ {
		b.tokens <- struct{}{}
	}
	return b
}

func unitsFor(size int64) int {
	if size <= 0 || size > maxResponseBody {
		size = maxResponseBody
	}
	return int((size + bufferUnit - 1) / bufferUnit)
}

func (b *budget) acquire(ctx context.Context, units int) (func(), bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	ctx, cancel := context.WithTimeout(ctx, budgetWait)
	defer cancel()
	for got := 0; got < units; got++ {
		select {
		case <-b.tokens:
		case <-ctx.Done():
			for i := 0; i < got; i++ {
				b.tokens <- struct{}{}
			}
			return nil, false
		}
	}
	return func() {
		for i := 0; i < units; i++ {
			b.tokens <- struct{}{}
		}
	}, true
}

// filterTools hides the tools “allowed“ refuses from every JSON-RPC
// answer in data that carries a tool list (“result.tools“), whatever its
// id: an answer the front did not ask for (a replayed stream, an id the
// server re-encoded) is filtered too. A batch of answers is filtered per
// answer; anything else passes unchanged.
func filterTools(data []byte, allowed func(string) bool) []byte {
	if !bytes.Contains(data, []byte(`"tools"`)) {
		return data
	}
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) > 0 && trimmed[0] == '[' {
		var answers []json.RawMessage
		if json.Unmarshal(trimmed, &answers) != nil {
			return data
		}
		for i, answer := range answers {
			answers[i] = filterAnswer(answer, allowed)
		}
		return marshal(answers)
	}
	return filterAnswer(trimmed, allowed)
}

func filterAnswer(data []byte, allowed func(string) bool) []byte {
	var answer map[string]json.RawMessage
	if json.Unmarshal(data, &answer) != nil {
		return data
	}
	var result map[string]json.RawMessage
	if json.Unmarshal(answer["result"], &result) != nil {
		return data
	}
	var tools []json.RawMessage
	if _, ok := result["tools"]; !ok || json.Unmarshal(result["tools"], &tools) != nil {
		return data
	}
	kept := make([]json.RawMessage, 0, len(tools))
	for _, tool := range tools {
		var named struct {
			Name string `json:"name"`
		}
		if json.Unmarshal(tool, &named) == nil && named.Name != "" && allowed(named.Name) {
			kept = append(kept, tool)
		}
	}
	result["tools"] = marshal(kept)
	answer["result"] = marshal(result)
	return marshal(answer)
}

// scrubber replaces the upstream credential in everything relayed to the
// caller, as sent and in the forms a server may echo it: JSON-escaped,
// URL-encoded, and base64 (standard and URL alphabets, padded or not, at
// any alignment inside a longer encoded value).
type scrubber struct {
	secrets [][]byte
}

func newScrubber(credential string) *scrubber {
	s := &scrubber{}
	if credential == "" {
		return s
	}
	add := func(form string) {
		if form == "" {
			return
		}
		for _, known := range s.secrets {
			if string(known) == form {
				return
			}
		}
		s.secrets = append(s.secrets, []byte(form))
	}
	add(credential)
	if quoted, err := json.Marshal(credential); err == nil {
		add(string(quoted[1 : len(quoted)-1]))
	}
	if len(credential) >= minScrubbed {
		add(url.QueryEscape(credential))
		add(url.PathEscape(credential))
		for _, encoding := range []*base64.Encoding{base64.StdEncoding, base64.URLEncoding} {
			for _, core := range base64Cores(encoding, []byte(credential)) {
				add(core)
			}
		}
	}
	return s
}

// base64Cores are the encoded characters that depend on the secret alone,
// for the secret starting at each of the three byte alignments: a secret
// inside a longer value encodes to one of them wherever it sits.
func base64Cores(encoding *base64.Encoding, secret []byte) []string {
	var cores []string
	for offset := 0; offset < 3; offset++ {
		padded := append(make([]byte, offset), secret...)
		encoded := encoding.EncodeToString(padded)
		start := 0
		if offset > 0 {
			start = 4 // the group that mixes the prefix in
		}
		end := 4 * (len(padded) / 3) // whole groups only
		if end-start >= minScrubbed {
			cores = append(cores, encoded[start:end])
		}
	}
	return cores
}

func (s *scrubber) apply(data []byte) []byte {
	for _, secret := range s.secrets {
		if bytes.Contains(data, secret) {
			data = bytes.ReplaceAll(data, secret, []byte(redacted))
		}
	}
	return data
}

func (s *scrubber) holds(data []byte) bool {
	for _, secret := range s.secrets {
		if bytes.Contains(data, secret) {
			return true
		}
	}
	return false
}

func scrubText(text, credential string) string {
	return string(newScrubber(credential).apply([]byte(text)))
}

// relayJSON reads one JSON answer within the budget, filters and scrubs
// it and writes it with its status. It returns the bytes written.
func relayJSON(ctx context.Context, w http.ResponseWriter, response *http.Response, buffers *budget, allowed func(string) bool, scrub *scrubber) (int, error) {
	release, ok := buffers.acquire(ctx, unitsFor(response.ContentLength))
	if !ok {
		return 0, errBusy
	}
	defer release()
	raw, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBody+1))
	if err != nil {
		return 0, errUnreadable
	}
	if len(raw) > maxResponseBody {
		return 0, errTooLarge
	}
	raw = scrub.apply(filterTools(raw, allowed))
	w.Header().Set("Content-Length", strconv.Itoa(len(raw)))
	w.WriteHeader(response.StatusCode)
	n, err := w.Write(raw)
	return n, err
}

var (
	errBusy       = errors.New("the front is at its buffer budget")
	errUnreadable = errors.New("the server's answer is unreadable")
	errTooLarge   = errors.New("the server's answer is too large")
)

// relayEvents copies a server-sent event stream event by event. Each
// event's data is joined, filtered (a tool list in it is narrowed to what
// the binding may call) and scrubbed, also across the lines it was split
// into, then written as one data line, flushed at once. An event is
// buffered within the budget and bounded by maxEventBytes.
func relayEvents(ctx context.Context, w http.ResponseWriter, body io.Reader, buffers *budget, allowed func(string) bool, scrub *scrubber) (int, error) {
	flusher, _ := w.(http.Flusher)
	reader := bufio.NewReaderSize(body, 64*1024)
	written := 0
	var event [][]byte
	size := 0
	var release func()
	emit := func() error {
		if len(event) == 0 {
			return nil
		}
		defer func() {
			event, size = nil, 0
			if release != nil {
				release()
				release = nil
			}
		}()
		var other, data [][]byte
		hasData := false
		for _, line := range event {
			if value, ok := bytes.CutPrefix(line, []byte("data:")); ok {
				data = append(data, bytes.TrimPrefix(value, []byte(" ")))
				hasData = true
			} else {
				other = append(other, line)
			}
		}
		var out bytes.Buffer
		for _, line := range other {
			out.Write(scrub.apply(line))
			out.WriteByte('\n')
		}
		if hasData {
			joined := filterTools(bytes.Join(data, []byte("\n")), allowed)
			if flat := bytes.Join(data, nil); !scrub.holds(joined) && scrub.holds(flat) {
				// A credential split across data lines: relay the
				// event on one line, scrubbed.
				joined = flat
			}
			for _, line := range bytes.Split(scrub.apply(joined), []byte("\n")) {
				out.WriteString("data: ")
				out.Write(line)
				out.WriteByte('\n')
			}
		}
		out.WriteByte('\n')
		n, err := w.Write(out.Bytes())
		written += n
		if flusher != nil {
			flusher.Flush()
		}
		return err
	}
	for {
		line, err := readLine(reader, maxEventBytes-size+2)
		if errors.Is(err, errTooLarge) {
			if release != nil {
				release()
			}
			return written, err
		}
		if len(line) > 0 {
			line = bytes.TrimRight(line, "\r\n")
			if len(line) == 0 {
				if werr := emit(); werr != nil {
					return written, werr
				}
			} else {
				if release == nil {
					var ok bool
					if release, ok = buffers.acquire(ctx, unitsFor(maxEventBytes)); !ok {
						return written, errBusy
					}
				}
				size += len(line)
				if size > maxEventBytes {
					if release != nil {
						release()
					}
					return written, errTooLarge
				}
				event = append(event, append([]byte(nil), line...))
			}
		}
		if err != nil {
			if werr := emit(); werr != nil {
				return written, werr
			}
			if errors.Is(err, io.EOF) {
				return written, nil
			}
			return written, err
		}
	}
}

// readLine reads one line, never more than limit bytes of it.
func readLine(reader *bufio.Reader, limit int) ([]byte, error) {
	var line []byte
	for {
		chunk, err := reader.ReadSlice('\n')
		if len(line)+len(chunk) > limit {
			return nil, errTooLarge
		}
		line = append(line, chunk...)
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		return line, err
	}
}
