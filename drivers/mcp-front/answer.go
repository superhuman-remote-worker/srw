package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"sort"
	"strconv"
	"strings"
)

const (
	// What a server's own use of the front's lease-ended code becomes:
	// only the front may tell a client its lease ended.
	reservedCodeMessage = "the server answered with an error code reserved for SRW's front"
)

// errUnparseable: a payload that may carry a tool list or an error the
// front must rewrite, and that the front cannot read as JSON. A client
// more lenient than the front (NaN, say) could still read it, so it is
// never relayed.
var errUnparseable = errors.New("the server's answer cannot be read as JSON")

// clean makes one JSON-RPC payload safe to relay (within its answer's
// budget claim): the credential scrubbed first (a credential broken across lines
// would keep the payload from parsing), then every tool list narrowed to
// what the binding may call and the front's lease-ended error code made
// unforgeable, then scrubbed again (the rewrite encodes keys a server may
// have escaped). A payload left unchanged is returned as it came.
func clean(payload []byte, allowed func(string) bool, scrub *scrubber) ([]byte, error) {
	payload = scrub.apply(payload)
	if !mayNeedRewrite(payload) {
		return payload, nil
	}
	rewritten, changed, err := rewriteAnswers(payload, allowed)
	if err != nil {
		return nil, err
	}
	if !changed {
		return payload, nil
	}
	return scrub.apply(rewritten), nil
}

// mayNeedRewrite: a member named "tools" or "error" may be in the payload.
// A key spelled with escapes ("tools") holds a backslash-u.
func mayNeedRewrite(payload []byte) bool {
	return bytes.Contains(payload, []byte("tools")) ||
		bytes.Contains(payload, []byte("error")) ||
		bytes.Contains(payload, []byte(`\u`))
}

// rewriteAnswers rewrites one message, or each message of a batch.
// Members are read by their exact keys, the last of a duplicate winning,
// as the SDK clients read them.
func rewriteAnswers(data []byte, allowed func(string) bool) ([]byte, bool, error) {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 {
		return data, false, nil
	}
	switch trimmed[0] {
	case '[':
		var messages []json.RawMessage
		if json.Unmarshal(trimmed, &messages) != nil {
			return nil, false, errUnparseable
		}
		changed := false
		for i, message := range messages {
			rewritten, did, err := rewriteAnswer(message, allowed)
			if err != nil {
				return nil, false, err
			}
			messages[i] = rewritten
			changed = changed || did
		}
		if !changed {
			return data, false, nil
		}
		return marshal(messages), true, nil
	case '{':
		return rewriteAnswer(trimmed, allowed)
	}
	if !json.Valid(trimmed) {
		return nil, false, errUnparseable
	}
	return data, false, nil // a bare value is no message
}

func rewriteAnswer(data []byte, allowed func(string) bool) ([]byte, bool, error) {
	var answer map[string]json.RawMessage
	if json.Unmarshal(data, &answer) != nil {
		if json.Valid(data) {
			return data, false, nil // not an object: no message
		}
		return nil, false, errUnparseable
	}
	changed := false
	var nested map[string]func(*bytes.Buffer)
	if raw, ok := answer["result"]; ok {
		var result map[string]json.RawMessage
		if json.Unmarshal(raw, &result) == nil {
			if listed, ok := result["tools"]; ok {
				// Each copy goes once read: the answer is written once,
				// into a buffer sized for it, the kept tools inside it.
				delete(answer, "result")
				delete(result, "tools")
				kept := keptTools(listed, allowed)
				listed = nil
				nested = map[string]func(*bytes.Buffer){"result": func(out *bytes.Buffer) {
					writeObject(out, result, map[string]func(*bytes.Buffer){"tools": func(out *bytes.Buffer) {
						writeArray(out, kept)
					}})
				}}
				changed = true
			}
		}
	}
	if raw, ok := answer["error"]; ok && reservedError(raw) {
		answer["error"] = marshal(map[string]any{"code": -32603, "message": reservedCodeMessage})
		changed = true
	}
	if !changed {
		return data, false, nil
	}
	out := bytes.NewBuffer(make([]byte, 0, len(data)+64))
	writeObject(out, answer, nested)
	return out.Bytes(), true, nil
}

// writeObject writes members (and the members nested writes) as one JSON
// object, keys in order, values as they were read.
func writeObject(out *bytes.Buffer, members map[string]json.RawMessage, nested map[string]func(*bytes.Buffer)) {
	keys := make([]string, 0, len(members)+len(nested))
	for key := range members {
		keys = append(keys, key)
	}
	for key := range nested {
		if _, both := members[key]; !both {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	out.WriteByte('{')
	for i, key := range keys {
		if i > 0 {
			out.WriteByte(',')
		}
		out.Write(marshal(key))
		out.WriteByte(':')
		if write, ok := nested[key]; ok {
			write(out)
		} else {
			out.Write(members[key])
		}
	}
	out.WriteByte('}')
}

func writeArray(out *bytes.Buffer, items []json.RawMessage) {
	out.WriteByte('[')
	for i, item := range items {
		if i > 0 {
			out.WriteByte(',')
		}
		out.Write(item)
	}
	out.WriteByte(']')
}

// keptTools: the listed tools the binding may call, each read by its exact
// "name" (the last one, as a client reads it). A list that is not a list
// lists nothing.
func keptTools(listed json.RawMessage, allowed func(string) bool) []json.RawMessage {
	var tools []json.RawMessage
	if json.Unmarshal(listed, &tools) != nil {
		return []json.RawMessage{}
	}
	kept := make([]json.RawMessage, 0, len(tools))
	for _, tool := range tools {
		var members map[string]json.RawMessage
		if json.Unmarshal(tool, &members) != nil {
			continue
		}
		var name string
		if json.Unmarshal(members["name"], &name) == nil && name != "" && allowed(name) {
			kept = append(kept, tool)
		}
	}
	return kept
}

// reservedError: an error whose code a client would read as the front's
// lease-ended code, as a number in any notation or a numeric string.
func reservedError(raw json.RawMessage) bool {
	var members map[string]json.RawMessage
	if json.Unmarshal(raw, &members) != nil {
		return false
	}
	code := members["code"]
	var number float64
	if json.Unmarshal(code, &number) == nil {
		return number == leaseEndedCode
	}
	var text string
	if json.Unmarshal(code, &text) == nil {
		parsed, err := strconv.ParseFloat(strings.TrimSpace(text), 64)
		return err == nil && parsed == leaseEndedCode
	}
	return false
}
