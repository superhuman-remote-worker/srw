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

// mayNeedRewrite: a member named "tools" or "error" may be in the payload,
// in any case: a client that folds case (encoding/json does, and folds the
// long s "ſ" to "s") reads "Tools" or "toolſ" as "tools". A key spelled with
// escapes ("tools") holds a backslash-u.
func mayNeedRewrite(payload []byte) bool {
	return containsFold(payload, "tools") ||
		containsFold(payload, "error") ||
		bytes.Contains(payload, []byte(`\u`)) ||
		bytes.Contains(payload, []byte("ſ"))
}

// containsFold reports whether data holds word in any ASCII case, without
// a copy (the payload's memory is the budget's).
func containsFold(data []byte, word string) bool {
	for i := 0; i+len(word) <= len(data); i++ {
		if bytes.EqualFold(data[i:i+len(word)], []byte(word)) {
			return true
		}
	}
	return false
}

// foldedVariant reports whether an object has a key a case-folding client
// reads as one of names without it being exactly that name ("Result",
// "TOOLS", "namſ"): such a client would read a member the front did not.
func foldedVariant(members map[string]json.RawMessage, names ...string) bool {
	for key := range members {
		for _, name := range names {
			if key != name && strings.EqualFold(key, name) {
				return true
			}
		}
	}
	return false
}

// rewriteAnswers rewrites one message, or each message of a batch.
// Members are read by their exact keys, the last of a duplicate winning,
// as the SDK clients read them; a key a case-folding client would read as
// a member the front decides on, but spelled otherwise, makes the answer
// one the front cannot read (it is never relayed), or its tool is dropped.
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
	// An exact duplicate top-level key resolves last-wins here, as it does
	// in the Go SDK's client (encoding/json), so both read the same member.
	var answer map[string]json.RawMessage
	if json.Unmarshal(data, &answer) != nil {
		if json.Valid(data) {
			return data, false, nil // not an object: no message
		}
		return nil, false, errUnparseable
	}
	if foldedVariant(answer, "result", "error") {
		return nil, false, errUnparseable
	}
	changed := false
	var nested map[string]func(*bytes.Buffer)
	if raw, ok := answer["result"]; ok {
		var result map[string]json.RawMessage
		if json.Unmarshal(raw, &result) == nil {
			if foldedVariant(result, "tools") {
				return nil, false, errUnparseable
			}
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
// "name" (the last one, as a client reads it); a tool with a key a
// case-folding client reads as its name ("Name") is dropped. A list that is
// not a list lists nothing.
func keptTools(listed json.RawMessage, allowed func(string) bool) []json.RawMessage {
	var tools []json.RawMessage
	if json.Unmarshal(listed, &tools) != nil {
		return []json.RawMessage{}
	}
	kept := make([]json.RawMessage, 0, len(tools))
	for _, tool := range tools {
		var members map[string]json.RawMessage
		if json.Unmarshal(tool, &members) != nil || foldedVariant(members, "name") {
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
// lease-ended code, as a number in any notation or a numeric string; an
// error with a key a case-folding client reads as its code ("Code") is
// rewritten too.
func reservedError(raw json.RawMessage) bool {
	var members map[string]json.RawMessage
	if json.Unmarshal(raw, &members) != nil {
		return false
	}
	if foldedVariant(members, "code") {
		return true
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
