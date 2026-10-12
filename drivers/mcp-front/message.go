package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"unicode/utf8"
)

// The front decides on what it parsed and forwards only that: a body
// re-encoded from the members it checked, never the caller's bytes. So a
// server that reads JSON another way (exact keys, first or last of a
// duplicate, its own escapes or case folding) sees exactly the message the
// front allowed.
//
// The parse is strict where Go's encoding/json is lenient: keys match
// exactly (encoding/json folds case and Unicode, so "Name", "NAME" and
// "nameſ"-style keys would reach a struct field), a duplicate key is
// refused, a key the message shape does not name is refused, and the body
// must be valid UTF-8.

// The JSON-RPC methods the front forwards. Every other request is
// answered "method not found", every other notification is accepted and
// dropped, until a driver spec declares it with a tool class.
var allowedMethods = map[string]bool{
	"initialize":                true,
	"notifications/initialized": true,
	"notifications/cancelled":   true,
	"ping":                      true,
	"tools/list":                true,
	"tools/call":                true,
}

var (
	requestKeys  = map[string]bool{"jsonrpc": true, "id": true, "method": true, "params": true}
	responseKeys = map[string]bool{"jsonrpc": true, "id": true, "result": true, "error": true}
	callKeys     = map[string]bool{"name": true, "arguments": true, "_meta": true}
	errBatch     = errors.New("JSON-RPC batches are not supported")
)

// member is one key of a JSON object and its value, as written.
type member struct {
	key   string
	value json.RawMessage
}

// rpcMessage is what the front decided on.
type rpcMessage struct {
	ID       json.RawMessage
	Method   string
	tool     string
	response bool
}

// notification is a request without an id: nothing may answer it.
func (m *rpcMessage) notification() bool {
	return !m.response && len(m.ID) == 0
}

// objectMembers decodes one JSON object into its members, in order,
// refusing a duplicate key and anything after the object.
func objectMembers(raw []byte) ([]member, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	token, err := decoder.Token()
	if err != nil {
		return nil, errors.New("the body is not JSON")
	}
	if delim, ok := token.(json.Delim); !ok || delim != '{' {
		return nil, errors.New("a JSON-RPC message is an object")
	}
	seen := map[string]bool{}
	var members []member
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return nil, errors.New("the body is not JSON")
		}
		key, ok := token.(string)
		if !ok {
			return nil, errors.New("the body is not JSON")
		}
		if seen[key] {
			return nil, fmt.Errorf("the key %q appears twice", key)
		}
		seen[key] = true
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return nil, errors.New("the body is not JSON")
		}
		members = append(members, member{key: key, value: value})
	}
	if _, err := decoder.Token(); err != nil {
		return nil, errors.New("the body is not JSON")
	}
	if _, err := decoder.Token(); err != io.EOF {
		return nil, errors.New("the body holds more than one message")
	}
	return members, nil
}

// closedMembers is objectMembers where only “keys“ may appear.
func closedMembers(raw []byte, keys map[string]bool, where string) ([]member, error) {
	members, err := objectMembers(raw)
	if err != nil {
		return nil, err
	}
	for _, m := range members {
		if !keys[m.key] {
			return nil, fmt.Errorf("%s has an unknown key %q", where, m.key)
		}
	}
	return members, nil
}

func lookup(members []member, key string) (json.RawMessage, bool) {
	for _, m := range members {
		if m.key == key {
			return m.value, true
		}
	}
	return nil, false
}

func jsonString(raw json.RawMessage) (string, bool) {
	var text string
	if len(raw) == 0 || raw[0] != '"' || json.Unmarshal(raw, &text) != nil {
		return "", false
	}
	return text, true
}

func isObject(raw json.RawMessage) bool {
	return len(raw) > 0 && raw[0] == '{'
}

// encodeMembers writes members as one compact object, keys re-encoded.
func encodeMembers(members []member) []byte {
	var out bytes.Buffer
	out.WriteByte('{')
	for i, m := range members {
		if i > 0 {
			out.WriteByte(',')
		}
		out.Write(marshal(m.key))
		out.WriteByte(':')
		if err := json.Compact(&out, m.value); err != nil {
			out.Write(m.value)
		}
	}
	out.WriteByte('}')
	return out.Bytes()
}

// parseMessage checks one JSON-RPC message and returns it with the body
// the front forwards in its place.
func parseMessage(body []byte) (*rpcMessage, []byte, error) {
	if !utf8.Valid(body) {
		return nil, nil, errors.New("the body is not UTF-8")
	}
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) > 0 && trimmed[0] == '[' {
		return nil, nil, errBatch
	}
	keys := map[string]bool{}
	for key := range requestKeys {
		keys[key] = true
	}
	for key := range responseKeys {
		keys[key] = true
	}
	members, err := closedMembers(trimmed, keys, "the message")
	if err != nil {
		return nil, nil, err
	}
	version, _ := lookup(members, "jsonrpc")
	if text, ok := jsonString(version); !ok || text != "2.0" {
		return nil, nil, errors.New(`the message must carry "jsonrpc": "2.0"`)
	}
	id, hasID := lookup(members, "id")
	if hasID && !validID(id) {
		return nil, nil, errors.New("the id must be a string or a number")
	}
	rawMethod, isRequest := lookup(members, "method")
	if !isRequest {
		return parseResponse(members, id, hasID)
	}
	method, ok := jsonString(rawMethod)
	if !ok {
		return nil, nil, errors.New("the method must be a string")
	}
	for _, m := range members {
		if !requestKeys[m.key] {
			return nil, nil, fmt.Errorf("a request has no %q", m.key)
		}
	}
	message := &rpcMessage{ID: id, Method: method}
	out := []member{{key: "jsonrpc", value: json.RawMessage(`"2.0"`)}}
	if hasID {
		out = append(out, member{key: "id", value: id})
	}
	out = append(out, member{key: "method", value: marshal(method)})
	params, hasParams := lookup(members, "params")
	if hasParams && !isObject(params) {
		return nil, nil, errors.New("params must be an object")
	}
	switch {
	case method == "tools/call":
		if !hasParams {
			return nil, nil, errors.New("tools/call needs params")
		}
		call, err := closedMembers(params, callKeys, "tools/call params")
		if err != nil {
			return nil, nil, err
		}
		rawName, _ := lookup(call, "name")
		name, ok := jsonString(rawName)
		if !ok || name == "" {
			return nil, nil, errors.New("tools/call needs a tool name")
		}
		if arguments, ok := lookup(call, "arguments"); ok && !isObject(arguments) && string(arguments) != "null" {
			return nil, nil, errors.New("tools/call arguments must be an object")
		}
		message.tool = name
		canonical := []member{{key: "name", value: marshal(name)}}
		for _, key := range []string{"arguments", "_meta"} {
			if value, ok := lookup(call, key); ok {
				canonical = append(canonical, member{key: key, value: value})
			}
		}
		out = append(out, member{key: "params", value: encodeMembers(canonical)})
	case hasParams:
		nested, err := objectMembers(params)
		if err != nil {
			return nil, nil, err
		}
		out = append(out, member{key: "params", value: encodeMembers(nested)})
	}
	return message, encodeMembers(out), nil
}

// parseResponse checks a client's answer to a server request (an id and a
// result or an error, nothing else).
func parseResponse(members []member, id json.RawMessage, hasID bool) (*rpcMessage, []byte, error) {
	for _, m := range members {
		if !responseKeys[m.key] {
			return nil, nil, fmt.Errorf("a response has no %q", m.key)
		}
	}
	result, hasResult := lookup(members, "result")
	failure, hasError := lookup(members, "error")
	if !hasID || hasResult == hasError {
		return nil, nil, errors.New("a message is a request (a method) or a response (an id and a result or an error)")
	}
	out := []member{{key: "jsonrpc", value: json.RawMessage(`"2.0"`)}, {key: "id", value: id}}
	if hasResult {
		out = append(out, member{key: "result", value: result})
	} else {
		out = append(out, member{key: "error", value: failure})
	}
	return &rpcMessage{ID: id, response: true}, encodeMembers(out), nil
}

func validID(raw json.RawMessage) bool {
	if len(raw) == 0 || string(raw) == "null" {
		return false
	}
	if raw[0] == '"' {
		_, ok := jsonString(raw)
		return ok
	}
	var number json.Number
	return json.Unmarshal(raw, &number) == nil
}

func marshal(value any) []byte {
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	encoder.Encode(value)
	return bytes.TrimRight(buffer.Bytes(), "\n")
}
