package main

import (
	"encoding/json"
	"strings"
	"unicode"
)

// Python str.strip also considers the four ASCII information separators whitespace.
func pythonSpace(r rune) bool { return unicode.IsSpace(r) || (r >= 0x1c && r <= 0x1f) }
func clipAppendContent(text string, limit int) string {
	text = strings.TrimFunc(text, pythonSpace)
	r := []rune(text)
	if len(r) <= limit {
		return text
	}
	cut := r[:limit]
	for _, mark := range []string{". ", "! ", "? ", "\n"} {
		idx := strings.LastIndex(string(cut), mark)
		if idx >= 0 {
			n := len([]rune(string(cut)[:idx]))
			if n >= limit/2 {
				return strings.TrimFunc(string(cut[:n+1]), pythonSpace)
			}
		}
	}
	idx := strings.LastIndex(string(cut), " ")
	if idx >= 0 {
		n := len([]rune(string(cut)[:idx]))
		if n >= limit/2 {
			cut = cut[:n]
		}
	}
	return strings.TrimRightFunc(string(cut), pythonSpace) + " …"
}

type event map[string]any

func object(v any) map[string]any {
	m, _ := v.(map[string]any)
	if m == nil {
		return map[string]any{}
	}
	return m
}
func str(v any) string { s, _ := v.(string); return s }
func optionalString(v any) any {
	if s, ok := v.(string); ok {
		return s
	}
	return nil
}
func number(v any) any {
	if n, ok := v.(float64); ok {
		return n
	}
	return nil
}
func integer(v any) any {
	if n, ok := v.(float64); ok {
		return int64(n)
	}
	return nil
}
func text(v any, def string) string {
	if v == nil || v == "" || v == false {
		return def
	}
	if s, ok := v.(string); ok {
		return s
	}
	b, _ := json.Marshal(v)
	return string(b)
}
func parseEvent(raw map[string]any) event {
	t := text(raw["type"], "")
	if t == "session.input_audio.append" || t == "session.output_audio.delta" {
		return nil
	}
	e := event{"event_id": optionalString(raw["event_id"])}
	other := func() event { e["type"] = "other"; e["wire_type"] = t; e["payload"] = raw; return e }
	switch t {
	case "session.input_transcript.delta", "session.output_transcript.delta":
		e["type"] = "transcript"
		e["speaker"] = "assistant"
		if t == "session.input_transcript.delta" {
			e["speaker"] = "caller"
		}
		e["text"] = text(raw["delta"], "")
		e["start_ms"] = integer(raw["start_ms"])
		e["end_ms"] = integer(raw["end_ms"])
	case "session.delegation.created":
		d := object(raw["delegation"])
		id := str(d["id"])
		if id == "" {
			return other()
		}
		e["type"] = "delegation"
		e["delegation_id"] = id
		e["offset_ms"] = integer(raw["offset_ms"])
		e["target"] = text(d["target"], "client")
	case "session.instructions.appended", "session.commentary.appended", "session.thinking.appended":
		e["type"] = "append_ack"
		e["kind"] = strings.Split(t, ".")[1]
		e["client_event_id"] = optionalString(raw["client_event_id"])
		e["start_ms"] = integer(raw["start_ms"])
		e["end_ms"] = integer(raw["end_ms"])
	case "error":
		d := object(raw["error"])
		e["type"] = "error"
		e["message"] = text(d["message"], "")
		for _, k := range []string{"code", "param", "client_event_id"} {
			e[k] = optionalString(d[k])
		}
		e["error_type"] = optionalString(d["type"])
	case "transport.dtmf.received":
		e["type"] = "dtmf"
		e["digit"] = text(raw["event"], "")
	case "transport.ringing", "transport.answered", "transport.failed":
		d := object(raw["error"])
		e["type"] = "transport"
		e["state"] = strings.TrimPrefix(t, "transport.")
		e["error_code"] = optionalString(d["code"])
		e["error_message"] = optionalString(d["message"])
	case "session.usage.updated":
		e["type"] = "usage"
		n := number(object(raw["usage"])["seconds"])
		if n == nil {
			n = float64(0)
		}
		e["seconds"] = n
		e["context_usage_ratio"] = number(object(raw["context_window"])["usage_ratio"])
	case "session.closed":
		e["type"] = "closed"
		e["reason"] = "unknown"
		e["raw_reason"] = optionalString(raw["reason"])
		switch str(raw["reason"]) {
		case "close_requested", "expired", "content", "remote_hangup", "connection_lost", "unknown":
			e["reason"] = raw["reason"]
		}
		e["usage_seconds"] = number(object(raw["usage"])["seconds"])
	default:
		return other()
	}
	return e
}
func journalEvent(e event, raw map[string]any) map[string]any {
	kind := str(e["type"])
	if kind == "usage" || kind == "append_ack" {
		return nil
	}
	payload := map[string]any{}
	for k, v := range e {
		if k != "type" {
			payload[k] = v
		}
	}
	row := map[string]any{"kind": kind, "payload": payload}
	if id := str(e["event_id"]); id != "" {
		row["event_id"] = id
	}
	switch kind {
	case "transcript":
		for _, k := range []string{"speaker", "text", "start_ms", "end_ms"} {
			row[k] = e[k]
		}
	case "delegation":
		row["delegation_id"] = e["delegation_id"]
		row["start_ms"] = e["offset_ms"]
	case "dtmf":
		row["text"] = e["digit"]
	case "error":
		row["text"] = e["message"]
		row["payload"] = map[string]any{"code": e["code"], "type": e["error_type"], "param": e["param"], "client_event_id": e["client_event_id"]}
	case "closed":
		row["payload"] = map[string]any{"reason": e["reason"], "raw_reason": e["raw_reason"], "usage_seconds": e["usage_seconds"]}
	default:
		row["kind"] = "system"
		row["payload"] = raw
	}
	return row
}

type dedupe struct {
	seen  map[string]bool
	order []string
}

func (d *dedupe) first(id string) bool {
	if id == "" {
		return true
	}
	if d.seen == nil {
		d.seen = map[string]bool{}
	}
	if d.seen[id] {
		return false
	}
	d.seen[id] = true
	d.order = append(d.order, id)
	if len(d.order) > 4096 {
		delete(d.seen, d.order[0])
		d.order = d.order[1:]
	}
	return true
}
