package main

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"sort"
	"strings"
)

const (
	redacted = "[redacted]"
	// The shortest credential whose derived forms (encodings, case, line
	// breaks) the scrubber looks for: shorter ones would match by chance.
	minScrubbed = 8
)

// scrubber replaces the upstream credential in everything relayed to the
// caller, as sent and in the forms a server may echo it: JSON-escaped (with
// "\/" for a slash, or every character \u-escaped), URL-encoded (or every
// byte percent-encoded), upper- and lower-case, hex, and base64 (standard
// and URL alphabets, padded or not, at any alignment inside a longer
// encoded value). A form broken across lines is found too: line breaks,
// real or JSON-escaped ("\n", "\r"), are looked through, as in base64
// wrapped at 76 columns or a value split across stream data lines; the
// break inside a match goes with it.
type scrubber struct {
	secrets [][]byte
	breaks  bool
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
		escaped := string(quoted[1 : len(quoted)-1])
		add(escaped)
		add(strings.ReplaceAll(escaped, "/", `\/`))
	}
	if len(credential) < minScrubbed {
		return s
	}
	s.breaks = true
	add(url.QueryEscape(credential))
	add(url.PathEscape(credential))
	var percentUpper, percentLower, unicodeLower, unicodeUpper strings.Builder
	for _, c := range []byte(credential) {
		fmt.Fprintf(&percentUpper, "%%%02X", c)
		fmt.Fprintf(&percentLower, "%%%02x", c)
		fmt.Fprintf(&unicodeLower, `\u%04x`, c)
		fmt.Fprintf(&unicodeUpper, `\u%04X`, c)
	}
	add(percentUpper.String())
	add(percentLower.String())
	add(unicodeLower.String())
	add(unicodeUpper.String())
	add(strings.ToUpper(credential))
	add(strings.ToLower(credential))
	add(hex.EncodeToString([]byte(credential)))
	add(strings.ToUpper(hex.EncodeToString([]byte(credential))))
	for _, encoding := range []*base64.Encoding{base64.StdEncoding, base64.URLEncoding} {
		for _, core := range base64Cores(encoding, []byte(credential)) {
			add(core)
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

// breakLen is the length of the line break at data[i]: a real CR or LF, or
// a JSON-escaped one; 0 when there is none.
func breakLen(data []byte, i int) int {
	switch data[i] {
	case '\n', '\r':
		return 1
	case '\\':
		if i+1 < len(data) && (data[i+1] == 'n' || data[i+1] == 'r') {
			return 2
		}
	}
	return 0
}

func hasBreak(data []byte) bool {
	return bytes.IndexByte(data, '\n') >= 0 || bytes.IndexByte(data, '\r') >= 0 ||
		bytes.Contains(data, []byte(`\n`)) || bytes.Contains(data, []byte(`\r`))
}

// span is one match, as positions in the view searched.
type span struct{ start, end int }

// find returns every match of a form in view, merged and in order.
func (s *scrubber) find(view []byte) []span {
	var found []span
	for _, secret := range s.secrets {
		for at := 0; ; {
			i := bytes.Index(view[at:], secret)
			if i < 0 {
				break
			}
			found = append(found, span{at + i, at + i + len(secret)})
			at += i + 1
		}
	}
	if len(found) < 2 {
		return found
	}
	sort.Slice(found, func(a, b int) bool { return found[a].start < found[b].start })
	merged := found[:1]
	for _, next := range found[1:] {
		last := &merged[len(merged)-1]
		if next.start <= last.end {
			if next.end > last.end {
				last.end = next.end
			}
			continue
		}
		merged = append(merged, next)
	}
	return merged
}

func (s *scrubber) holds(data []byte) bool {
	return len(s.find(data)) > 0
}

// apply scrubs data: a copy with every match redacted, or data itself when
// nothing matches. Looking through line breaks it makes one more copy (the
// view searched).
func (s *scrubber) apply(data []byte) []byte {
	if len(s.secrets) == 0 || len(data) == 0 {
		return data
	}
	if !s.breaks || !hasBreak(data) {
		found := s.find(data)
		if len(found) == 0 {
			return data
		}
		out := make([]byte, 0, len(data))
		at := 0
		for _, match := range found {
			out = append(out, data[at:match.start]...)
			out = append(out, redacted...)
			at = match.end
		}
		return append(out, data[at:]...)
	}
	// Look through line breaks: search a view without them, then redact
	// in data what each match covers, the breaks inside it included.
	view := make([]byte, 0, len(data))
	for i := 0; i < len(data); {
		if n := breakLen(data, i); n > 0 {
			i += n
			continue
		}
		view = append(view, data[i])
		i++
	}
	found := s.find(view)
	view = nil
	if len(found) == 0 {
		return data
	}
	out := make([]byte, 0, len(data))
	next, inside := 0, false
	for i, v := 0, 0; i < len(data); {
		if n := breakLen(data, i); n > 0 {
			if !inside {
				out = append(out, data[i:i+n]...)
			}
			i += n
			continue
		}
		if !inside && next < len(found) && v == found[next].start {
			out = append(out, redacted...)
			inside = true
		}
		if !inside {
			out = append(out, data[i])
		}
		i++
		v++
		if inside && v == found[next].end {
			inside = false
			next++
		}
	}
	return out
}

func scrubText(text, credential string) string {
	return string(newScrubber(credential).apply([]byte(text)))
}
