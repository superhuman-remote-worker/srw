package main

import (
	"bytes"
	"encoding/base64"
	"io"
	"net/url"
)

// scrubber writes a response through to dst with every occurrence of the
// upstream credential (and of the encodings it travels in) replaced by as
// many '*': the same length, so pkt-line framing stays intact. It streams:
// it holds back only a tail that could still begin a credential (at most
// one credential's length), never the response.
type scrubber struct {
	dst     io.Writer
	needles [][]byte
	longest int
	held    []byte
}

// newScrubber scrubs the credential itself, its URL escape and the Basic
// authorization value the driver sends upstream (base64 of
// "oauth2:<credential>"), as the upstream could echo any of them.
func newScrubber(dst io.Writer, credential string) *scrubber {
	s := &scrubber{dst: dst}
	if credential == "" {
		return s
	}
	seen := map[string]bool{}
	for _, needle := range []string{
		credential,
		url.QueryEscape(credential),
		basicValue(credential),
		base64.StdEncoding.EncodeToString([]byte(credential)),
	} {
		if needle == "" || seen[needle] {
			continue
		}
		seen[needle] = true
		s.needles = append(s.needles, []byte(needle))
		s.longest = max(s.longest, len(needle))
	}
	return s
}

// basicValue is the Basic credential the driver presents upstream; the
// username is the one SRW's clone URL always used (oauth2).
func basicValue(credential string) string {
	return base64.StdEncoding.EncodeToString([]byte(upstreamUsername + ":" + credential))
}

func (s *scrubber) Write(p []byte) (int, error) {
	if len(s.needles) == 0 {
		return s.dst.Write(p)
	}
	buf := make([]byte, 0, len(s.held)+len(p))
	buf = append(buf, s.held...)
	buf = append(buf, p...)
	s.mask(buf)
	keep := s.partial(buf)
	s.held = append(s.held[:0], buf[len(buf)-keep:]...)
	if out := buf[:len(buf)-keep]; len(out) > 0 {
		if _, err := s.dst.Write(out); err != nil {
			return 0, err
		}
	}
	return len(p), nil
}

// finish writes what was held back; a tail shorter than a credential cannot
// be one.
func (s *scrubber) finish() error {
	if len(s.held) == 0 {
		return nil
	}
	held := s.held
	s.held = nil
	_, err := s.dst.Write(held)
	return err
}

func (s *scrubber) mask(buf []byte) {
	for _, needle := range s.needles {
		for start := 0; ; {
			index := bytes.Index(buf[start:], needle)
			if index < 0 {
				break
			}
			at := start + index
			for i := at; i < at+len(needle); i++ {
				buf[i] = '*'
			}
			start = at + len(needle)
		}
	}
}

// partial is the length of the longest tail of buf that begins a needle.
func (s *scrubber) partial(buf []byte) int {
	for n := min(len(buf), s.longest-1); n > 0; n-- {
		tail := buf[len(buf)-n:]
		for _, needle := range s.needles {
			if len(needle) > n && bytes.HasPrefix(needle, tail) {
				return n
			}
		}
	}
	return 0
}

// scrubText scrubs a short text (an error message) whole.
func scrubText(text, credential string) string {
	var out bytes.Buffer
	s := newScrubber(&out, credential)
	_, _ = s.Write([]byte(text))
	_ = s.finish()
	return out.String()
}
