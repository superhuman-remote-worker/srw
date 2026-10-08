package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strings"
)

// An upload-pack ref advertisement. Under protocol v2 (Git-Protocol:
// version=2) it is a capability list, and two capabilities would send the
// client elsewhere for objects, past the driver and its lease check:
// "bundle-uri" (a command) and "packfile-uris" (a feature of "fetch"). The
// driver strips both, so every object comes through it. A v0/v1
// advertisement (every ref, possibly large) streams through untouched.

const (
	// What the driver reads of an advertisement to tell v2 from v0 and to
	// filter a v2 capability list (a few hundred bytes in practice); a v0
	// first ref line alone may be a full packet.
	maxAdvertisementHead = 256 * 1024
	serviceLine          = "# service=git-upload-pack\n"
	versionTwoLine       = "version 2\n"
)

// filterAdvertisement copies an upload-pack advertisement from src to dst
// with the v2 capabilities stripped.
func filterAdvertisement(dst io.Writer, src *bufio.Reader) error {
	p := newPktReader(src, maxAdvertisementHead)
	payload, flush, err := p.read()
	if err != nil {
		return err
	}
	if !flush && string(payload) == serviceLine {
		// The smart HTTP preamble: the service line and a flush.
		if _, flush, err = p.read(); err != nil {
			return err
		}
		if !flush {
			return fmt.Errorf("%w: no flush after the service line", errMalformed)
		}
		if payload, flush, err = p.read(); err != nil {
			if errors.Is(err, errStreamEnded) {
				// Nothing after the preamble: pass on what came.
				_, werr := dst.Write(p.out.Bytes())
				return werr
			}
			return err
		}
	}
	if flush || string(payload) != versionTwoLine {
		// v0 or v1: what was read, then the rest as it comes.
		if _, err := dst.Write(p.out.Bytes()); err != nil {
			return err
		}
		_, err := io.Copy(dst, src)
		return err
	}
	// Everything up to the version line stays as read; then the capability
	// lines, filtered, up to the flush.
	kept := p.out.Bytes()
	out := append([]byte(nil), kept...)
	for {
		payload, flush, err := p.read()
		if err != nil {
			return err
		}
		if flush {
			out = append(out, flushPkt...)
			break
		}
		if line, keep := filterCapability(string(payload)); keep {
			out = append(out, pktLine(line)...)
		}
	}
	if _, err := dst.Write(out); err != nil {
		return err
	}
	_, err = io.Copy(dst, src)
	return err
}

// filterCapability is one v2 capability line as the driver advertises it.
func filterCapability(line string) (string, bool) {
	text := strings.TrimSuffix(line, "\n")
	newline := line[len(text):]
	name, value, hasValue := strings.Cut(text, "=")
	switch name {
	case "bundle-uri", "packfile-uris":
		return "", false
	case "fetch":
		if !hasValue {
			return line, true
		}
		var features []string
		for _, feature := range strings.Fields(value) {
			if feature == "packfile-uris" || strings.HasPrefix(feature, "packfile-uris=") {
				continue
			}
			features = append(features, feature)
		}
		if len(features) == 0 {
			return "fetch" + newline, true
		}
		return "fetch=" + strings.Join(features, " ") + newline, true
	}
	return line, true
}
