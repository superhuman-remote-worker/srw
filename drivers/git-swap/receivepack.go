package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strings"
)

// A push's request head (gitprotocol-pack(5), "Reference Update Request"):
//
//	update-requests = *shallow ( command-list | push-cert )
//	command-list    = PKT-LINE(command NUL capability-list) *PKT-LINE(command) flush-pkt
//	push-cert       = PKT-LINE("push-cert" NUL capability-list LF) ... PKT-LINE("push-cert-end" LF)
//
// then, when push-options was negotiated, option lines up to a flush, and
// then the pack, which the driver streams through untouched. The driver
// parses the head as git's receive-pack does (receive-pack.c read_head_info
// and queue_commands_from_cert) and refuses anything git would read
// differently: a NUL outside the first line, a line after push-cert-end, a
// malformed command anywhere a command may be.

// command is one reference update.
type command struct {
	old, new, ref string
}

func (c command) delete() bool { return allZero(c.new) }

func allZero(id string) bool { return strings.Trim(id, "0") == "" }

// pushHead is a parsed head: the commands, the capabilities the client
// asked for and the head's canonical bytes (what is forwarded).
type pushHead struct {
	commands []command
	caps     map[string]bool
	encoded  []byte
}

// The lines a push certificate's signature starts with (gpg-interface.c):
// the commands are what lies between the certificate's first blank line
// and the last such line.
var signatureStarts = []string{
	"-----BEGIN PGP SIGNATURE-----",
	"-----BEGIN PGP MESSAGE-----",
	"-----BEGIN SIGNED MESSAGE-----",
	"-----BEGIN SSH SIGNATURE-----",
}

// parsePushHead reads the head of a receive-pack request from r; what
// follows it in r is the pack.
func parsePushHead(r *bufio.Reader) (*pushHead, error) {
	p := newPktReader(r, maxPushHead)
	head := &pushHead{caps: map[string]bool{}}
	idLength := 0
	started := false
	for {
		payload, flush, err := p.read()
		if err != nil {
			return nil, err
		}
		if flush {
			break
		}
		line := strings.TrimSuffix(string(payload), "\n")
		if !started && strings.HasPrefix(line, "shallow ") {
			// receive-pack reads "shallow " lines only before the commands.
			if !objectID(line[len("shallow "):]) {
				return nil, fmt.Errorf("%w: a shallow line names no object", errMalformed)
			}
			continue
		}
		if strings.IndexByte(line, 0) >= 0 {
			if started {
				return nil, fmt.Errorf("%w: capabilities after the first command", errMalformed)
			}
			text, capabilities, _ := strings.Cut(line, "\x00")
			if strings.IndexByte(capabilities, 0) >= 0 {
				return nil, fmt.Errorf("%w: two capability lists", errMalformed)
			}
			for _, capability := range strings.Fields(capabilities) {
				head.caps[capability] = true
			}
			idLength = 40
			if head.caps["object-format=sha256"] {
				idLength = 64
			}
			line = text
			if line == "push-cert" {
				commands, err := readPushCert(p, idLength)
				if err != nil {
					return nil, err
				}
				head.commands = commands
				started = true
				break
			}
		} else if !started {
			idLength = 40
		}
		started = true
		parsed, err := parseCommand(line, idLength)
		if err != nil {
			return nil, err
		}
		head.commands = append(head.commands, parsed)
		if len(head.commands) > maxCommands {
			return nil, errTooLarge
		}
	}
	if head.caps["push-options"] {
		for {
			payload, flush, err := p.read()
			if err != nil {
				return nil, err
			}
			if flush {
				break
			}
			if bytes.IndexByte(payload, 0) >= 0 {
				return nil, fmt.Errorf("%w: a NUL in a push option", errMalformed)
			}
		}
	}
	head.encoded = p.out.Bytes()
	return head, nil
}

// readPushCert reads a push certificate after its first line, up to
// "push-cert-end\n", which must be followed by the head's flush, and
// returns the commands it certifies.
func readPushCert(p *pktReader, idLength int) ([]command, error) {
	var cert strings.Builder
	for {
		payload, flush, err := p.read()
		if err != nil {
			return nil, err
		}
		if flush {
			// git accepts a certificate a flush ends; the head ends with it.
			return certCommands(cert.String(), idLength)
		}
		if bytes.IndexByte(payload, 0) >= 0 {
			// git keeps a certificate line up to its first NUL.
			return nil, fmt.Errorf("%w: a NUL in a push certificate", errMalformed)
		}
		if string(payload) == "push-cert-end\n" {
			break
		}
		cert.Write(payload)
		if cert.Len() > maxPushHead {
			return nil, errTooLarge
		}
	}
	commands, err := certCommands(cert.String(), idLength)
	if err != nil {
		return nil, err
	}
	// git reads on after push-cert-end as if it were a new head; git's own
	// clients send the flush at once, so anything else is refused.
	_, flush, err := p.read()
	if err != nil {
		return nil, err
	}
	if !flush {
		return nil, fmt.Errorf("%w: lines after push-cert-end", errMalformed)
	}
	return commands, nil
}

func certCommands(cert string, idLength int) ([]command, error) {
	start := strings.Index(cert, "\n\n")
	if start < 0 || start+2 >= len(cert) {
		return nil, fmt.Errorf("%w: a push certificate without commands", errMalformed)
	}
	start += 2
	end := len(cert)
	for offset := 0; offset < len(cert); {
		for _, prefix := range signatureStarts {
			if strings.HasPrefix(cert[offset:], prefix) {
				end = offset
			}
		}
		next := strings.IndexByte(cert[offset:], '\n')
		if next < 0 {
			break
		}
		offset += next + 1
	}
	if end < start {
		return nil, fmt.Errorf("%w: a push certificate signs before its commands", errMalformed)
	}
	var commands []command
	region := cert[start:end]
	for len(region) > 0 {
		line, rest, _ := strings.Cut(region, "\n")
		parsed, err := parseCommand(line, idLength)
		if err != nil {
			return nil, err
		}
		commands = append(commands, parsed)
		if len(commands) > maxCommands {
			return nil, errTooLarge
		}
		region = rest
	}
	return commands, nil
}

func parseCommand(line string, idLength int) (command, error) {
	if len(line) < 2*idLength+3 || line[idLength] != ' ' || line[2*idLength+1] != ' ' {
		return command{}, fmt.Errorf("%w: a command is not 'old new ref'", errMalformed)
	}
	parsed := command{old: line[:idLength], new: line[idLength+1 : 2*idLength+1], ref: line[2*idLength+2:]}
	if !hexOfLength(parsed.old, idLength) || !hexOfLength(parsed.new, idLength) {
		return command{}, fmt.Errorf("%w: a command's object ids are not hex", errMalformed)
	}
	if parsed.ref == "" {
		return command{}, fmt.Errorf("%w: a command names no ref", errMalformed)
	}
	return parsed, nil
}

func objectID(text string) bool {
	return hexOfLength(text, 40) || hexOfLength(text, 64)
}

func hexOfLength(text string, length int) bool {
	if len(text) != length {
		return false
	}
	for i := 0; i < len(text); i++ {
		if _, ok := hexValue(text[i]); !ok {
			return false
		}
	}
	return true
}

// refusal is why the driver refuses one command, or "".
func refusal(c command) string {
	switch {
	case c.delete():
		return "deleting a ref is not allowed through SRW's git swap driver"
	case !strings.HasPrefix(c.ref, "refs/heads/"):
		return "only branches (refs/heads/*) may be pushed through SRW's git swap driver"
	case !validRefName(c.ref):
		return "the ref name is not valid"
	}
	return ""
}

// validRefName applies git's check_refname_format rules a ref under
// refs/heads/ must pass; the upstream checks again.
func validRefName(ref string) bool {
	if strings.HasSuffix(ref, "/") || strings.HasSuffix(ref, ".") || strings.Contains(ref, "..") ||
		strings.Contains(ref, "@{") || strings.Contains(ref, "//") || ref == "@" {
		return false
	}
	for _, r := range ref {
		if r < 0x20 || r == 0x7f || strings.ContainsRune(" ~^:?*[\\", r) {
			return false
		}
	}
	for _, component := range strings.Split(ref, "/") {
		if component == "" || strings.HasPrefix(component, ".") || strings.HasSuffix(component, ".lock") {
			return false
		}
	}
	return true
}

// writeReport answers a refused push the way receive-pack reports one, so
// git shows each ref's reason: "unpack ok", then "ng <ref> <reason>" for
// every command (the refused ones with their reason, the others with the
// refusal they share), then a flush, inside side-band 1 when the client
// asked for side-band. It returns false when the client asked for no
// report at all (the caller answers with an HTTP error instead).
func writeReport(w io.Writer, head *pushHead, reasons []string) (bool, error) {
	if !head.caps["report-status"] && !head.caps["report-status-v2"] {
		return false, nil
	}
	var report bytes.Buffer
	report.Write(pktLine("unpack ok\n"))
	for i, c := range head.commands {
		reason := reasons[i]
		if reason == "" {
			reason = "not pushed: another ref in this push was refused"
		}
		report.Write(pktLine("ng " + c.ref + " " + reason + "\n"))
	}
	report.WriteString(flushPkt)
	band := 0
	switch {
	case head.caps["side-band-64k"]:
		band = maxPktLen - 5
	case head.caps["side-band"]:
		band = 1000 - 5
	}
	if band == 0 {
		_, err := w.Write(report.Bytes())
		return true, err
	}
	data := report.Bytes()
	var out bytes.Buffer
	for len(data) > 0 {
		n := min(len(data), band)
		fmt.Fprintf(&out, "%04x\x01", n+5)
		out.Write(data[:n])
		data = data[n:]
	}
	out.WriteString(flushPkt)
	_, err := w.Write(out.Bytes())
	return true, err
}
