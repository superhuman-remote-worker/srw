package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
)

const (
	zero40 = "0000000000000000000000000000000000000000"
	oldID  = "1111111111111111111111111111111111111111"
	newID  = "2222222222222222222222222222222222222222"
	newID2 = "3333333333333333333333333333333333333333"
)

func pkt(payload string) string { return string(pktLine(payload)) }

func push(lines ...string) string {
	var b strings.Builder
	for _, line := range lines {
		if line == flushPkt {
			b.WriteString(flushPkt)
			continue
		}
		b.WriteString(pkt(line))
	}
	return b.String()
}

func parse(t *testing.T, body string) (*pushHead, string) {
	t.Helper()
	r := bufio.NewReader(strings.NewReader(body))
	head, err := parsePushHead(r)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	rest, _ := io.ReadAll(r)
	return head, string(rest)
}

func TestPushHeadCommandList(t *testing.T) {
	body := push(
		oldID+" "+newID+" refs/heads/main\x00report-status side-band-64k agent=git/2.43\n",
		zero40+" "+newID2+" refs/heads/feature\n",
		flushPkt,
	) + "PACK....pack bytes"
	head, rest := parse(t, body)
	if len(head.commands) != 2 || head.commands[0].ref != "refs/heads/main" || head.commands[1].old != zero40 {
		t.Fatalf("commands %+v", head.commands)
	}
	if !head.caps["report-status"] || !head.caps["side-band-64k"] || head.caps["push-options"] {
		t.Fatalf("caps %v", head.caps)
	}
	if rest != "PACK....pack bytes" {
		t.Fatalf("the pack after the head is %q", rest)
	}
	if string(head.encoded)+rest != body {
		t.Fatal("the head is not forwarded byte for byte")
	}
}

func TestPushHeadShallowLinesComeFirst(t *testing.T) {
	head, _ := parse(t, push("shallow "+oldID+"\n", "shallow "+newID+"\n",
		oldID+" "+newID+" refs/heads/main\x00report-status\n", flushPkt))
	if len(head.commands) != 1 {
		t.Fatalf("commands %+v", head.commands)
	}
	// After a command, git would read "shallow <id>" as a shallow line and
	// the driver cannot tell what else it might be: refused.
	_, err := parsePushHead(bufio.NewReader(strings.NewReader(push(
		oldID+" "+newID+" refs/heads/main\x00report-status\n", "shallow "+newID+"\n", flushPkt))))
	if !errors.Is(err, errMalformed) {
		t.Fatalf("a shallow line after a command: %v", err)
	}
	_, err = parsePushHead(bufio.NewReader(strings.NewReader(push("shallow "+oldID+"garbage\n", flushPkt))))
	if !errors.Is(err, errMalformed) {
		t.Fatalf("a malformed shallow line: %v", err)
	}
}

func TestPushHeadCanonicalLengths(t *testing.T) {
	// A length with a hex letter, sent in upper case (git reads either):
	// forwarded in lower case, so the upstream reads what the driver read.
	ref := "refs/heads/m"
	line := ""
	for {
		line = oldID + " " + newID + " " + ref + "\x00report-status\n"
		if strings.ContainsAny(fmt.Sprintf("%04x", len(line)+4), "abcdef") {
			break
		}
		ref += "x"
	}
	upper := fmt.Sprintf("%04X", len(line)+4) + line
	head, _ := parse(t, upper+flushPkt)
	if !bytes.Equal(head.encoded, []byte(pkt(line)+flushPkt)) {
		t.Fatalf("encoded %q", head.encoded)
	}
	if len(head.encoded) != len(upper+flushPkt) {
		t.Fatal("canonical encoding changed the head's length")
	}
}

func TestPushHeadProbeAndEmptyHead(t *testing.T) {
	// git's large-push probe: a POST whose body is a flush.
	head, rest := parse(t, flushPkt)
	if len(head.commands) != 0 || rest != "" || string(head.encoded) != flushPkt {
		t.Fatalf("probe %+v %q", head, rest)
	}
}

func TestPushHeadPushOptions(t *testing.T) {
	head, rest := parse(t, push(
		oldID+" "+newID+" refs/heads/main\x00report-status push-options\n", flushPkt,
		"ci.skip\n", "merge_request.create\n", flushPkt)+"PACK")
	if len(head.commands) != 1 || rest != "PACK" {
		t.Fatalf("%+v %q", head.commands, rest)
	}
	_, err := parsePushHead(bufio.NewReader(strings.NewReader(push(
		oldID+" "+newID+" refs/heads/main\x00push-options\n", flushPkt, "a\x00b\n", flushPkt))))
	if !errors.Is(err, errMalformed) {
		t.Fatalf("a NUL in a push option: %v", err)
	}
}

func TestPushHeadSHA256(t *testing.T) {
	old64 := strings.Repeat("a", 64)
	new64 := strings.Repeat("b", 64)
	head, _ := parse(t, push(old64+" "+new64+" refs/heads/main\x00object-format=sha256 report-status\n", flushPkt))
	if head.commands[0].new != new64 {
		t.Fatalf("%+v", head.commands)
	}
	// SHA-1 ids under object-format=sha256 do not parse.
	_, err := parsePushHead(bufio.NewReader(strings.NewReader(push(
		oldID+" "+newID+" refs/heads/main\x00object-format=sha256\n", flushPkt))))
	if !errors.Is(err, errMalformed) {
		t.Fatalf("sha1 ids under sha256: %v", err)
	}
}

func cert(commands ...string) []string {
	lines := []string{
		"push-cert\x00report-status side-band-64k\n",
		"certificate version 0.1\n",
		"pusher Agent <agent@srw.local> 1700000000 +0000\n",
		"pushee https://example.com/o/r.git\n",
		"nonce 1700000000-abc\n",
		"\n",
	}
	for _, c := range commands {
		lines = append(lines, c+"\n")
	}
	return append(lines,
		"-----BEGIN PGP SIGNATURE-----\n",
		"iQEzBAABCAAdFiEE\n",
		"-----END PGP SIGNATURE-----\n",
		"push-cert-end\n",
	)
}

func TestPushHeadPushCertificate(t *testing.T) {
	lines := cert(oldID+" "+newID+" refs/heads/main", zero40+" "+newID2+" refs/tags/v1")
	head, rest := parse(t, push(append(lines, flushPkt)...)+"PACK")
	if len(head.commands) != 2 || head.commands[1].ref != "refs/tags/v1" || rest != "PACK" {
		t.Fatalf("%+v %q", head.commands, rest)
	}
	if !head.caps["report-status"] {
		t.Fatalf("caps %v", head.caps)
	}
	// A command after push-cert-end is one git would also run: refused.
	extra := append(lines, oldID+" "+newID+" refs/heads/other\n", flushPkt)
	if _, err := parsePushHead(bufio.NewReader(strings.NewReader(push(extra...)))); !errors.Is(err, errMalformed) {
		t.Fatalf("a command after the certificate: %v", err)
	}
	// Every line between the blank line and the last signature start is a
	// command to git: a stray one is refused, not skipped.
	stray := cert(oldID+" "+newID+" refs/heads/main", "-----BEGIN FOO-----")
	if _, err := parsePushHead(bufio.NewReader(strings.NewReader(push(append(stray, flushPkt)...)))); !errors.Is(err, errMalformed) {
		t.Fatalf("a malformed certified command: %v", err)
	}
	// With two signature starts, git's commands run to the last one.
	double := cert(oldID + " " + newID + " refs/heads/main")
	double = append(double[:len(double)-1], "-----BEGIN SSH SIGNATURE-----\n", "push-cert-end\n")
	if _, err := parsePushHead(bufio.NewReader(strings.NewReader(push(append(double, flushPkt)...)))); !errors.Is(err, errMalformed) {
		t.Fatalf("a signature read as commands: %v", err)
	}
	noBlank := []string{"push-cert\x00report-status\n", "certificate version 0.1\n", oldID + " " + newID + " refs/heads/main\n", "push-cert-end\n", flushPkt}
	if _, err := parsePushHead(bufio.NewReader(strings.NewReader(push(noBlank...)))); !errors.Is(err, errMalformed) {
		t.Fatalf("a certificate without its blank line: %v", err)
	}
}

func TestPushHeadMalformed(t *testing.T) {
	cases := map[string]string{
		"not hex length":       "zz12" + oldID,
		"short packet":         "0003",
		"delimiter":            "0001",
		"truncated":            pkt(oldID + " " + newID + " refs/heads/main\n")[:20],
		"no flush":             pkt(oldID + " " + newID + " refs/heads/main\x00report-status\n"),
		"capabilities twice":   push(oldID+" "+newID+" refs/heads/a\x00report-status\n", oldID+" "+newID+" refs/heads/b\x00x\n", flushPkt),
		"two NULs":             push(oldID+" "+newID+" refs/heads/a\x00x\x00y\n", flushPkt),
		"no ref":               push(oldID+" "+newID+" \x00report-status\n", flushPkt),
		"short id":             push(oldID[:39]+" "+newID+" refs/heads/a\n", flushPkt),
		"non-hex id":           push(strings.Repeat("g", 40)+" "+newID+" refs/heads/a\n", flushPkt),
		"push-cert never ends": push(cert(oldID + " " + newID + " refs/heads/main")[:4]...),
	}
	for name, body := range cases {
		if _, err := parsePushHead(bufio.NewReader(strings.NewReader(body))); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	huge := strings.Repeat(pkt(oldID+" "+newID+" refs/heads/"+strings.Repeat("x", 900)+"\n"), 1200)
	if _, err := parsePushHead(bufio.NewReader(strings.NewReader(huge))); !errors.Is(err, errTooLarge) {
		t.Fatalf("a head over the cap: %v", err)
	}
}

func TestRefusals(t *testing.T) {
	cases := map[string]string{
		oldID + " " + newID + " refs/heads/main":         "",
		zero40 + " " + newID + " refs/heads/new/branch":  "",
		oldID + " " + zero40 + " refs/heads/main":        "deleting a ref is not allowed",
		zero40 + " " + newID + " refs/tags/v1":           "only branches",
		oldID + " " + zero40 + " refs/tags/v1":           "deleting a ref",
		zero40 + " " + newID + " refs/notes/commits":     "only branches",
		zero40 + " " + newID + " refs/heads":             "only branches",
		zero40 + " " + newID + " refs/heads/":            "not valid",
		zero40 + " " + newID + " refs/heads/../tags/v1":  "not valid",
		zero40 + " " + newID + " refs/heads/a..b":        "not valid",
		zero40 + " " + newID + " refs/heads/.hidden":     "not valid",
		zero40 + " " + newID + " refs/heads/x.lock":      "not valid",
		zero40 + " " + newID + " refs/heads/a b":         "not valid",
		zero40 + " " + newID + " refs/heads/a@{1}":       "not valid",
		zero40 + " " + newID + " refs/heads/a//b":        "not valid",
		zero40 + " " + newID + " refs/heads/x\n":         "not valid",
		zero40 + " " + newID + " refs/heads/ok-name_1.2": "",
	}
	for line, want := range cases {
		parsed, err := parseCommand(line, 40)
		if err != nil {
			t.Fatalf("%q: %v", line, err)
		}
		got := refusal(parsed)
		if (want == "") != (got == "") || !strings.Contains(got, want) {
			t.Errorf("%q: refusal %q, want %q", line, got, want)
		}
	}
}

func TestReportIsWhatReceivePackSends(t *testing.T) {
	head := &pushHead{
		caps: map[string]bool{"report-status": true},
		commands: []command{
			{old: oldID, new: newID, ref: "refs/heads/main"},
			{old: zero40, new: newID, ref: "refs/tags/v1"},
		},
	}
	reasons := []string{"", "only branches (refs/heads/*) may be pushed through SRW's git swap driver"}
	var plain bytes.Buffer
	if wrote, err := writeReport(&plain, head, reasons); !wrote || err != nil {
		t.Fatal(wrote, err)
	}
	want := pkt("unpack ok\n") +
		pkt("ng refs/heads/main not pushed: another ref in this push was refused\n") +
		pkt("ng refs/tags/v1 only branches (refs/heads/*) may be pushed through SRW's git swap driver\n") +
		flushPkt
	if plain.String() != want {
		t.Fatalf("report %q", plain.String())
	}
	// In side-band 1, then a flush: the client demultiplexes it back.
	head.caps["side-band-64k"] = true
	var banded bytes.Buffer
	if _, err := writeReport(&banded, head, reasons); err != nil {
		t.Fatal(err)
	}
	r := newPktReader(bufio.NewReader(&banded), 1<<20)
	var inner bytes.Buffer
	for {
		payload, flush, err := r.read()
		if err != nil {
			t.Fatal(err)
		}
		if flush {
			break
		}
		if payload[0] != 1 {
			t.Fatalf("band %d", payload[0])
		}
		inner.Write(payload[1:])
	}
	if inner.String() != want {
		t.Fatalf("side-band report %q", inner.String())
	}
	// No report asked for: the caller answers with an HTTP refusal.
	if wrote, _ := writeReport(io.Discard, &pushHead{caps: map[string]bool{}, commands: head.commands}, reasons); wrote {
		t.Fatal("a report without report-status")
	}
}

func TestSmallSideBandSplitsTheReport(t *testing.T) {
	head := &pushHead{caps: map[string]bool{"report-status": true, "side-band": true}}
	reasons := []string{}
	for i := 0; i < 40; i++ {
		head.commands = append(head.commands, command{old: zero40, new: newID, ref: "refs/tags/" + strings.Repeat("t", 40)})
		reasons = append(reasons, "only branches")
	}
	var out bytes.Buffer
	if _, err := writeReport(&out, head, reasons); err != nil {
		t.Fatal(err)
	}
	r := newPktReader(bufio.NewReader(&out), 1<<20)
	packets := 0
	for {
		payload, flush, err := r.read()
		if err != nil {
			t.Fatal(err)
		}
		if flush {
			break
		}
		if len(payload)+4 > 1000 {
			t.Fatalf("a side-band packet of %d bytes", len(payload)+4)
		}
		packets++
	}
	if packets < 2 {
		t.Fatalf("%d packets", packets)
	}
}
