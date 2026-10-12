package main

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"net/url"
	"strings"
	"testing"
)

func advertise(t *testing.T, upstream string) string {
	t.Helper()
	var out bytes.Buffer
	if err := filterAdvertisement(&out, bufio.NewReader(strings.NewReader(upstream))); err != nil {
		t.Fatalf("filter: %v", err)
	}
	return out.String()
}

func TestV2CapabilitiesLoseBundleURIAndPackfileURIs(t *testing.T) {
	upstream := push(serviceLine, flushPkt,
		versionTwoLine,
		"agent=git/2.45.0\n",
		"ls-refs=unborn\n",
		"fetch=shallow wait-for-done packfile-uris filter\n",
		"server-option\n",
		"object-format=sha1\n",
		"object-info\n",
		"bundle-uri\n",
		flushPkt)
	want := push(serviceLine, flushPkt,
		versionTwoLine,
		"agent=git/2.45.0\n",
		"ls-refs=unborn\n",
		"fetch=shallow wait-for-done filter\n",
		"server-option\n",
		"object-format=sha1\n",
		"object-info\n",
		flushPkt)
	if got := advertise(t, upstream); got != want {
		t.Fatalf("got %q\nwant %q", got, want)
	}
}

func TestV2WithoutTheServicePreamble(t *testing.T) {
	// Some servers answer a v2 discovery without "# service=".
	upstream := push(versionTwoLine, "fetch=packfile-uris\n", "bundle-uri=uri\n", "packfile-uris\n", flushPkt)
	want := push(versionTwoLine, "fetch\n", flushPkt)
	if got := advertise(t, upstream); got != want {
		t.Fatalf("got %q", got)
	}
}

func TestV0AdvertisementStreamsThroughUntouched(t *testing.T) {
	upstream := push(serviceLine, flushPkt,
		oldID+" HEAD\x00multi_ack thin-pack side-band-64k bundle-uri packfile-uris\n",
		oldID+" refs/heads/main\n",
		flushPkt)
	if got := advertise(t, upstream); got != upstream {
		t.Fatalf("got %q", got)
	}
	// A large v0 advertisement is not held: only its first packets are read.
	big := upstream + strings.Repeat("x", 4<<20)
	if got := advertise(t, big); got != big {
		t.Fatal("a large advertisement changed")
	}
}

func TestMalformedAdvertisementsFail(t *testing.T) {
	for name, upstream := range map[string]string{
		"empty":            "",
		"no flush":         push(serviceLine, versionTwoLine),
		"unterminated v2":  push(versionTwoLine, "agent=x\n"),
		"garbage":          "HTTP/1.1 200 OK",
		"oversized v2 cap": push(versionTwoLine) + strings.Repeat(pkt(strings.Repeat("c", 60000)+"\n"), 6),
	} {
		var out bytes.Buffer
		if err := filterAdvertisement(&out, bufio.NewReader(strings.NewReader(upstream))); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestScrubberMasksTheCredentialAcrossChunks(t *testing.T) {
	credential := "ghp_" + strings.Repeat("s", 36)
	var out bytes.Buffer
	s := newScrubber(&out, credential)
	stream := "remote: token=" + credential + " and Basic " + basicValue(credential) + " done"
	// Every split point, one byte at a time.
	for i := 0; i < len(stream); i++ {
		if _, err := s.Write([]byte{stream[i]}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.finish(); err != nil {
		t.Fatal(err)
	}
	got := out.String()
	if strings.Contains(got, credential) || strings.Contains(got, basicValue(credential)) {
		t.Fatalf("leaked: %q", got)
	}
	if len(got) != len(stream) {
		t.Fatal("scrubbing changed the length (pkt-line framing would break)")
	}
	if !strings.HasPrefix(got, "remote: token=****") || !strings.HasSuffix(got, " done") {
		t.Fatalf("got %q", got)
	}
}

func TestScrubberHoldsOnlyAPossibleCredential(t *testing.T) {
	credential := "glpat-abcdefghijklmnop"
	var out bytes.Buffer
	s := newScrubber(&out, credential)
	s.Write([]byte("progress 10%\r"))
	if out.String() != "progress 10%\r" {
		t.Fatalf("held back a tail that begins no credential: %q", out.String())
	}
	s.Write([]byte("glpat-abc"))
	if out.String() != "progress 10%\r" {
		t.Fatalf("passed on the start of a credential: %q", out.String())
	}
	s.Write([]byte("!"))
	if out.String() != "progress 10%\rglpat-abc!" {
		t.Fatalf("got %q", out.String())
	}
	// "Z" begins the credential's base64: held until it cannot be one.
	s.Write([]byte("Z"))
	if out.String() != "progress 10%\rglpat-abc!" {
		t.Fatalf("passed on the start of an encoded credential: %q", out.String())
	}
	s.finish()
	if out.String() != "progress 10%\rglpat-abc!Z" {
		t.Fatalf("got %q", out.String())
	}
	var plain bytes.Buffer
	none := newScrubber(&plain, "")
	none.Write([]byte("anything"))
	if plain.String() != "anything" {
		t.Fatal("no credential, no scrubbing")
	}
}

func TestScrubText(t *testing.T) {
	if got := scrubText("dial https://oauth2:tok123456@host failed", "tok123456"); strings.Contains(got, "tok123456") {
		t.Fatalf("got %q", got)
	}
	credential := "glpat-Secret_Token-9"
	for _, form := range []string{
		hex.EncodeToString([]byte(credential)),
		strings.ToUpper(hex.EncodeToString([]byte(credential))),
		base64.StdEncoding.EncodeToString([]byte(credential)),
		url.QueryEscape(credential),
	} {
		if got := scrubText("remote: "+form+"\n", credential); strings.Contains(got, form) {
			t.Fatalf("an encoded credential passed: %q", got)
		}
	}
}
