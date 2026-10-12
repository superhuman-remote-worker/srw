package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
)

// Git's pkt-line framing (gitprotocol-common(5)): four hex digits of length
// (the four included), then the payload; "0000" is a flush. The driver
// reads only what it must check (a push's command list, a v2 capability
// advertisement), under a byte budget, and writes back exactly what it read
// with a canonical (lowercase) length, so the upstream parses the bytes the
// driver checked.

const (
	// The longest pkt-line git writes (LARGE_PACKET_MAX).
	maxPktLen = 65520
	// What the driver keeps of one request head: a push's commands, its
	// push certificate and its push options.
	maxPushHead = 1 << 20
	maxCommands = 10000
)

var (
	errMalformed = errors.New("malformed pkt-line stream")
	// errStreamEnded: the stream ended between two packets.
	errStreamEnded = fmt.Errorf("%w: the stream ended", errMalformed)
)

// pktReader reads pkt-lines from a buffered stream, at most budget bytes,
// and keeps their canonical encoding.
type pktReader struct {
	r      *bufio.Reader
	budget int
	out    bytes.Buffer
}

func newPktReader(r *bufio.Reader, budget int) *pktReader {
	return &pktReader{r: r, budget: budget}
}

// read returns the next payload, or flush when it was "0000". A delimiter
// or response-end packet (0001, 0002) and lengths 0003 or over the maximum
// are malformed in what the driver parses.
func (p *pktReader) read() (payload []byte, flush bool, err error) {
	var head [4]byte
	if _, err := io.ReadFull(p.r, head[:]); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, false, errStreamEnded
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, false, fmt.Errorf("%w: the stream ended inside a length", errMalformed)
		}
		return nil, false, err
	}
	n := 0
	for _, c := range head {
		v, ok := hexValue(c)
		if !ok {
			return nil, false, fmt.Errorf("%w: a length is not hex", errMalformed)
		}
		n = n<<4 | v
	}
	if n == 0 {
		p.budget -= 4
		if p.budget < 0 {
			return nil, false, errTooLarge
		}
		p.out.WriteString("0000")
		return nil, true, nil
	}
	if n < 4 || n > maxPktLen {
		return nil, false, fmt.Errorf("%w: a packet length of %d", errMalformed, n)
	}
	p.budget -= n
	if p.budget < 0 {
		return nil, false, errTooLarge
	}
	payload = make([]byte, n-4)
	if _, err := io.ReadFull(p.r, payload); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, false, fmt.Errorf("%w: the stream ended inside a packet", errMalformed)
		}
		return nil, false, err
	}
	fmt.Fprintf(&p.out, "%04x", n)
	p.out.Write(payload)
	return payload, false, nil
}

var errTooLarge = errors.New("the request head is larger than the driver reads")

func hexValue(c byte) (int, bool) {
	switch {
	case c >= '0' && c <= '9':
		return int(c - '0'), true
	case c >= 'a' && c <= 'f':
		return int(c-'a') + 10, true
	case c >= 'A' && c <= 'F':
		return int(c-'A') + 10, true
	}
	return 0, false
}

// pktLine encodes one payload.
func pktLine(payload string) []byte {
	return []byte(fmt.Sprintf("%04x%s", len(payload)+4, payload))
}

const flushPkt = "0000"
