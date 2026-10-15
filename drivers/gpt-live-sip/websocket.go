package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha1"
	"crypto/tls"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

const maxMessage = 8 * 1024 * 1024

var errWSProtocol = errors.New("invalid WebSocket frame")
var errWSCap = errors.New("WebSocket message exceeds 8 MiB")

type wsHTTPError struct{ status int }

func (e *wsHTTPError) Error() string { return "WebSocket attach refused" }

type websocket struct {
	conn      net.Conn
	reader    *bufio.Reader
	writeMu   sync.Mutex
	closeOnce sync.Once
}

func headerToken(h, token string) bool {
	for _, v := range strings.Split(h, ",") {
		if strings.EqualFold(strings.TrimSpace(v), token) {
			return true
		}
	}
	return false
}

type wsDialer func(context.Context, string, string) (net.Conn, error)

func dialWebsocket(ctx context.Context, endpoint string, headers http.Header) (*websocket, error) {
	return dialWebsocketWithDial(ctx, endpoint, headers, (&net.Dialer{}).DialContext)
}
func dialWebsocketWithDial(ctx context.Context, endpoint string, headers http.Header, dial wsDialer) (*websocket, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	u, err := url.Parse(endpoint)
	if err != nil || (u.Scheme != "ws" && u.Scheme != "wss") || u.Hostname() == "" || u.User != nil {
		return nil, errors.New("invalid WebSocket URL")
	}
	port := u.Port()
	if port == "" {
		port = "80"
		if u.Scheme == "wss" {
			port = "443"
		}
	}
	conn, err := dial(ctx, "tcp", net.JoinHostPort(u.Hostname(), port))
	if err != nil {
		return nil, errors.New("WebSocket connection failed")
	}
	ok := false
	defer func() {
		if !ok {
			conn.Close()
		}
	}()
	baseConn := conn
	stop := context.AfterFunc(ctx, func() { baseConn.Close() })
	defer stop()
	deadline, _ := ctx.Deadline()
	_ = conn.SetDeadline(deadline)
	if u.Scheme == "wss" {
		tc := tls.Client(conn, &tls.Config{ServerName: u.Hostname(), MinVersion: tls.VersionTLS12, NextProtos: []string{"http/1.1"}})
		if tc.HandshakeContext(ctx) != nil {
			return nil, errors.New("WebSocket TLS failed")
		}
		conn = tc
	}
	nonce := make([]byte, 16)
	if _, err = rand.Read(nonce); err != nil {
		return nil, err
	}
	key := base64.StdEncoding.EncodeToString(nonce)
	hu := *u
	hu.Scheme = "http"
	if u.Scheme == "wss" {
		hu.Scheme = "https"
	}
	req, err := http.NewRequestWithContext(ctx, "GET", hu.String(), nil)
	if err != nil {
		return nil, errors.New("invalid WebSocket request")
	}
	req.Header = headers.Clone()
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", "websocket")
	req.Header.Set("Sec-WebSocket-Version", "13")
	req.Header.Set("Sec-WebSocket-Key", key)
	if req.Write(conn) != nil {
		return nil, errors.New("WebSocket handshake failed")
	}
	reader := bufio.NewReaderSize(conn, 32*1024)
	resp, err := http.ReadResponse(reader, req)
	if err != nil {
		return nil, errors.New("WebSocket handshake failed")
	}
	if resp.StatusCode != 101 {
		resp.Body.Close()
		return nil, &wsHTTPError{resp.StatusCode}
	}
	digest := sha1.Sum([]byte(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
	if !headerToken(resp.Header.Get("Connection"), "upgrade") || !strings.EqualFold(resp.Header.Get("Upgrade"), "websocket") || resp.Header.Get("Sec-WebSocket-Accept") != base64.StdEncoding.EncodeToString(digest[:]) || resp.Header.Get("Sec-WebSocket-Extensions") != "" || resp.Header.Get("Sec-WebSocket-Protocol") != "" {
		return nil, errors.New("invalid WebSocket handshake")
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	_ = conn.SetDeadline(time.Time{})
	ok = true
	return &websocket{conn: conn, reader: reader}, nil
}
func (w *websocket) write(op byte, payload []byte) error {
	if len(payload) > maxMessage || (op >= 8 && len(payload) > 125) {
		return errWSCap
	}
	w.writeMu.Lock()
	defer w.writeMu.Unlock()
	return w.writeLocked(op, payload, 5*time.Second)
}
func (w *websocket) writeLocked(op byte, payload []byte, timeout time.Duration) error {
	_ = w.conn.SetWriteDeadline(time.Now().Add(timeout))
	h := []byte{0x80 | op, 0x80}
	n := len(payload)
	switch {
	case n < 126:
		h[1] |= byte(n)
	case n <= 65535:
		h[1] |= 126
		h = append(h, byte(n>>8), byte(n))
	default:
		h[1] |= 127
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], uint64(n))
		h = append(h, b[:]...)
	}
	var mask [4]byte
	if _, err := rand.Read(mask[:]); err != nil {
		return err
	}
	h = append(h, mask[:]...)
	buf := make([]byte, len(h)+n)
	copy(buf, h)
	for i, v := range payload {
		buf[len(h)+i] = v ^ mask[i%4]
	}
	for len(buf) > 0 {
		n, err := w.conn.Write(buf)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		buf = buf[n:]
	}
	return nil
}
func validClose(payload []byte) bool {
	if len(payload) == 0 {
		return true
	}
	if len(payload) == 1 {
		return false
	}
	code := binary.BigEndian.Uint16(payload)
	return (code >= 3000 && code <= 4999 || code >= 1000 && code <= 1014 && code != 1004 && code != 1005 && code != 1006) && utf8.Valid(payload[2:])
}

// readText permits interleaved control frames in a fragmented text message.
// Server masking, binary messages, extensions and noncanonical lengths are refused.
func (w *websocket) readText() (out []byte, err error) {
	defer func() {
		if err != nil {
			w.conn.Close()
		}
	}()
	fragmented := false
	for {
		_ = w.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		var h [2]byte
		if _, err = io.ReadFull(w.reader, h[:]); err != nil {
			return nil, err
		}
		fin := h[0]&0x80 != 0
		op := h[0] & 15
		n := uint64(h[1] & 127)
		if h[0]&0x70 != 0 || h[1]&0x80 != 0 {
			return nil, errWSProtocol
		}
		switch n {
		case 126:
			var b [2]byte
			if _, err = io.ReadFull(w.reader, b[:]); err != nil {
				return nil, err
			}
			n = uint64(binary.BigEndian.Uint16(b[:]))
			if n < 126 {
				return nil, errWSProtocol
			}
		case 127:
			var b [8]byte
			if _, err = io.ReadFull(w.reader, b[:]); err != nil {
				return nil, err
			}
			n = binary.BigEndian.Uint64(b[:])
			if n < 65536 || n>>63 != 0 {
				return nil, errWSProtocol
			}
		}
		if op >= 8 {
			if !fin || n > 125 {
				return nil, errWSProtocol
			}
		} else {
			if n > maxMessage || uint64(len(out))+n > maxMessage {
				return nil, errWSCap
			}
		}
		if op != 0 && op != 1 && op != 8 && op != 9 && op != 10 {
			return nil, errWSProtocol
		}
		if op == 0 && !fragmented || op == 1 && fragmented {
			return nil, errWSProtocol
		}
		p := make([]byte, int(n))
		if _, err = io.ReadFull(w.reader, p); err != nil {
			return nil, err
		}
		switch op {
		case 8:
			if !validClose(p) {
				return nil, errWSProtocol
			}
			_ = w.write(8, p)
			return nil, io.EOF
		case 9:
			if err = w.write(10, p); err != nil {
				return nil, err
			}
			continue
		case 10:
			continue
		case 1:
			fragmented = !fin
		}
		out = append(out, p...)
		if fin {
			if !utf8.Valid(out) {
				return nil, errWSProtocol
			}
			return out, nil
		}
	}
}
func (w *websocket) close() {
	w.closeOnce.Do(func() {
		if w.writeMu.TryLock() {
			_ = w.writeLocked(8, []byte{3, 232}, 100*time.Millisecond)
			w.writeMu.Unlock()
		}
		w.conn.Close()
	})
}
