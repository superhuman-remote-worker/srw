package main

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func wsServer(t *testing.T, bad bool, fn func(*testSocket)) (string, func()) {
	t.Helper()
	server := newTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sock, err := upgrade(w, r, bad)
		if err != nil {
			return
		}
		defer sock.conn.Close()
		fn(sock)
	}))
	return "ws" + strings.TrimPrefix(server.URL, "http"), server.Close
}
func TestWebsocketHandshake(t *testing.T) {
	for _, bad := range []bool{false, true} {
		endpoint, stop := wsServer(t, bad, func(s *testSocket) { _, _, _, _ = s.read() })
		w, err := testDialWebsocket(context.Background(), endpoint, http.Header{})
		if bad && err == nil || !bad && err != nil {
			t.Fatal(err)
		}
		if w != nil {
			w.close()
		}
		stop()
	}
	server := newTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(409) }))
	defer server.Close()
	_, err := testDialWebsocket(context.Background(), "ws"+strings.TrimPrefix(server.URL, "http"), http.Header{})
	var he *wsHTTPError
	if !errors.As(err, &he) || he.status != 409 {
		t.Fatal(err)
	}
	if _, err := testDialWebsocket(context.Background(), "http://example", http.Header{}); err == nil {
		t.Fatal("invalid URL")
	}
}
func TestWebsocketMaskingLengths(t *testing.T) {
	for _, size := range []int{2, 126, 65536} {
		t.Run(string(rune(size%90+33)), func(t *testing.T) {
			payload := strings.Repeat("x", size)
			result := make(chan bool, 1)
			endpoint, stop := wsServer(t, false, func(s *testSocket) {
				op, p, mask, err := s.read()
				result <- err == nil && op == 1 && mask && string(p) == payload
			})
			defer stop()
			w, err := testDialWebsocket(context.Background(), endpoint, http.Header{})
			if err != nil {
				t.Fatal(err)
			}
			defer w.close()
			if err = w.write(1, []byte(payload)); err != nil {
				t.Fatal(err)
			}
			if !<-result {
				t.Fatal("bad masked text frame")
			}
		})
	}
}
func TestWebsocketFragmentPingClose(t *testing.T) {
	result := make(chan bool, 1)
	endpoint, stop := wsServer(t, false, func(s *testSocket) {
		_ = s.sendFrame(1, false, []byte("hel"))
		_ = s.sendFrame(9, true, []byte("ping"))
		op, p, masked, err := s.read()
		result <- err == nil && op == 10 && masked && string(p) == "ping"
		_ = s.sendFrame(0, true, []byte("lo"))
		_ = s.sendFrame(8, true, []byte{3, 232})
		op, _, masked, err = s.read()
		result <- err == nil && op == 8 && masked
	})
	defer stop()
	w, err := testDialWebsocket(context.Background(), endpoint, http.Header{})
	if err != nil {
		t.Fatal(err)
	}
	defer w.close()
	p, err := w.readText()
	if err != nil || string(p) != "hello" || !<-result {
		t.Fatal(string(p), err)
	}
	if _, err = w.readText(); err != io.EOF {
		t.Fatal(err)
	}
	if !<-result {
		t.Fatal("close not echoed")
	}
}
func TestWebsocketMalformedAndCap(t *testing.T) {
	capHeader := []byte{0x81, 127}
	var n [8]byte
	binary.BigEndian.PutUint64(n[:], maxMessage+1)
	capHeader = append(capHeader, n[:]...)
	cases := [][]byte{capHeader, serverFrame(0, true, []byte("orphan")), serverFrame(1, true, []byte{0xff}), serverFrame(8, true, []byte{1}), serverFrame(9, false, []byte("ping")), {0xc1, 0}, {0x81, 0x80}, {0x81, 126, 0, 1}, serverFrame(8, true, []byte{3, 237}), serverFrame(2, true, []byte("binary"))}
	for i, frame := range cases {
		t.Run(strings.Repeat("x", i+1), func(t *testing.T) {
			endpoint, stop := wsServer(t, false, func(s *testSocket) { _, _ = s.conn.Write(frame) })
			defer stop()
			w, err := testDialWebsocket(context.Background(), endpoint, http.Header{})
			if err != nil {
				t.Fatal(err)
			}
			defer w.close()
			if _, err = w.readText(); err == nil {
				t.Fatal("invalid frame accepted")
			}
			if i == 0 && !errors.Is(err, errWSCap) {
				t.Fatal(err)
			}
		})
	}
	endpoint, stop := wsServer(t, false, func(s *testSocket) {
		_ = s.sendFrame(1, false, []byte(strings.Repeat("x", maxMessage)))
		_ = s.sendFrame(0, true, []byte("x"))
	})
	defer stop()
	w, err := testDialWebsocket(context.Background(), endpoint, http.Header{})
	if err != nil {
		t.Fatal(err)
	}
	defer w.close()
	if _, err = w.readText(); !errors.Is(err, errWSCap) {
		t.Fatal(err)
	}
}
func TestWebsocketCanceledHandshake(t *testing.T) {
	endpoint, stop := wsServer(t, false, func(s *testSocket) {})
	defer stop()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	begin := time.Now()
	_, err := testDialWebsocket(ctx, endpoint, http.Header{})
	if err == nil || time.Since(begin) > time.Second {
		t.Fatal(err)
	}
}
