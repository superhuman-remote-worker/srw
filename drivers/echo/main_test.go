package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const identity = "sdi_" + "0123456789ABCDEFGHIJabcdefghij0123456789ABCDEFGHI"

func setup(t *testing.T, exchangeURL string) *server {
	t.Helper()
	dir := t.TempDir()
	request := map[string]any{
		"protocol_version": "1.0",
		"plane":            "service",
		"driver":           "srw.echo-service/v1",
		"connector":        map[string]any{"id": "c1", "config": map[string]any{"host": "one.one.one.one", "port": 443}},
		"credentials":      map[string]any{"secret": "never-shown"},
		"exchange":         map[string]any{"url": exchangeURL},
	}
	raw, _ := json.Marshal(request)
	os.WriteFile(filepath.Join(dir, "request.json"), raw, 0o444)
	os.WriteFile(filepath.Join(dir, "identity"), []byte(identity), 0o444)
	env := map[string]string{
		"SRW_REQUEST_FILE":         filepath.Join(dir, "request.json"),
		"SRW_DRIVER_IDENTITY_FILE": filepath.Join(dir, "identity"),
	}
	s, err := loadServer(func(name string) string { return env[name] })
	if err != nil {
		t.Fatal(err)
	}
	s.dialTimeout = time.Second
	return s
}

func get(t *testing.T, handler http.Handler, method, target, body string) (int, map[string]any, string) {
	t.Helper()
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(method, target, strings.NewReader(body)))
	raw, _ := io.ReadAll(recorder.Body)
	var out map[string]any
	json.Unmarshal(raw, &out)
	return recorder.Code, out, string(raw)
}

func TestTheRootAnswersTheRequestWithoutSecrets(t *testing.T) {
	s := setup(t, "http://exchange.invalid")
	code, body, raw := get(t, s.routes(), http.MethodGet, "/", "")
	if code != 200 || body["driver"] != "srw.echo-service/v1" || body["has_identity"] != true {
		t.Fatalf("%d %v", code, body)
	}
	if strings.Contains(raw, "never-shown") || strings.Contains(raw, identity) {
		t.Fatalf("a secret leaked: %s", raw)
	}
	if keys := body["credential_keys"].([]any); len(keys) != 1 || keys[0] != "secret" {
		t.Fatalf("keys %v", keys)
	}
}

func TestTheExchangeIsCalledWithTheIdentityAndTheCredentialIsHashed(t *testing.T) {
	var auth string
	var sent map[string]string
	exchange := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth = r.Header.Get("Authorization")
		json.NewDecoder(r.Body).Decode(&sent)
		if r.URL.Path != "/v1/leases/exchange" {
			w.WriteHeader(404)
			return
		}
		w.Header().Set("Cache-Control", "no-store")
		json.NewEncoder(w).Encode(map[string]any{"credential": "upstream", "access": "ReadWrite", "max_cache_seconds": 30})
	}))
	defer exchange.Close()
	s := setup(t, exchange.URL)
	code, body, raw := get(t, s.routes(), http.MethodPost, "/exchange", `{"lease_token":"scl_x","operation":"write"}`)
	if code != 200 || body["exchange_status"].(float64) != 200 || body["cache_control"] != "no-store" {
		t.Fatalf("%d %v", code, body)
	}
	digest := sha256.Sum256([]byte("upstream"))
	if body["credential_sha256"] != hex.EncodeToString(digest[:]) || strings.Contains(raw, "\"upstream\"") {
		t.Fatalf("credential not hashed: %s", raw)
	}
	if auth != "Bearer "+identity || sent["lease_token"] != "scl_x" || sent["operation"] != "write" {
		t.Fatalf("auth %q sent %v", auth, sent)
	}
}

func TestAnExchangeRefusalIsPassedThrough(t *testing.T) {
	exchange := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		json.NewEncoder(w).Encode(map[string]any{"error": "driver_identity_of_another_connector"})
	}))
	defer exchange.Close()
	s := setup(t, exchange.URL)
	_, body, _ := get(t, s.routes(), http.MethodPost, "/exchange", `{"lease_token":"scl_x"}`)
	if body["exchange_status"].(float64) != 403 || body["error"] != "driver_identity_of_another_connector" {
		t.Fatalf("%v", body)
	}
}

func TestProbeReportsReachability(t *testing.T) {
	listener, _ := net.Listen("tcp", "127.0.0.1:0")
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			conn.Close()
		}
	}()
	s := setup(t, "http://exchange.invalid")
	_, body, _ := get(t, s.routes(), http.MethodGet, "/probe?addr="+listener.Addr().String(), "")
	if body["reachable"] != true {
		t.Fatalf("%v", body)
	}
	addr := listener.Addr().String()
	listener.Close()
	_, body, _ = get(t, s.routes(), http.MethodGet, "/probe?addr="+addr, "")
	if body["reachable"] != false {
		t.Fatalf("%v", body)
	}
	code, _, _ := get(t, s.routes(), http.MethodGet, "/probe?addr=nohost", "")
	if code != 400 {
		t.Fatalf("code %d", code)
	}
}

func TestResolveReportsFailure(t *testing.T) {
	s := setup(t, "http://exchange.invalid")
	_, body, _ := get(t, s.routes(), http.MethodGet, "/resolve?name=does-not-exist.invalid", "")
	if body["resolved"] != false {
		t.Fatalf("%v", body)
	}
}

func TestSelfReportsTheProcessFacts(t *testing.T) {
	s := setup(t, "http://exchange.invalid")
	code, body, _ := get(t, s.routes(), http.MethodGet, "/self", "")
	if code != 200 || body["identity_file_readable"] != true {
		t.Fatalf("%d %v", code, body)
	}
	process := body["process"].(map[string]any)
	if _, ok := process["CapEff"]; !ok {
		t.Fatalf("no CapEff in %v", process)
	}
	facts := processFacts("Name:\techo\nUid:\t65532\t65532\t65532\t65532\nCapEff:\t0000000000000000\nNoNewPrivs:\t1\n")
	if facts["Uid"] != "65532 65532 65532 65532" || facts["CapEff"] != "0000000000000000" || facts["NoNewPrivs"] != "1" {
		t.Fatalf("facts %v", facts)
	}
	if _, ok := facts["Name"]; ok {
		t.Fatal("only the security facts are reported")
	}
}

func TestTheServerNeedsTheShim(t *testing.T) {
	if _, err := loadServer(func(string) string { return "" }); err == nil {
		t.Fatal("must refuse without the request and identity files")
	}
}
