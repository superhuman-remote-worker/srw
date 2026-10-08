package main

import (
	"encoding/json"
	"os"
	"testing"
)

// The vectors shared with shared/connectors/git_swap.py
// (tests/test_connector_git_swap_contract.py runs the same file).
func TestUpstreamVectors(t *testing.T) {
	raw, err := os.ReadFile("testdata/upstream_vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var vectors struct {
		Cases []struct {
			Input string `json:"input"`
			OK    bool   `json:"ok"`
			URL   string `json:"url"`
			Host  string `json:"host"`
			Path  string `json:"path"`
		} `json:"cases"`
	}
	if err := json.Unmarshal(raw, &vectors); err != nil {
		t.Fatal(err)
	}
	if len(vectors.Cases) < 20 {
		t.Fatalf("only %d vectors", len(vectors.Cases))
	}
	for _, c := range vectors.Cases {
		got, err := parseUpstream(c.Input)
		if !c.OK {
			if err == nil {
				t.Errorf("%q: served as %+v, want refused", c.Input, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("%q: refused (%v)", c.Input, err)
			continue
		}
		if got.url != c.URL || got.host != c.Host || got.path != c.Path {
			t.Errorf("%q: got %+v, want url=%q host=%q path=%q", c.Input, got, c.URL, c.Host, c.Path)
		}
	}
}

func TestConfigNeedsTheCleanUpstreamAndTLS(t *testing.T) {
	identity := "sdi_" + repeat("A", 49)
	valid := func() requestFile {
		var r requestFile
		r.ProtocolVersion = "1.0"
		r.Plane = "service"
		r.Driver = "srw.git-swap/v1"
		r.Connector.ID = testConnector
		r.Connector.Config.Upstream = "https://example.com/o/r.git"
		r.Connector.Config.Host = "example.com"
		r.Service.Port = 8443
		r.Exchange.URL = "http://srw-exchange.srw.svc:8088"
		r.TLS = &struct {
			CertFile string `json:"cert_file"`
			KeyFile  string `json:"key_file"`
		}{"/run/srw/tls.crt", "/run/srw/tls.key"}
		return r
	}
	cfg, err := parseConfig(valid(), identity)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.upstream.path != "o/r" || cfg.port != "8443" || cfg.connectorID != testConnector {
		t.Fatalf("config %+v", cfg)
	}
	broken := map[string]func(*requestFile){
		"protocol 2":       func(r *requestFile) { r.ProtocolVersion = "2.0" },
		"bind-time":        func(r *requestFile) { r.Plane = "bind_time" },
		"no connector":     func(r *requestFile) { r.Connector.ID = "x" },
		"plain http":       func(r *requestFile) { r.Connector.Config.Upstream = "http://example.com/o/r" },
		"not clean":        func(r *requestFile) { r.Connector.Config.Upstream = "https://Example.com/o/r.git/" },
		"another host":     func(r *requestFile) { r.Connector.Config.Host = "evil.example" },
		"no tls":           func(r *requestFile) { r.TLS = nil },
		"https exchange":   func(r *requestFile) { r.Exchange.URL = "https://x:8088" },
		"no service port":  func(r *requestFile) { r.Service.Port = 0 },
		"credentials":      func(r *requestFile) { r.Connector.Config.Upstream = "https://t@example.com/o/r.git" },
		"another port":     func(r *requestFile) { r.Connector.Config.Upstream = "https://example.com:444/o/r.git" },
		"query in upstrea": func(r *requestFile) { r.Connector.Config.Upstream = "https://example.com/o/r.git?x" },
	}
	for name, mutate := range broken {
		request := valid()
		mutate(&request)
		if _, err := parseConfig(request, identity); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if _, err := parseConfig(valid(), "sdi_short"); err == nil {
		t.Error("a malformed identity was accepted")
	}
}

func repeat(s string, n int) string {
	out := ""
	for i := 0; i < n; i++ {
		out += s
	}
	return out
}
