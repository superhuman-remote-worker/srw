package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func validRequest() requestFile {
	r := requestFile{ProtocolVersion: "1.0", Plane: "service", Driver: driverName}
	r.Connector.ID = "connector"
	r.Exchange.URL = "http://exchange:8088"
	r.Service.Port = 8080
	r.Control.Token = strings.Repeat("c", 32)
	return r
}
func TestConfigRefusals(t *testing.T) {
	cases := []struct {
		name   string
		change func(*requestFile)
	}{
		{"protocol", func(r *requestFile) { r.ProtocolVersion = "2.0" }}, {"malformed protocol", func(r *requestFile) { r.ProtocolVersion = "1.foo" }}, {"plane", func(r *requestFile) { r.Plane = "job" }}, {"driver", func(r *requestFile) { r.Driver = "wrong" }}, {"connector", func(r *requestFile) { r.Connector.ID = "" }}, {"https exchange", func(r *requestFile) { r.Exchange.URL = "https://exchange" }}, {"empty exchange", func(r *requestFile) { r.Exchange.URL = "" }}, {"credentials URL", func(r *requestFile) { r.Exchange.URL = "http://secret@exchange" }}, {"short control", func(r *requestFile) { r.Control.Token = "secret" }}, {"empty control", func(r *requestFile) { r.Control.Token = "" }}, {"invalid control", func(r *requestFile) { r.Control.Token = strings.Repeat("!", 32) }}, {"port", func(r *requestFile) { r.Service.Port = 0 }}, {"limits", func(r *requestFile) { n := 2; r.Connector.Config.MaxDuration = &n }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := validRequest()
			tc.change(&r)
			_, err := parseConfig(r, "sdi_"+strings.Repeat("A", 49))
			if err == nil {
				t.Fatal("accepted invalid config")
			}
			if strings.Contains(err.Error(), "secret") {
				t.Fatal("secret in error")
			}
		})
	}
	for _, id := range []string{"", "sdi_wrong", "sdi_" + strings.Repeat("A", 48), "scl_" + strings.Repeat("A", 49)} {
		if _, err := parseConfig(validRequest(), id); err == nil {
			t.Fatal("bad identity accepted")
		}
	}
	r := validRequest()
	r.ProtocolVersion = "1.9"
	cfg, err := parseConfig(r, "sdi_"+strings.Repeat("A", 49))
	if err != nil || cfg.limits != (limits{900, 8}) {
		t.Fatal(cfg, err)
	}
}
func TestConfigLoadOverrides(t *testing.T) {
	dir := t.TempDir()
	req := filepath.Join(dir, "request.json")
	identity := filepath.Join(dir, "identity")
	r := validRequest()
	r.Exchange.IdentityFile = identity
	b, _ := json.Marshal(r)
	_ = os.WriteFile(req, b, 0600)
	_ = os.WriteFile(identity, []byte(" sdi_"+strings.Repeat("B", 49)+"\n"), 0600)
	env := map[string]string{"SRW_REQUEST_FILE": req, "SRW_EXCHANGE_URL": "http://override:99/", "SRW_DRIVER_PORT": "9090"}
	get := func(s string) string { return env[s] }
	cfg, err := loadConfig(get)
	if err != nil || cfg.port != "9090" || cfg.exchangeURL != "http://override:99" {
		t.Fatal(cfg, err)
	}
	env["SRW_EXCHANGE_URL"] = "https://bad"
	if _, err = loadConfig(get); err == nil {
		t.Fatal("invalid override accepted")
	}
	delete(env, "SRW_EXCHANGE_URL")
	env["SRW_DRIVER_PORT"] = "word"
	if _, err = loadConfig(get); err == nil {
		t.Fatal("invalid port accepted")
	}
	delete(env, "SRW_DRIVER_PORT")
	env["SRW_DRIVER_IDENTITY_FILE"] = filepath.Join(dir, "missing")
	if _, err = loadConfig(get); err == nil {
		t.Fatal("missing identity accepted")
	}
	delete(env, "SRW_DRIVER_IDENTITY_FILE")
	for _, content := range []string{"null", "[]", "{broken", strings.Repeat(" ", maxJSON+1)} {
		_ = os.WriteFile(req, []byte(content), 0600)
		if _, err = loadConfig(get); err == nil {
			t.Fatal("bad request file accepted")
		}
	}
	env["SRW_REQUEST_FILE"] = filepath.Join(dir, "missing")
	if _, err = loadConfig(get); err == nil {
		t.Fatal("missing file accepted")
	}
}
func TestHandoffValidation(t *testing.T) {
	cfg := testConfig()
	h := testHandoff("id")
	if !h.normalize(cfg) || *h.Limits.MaxDuration != 900 || *h.Limits.Fallback != 8 || h.TransferTarget != cfg.transferTarget {
		t.Fatal("defaults")
	}
	cases := []func(*handoff){func(h *handoff) { h.SessionID = "" }, func(h *handoff) { h.ProviderID = "" }, func(h *handoff) { h.JobID = "" }, func(h *handoff) { h.Model = "" }, func(h *handoff) { h.Accept.Instructions = " " }, func(h *handoff) { n := 59; h.Limits.MaxDuration = &n }, func(h *handoff) { n := 14401; h.Limits.MaxDuration = &n }, func(h *handoff) { n := 0; h.Limits.Fallback = &n }, func(h *handoff) { n := 121; h.Limits.Fallback = &n }}
	for _, change := range cases {
		h := testHandoff("id")
		change(&h)
		if h.normalize(cfg) {
			t.Fatal("invalid handoff accepted")
		}
	}
}
