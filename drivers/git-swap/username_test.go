package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// The exchange names the username each upstream credential is presented
// with: x-access-token for a GitHub App installation token, oauth2 for a
// static forge token (GitLab expects it for some kinds); an answer that
// names none (an orchestrator before C5) means oauth2.
func TestTheExchangeNamesTheUsernamePerCredential(t *testing.T) {
	cases := []struct {
		name    string
		answer  map[string]any
		want    string
		refused bool
	}{
		{"github app", map[string]any{"username": "x-access-token"}, "x-access-token", false},
		{"static token", map[string]any{"username": "oauth2"}, "oauth2", false},
		{"none named", map[string]any{}, "oauth2", false},
		{"null", map[string]any{"username": nil}, "oauth2", false},
		{"a colon", map[string]any{"username": "a:b"}, "", true},
		{"a space", map[string]any{"username": "a b"}, "", true},
		{"not text", map[string]any{"username": 7}, "", true},
		{"too long", map[string]any{"username": strings.Repeat("u", 65)}, "", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				answer := map[string]any{
					"credential":        testCredential,
					"allowed_upstream":  []string{"https://example.com/o/r.git"},
					"max_cache_seconds": 30,
				}
				for key, value := range c.answer {
					answer[key] = value
				}
				_ = json.NewEncoder(w).Encode(answer)
			}))
			defer server.Close()
			found, err := newHTTPAuthority(server.URL, "sdi_identity").exchange(context.Background(), readWriteLease, "read")
			if c.refused {
				if !errors.Is(err, errUnavailable) {
					t.Fatalf("want errUnavailable, got %v (%q)", err, found.username)
				}
				return
			}
			if err != nil || found.username != c.want || found.credential != testCredential {
				t.Fatalf("got %q, %v", found.username, err)
			}
		})
	}
}

// The upstream sees the credential under the username the exchange named,
// and the scrubber masks that Basic value as well as the default one.
func TestTheUpstreamSeesTheNamedUsername(t *testing.T) {
	for _, username := range []string{"", "oauth2", "x-access-token"} {
		t.Run("username="+username, func(t *testing.T) {
			h := newHarness(t)
			h.auth.username = username
			response := h.do("GET", repoPath+"/info/refs?service=git-upload-pack", readWriteLease, nil, nil)
			if response.StatusCode != http.StatusOK {
				t.Fatalf("status %d", response.StatusCode)
			}
			calls := h.upstreamCalls()
			if len(calls) != 1 {
				t.Fatalf("%d upstream calls", len(calls))
			}
			want := username
			if want == "" {
				want = "oauth2"
			}
			sent := calls[0].header.Get("Authorization")
			expected := "Basic " + base64.StdEncoding.EncodeToString([]byte(want+":"+testCredential))
			if sent != expected {
				t.Fatalf("upstream got %q, want the %s username", sent, want)
			}
		})
	}
	var out bytes.Buffer
	s := newScrubberFor(&out, "x-access-token", testCredential)
	echoed := "remote: " + basicValueFor("x-access-token", testCredential) + " and " + basicValue(testCredential)
	if _, err := s.Write([]byte(echoed)); err != nil {
		t.Fatal(err)
	}
	if err := s.finish(); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out.String(), basicValueFor("x-access-token", testCredential)) ||
		strings.Contains(out.String(), basicValue(testCredential)) {
		t.Fatalf("a Basic value was not masked: %q", out.String())
	}
}
