package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func post(t *testing.T, s *server, session, credential, body string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(body))
	if session != "" {
		request.Header.Set("Mcp-Session-Id", session)
	}
	if credential != "" {
		request.Header.Set("Authorization", "Bearer "+credential)
	}
	recorder := httptest.NewRecorder()
	s.ServeHTTP(recorder, request)
	return recorder
}

func callText(t *testing.T, recorder *httptest.ResponseRecorder) (string, bool) {
	t.Helper()
	var answer struct {
		Result struct {
			Content []struct {
				Text string `json:"text"`
			} `json:"content"`
			IsError bool `json:"isError"`
		} `json:"result"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &answer); err != nil || len(answer.Result.Content) == 0 {
		t.Fatalf("not a call result: %s", recorder.Body)
	}
	return answer.Result.Content[0].Text, answer.Result.IsError
}

func TestASessionListsToolsWithoutACredentialAndCallsNeedOne(t *testing.T) {
	s := newServer("hello", "pod-1")
	opened := post(t, s, "", "", `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}`)
	session := opened.Header().Get("Mcp-Session-Id")
	if opened.Code != http.StatusOK || session == "" {
		t.Fatalf("initialize: %d", opened.Code)
	}
	if code := post(t, s, session, "", `{"jsonrpc":"2.0","method":"notifications/initialized"}`).Code; code != http.StatusAccepted {
		t.Fatalf("notification: %d", code)
	}
	listed := post(t, s, session, "", `{"jsonrpc":"2.0","id":2,"method":"tools/list"}`)
	if !strings.Contains(listed.Body.String(), `"notes_delete"`) {
		t.Fatalf("tools/list: %s", listed.Body)
	}
	if _, failed := callText(t, post(t, s, session, "", `{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"whoami"}}`)); !failed {
		t.Fatal("a call without a credential succeeded")
	}
	answer, failed := callText(t, post(t, s, session, "secret-1", `{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"whoami"}}`))
	digest := sha256.Sum256([]byte("secret-1"))
	if failed || !strings.Contains(answer, hex.EncodeToString(digest[:])) || !strings.Contains(answer, `"pod":"pod-1"`) || !strings.Contains(answer, `"message":"hello"`) {
		t.Fatalf("whoami: %s", answer)
	}
	if answer, _ := callText(t, post(t, s, session, "secret-1", `{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"leak_credential"}}`)); answer != "credential: secret-1" {
		t.Fatalf("leak: %s", answer)
	}
}

func TestNotesAndUnknownSessions(t *testing.T) {
	s := newServer("", "pod-1")
	session := post(t, s, "", "", `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}`).Header().Get("Mcp-Session-Id")
	post(t, s, session, "c", `{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"notes_write","arguments":{"name":"a","text":"x"}}}`)
	if answer, _ := callText(t, post(t, s, session, "c", `{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"notes_read","arguments":{"name":"a"}}}`)); answer != "x" {
		t.Fatalf("notes_read: %s", answer)
	}
	if code := post(t, s, "gone", "c", `{"jsonrpc":"2.0","id":4,"method":"tools/list"}`).Code; code != http.StatusNotFound {
		t.Fatalf("unknown session: %d", code)
	}
	request := httptest.NewRequest(http.MethodDelete, "/mcp", nil)
	request.Header.Set("Mcp-Session-Id", session)
	s.ServeHTTP(httptest.NewRecorder(), request)
	if code := post(t, s, session, "c", `{"jsonrpc":"2.0","id":5,"method":"tools/list"}`).Code; code != http.StatusNotFound {
		t.Fatalf("closed session: %d", code)
	}
}
