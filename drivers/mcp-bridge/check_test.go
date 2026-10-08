package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"testing"
)

// The bridge forwards what the front checked: it accepts only the exact
// form the front forwards, and the process reads exactly those bytes.

// The front's own forms (drivers/mcp-front/review_test.go
// TestTheServerGetsTheBodyTheFrontChecked and its re-encoding of each
// message kind).
var frontForms = []string{
	`{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"whoami","arguments":{"x":[1,2]}}}`,
	`{"jsonrpc":"2.0","id":"call-7","method":"tools/call","params":{"name":"whoami"}}`,
	`{"jsonrpc":"2.0","id":0,"method":"ping"}`,
	`{"jsonrpc":"2.0","id":-3,"method":"tools/list","params":{}}`,
	`{"jsonrpc":"2.0","method":"notifications/initialized"}`,
	`{"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":6}}`,
	`{"jsonrpc":"2.0","id":"srv-1","result":{}}`,
	`{"jsonrpc":"2.0","id":4,"error":{"code":-32601,"message":"no"}}`,
	`{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"a<b>&c","arguments":{"t":"  \\u0041 é"}}}`,
}

func TestTheFrontsFormsAreAcceptedAsTheyAre(t *testing.T) {
	for _, body := range frontForms {
		if _, err := checkMessage([]byte(body)); err != nil {
			t.Errorf("%s: %v", body, err)
		}
	}
}

func TestAnyOtherFormIsRefused(t *testing.T) {
	for _, body := range []string{
		``,
		`[{"jsonrpc":"2.0","id":1,"method":"ping"}]`,
		`{"jsonrpc":"2.0","id":1,"method":"ping"}` + "\n",
		`{"jsonrpc":"2.0","id":1,"method":"ping"}{"jsonrpc":"2.0","id":2,"method":"ping"}`,
		` {"jsonrpc":"2.0","id":1,"method":"ping"}`,
		`{"jsonrpc": "2.0","id":1,"method":"ping"}`,
		`{"id":1,"jsonrpc":"2.0","method":"ping"}`,
		`{"jsonrpc":"2.0","id":1.0,"method":"ping"}`,
		`{"jsonrpc":"2.0","id":1e0,"method":"ping"}`,
		`{"jsonrpc":"2.0","id":1.5,"method":"ping"}`,
		`{"jsonrpc":"2.0","id":9007199254740993,"method":"ping"}`,
		`{"jsonrpc":"2.0","id":"\u0061","method":"ping"}`,
		`{"jsonrpc":"2.0","id":null,"method":"ping"}`,
		`{"jsonrpc":"2.0","id":1,"method":"tools\/call","params":{"name":"x"}}`,
		`{"jsonrpc":"2.0","id":1,"method":"ping","Method":"tools/call"}`,
		`{"jsonrpc":"2.0","id":1,"method":"ping","extra":true}`,
		`{"jsonrpc":"2.0","id":1,"method":"ping","method":"tools/call"}`,
		`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{ "name":"x"}}`,
		`{"jsonrpc":"1.0","id":1,"method":"ping"}`,
		`{"jsonrpc":"2.0","id":4,"error":{"message":"no","code":-32601}}`,
		`not json`,
	} {
		if _, err := checkMessage([]byte(body)); err == nil {
			t.Errorf("%q was accepted", body)
		}
	}
}

type corpus struct {
	WriteTool    string   `json:"write_tool"`
	ReadTool     string   `json:"read_tool"`
	Bodies       []string `json:"bodies"`
	BodiesBase64 []string `json:"bodies_base64"`
}

// loadCorpus reads the D5a review's differential corpus, shared with the
// front's tests, the Python integration test and the D5b k3d gate.
func loadCorpus(t *testing.T) ([][]byte, corpus) {
	t.Helper()
	raw, err := os.ReadFile("../mcp-front/testdata/bypass_corpus.json")
	if err != nil {
		t.Fatal(err)
	}
	var found corpus
	if err := json.Unmarshal(raw, &found); err != nil {
		t.Fatal(err)
	}
	var bodies [][]byte
	for _, body := range found.Bodies {
		bodies = append(bodies, []byte(body))
	}
	for _, encoded := range found.BodiesBase64 {
		body, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			t.Fatal(err)
		}
		bodies = append(bodies, body)
	}
	if len(bodies) < 50 || found.WriteTool == "" || found.ReadTool == "" {
		t.Fatalf("a corpus of %d bodies", len(bodies))
	}
	return bodies, found
}

// views are the ways a server may read a body: the last of a duplicate key
// (a map), the first (a streaming parser) and Go's case-insensitive struct
// decoding. Each returns method/tool.
func views(body []byte) []string {
	var last map[string]any
	json.Unmarshal(body, &last)
	params, _ := last["params"].(map[string]any)
	name, _ := params["name"].(string)
	method, _ := last["method"].(string)
	out := []string{method + "/" + name}
	decoder := json.NewDecoder(bytes.NewReader(body))
	firstMethod, firstName := "", ""
	if token, err := decoder.Token(); err == nil && token == json.Delim('{') {
		seen := map[string]bool{}
		for decoder.More() {
			key, _ := decoder.Token()
			var value json.RawMessage
			decoder.Decode(&value)
			text, _ := key.(string)
			if seen[text] {
				continue
			}
			seen[text] = true
			switch text {
			case "method":
				json.Unmarshal(value, &firstMethod)
			case "params":
				inner := json.NewDecoder(bytes.NewReader(value))
				if token, err := inner.Token(); err == nil && token == json.Delim('{') {
					innerSeen := map[string]bool{}
					for inner.More() {
						innerKey, _ := inner.Token()
						var innerValue json.RawMessage
						inner.Decode(&innerValue)
						name, _ := innerKey.(string)
						if name == "name" && !innerSeen[name] {
							json.Unmarshal(innerValue, &firstName)
						}
						innerSeen[name] = true
					}
				}
			}
		}
	}
	out = append(out, firstMethod+"/"+firstName)
	var folded struct {
		Method string `json:"method"`
		Params struct {
			Name string `json:"name"`
		} `json:"params"`
	}
	json.Unmarshal(body, &folded)
	return append(out, folded.Method+"/"+folded.Params.Name)
}

// The bridge decides on a message's top level (a request or an answer, its
// id and method) and forwards the rest byte for byte: what a tool call
// names is the front's decision, taken on these same bytes. So every body
// of the review's corpus the bridge accepts has only the JSON-RPC keys
// themselves at its top level and a method every reader agrees on; the
// variants of the top level (a "Method", a "method" twice, a key that folds
// to "params") are all refused.
func TestEveryReaderAgreesOnTheTopLevelOfWhatTheBridgeAccepts(t *testing.T) {
	bodies, _ := loadCorpus(t)
	accepted := 0
	for _, body := range bodies {
		if _, err := checkMessage(body); err != nil {
			continue
		}
		accepted++
		var members map[string]json.RawMessage
		if err := json.Unmarshal(body, &members); err != nil {
			t.Fatalf("%s: %v", body, err)
		}
		for key := range members {
			if key != "jsonrpc" && key != "id" && key != "method" && key != "params" {
				t.Fatalf("%s was accepted with a top-level %q", body, key)
			}
		}
		got := views(body)
		method := func(view string) string { return strings.Join(strings.SplitN(view, "/", 3)[:2], "/") }
		if method(got[0]) != "tools/call" || method(got[1]) != "tools/call" || method(got[2]) != "tools/call" {
			t.Fatalf("%s was accepted, but readers disagree on its method: %v", body, got)
		}
	}
	if accepted == 0 || accepted == len(bodies) {
		t.Fatalf("%d of %d corpus bodies accepted", accepted, len(bodies))
	}
}

func TestTheProcessReadsExactlyTheBytesTheBridgeAccepted(t *testing.T) {
	h := newHarness(t, nil)
	session := h.open("lease-a", "credential-a")
	pid := h.whoami("lease-a", "credential-a", session).PID
	bodies, _ := loadCorpus(t)
	sent := []string{}
	for _, body := range append(bodies, []byte(frontForms[0]), []byte(frontForms[8])) {
		// Every call in the corpus has id 7; they are sent one at a time.
		response, _ := h.do(http.MethodPost, "lease-a", "credential-a", session, string(body))
		if _, err := checkMessage(body); err != nil {
			if response.StatusCode != http.StatusBadRequest {
				t.Fatalf("%s: %d", body, response.StatusCode)
			}
			continue
		}
		if response.StatusCode != http.StatusOK {
			t.Fatalf("%s: %d", body, response.StatusCode)
		}
		sent = append(sent, string(body))
	}
	lines := received(t, h.logDir, pid)
	// initialize, initialized, whoami, then exactly what was accepted.
	got := lines[3:]
	if strings.Join(got, "\n") != strings.Join(sent, "\n") {
		t.Fatalf("the process read\n%s\nnot\n%s", strings.Join(got, "\n"), strings.Join(sent, "\n"))
	}
	if !strings.Contains(h.logs.text(), "refused a message for binding=lease-a") {
		t.Fatal("a refusal was not logged")
	}
}
