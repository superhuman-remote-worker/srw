package output

import (
	"bytes"
	"strings"
	"testing"

	"go.yaml.in/yaml/v3"

	"github.com/superhuman-remote-worker/srw/cli/internal/exit"
)

const document = `{"zeta":1,"alpha":{"id":9007199254740993,"ratio":0.10,"big":1e21},"list":[true,null,"yes","007","1:30","Off","1_000","0x1F","plain text"],"on":"2026-11-17","empty":{},"text":"line: one"}`

func TestJSONIsTheServersDocumentReindented(t *testing.T) {
	var out bytes.Buffer
	if err := WriteJSON(&out, []byte(document)); err != nil {
		t.Fatal(err)
	}
	text := out.String()
	// Key order and number spelling are the server's.
	if strings.Index(text, `"zeta"`) > strings.Index(text, `"alpha"`) {
		t.Errorf("keys were reordered:\n%s", text)
	}
	for _, number := range []string{"9007199254740993", "0.10", "1e21"} {
		if !strings.Contains(text, number) {
			t.Errorf("%s was respelled:\n%s", number, text)
		}
	}
	if !strings.HasSuffix(text, "}\n") {
		t.Errorf("no final newline")
	}
	if WriteJSON(&out, []byte("<html>")) == nil {
		t.Error("a body that is not JSON was accepted")
	}
}

func TestYAMLKeepsKeyOrderAndNumberSpelling(t *testing.T) {
	var out bytes.Buffer
	if err := WriteYAML(&out, []byte(document)); err != nil {
		t.Fatal(err)
	}
	want := `zeta: 1
alpha:
  id: 9007199254740993
  ratio: 0.10
  big: 1e21
list:
  - true
  - null
  - "yes"
  - "007"
  - "1:30"
  - "Off"
  - "1_000"
  - "0x1F"
  - plain text
"on": "2026-11-17"
empty: {}
text: 'line: one'
`
	if out.String() != want {
		t.Errorf("got:\n%s\nwant:\n%s", out.String(), want)
	}
}

// What the YAML output prints must read back as the same values with the
// server's reader, which follows YAML 1.1 and refuses ambiguous scalars.
func TestYAMLOutputReadsBackWithTheSameReader(t *testing.T) {
	var out bytes.Buffer
	if err := WriteYAML(&out, []byte(document)); err != nil {
		t.Fatal(err)
	}
	var back map[string]any
	if err := yaml.Unmarshal(out.Bytes(), &back); err != nil {
		t.Fatal(err)
	}
	list := back["list"].([]any)
	for index, want := range map[int]any{2: "yes", 3: "007", 4: "1:30", 5: "Off", 6: "1_000", 7: "0x1F"} {
		if list[index] != want {
			t.Errorf("list[%d] read back as %#v, want the text %q", index, list[index], want)
		}
	}
	if back["on"] != "2026-11-17" {
		t.Errorf("the key or the date changed type: %#v", back)
	}
}

func TestFormats(t *testing.T) {
	for value, want := range map[string]Format{"": Default, "json": JSON, " YAML ": YAML, "name": Name} {
		if got, err := Parse(value); err != nil || got != want {
			t.Errorf("%q: %q %v", value, got, err)
		}
	}
	if _, err := Parse("table"); err == nil {
		t.Error("an unknown format was accepted")
	}
}

func TestErrorShapes(t *testing.T) {
	failure := &exit.Error{
		Code: exit.Failed, Status: 422, ErrCode: "DuplicateKey", Message: "Duplicate object key.",
		Document: 2, Path: "/spec/runtime", File: "experts/b.yaml", Hint: "fix it",
	}
	var human bytes.Buffer
	Error(&human, false, failure)
	if human.String() != "error: Duplicate object key. (experts/b.yaml, document 2, /spec/runtime)\nhint: fix it\n" {
		t.Errorf("human: %q", human.String())
	}
	var machine bytes.Buffer
	Error(&machine, true, failure)
	want := `{"error":{"code":"DuplicateKey","document":2,"file":"experts/b.yaml","hint":"fix it","message":"Duplicate object key.","path":"/spec/runtime","status":422}}` + "\n"
	if machine.String() != want {
		t.Errorf("machine: %s", machine.String())
	}
	// One source and its first document need no location.
	var plain bytes.Buffer
	Error(&plain, false, &exit.Error{Message: "no", Document: 1, Path: "/"})
	if plain.String() != "error: no\n" {
		t.Errorf("plain: %q", plain.String())
	}
}
