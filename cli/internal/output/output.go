// Package output prints what a command produced in the format it was asked
// for. The format never depends on whether stdout is a terminal; a terminal
// decides presentation only.
package output

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"

	"go.yaml.in/yaml/v3"

	"github.com/superhuman-remote-worker/srw/cli/internal/exit"
)

// Format is an output format. Default means the command's own: text, a
// table, or JSON for the commands that have always printed JSON.
type Format string

// The formats.
const (
	Default Format = ""
	JSON    Format = "json"
	YAML    Format = "yaml"
	Name    Format = "name"
)

// Parse reads -o or SRW_OUTPUT.
func Parse(value string) (Format, error) {
	switch format := Format(strings.ToLower(strings.TrimSpace(value))); format {
	case Default, JSON, YAML, Name:
		return format, nil
	}
	return Default, exit.Usagef("unknown output format %q; choose json, yaml or name", value)
}

// WriteJSON prints a JSON document as the server sent it: same key order,
// same number spelling, re-indented.
func WriteJSON(w io.Writer, raw []byte) error {
	var buffer bytes.Buffer
	if err := json.Indent(&buffer, bytes.TrimSpace(raw), "", "  "); err != nil {
		return fmt.Errorf("the server's answer is not JSON: %w", err)
	}
	buffer.WriteByte('\n')
	_, err := w.Write(buffer.Bytes())
	return err
}

// WriteYAML prints a JSON document as YAML with the server's key order and
// number spelling. A plain yaml.Marshal of a decoded map would sort the keys
// and print large integers in exponent form.
func WriteYAML(w io.Writer, raw []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	node, err := decodeNode(decoder)
	if err != nil {
		return fmt.Errorf("the server's answer is not JSON: %w", err)
	}
	encoder := yaml.NewEncoder(w)
	encoder.SetIndent(2)
	if err := encoder.Encode(node); err != nil {
		return err
	}
	return encoder.Close()
}

// decodeNode builds a YAML node tree from a JSON token stream, in arrival
// order.
func decodeNode(decoder *json.Decoder) (*yaml.Node, error) {
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	switch value := token.(type) {
	case json.Delim:
		switch value {
		case '{':
			node := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
			for decoder.More() {
				key, err := decoder.Token()
				if err != nil {
					return nil, err
				}
				name, ok := key.(string)
				if !ok {
					return nil, errors.New("an object key is not a string")
				}
				child, err := decodeNode(decoder)
				if err != nil {
					return nil, err
				}
				node.Content = append(node.Content, text(name), child)
			}
			_, err := decoder.Token()
			return node, err
		case '[':
			node := &yaml.Node{Kind: yaml.SequenceNode, Tag: "!!seq"}
			for decoder.More() {
				child, err := decodeNode(decoder)
				if err != nil {
					return nil, err
				}
				node.Content = append(node.Content, child)
			}
			_, err := decoder.Token()
			return node, err
		}
		return nil, fmt.Errorf("unexpected %q", value)
	case string:
		return text(value), nil
	case json.Number:
		tag := "!!int"
		if strings.ContainsAny(value.String(), ".eE") {
			tag = "!!float"
		}
		return &yaml.Node{Kind: yaml.ScalarNode, Tag: tag, Value: value.String()}, nil
	case bool:
		return &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!bool", Value: fmt.Sprint(value)}, nil
	case nil:
		return &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!null", Value: "null"}, nil
	}
	return nil, fmt.Errorf("unexpected token %v", token)
}

// Words and number shapes that a YAML 1.1 reader takes for something other
// than text. The server reads manifests with such a reader and refuses an
// ambiguous scalar, so a string of this shape must be printed quoted or the
// output could not be applied back. The number shape is the server's own
// (shared/manifests/parsing.py, _NUMERIC), plus a float with a leading dot.
var (
	yaml11Words  = map[string]bool{"y": true, "n": true, "yes": true, "no": true, "on": true, "off": true}
	yaml11Number = regexp.MustCompile(`^[+-]?(?:[0-9][0-9_]*(?:\.[0-9_]*)?(?:[eE][+-]?[0-9_]+)?|0[xXoObB][0-9a-fA-F_]+|[0-9_]+(?::[0-9_.]+)+|\.[0-9_]+(?:[eE][+-]?[0-9]+)?)$`)
)

// text is a string scalar. The encoder quotes what its own reader would take
// for another type; this adds what a YAML 1.1 reader would.
func text(value string) *yaml.Node {
	node := &yaml.Node{}
	node.SetString(value)
	if node.Style == 0 && (yaml11Words[strings.ToLower(value)] || yaml11Number.MatchString(value)) {
		node.Style = yaml.DoubleQuotedStyle
	}
	return node
}

// Document prints a server object in a machine format.
func Document(w io.Writer, format Format, raw []byte) error {
	if format == YAML {
		return WriteYAML(w, raw)
	}
	return WriteJSON(w, raw)
}

// Error prints a failure to stderr. The shape follows the effective output
// format: one line for a person, or {"error": {...}} when the command's
// output is JSON.
func Error(w io.Writer, asJSON bool, err error) {
	var coded *exit.Error
	if !errors.As(err, &coded) {
		coded = &exit.Error{Code: exit.CodeOf(err), Message: err.Error()}
	}
	if asJSON {
		body := map[string]any{"message": coded.Message}
		if coded.ErrCode != "" {
			body["code"] = coded.ErrCode
		}
		if coded.Status != 0 {
			body["status"] = coded.Status
		}
		if coded.Document != 0 {
			body["document"] = coded.Document
		}
		if coded.Path != "" {
			body["path"] = coded.Path
		}
		if coded.File != "" {
			body["file"] = coded.File
		}
		if coded.Hint != "" {
			body["hint"] = coded.Hint
		}
		encoded, _ := json.Marshal(map[string]any{"error": body})
		fmt.Fprintln(w, string(encoded))
		return
	}
	line := "error: " + coded.Message
	var where []string
	if coded.File != "" {
		where = append(where, coded.File)
	}
	if coded.Document != 0 && (coded.File != "" || coded.Document > 1) {
		where = append(where, fmt.Sprintf("document %d", coded.Document))
	}
	if coded.Path != "" && coded.Path != "/" {
		where = append(where, coded.Path)
	}
	if len(where) > 0 {
		line += " (" + strings.Join(where, ", ") + ")"
	}
	fmt.Fprintln(w, line)
	if coded.Hint != "" {
		fmt.Fprintln(w, "hint: "+coded.Hint)
	}
}
