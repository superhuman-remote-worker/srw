// Compile beside an unchanged copy of drivers/gpt-live-sip/contract.go.
// This bridge checks engine frames through the Go parser used in production.
package main

import (
	"bufio"
	"encoding/json"
	"os"
)

func main() {
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 4096), 1024*1024)
	encoder := json.NewEncoder(os.Stdout)
	for scanner.Scan() {
		var raw map[string]any
		if json.Unmarshal(scanner.Bytes(), &raw) != nil {
			os.Exit(2)
		}
		if len(os.Args) > 1 && os.Args[1] == "clip" {
			if encoder.Encode(map[string]any{"content": clipAppendContent(str(raw["text"]), 1500)}) != nil {
				os.Exit(2)
			}
			continue
		}
		e := parseEvent(raw)
		if encoder.Encode(map[string]any{"event": e, "journal": journalEvent(e, raw)}) != nil {
			os.Exit(2)
		}
	}
	if scanner.Err() != nil {
		os.Exit(2)
	}
}
