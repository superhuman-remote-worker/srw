package cmd

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"unicode/utf8"

	"github.com/superhuman-remote-worker/srw/cli/internal/exit"
)

// maxManifestBytes is the server's limit for the manifest text of one
// request, all files together.
const maxManifestBytes = 1 << 20

// manifestFile is one manifest text as it was read. It is sent unparsed.
type manifestFile struct {
	Name   string `json:"name"`
	Source string `json:"source"`
	Format string `json:"format"`
}

// readManifests reads the -f arguments. A dash is standard input, once. A
// directory is its *.yaml, *.yml and *.json files, not recursive. The format
// follows the extension unless format names one.
//
// The files are not joined as text. A second file that starts with "---", a
// trailing "---" or a comment-only file would each turn into an empty
// document that the server refuses with an error that names no file.
func (a *App) readManifests(arguments []string, format string) ([]manifestFile, error) {
	if len(arguments) == 0 {
		return nil, exit.Usagef("give at least one manifest with -f FILE (or -f - for standard input)")
	}
	if format != "" && format != "json" && format != "yaml" {
		return nil, exit.Usagef("--format is json or yaml, not %q", format)
	}
	var paths []string
	stdin := 0
	for _, argument := range arguments {
		if argument == "-" {
			stdin++
			paths = append(paths, argument)
			continue
		}
		info, err := os.Stat(argument)
		if err != nil {
			return nil, exit.Usagef("cannot read %s: %v", argument, unwrapPath(err))
		}
		if !info.IsDir() {
			paths = append(paths, argument)
			continue
		}
		entries, err := os.ReadDir(argument)
		if err != nil {
			return nil, exit.Usagef("cannot read %s: %v", argument, unwrapPath(err))
		}
		found := 0
		for _, entry := range entries {
			switch strings.ToLower(filepath.Ext(entry.Name())) {
			case ".yaml", ".yml", ".json":
				if !entry.IsDir() {
					paths = append(paths, filepath.Join(argument, entry.Name()))
					found++
				}
			}
		}
		if found == 0 {
			return nil, exit.Usagef("%s holds no .yaml, .yml or .json file (subdirectories are not read)", argument)
		}
	}
	if stdin > 1 {
		return nil, exit.Usagef("standard input can be read only once; give -f - once")
	}
	if stdin == 1 && a.flags.tokenStdin {
		return nil, exit.Usagef("-f - and --token-stdin both read standard input; give the manifest as a file")
	}
	seen := map[string]bool{}
	total := 0
	files := make([]manifestFile, 0, len(paths))
	for _, path := range paths {
		name := path
		var data []byte
		var err error
		if path == "-" {
			name = "<stdin>"
			data, err = io.ReadAll(io.LimitReader(a.In, maxManifestBytes+1))
		} else {
			data, err = readLimited(path)
		}
		if err != nil {
			return nil, exit.Usagef("cannot read %s: %v", name, unwrapPath(err))
		}
		if seen[name] {
			return nil, exit.Usagef("%s is given twice", name)
		}
		seen[name] = true
		total += len(data)
		if total > maxManifestBytes {
			return nil, exit.Usagef("the manifests exceed 1 MiB together (reached at %s); send them in several calls", name)
		}
		// The text travels inside JSON. Invalid UTF-8 would be replaced on
		// the way without a word.
		if !utf8.Valid(data) {
			return nil, exit.Usagef("%s is not valid UTF-8", name)
		}
		if strings.TrimSpace(string(data)) == "" {
			return nil, exit.Usagef("%s is empty", name)
		}
		chosen := format
		if chosen == "" {
			chosen = "yaml"
			if strings.EqualFold(filepath.Ext(path), ".json") {
				chosen = "json"
			}
		}
		files = append(files, manifestFile{Name: name, Source: string(data), Format: chosen})
	}
	return files, nil
}

func readLimited(path string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return io.ReadAll(io.LimitReader(file, maxManifestBytes+1))
}

// unwrapPath drops the path from an os error; the caller names the file.
func unwrapPath(err error) error {
	if pathError, ok := err.(*os.PathError); ok {
		return pathError.Err
	}
	return err
}

// manifestBody starts a request body. One file is sent as {source, format},
// which every server accepts. Several are sent as sources, each unparsed, so
// YAML and JSON can be mixed and an error names the file.
func manifestBody(files []manifestFile) map[string]any {
	if len(files) == 1 {
		return map[string]any{"source": files[0].Source, "format": files[0].Format}
	}
	return map[string]any{"sources": files}
}

// severalFilesRefused recognises the answer of a server that does not take
// sources yet: its request model forbids the unknown field.
func severalFilesRefused(status int, body []byte) bool {
	if status != 422 {
		return false
	}
	var envelope struct {
		Detail []struct {
			Type     string `json:"type"`
			Location []any  `json:"loc"`
		} `json:"detail"`
	}
	if json.Unmarshal(body, &envelope) != nil {
		return false
	}
	for _, issue := range envelope.Detail {
		if issue.Type == "extra_forbidden" && len(issue.Location) > 0 && fmt.Sprint(issue.Location[len(issue.Location)-1]) == "sources" {
			return true
		}
	}
	return false
}
