// Package config reads and writes the two files srw keeps on a person's
// machine: config.yaml (no secrets) and credentials.yaml (tokens).
//
// The config file can name a command to run (token-command), so it is read
// only from the config directory, never from the working directory.
package config

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"time"

	"go.yaml.in/yaml/v3"
)

// Context is one server a person works with.
type Context struct {
	Server string `yaml:"server"`
	// CAFile replaces the system roots for this server.
	CAFile string `yaml:"ca-file,omitempty"`
	// Project is the default project for commands that take one.
	Project string `yaml:"project,omitempty"`
	// TokenCommand is run without a shell; its first line of output is the
	// token.
	TokenCommand []string `yaml:"token-command,omitempty"`
	// InsecureHTTP allows http:// for a host that is not loopback.
	InsecureHTTP bool `yaml:"insecure-http,omitempty"`
}

// File is config.yaml.
type File struct {
	CurrentContext string              `yaml:"current-context,omitempty"`
	Contexts       map[string]*Context `yaml:"contexts,omitempty"`
}

// Credentials is credentials.yaml: one token per context.
type Credentials struct {
	Tokens map[string]string `yaml:"tokens,omitempty"`
}

// Paths are the files of one config directory.
type Paths struct {
	Dir         string
	Config      string
	Credentials string
	// EnvToken is the token file of environment mode. The SSH helper
	// (scripts/srw-ssh-proxy) reads the same file.
	EnvToken string
}

// Locate finds the config directory. One rule on every operating system:
// the directory of SRW_CONFIG when that names a config file, else
// $XDG_CONFIG_HOME/srw when the variable is set and absolute, else
// <home>/.config/srw. os.UserConfigDir is not used: on macOS it answers
// ~/Library/Application Support, where no comparable tool keeps its config.
func Locate(getenv func(string) string, home string) (Paths, error) {
	fallback := ""
	if home != "" {
		fallback = filepath.Join(home, ".config", "srw")
	}
	paths := Paths{}
	switch {
	case getenv("SRW_CONFIG") != "":
		file := getenv("SRW_CONFIG")
		if !filepath.IsAbs(file) {
			return paths, fmt.Errorf("SRW_CONFIG must be an absolute path to a config file")
		}
		paths.Dir, paths.Config = filepath.Dir(file), file
	case filepath.IsAbs(getenv("XDG_CONFIG_HOME")):
		paths.Dir = filepath.Join(getenv("XDG_CONFIG_HOME"), "srw")
	case fallback != "":
		paths.Dir = fallback
	default:
		return paths, fmt.Errorf("cannot find the home directory; set SRW_CONFIG to a config file")
	}
	if paths.Config == "" {
		paths.Config = filepath.Join(paths.Dir, "config.yaml")
	}
	paths.Credentials = filepath.Join(paths.Dir, "credentials.yaml")
	if fallback != "" {
		paths.EnvToken = filepath.Join(fallback, "token")
	}
	return paths, nil
}

var contextName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,62}$`)

// ValidContextName says whether name can be a context's name.
func ValidContextName(name string) bool { return contextName.MatchString(name) }

// Load reads config.yaml. A missing file is an empty config.
func Load(paths Paths) (*File, error) {
	file := &File{}
	data, err := os.ReadFile(paths.Config)
	if errors.Is(err, fs.ErrNotExist) {
		return file, nil
	}
	if err != nil {
		return nil, err
	}
	if err := yaml.Unmarshal(data, file); err != nil {
		return nil, fmt.Errorf("%s: %w", paths.Config, err)
	}
	for name, context := range file.Contexts {
		if context == nil {
			return nil, fmt.Errorf("%s: context %q is empty", paths.Config, name)
		}
	}
	return file, nil
}

// Names returns the context names in a stable order.
func (f *File) Names() []string {
	names := make([]string, 0, len(f.Contexts))
	for name := range f.Contexts {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// Save writes config.yaml.
func (f *File) Save(paths Paths) error {
	return write(paths.Dir, paths.Config, f, 0o644)
}

// LoadCredentials reads credentials.yaml. A missing file is empty. The
// warning is set when the file is readable by group or others; that is a
// warning, not a refusal.
func LoadCredentials(paths Paths) (credentials *Credentials, warning string, err error) {
	credentials = &Credentials{}
	info, err := os.Stat(paths.Credentials)
	if errors.Is(err, fs.ErrNotExist) {
		return credentials, "", nil
	}
	if err != nil {
		return nil, "", err
	}
	// Mode bits are synthetic on Windows; never reason about them there.
	if runtime.GOOS != "windows" && info.Mode().Perm()&0o077 != 0 {
		warning = fmt.Sprintf("%s is readable by other users; run: chmod 600 %s", paths.Credentials, paths.Credentials)
	}
	data, err := os.ReadFile(paths.Credentials)
	if err != nil {
		return nil, "", err
	}
	if err := yaml.Unmarshal(data, credentials); err != nil {
		// The parser's message can quote a line of the file, which holds
		// tokens; say where, not what.
		return nil, "", fmt.Errorf("%s is not valid YAML", paths.Credentials)
	}
	return credentials, warning, nil
}

// Save writes credentials.yaml with mode 0600.
func (c *Credentials) Save(paths Paths) error {
	return write(paths.Dir, paths.Credentials, c, 0o600)
}

// write replaces a file atomically: a temporary file in the same directory,
// synced, then renamed over the real file. No lock file.
func write(dir, path string, value any, mode fs.FileMode) error {
	var buffer bytes.Buffer
	encoder := yaml.NewEncoder(&buffer)
	encoder.SetIndent(2)
	if err := encoder.Encode(value); err != nil {
		return err
	}
	if err := encoder.Close(); err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	temporary, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".*")
	if err != nil {
		return err
	}
	name := temporary.Name()
	keep := false
	defer func() {
		if !keep {
			_ = os.Remove(name)
		}
	}()
	if err := temporary.Chmod(mode); err != nil && runtime.GOOS != "windows" {
		_ = temporary.Close()
		return err
	}
	if _, err := temporary.Write(buffer.Bytes()); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := rename(name, path); err != nil {
		return err
	}
	keep = true
	return nil
}

// rename retries for about two seconds on Windows, where a virus scanner or
// an editor can hold the target open for a moment (a sharing violation).
func rename(from, to string) error {
	err := os.Rename(from, to)
	if err == nil || runtime.GOOS != "windows" {
		return err
	}
	for attempt := 0; attempt < 20 && err != nil; attempt++ {
		time.Sleep(100 * time.Millisecond)
		err = os.Rename(from, to)
	}
	return err
}
