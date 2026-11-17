package config

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func env(values map[string]string) func(string) string {
	return func(name string) string { return values[name] }
}

func TestLocateFollowsOneRuleOnEveryOperatingSystem(t *testing.T) {
	home := t.TempDir()
	xdg := filepath.Join(t.TempDir(), "xdg")
	explicit := filepath.Join(t.TempDir(), "elsewhere", "my.yaml")
	cases := []struct {
		name   string
		env    map[string]string
		dir    string
		config string
	}{
		{"home", nil, filepath.Join(home, ".config", "srw"), filepath.Join(home, ".config", "srw", "config.yaml")},
		{"xdg", map[string]string{"XDG_CONFIG_HOME": xdg}, filepath.Join(xdg, "srw"), filepath.Join(xdg, "srw", "config.yaml")},
		{"a relative XDG is ignored", map[string]string{"XDG_CONFIG_HOME": "relative"}, filepath.Join(home, ".config", "srw"), filepath.Join(home, ".config", "srw", "config.yaml")},
		{"SRW_CONFIG wins", map[string]string{"SRW_CONFIG": explicit, "XDG_CONFIG_HOME": xdg}, filepath.Dir(explicit), explicit},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			paths, err := Locate(env(test.env), home)
			if err != nil {
				t.Fatal(err)
			}
			if paths.Dir != test.dir || paths.Config != test.config {
				t.Errorf("got %s and %s", paths.Dir, paths.Config)
			}
			if paths.Credentials != filepath.Join(test.dir, "credentials.yaml") {
				t.Errorf("credentials at %s", paths.Credentials)
			}
			// The environment token is the SSH helper's file, wherever the
			// config directory is.
			if paths.EnvToken != filepath.Join(home, ".config", "srw", "token") {
				t.Errorf("environment token at %s", paths.EnvToken)
			}
		})
	}
	if _, err := Locate(env(map[string]string{"SRW_CONFIG": "relative.yaml"}), home); err == nil {
		t.Error("a relative SRW_CONFIG was accepted")
	}
	if _, err := Locate(env(nil), ""); err == nil {
		t.Error("no home directory and no SRW_CONFIG was accepted")
	}
}

func TestConfigAndCredentialsRoundTrip(t *testing.T) {
	paths, err := Locate(env(nil), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	empty, err := Load(paths)
	if err != nil || len(empty.Contexts) != 0 {
		t.Fatalf("a missing config file is an empty config, got %v %v", empty, err)
	}
	file := &File{CurrentContext: "dev", Contexts: map[string]*Context{
		"dev":   {Server: "https://api.dev.example.com", CAFile: "/etc/ca.pem", Project: "p1"},
		"local": {Server: "https://api.localhost", TokenCommand: []string{"pass", "show", "srw/local"}},
	}}
	if err := file.Save(paths); err != nil {
		t.Fatal(err)
	}
	again, err := Load(paths)
	if err != nil {
		t.Fatal(err)
	}
	if again.CurrentContext != "dev" || again.Contexts["dev"].CAFile != "/etc/ca.pem" ||
		strings.Join(again.Contexts["local"].TokenCommand, " ") != "pass show srw/local" {
		t.Errorf("round trip changed the config: %+v", again)
	}
	if names := again.Names(); len(names) != 2 || names[0] != "dev" || names[1] != "local" {
		t.Errorf("names: %v", names)
	}

	credentials := &Credentials{Tokens: map[string]string{"dev": "ak_secret"}}
	if err := credentials.Save(paths); err != nil {
		t.Fatal(err)
	}
	loaded, warning, err := LoadCredentials(paths)
	if err != nil || warning != "" || loaded.Tokens["dev"] != "ak_secret" {
		t.Fatalf("credentials: %v %q %v", loaded, warning, err)
	}
	if runtime.GOOS != "windows" {
		for path, mode := range map[string]os.FileMode{paths.Credentials: 0o600, paths.Dir: 0o700} {
			info, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			if info.Mode().Perm() != mode {
				t.Errorf("%s has mode %o, want %o", path, info.Mode().Perm(), mode)
			}
		}
	}
	// A write leaves nothing behind but the two files.
	entries, _ := os.ReadDir(paths.Dir)
	if len(entries) != 2 {
		t.Errorf("the config directory holds %d entries", len(entries))
	}
}

func TestCredentialsReadableByOthersWarnAndStillLoad(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("mode bits are synthetic on Windows")
	}
	paths, _ := Locate(env(nil), t.TempDir())
	if err := (&Credentials{Tokens: map[string]string{"dev": "ak_secret"}}).Save(paths); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(paths.Credentials, 0o644); err != nil {
		t.Fatal(err)
	}
	loaded, warning, err := LoadCredentials(paths)
	if err != nil || loaded.Tokens["dev"] != "ak_secret" {
		t.Fatalf("a readable file must still load: %v", err)
	}
	if !strings.Contains(warning, "chmod 600") || strings.Contains(warning, "ak_secret") {
		t.Errorf("warning: %q", warning)
	}
}

func TestABrokenCredentialsFileIsReportedWithoutItsContent(t *testing.T) {
	paths, _ := Locate(env(nil), t.TempDir())
	if err := os.MkdirAll(paths.Dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(paths.Credentials, []byte("tokens:\n  dev: ak_secret\n bad indent: [\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, _, err := LoadCredentials(paths)
	if err == nil || strings.Contains(err.Error(), "ak_secret") {
		t.Errorf("error: %v", err)
	}
}

func TestContextNames(t *testing.T) {
	for name, valid := range map[string]bool{
		"dev": true, "api.dev.example.com": true, "localhost-8085": true, "a_b": true,
		"": false, "-dev": false, "has space": false, "a/b": false, strings.Repeat("x", 64): false,
	} {
		if ValidContextName(name) != valid {
			t.Errorf("%q: valid=%v", name, !valid)
		}
	}
}
