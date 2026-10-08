package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
)

const (
	defaultRequestFile  = "/run/srw/request.json"
	defaultIdentityFile = "/run/srw/identity"
	maxRequestFile      = 512 * 1024
	classRead           = "read"
	classWrite          = "write"
)

// An sdi_ driver identity and an scl_ lease token: the prefix, 43 base62
// characters and a 6-character checksum (shared/connectors/leases.py).
var (
	identityShape = regexp.MustCompile(`\Asdi_[0-9A-Za-z]{49}\z`)
	leaseShape    = regexp.MustCompile(`\Ascl_[0-9A-Za-z]{49}\z`)
	patternShape  = regexp.MustCompile(`\A[A-Za-z0-9_.*-]{1,128}\z`)
	headerShape   = regexp.MustCompile(`\A[A-Za-z0-9-]{1,64}\z`)
)

// reservedHeaders are the headers the front forwards or sets, or the
// transport owns: the credential may never be written over one of them.
var reservedHeaders = func() map[string]bool {
	reserved := map[string]bool{}
	for _, name := range append([]string{
		"Connection", "Content-Length", "Cookie", "Host", "Keep-Alive",
		"Origin", "Proxy-Connection", "Te", "Trailer", "Transfer-Encoding",
		"Upgrade", bridgeBindingHeader, bridgeCredentialHeader,
	}, forwardRequestHeaders...) {
		reserved[http.CanonicalHeaderKey(name)] = true
	}
	return reserved
}()

// credentialRule is how the server receives the upstream credential: in a
// header on each request (an HTTP server), or in an environment variable
// of its binding's process (a stdio server, through the bridge).
type credentialRule struct {
	Header string `json:"header"`
	Scheme string `json:"scheme"`
	Env    string `json:"env"`
}

// mcpBlock is the front's part of the pod's request file, written by the
// orchestrator from the driver spec's mcp block (shared/connectors/mcp.py).
type mcpBlock struct {
	// "http", or "stdio": the upstream is SRW's stdio bridge, which runs
	// one process of the server per binding (D5b).
	Transport string `json:"transport"`
	Upstream  string `json:"upstream"`
	Protocol  string `json:"protocol"`
	Tools     struct {
		Read []string `json:"read"`
	} `json:"tools"`
	Access                map[string][]string `json:"access"`
	Credential            *credentialRule     `json:"credential"`
	MaxInFlightPerBinding int                 `json:"max_in_flight_per_binding"`
	ToolPinning           string              `json:"tool_pinning"`
}

type requestFile struct {
	ProtocolVersion string `json:"protocol_version"`
	Plane           string `json:"plane"`
	Driver          string `json:"driver"`
	Connector       struct {
		ID string `json:"id"`
	} `json:"connector"`
	Service struct {
		Port int `json:"port"`
	} `json:"service"`
	Exchange struct {
		URL string `json:"url"`
	} `json:"exchange"`
	MCP *mcpBlock `json:"mcp"`
}

type config struct {
	// The upstream is SRW's stdio bridge: each request names its binding,
	// a binding's end stops its process, and readiness is probed in the
	// background (each probe starts a process).
	bridge      bool
	driver      string
	connectorID string
	exchangeURL string
	identity    string
	port        string
	upstream    *url.URL
	readTools   []string
	access      map[string]map[string]bool
	credential  *credentialRule
	maxInFlight int
	toolPinning string
}

// loadConfig reads the pod's request file and identity, which the
// orchestrator mounted from the pod's immutable Secret.
func loadConfig(getenv func(string) string) (*config, error) {
	path := getenv("SRW_REQUEST_FILE")
	if path == "" {
		path = defaultRequestFile
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("the request file: %w", err)
	}
	if len(raw) > maxRequestFile {
		return nil, errors.New("the request file is too large")
	}
	var request requestFile
	if err := json.Unmarshal(raw, &request); err != nil {
		return nil, errors.New("the request file is not a JSON object")
	}
	identityPath := getenv("SRW_DRIVER_IDENTITY_FILE")
	if identityPath == "" {
		identityPath = defaultIdentityFile
	}
	identity, err := os.ReadFile(identityPath)
	if err != nil {
		return nil, fmt.Errorf("the identity file: %w", err)
	}
	cfg, err := parseConfig(request, strings.TrimSpace(string(identity)))
	if err != nil {
		return nil, err
	}
	if exchange := getenv("SRW_EXCHANGE_URL"); exchange != "" {
		cfg.exchangeURL = strings.TrimRight(exchange, "/")
	}
	if port := getenv("SRW_DRIVER_PORT"); port != "" {
		cfg.port = port
	}
	return cfg, nil
}

func parseConfig(request requestFile, identity string) (*config, error) {
	if !strings.HasPrefix(request.ProtocolVersion, "1.") {
		return nil, fmt.Errorf("protocol_version %q is not one this front speaks", request.ProtocolVersion)
	}
	if request.Plane != "service" || request.MCP == nil {
		return nil, errors.New("the request is not a managed MCP service pod's")
	}
	if !identityShape.MatchString(identity) {
		return nil, errors.New("the identity file holds no sdi_ driver identity")
	}
	if request.Connector.ID == "" {
		return nil, errors.New("the request names no connector")
	}
	exchange, err := url.Parse(request.Exchange.URL)
	if err != nil || exchange.Scheme != "http" || exchange.Host == "" {
		return nil, errors.New("the request names no lease exchange")
	}
	block := request.MCP
	upstream, err := url.Parse(block.Upstream)
	if err != nil || upstream.Scheme != "http" || !loopback(upstream.Hostname()) || upstream.Port() == "" {
		// The front only ever forwards to the server beside it.
		return nil, errors.New("the mcp upstream must be http on the pod's loopback address")
	}
	port := strconv.Itoa(request.Service.Port)
	if request.Service.Port < 1 || request.Service.Port > 65535 || port == upstream.Port() {
		return nil, errors.New("the front's port must be a port of its own")
	}
	for _, pattern := range block.Tools.Read {
		if !patternShape.MatchString(pattern) {
			return nil, fmt.Errorf("%q is no tool name or pattern", pattern)
		}
	}
	if len(block.Access) == 0 {
		return nil, errors.New("the mcp block names no access level")
	}
	access := map[string]map[string]bool{}
	for level, classes := range block.Access {
		access[level] = map[string]bool{}
		for _, class := range classes {
			if class != classRead && class != classWrite {
				return nil, fmt.Errorf("access level %q names an unknown tool class %q", level, class)
			}
			access[level][class] = true
		}
	}
	bridge := false
	switch block.Transport {
	case "", "http":
	case "stdio":
		bridge = true
	default:
		return nil, fmt.Errorf("mcp transport %q is not one the front serves", block.Transport)
	}
	switch {
	case block.Credential == nil:
	case bridge:
		// The bridge delivers it in the binding process's environment: the
		// front only hands it over.
		if block.Credential.Env == "" || block.Credential.Header != "" || block.Credential.Scheme != "" {
			return nil, errors.New("a stdio server's credential is an environment variable of its process")
		}
	default:
		header := http.CanonicalHeaderKey(block.Credential.Header)
		if block.Credential.Env != "" || !headerShape.MatchString(block.Credential.Header) || reservedHeaders[header] {
			return nil, errors.New("the mcp credential header is no header name the front may set")
		}
	}
	inFlight := block.MaxInFlightPerBinding
	if inFlight < 1 {
		inFlight = 4
	}
	pinning := block.ToolPinning
	if pinning != "block" {
		pinning = "warn"
	}
	return &config{
		bridge:      bridge,
		driver:      request.Driver,
		connectorID: strings.ToLower(request.Connector.ID),
		exchangeURL: strings.TrimRight(request.Exchange.URL, "/"),
		identity:    identity,
		port:        port,
		upstream:    upstream,
		readTools:   append([]string(nil), block.Tools.Read...),
		access:      access,
		credential:  block.Credential,
		maxInFlight: inFlight,
		toolPinning: pinning,
	}, nil
}

func loopback(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// patternMatches reports whether a tool name matches a class pattern: '*'
// matches any run of characters, everything else itself. The orchestrator
// applies the same rule (shared/connectors/mcp.py pattern_matches).
func patternMatches(pattern, name string) bool {
	parts := strings.Split(pattern, "*")
	if len(parts) == 1 {
		return pattern == name
	}
	if !strings.HasPrefix(name, parts[0]) {
		return false
	}
	rest := name[len(parts[0]):]
	last := parts[len(parts)-1]
	for _, part := range parts[1 : len(parts)-1] {
		index := strings.Index(rest, part)
		if index < 0 {
			return false
		}
		rest = rest[index+len(part):]
	}
	return len(rest) >= len(last) && strings.HasSuffix(rest, last)
}

// toolClass is "read" when a read pattern names the tool, else "write": a
// tool the server adds later stays hidden from read-only bindings.
func (c *config) toolClass(name string) string {
	for _, pattern := range c.readTools {
		if patternMatches(pattern, name) {
			return classRead
		}
	}
	return classWrite
}

// allowed reports whether a binding at access may see and call a tool. An
// access level the spec does not name sees nothing.
func (c *config) allowed(name, access string) bool {
	return c.access[access][c.toolClass(name)]
}
