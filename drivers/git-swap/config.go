package main

import (
	"bytes"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
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
)

// An sdi_ driver identity and an scl_ lease token: the prefix, 43 base62
// characters and a 6-character checksum (shared/connectors/leases.py).
var (
	identityShape  = regexp.MustCompile(`\Asdi_[0-9A-Za-z]{49}\z`)
	leaseShape     = regexp.MustCompile(`\Ascl_[0-9A-Za-z]{49}\z`)
	connectorShape = regexp.MustCompile(`\A[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\z`)
)

// requestFile is the pod's /run/srw/request.json, written by the
// orchestrator (connector_service_launch.service_request).
type requestFile struct {
	ProtocolVersion string `json:"protocol_version"`
	Plane           string `json:"plane"`
	Driver          string `json:"driver"`
	Connector       struct {
		ID     string `json:"id"`
		Config struct {
			Upstream   string `json:"upstream"`
			Host       string `json:"host"`
			UpstreamCA string `json:"upstream_ca"`
		} `json:"config"`
	} `json:"connector"`
	Service struct {
		Port int `json:"port"`
	} `json:"service"`
	Exchange struct {
		URL string `json:"url"`
	} `json:"exchange"`
	TLS *struct {
		CertFile string `json:"cert_file"`
		KeyFile  string `json:"key_file"`
	} `json:"tls"`
}

type config struct {
	driver      string
	connectorID string
	upstream    upstream
	// The only roots the upstream's certificate is checked against, when
	// the connector names a CA (a forge behind a private CA); nil means the
	// system's public roots.
	upstreamRoots *x509.CertPool
	exchangeURL   string
	identity      string
	port          string
	certFile      string
	keyFile       string
}

// loadConfig reads the pod's request file and identity, which the
// orchestrator mounted from the pod's immutable Secret (the shim names
// them in the environment).
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
		return nil, fmt.Errorf("protocol_version %q is not one this driver speaks", request.ProtocolVersion)
	}
	if request.Plane != "service" {
		return nil, errors.New("the request is not a service pod's")
	}
	if !identityShape.MatchString(identity) {
		return nil, errors.New("the identity file holds no sdi_ driver identity")
	}
	connector := strings.ToLower(request.Connector.ID)
	if !connectorShape.MatchString(connector) {
		return nil, errors.New("the request names no connector")
	}
	served, err := parseUpstream(request.Connector.Config.Upstream)
	if err != nil {
		return nil, fmt.Errorf("the connector's upstream: %w", err)
	}
	// The orchestrator writes the clean form; anything else is not the URL
	// it checked, so the driver serves nothing.
	if served.url != request.Connector.Config.Upstream || served.host != request.Connector.Config.Host {
		return nil, errors.New("the connector's upstream is not in its clean form")
	}
	exchange, err := url.Parse(request.Exchange.URL)
	if err != nil || exchange.Scheme != "http" || exchange.Host == "" {
		return nil, errors.New("the request names no lease exchange")
	}
	if request.Service.Port < 1 || request.Service.Port > 65535 {
		return nil, errors.New("the request names no service port")
	}
	if request.TLS == nil || request.TLS.CertFile == "" || request.TLS.KeyFile == "" {
		// Workspaces reach the driver over TLS only (SRW's driver CA).
		return nil, errors.New("the request names no TLS certificate")
	}
	roots, err := upstreamRoots(request.Connector.Config.UpstreamCA)
	if err != nil {
		return nil, err
	}
	return &config{
		driver:        request.Driver,
		connectorID:   connector,
		upstream:      served,
		upstreamRoots: roots,
		exchangeURL:   strings.TrimRight(request.Exchange.URL, "/"),
		identity:      identity,
		port:          strconv.Itoa(request.Service.Port),
		certFile:      request.TLS.CertFile,
		keyFile:       request.TLS.KeyFile,
	}, nil
}

// upstreamRoots is the connector's upstream CA as a pool: PEM certificates
// and nothing else, or nil (public roots) when it names none.
func upstreamRoots(text string) (*x509.CertPool, error) {
	if strings.TrimSpace(text) == "" {
		return nil, nil
	}
	pool := x509.NewCertPool()
	rest := []byte(text)
	found := 0
	for {
		// pem.Decode skips any text before a block: only whitespace may
		// come between blocks, as SRW's validation of the connector holds.
		rest = bytes.TrimLeft(rest, " \t\r\n")
		if len(rest) == 0 {
			break
		}
		if !bytes.HasPrefix(rest, []byte("-----BEGIN ")) {
			return nil, fmt.Errorf("%w: it holds text that is not a PEM certificate", errBadUpstreamCA)
		}
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			return nil, fmt.Errorf("%w: a PEM block does not decode", errBadUpstreamCA)
		}
		if block.Type != "CERTIFICATE" || len(block.Headers) != 0 {
			return nil, fmt.Errorf("%w: it holds a %s block", errBadUpstreamCA, block.Type)
		}
		certificate, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", errBadUpstreamCA, err)
		}
		pool.AddCert(certificate)
		found++
	}
	if found == 0 {
		return nil, fmt.Errorf("%w: it holds no certificate", errBadUpstreamCA)
	}
	return pool, nil
}

// errBadUpstreamCA: the connector's upstream CA is not PEM certificates.
// The driver reports it as it reports an upstream it cannot trust (exit
// code 78), so SRW stops the pod at once instead of a crash loop.
var errBadUpstreamCA = errors.New("the connector's upstream CA is not usable")
