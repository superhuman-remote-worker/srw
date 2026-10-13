package main

import (
	"encoding/json"
	"errors"
	"io"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
)

const driverName = "srw.gpt-live-sip/v1"
const maxJSON = 1024 * 1024

var identityShape = regexp.MustCompile(`\Asdi_[0-9A-Za-z]{49}\z`)
var leaseShape = regexp.MustCompile(`\Ascl_[0-9A-Za-z]{49}\z`)
var controlShape = regexp.MustCompile(`\A[A-Za-z0-9_-]{32,}\z`)

type limits struct {
	MaxDuration int `json:"max_duration_s"`
	Fallback    int `json:"no_job_fallback_s"`
}
type connectorConfig struct {
	Model          string `json:"model"`
	MaxDuration    *int   `json:"max_duration_s"`
	Fallback       *int   `json:"no_job_fallback_s"`
	TransferTarget string `json:"transfer_target"`
	APIHost        string `json:"api_host"`
	APIPort        int    `json:"api_port"`
}
type requestFile struct {
	ProtocolVersion string `json:"protocol_version"`
	Plane           string `json:"plane"`
	Driver          string `json:"driver"`
	Connector       struct {
		ID     string          `json:"id"`
		Config connectorConfig `json:"config"`
	} `json:"connector"`
	Service struct {
		Port int `json:"port"`
	} `json:"service"`
	Exchange struct {
		URL          string `json:"url"`
		IdentityFile string `json:"identity_file"`
	} `json:"exchange"`
	Control struct {
		Token string `json:"token"`
	} `json:"control"`
}
type config struct {
	connectorID, exchangeURL, identity, controlToken, port, transferTarget string
	limits                                                                 limits
}

func loadConfig(getenv func(string) string) (*config, error) {
	path := getenv("SRW_REQUEST_FILE")
	if path == "" {
		path = "/run/srw/request.json"
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, errors.New("cannot read request file")
	}
	defer f.Close()
	raw, err := io.ReadAll(io.LimitReader(f, maxJSON+1))
	if err != nil || len(raw) > maxJSON {
		return nil, errors.New("request file is unreadable or too large")
	}
	var r requestFile
	if json.Unmarshal(raw, &r) != nil {
		return nil, errors.New("request file must be a JSON object")
	}
	if v := getenv("SRW_EXCHANGE_URL"); v != "" {
		r.Exchange.URL = v
	}
	if v := getenv("SRW_DRIVER_PORT"); v != "" {
		r.Service.Port, err = strconv.Atoi(v)
		if err != nil {
			return nil, errors.New("invalid driver port")
		}
	}
	ip := getenv("SRW_DRIVER_IDENTITY_FILE")
	if ip == "" {
		ip = r.Exchange.IdentityFile
	}
	if ip == "" {
		ip = "/run/srw/identity"
	}
	identity, err := os.ReadFile(ip)
	if err != nil {
		return nil, errors.New("cannot read identity file")
	}
	return parseConfig(r, strings.TrimSpace(string(identity)))
}
func parseConfig(r requestFile, identity string) (*config, error) {
	if !regexp.MustCompile(`\A1\.[0-9]+\z`).MatchString(r.ProtocolVersion) {
		return nil, errors.New("unsupported protocol_version: expected 1.x")
	}
	if r.Plane != "service" {
		return nil, errors.New("plane must be service")
	}
	if r.Driver != driverName {
		return nil, errors.New("driver must be srw.gpt-live-sip/v1")
	}
	if strings.TrimSpace(r.Connector.ID) == "" {
		return nil, errors.New("connector id is required")
	}
	u, err := url.Parse(r.Exchange.URL)
	if err != nil || u.Scheme != "http" || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, errors.New("exchange URL must be http://host")
	}
	if !identityShape.MatchString(identity) {
		return nil, errors.New("identity file holds no sdi_ driver identity")
	}
	if !controlShape.MatchString(r.Control.Token) {
		return nil, errors.New("control.token must be base64url and at least 32 characters")
	}
	if r.Service.Port < 1 || r.Service.Port > 65535 {
		return nil, errors.New("service port must be 1..65535")
	}
	l := limits{900, 8}
	if r.Connector.Config.MaxDuration != nil {
		l.MaxDuration = *r.Connector.Config.MaxDuration
	}
	if r.Connector.Config.Fallback != nil {
		l.Fallback = *r.Connector.Config.Fallback
	}
	if !l.valid() {
		return nil, errors.New("invalid connector limits: duration 60..14400, fallback 1..120")
	}
	return &config{connectorID: r.Connector.ID, exchangeURL: strings.TrimRight(r.Exchange.URL, "/"), identity: identity, controlToken: r.Control.Token, port: strconv.Itoa(r.Service.Port), transferTarget: r.Connector.Config.TransferTarget, limits: l}, nil
}
func (l limits) valid() bool {
	return l.MaxDuration >= 60 && l.MaxDuration <= 14400 && l.Fallback >= 1 && l.Fallback <= 120
}
