package main

import (
	"errors"
	"net"
	"net/url"
	"regexp"
	"strings"
)

// The repository URLs the driver serves, as shared/connectors/git_swap.py
// decides them (testdata/upstream_vectors.json holds the cases both are
// tested against): HTTPS on port 443, no credentials, no query or fragment,
// a plain repository path.

const (
	upstreamPort = "443"
	maxSegments  = 16
)

var (
	segmentShape  = regexp.MustCompile(`\A[A-Za-z0-9._~-]{1,128}\z`)
	hostnameShape = regexp.MustCompile(
		`\A[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)*\z`)
)

// upstream is a repository the driver serves: url is its clean form (a
// checkout's remote), path what a request's repository path must equal.
type upstream struct {
	url  string
	host string
	path string
}

func parseUpstream(raw string) (upstream, error) {
	text := strings.TrimSpace(raw)
	if text == "" {
		return upstream{}, errors.New("the connector has no repository URL")
	}
	for _, r := range text {
		if r <= 0x20 || r == 0x7f || strings.ContainsRune("\\\"'`", r) || isSpace(r) {
			return upstream{}, errors.New("the repository URL has characters it may not have")
		}
	}
	if strings.ContainsAny(text, "?#") {
		return upstream{}, errors.New("the repository URL has a query or a fragment")
	}
	if strings.Contains(text, "%") {
		// Nothing the driver serves is escaped: a plain path, a plain host.
		return upstream{}, errors.New("the repository path has a segment it may not have")
	}
	parsed, err := url.Parse(text)
	if err != nil {
		return upstream{}, errors.New("the repository URL does not parse")
	}
	if strings.ToLower(parsed.Scheme) != "https" {
		return upstream{}, errors.New("the repository URL is not HTTPS")
	}
	if parsed.User != nil || strings.Contains(parsed.Host, "@") {
		return upstream{}, errors.New("the repository URL carries credentials")
	}
	if parsed.Opaque != "" || parsed.RawPath != "" {
		return upstream{}, errors.New("the repository path has a segment it may not have")
	}
	if port := parsed.Port(); port != "" && port != upstreamPort {
		return upstream{}, errors.New("the repository is not on port 443")
	}
	host := strings.ToLower(parsed.Hostname())
	if !servableHost(host) {
		return upstream{}, errors.New("the repository URL has no host the driver can pin")
	}
	trimmed := strings.Trim(parsed.Path, "/")
	if strings.HasPrefix(parsed.Path, "//") || strings.Contains(trimmed, "//") {
		return upstream{}, errors.New("the repository path has an empty segment")
	}
	if trimmed == "" {
		return upstream{}, errors.New("the repository URL names no repository path")
	}
	segments := strings.Split(trimmed, "/")
	if len(segments) > maxSegments {
		return upstream{}, errors.New("the repository URL names no repository path")
	}
	for _, segment := range segments {
		if !segmentShape.MatchString(segment) || segment == "." || segment == ".." {
			return upstream{}, errors.New("the repository path has a segment it may not have")
		}
	}
	path := strings.Join(segments, "/")
	stripped := strings.TrimSuffix(path, ".git")
	last := stripped[strings.LastIndex(stripped, "/")+1:]
	if stripped == "" || strings.HasSuffix(stripped, "/") || last == "" || last == "." {
		return upstream{}, errors.New("the repository URL names no repository path")
	}
	return upstream{url: "https://" + host + "/" + path, host: host, path: stripped}, nil
}

func servableHost(host string) bool {
	if ip := net.ParseIP(host); ip != nil {
		// A literal address is pinned as given; IPv6 literals are not served.
		return ip.To4() != nil && !strings.Contains(host, ":")
	}
	return len(host) <= 253 && hostnameShape.MatchString(host)
}

// isSpace is Python's str.isspace for the characters a URL could carry.
func isSpace(r rune) bool {
	switch r {
	case 0x85, 0xa0, 0x1680, 0x2028, 0x2029, 0x202f, 0x205f, 0x3000:
		return true
	}
	return r >= 0x2000 && r <= 0x200a
}
