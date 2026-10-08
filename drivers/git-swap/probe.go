package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"syscall"
	"time"
	"unicode"
)

const (
	// upstreamExitCode tells SRW's reconciler that the driver cannot use
	// its upstream (connector_service_hosting.UPSTREAM_EXIT_CODE): it stops
	// the pod at once and deliveries take the installation's fallback.
	upstreamExitCode = 78
	probeAttempts    = 4
	probePause       = 5 * time.Second
	maxProbeReport   = 300
	// The termination message's classes (connector_git_swap_delivery reads
	// the prefix): only these words, never the upstream's own text (a
	// certificate's names, an error string), which may say anything.
	untrustedPrefix   = "untrusted certificate: "
	unreachablePrefix = "unreachable: "
	badCAReport       = "upstream CA unusable: the connector's upstream CA is not PEM certificates"
)

// terminationLog is the container's termination message: why, in one line
// (a variable so a test can point it elsewhere).
var terminationLog = "/dev/termination-log"

// probeUpstream checks, before the driver serves, that the configured
// upstream answers HTTPS with a certificate the driver trusts: a GET of its
// ref advertisement without any credential. Any HTTP answer will do (a
// 401, a 404, a redirect: the forge is there and verified). A certificate
// that does not verify is final at once; a connection that fails is tried
// attempts times. It returns the report for the termination message (a
// fixed class, "" when the upstream serves) and the raw detail for the
// pod's log, cleaned.
func probeUpstream(ctx context.Context, client *http.Client, served upstream, attempts int, pause time.Duration) (string, string) {
	target := served.url + "/info/refs?service=git-upload-pack"
	var last error
	for i := 0; i < attempts; i++ {
		if i > 0 {
			select {
			case <-ctx.Done():
				return unreachablePrefix + "not checked", clean(ctx.Err().Error())
			case <-time.After(pause):
			}
		}
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
		if err != nil {
			return unreachablePrefix + "bad URL", clean(err.Error())
		}
		request.Header.Set("User-Agent", "git/srw-git-swap-probe")
		response, err := client.Do(request)
		if err == nil {
			io.Copy(io.Discard, io.LimitReader(response.Body, 64<<10))
			response.Body.Close()
			return "", ""
		}
		if class := untrustedClass(err); class != "" {
			return untrustedPrefix + class, clean(rootCause(err).Error())
		}
		last = err
	}
	return unreachablePrefix + unreachableClass(last), clean(rootCause(last).Error())
}

// untrustedClass names why a certificate did not verify, or "" when err is
// no verification failure.
func untrustedClass(err error) string {
	var unknown x509.UnknownAuthorityError
	var hostname x509.HostnameError
	var invalid x509.CertificateInvalidError
	var verification *tls.CertificateVerificationError
	switch {
	case errors.As(err, &unknown):
		return "unknown authority"
	case errors.As(err, &hostname):
		return "name mismatch"
	case errors.As(err, &invalid):
		if invalid.Reason == x509.Expired {
			return "expired or not yet valid"
		}
		return "invalid"
	case errors.As(err, &verification):
		return "not verified"
	}
	return ""
}

// unreachableClass names why the upstream did not answer.
func unreachableClass(err error) string {
	var dns *net.DNSError
	var timeout interface{ Timeout() bool }
	switch {
	case err == nil:
		return "no answer"
	case errors.As(err, &dns):
		return "dns"
	case errors.Is(err, syscall.ECONNREFUSED):
		return "connection refused"
	case errors.As(err, &timeout) && timeout.Timeout():
		return "timeout"
	case strings.Contains(err.Error(), "tls"):
		return "tls handshake"
	}
	return "connection failed"
}

// rootCause drops the request line net/http wraps an error in.
func rootCause(err error) error {
	var wrapped *url.Error
	if errors.As(err, &wrapped) {
		return wrapped.Err
	}
	return err
}

// clean makes raw text safe to log: no control or format characters (an
// ANSI escape, a bidi override), whitespace collapsed, capped.
func clean(text string) string {
	mapped := strings.Map(func(r rune) rune {
		if unicode.IsControl(r) || unicode.Is(unicode.Cf, r) {
			return ' '
		}
		return r
	}, text)
	mapped = strings.Join(strings.Fields(mapped), " ")
	if runes := []rune(mapped); len(runes) > maxProbeReport {
		mapped = string(runes[:maxProbeReport-3]) + "..."
	}
	return mapped
}

// writeTermination leaves the reason where Kubernetes reads a container's
// termination message (best effort: the log line says it too).
func writeTermination(path, message string) {
	_ = os.WriteFile(path, []byte(message+"\n"), 0o644)
}
