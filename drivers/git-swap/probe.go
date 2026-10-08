package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

const (
	// upstreamExitCode tells SRW's reconciler that the driver cannot use
	// its upstream (connector_service_hosting.UPSTREAM_EXIT_CODE): it stops
	// the pod at once and deliveries take the installation's fallback.
	upstreamExitCode = 78
	// The container's termination message: why, in one line.
	terminationLog = "/dev/termination-log"
	probeAttempts  = 4
	probePause     = 5 * time.Second
	maxProbeReport = 300
)

// probeUpstream checks, before the driver serves, that the configured
// upstream answers HTTPS with a certificate the driver trusts: a GET of its
// ref advertisement without any credential. Any HTTP answer will do (a
// 401, a 404, a redirect: the forge is there and verified). A certificate
// that does not verify is final at once; a connection that fails is tried
// attempts times. It returns why the upstream cannot be used, or "".
func probeUpstream(ctx context.Context, client *http.Client, served upstream, attempts int, pause time.Duration) string {
	target := served.url + "/info/refs?service=git-upload-pack"
	var last error
	for i := 0; i < attempts; i++ {
		if i > 0 {
			select {
			case <-ctx.Done():
				return report("the upstream %s could not be checked: %v", served.host, ctx.Err())
			case <-time.After(pause):
			}
		}
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
		if err != nil {
			return report("the upstream URL is not usable: %v", err)
		}
		request.Header.Set("User-Agent", "git/srw-git-swap-probe")
		response, err := client.Do(request)
		if err == nil {
			io.Copy(io.Discard, io.LimitReader(response.Body, 64<<10))
			response.Body.Close()
			return ""
		}
		if untrusted(err) {
			return report("the certificate of %s does not verify: %v", served.host, rootCause(err))
		}
		last = err
	}
	return report("the upstream %s is unreachable: %v", served.host, rootCause(last))
}

func untrusted(err error) bool {
	var verification *tls.CertificateVerificationError
	var unknown x509.UnknownAuthorityError
	var hostname x509.HostnameError
	var invalid x509.CertificateInvalidError
	return errors.As(err, &verification) || errors.As(err, &unknown) ||
		errors.As(err, &hostname) || errors.As(err, &invalid)
}

// rootCause drops the request line net/http wraps an error in.
func rootCause(err error) error {
	var wrapped *url.Error
	if errors.As(err, &wrapped) {
		return wrapped.Err
	}
	return err
}

func report(format string, a ...any) string {
	text := strings.Join(strings.Fields(fmt.Sprintf(format, a...)), " ")
	if len(text) > maxProbeReport {
		text = text[:maxProbeReport-3] + "..."
	}
	return text
}

// writeTermination leaves the reason where Kubernetes reads a container's
// termination message (best effort: the log line says it too).
func writeTermination(path, message string) {
	_ = os.WriteFile(path, []byte(message+"\n"), 0o644)
}
