// Package api is the HTTP client of srw: one place for the transport rules,
// the retry rules and the mapping from a failed request to an exit code.
package api

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/superhuman-remote-worker/srw/cli/internal/exit"
	"github.com/superhuman-remote-worker/srw/cli/internal/remote"
)

// Deadlines per request. A cancel has been seen to take more than 30 s, so
// mutations get longer than reads.
const (
	ReadTimeout     = 30 * time.Second
	MutationTimeout = 60 * time.Second
	// TeardownTimeout is for cancelling and deleting a job. The server
	// releases the job's workspace before it answers, which has taken more
	// than a minute on a busy cluster.
	TeardownTimeout = 3 * time.Minute
	// getAttempts is the total number of tries for a GET.
	getAttempts = 3
	maxBody     = 64 << 20
)

// Client talks to one server with one token.
type Client struct {
	base    *url.URL
	token   string
	http    *http.Client
	agent   string
	pauses  []time.Duration
	sleep   func(context.Context, time.Duration) error
	timeout func(method string) time.Duration
}

// Response is a complete HTTP response.
type Response struct {
	Status int
	Header http.Header
	Body   []byte
}

// Options change the transport. The zero value is what a command wants.
type Options struct {
	// Transport replaces the default transport (tests).
	Transport http.RoundTripper
	// Proxy replaces the proxy function (tests; the default reads the
	// proxy variables once per process and never applies them to loopback).
	Proxy func(*http.Request) (*url.URL, error)
	// Pauses between the tries of a GET; the default is 1 s, then 2 s.
	Pauses []time.Duration
}

// New builds the client for a target.
func New(to *remote.Target, version string, options Options) (*Client, error) {
	transport := options.Transport
	if transport == nil {
		// Clone the default: proxy variables, HTTP/2 and the dial timeouts
		// stay as Go ships them.
		base := http.DefaultTransport.(*http.Transport).Clone()
		if options.Proxy != nil {
			base.Proxy = options.Proxy
		}
		if to.CAFile != "" {
			pool, err := loadCAFile(to.CAFile)
			if err != nil {
				return nil, err
			}
			base.TLSClientConfig = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
		}
		transport = base
	}
	pauses := options.Pauses
	if pauses == nil {
		pauses = []time.Duration{time.Second, 2 * time.Second}
	}
	return &Client{
		base:  to.Server,
		token: to.Token,
		agent: "srw/" + version,
		http: &http.Client{
			Transport: transport,
			// A redirect is never followed: Go would keep the Authorization
			// header within the same domain. It is reported instead.
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		pauses: pauses,
		sleep: func(ctx context.Context, pause time.Duration) error {
			timer := time.NewTimer(pause)
			defer timer.Stop()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-timer.C:
				return nil
			}
		},
		timeout: func(method string) time.Duration {
			if isRead(method) {
				return ReadTimeout
			}
			return MutationTimeout
		},
	}, nil
}

// loadCAFile reads a PEM bundle that replaces the system roots.
func loadCAFile(path string) (*x509.CertPool, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, exit.Usagef("cannot read the CA file %s: %v", path, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(data) {
		return nil, exit.Usagef("the CA file %s holds no certificate", path)
	}
	return pool, nil
}

func isRead(method string) bool {
	return method == http.MethodGet || method == http.MethodHead
}

// Server is the base URL the client talks to.
func (c *Client) Server() string { return c.base.String() }

// Redact removes the token from a text. Nothing srw prints may carry it.
func (c *Client) Redact(text string) string {
	if c.token == "" {
		return text
	}
	return strings.ReplaceAll(text, c.token, "<token>")
}

// Request is one call. Path starts with a slash and is appended to the base
// URL. Body is sent as JSON when it is not nil.
type Request struct {
	Method string
	Path   string
	Query  url.Values
	Body   any
	// RawBody is sent as it is, with ContentType; it wins over Body.
	RawBody     []byte
	ContentType string
	Header      http.Header
	// NoTimeout drops the per-request deadline (wait, logs -f).
	NoTimeout bool
	// Timeout replaces the method's deadline; 0 keeps it.
	Timeout time.Duration
	// Known marks a route from srw's own table. When the server does not
	// have it, the server is probably older than this srw.
	Known bool
}

// Do sends a request and returns the response whatever its status. A GET is
// tried up to three times on a connection error or a timeout, and on nothing
// else. A mutation is never repeated: when its response is lost the outcome
// is unknown, and the error says so.
func (c *Client) Do(ctx context.Context, request Request) (*Response, error) {
	if !strings.HasPrefix(request.Path, "/") || strings.Contains(request.Path, "://") {
		return nil, exit.Usagef("%q is not a path on the server; it must start with /", request.Path)
	}
	payload := request.RawBody
	contentType := request.ContentType
	if payload == nil && request.Body != nil {
		encoded, err := json.Marshal(request.Body)
		if err != nil {
			return nil, err
		}
		payload, contentType = encoded, "application/json"
	}
	attempts := 1
	if request.Method == http.MethodGet {
		attempts = getAttempts
	}
	var last error
	for attempt := 0; attempt < attempts; attempt++ {
		if attempt > 0 {
			pause := c.pauses[min(attempt-1, len(c.pauses)-1)]
			if err := c.sleep(ctx, pause); err != nil {
				return nil, err
			}
		}
		response, wrote, err := c.once(ctx, request, payload, contentType)
		if err == nil {
			return response, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		last = c.transportError(request.Method, wrote, err)
	}
	return nil, last
}

func (c *Client) once(ctx context.Context, request Request, payload []byte, contentType string) (*Response, bool, error) {
	if !request.NoTimeout {
		var cancel context.CancelFunc
		deadline := request.Timeout
		if deadline == 0 {
			deadline = c.timeout(request.Method)
		}
		ctx, cancel = context.WithTimeout(ctx, deadline)
		defer cancel()
	}
	// A request that never left the machine cannot have had an effect.
	var wrote atomic.Bool
	ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
		WroteHeaders: func() { wrote.Store(true) },
	})
	target := *c.base
	target.Path = strings.TrimRight(c.base.Path, "/") + request.Path
	target.RawQuery = request.Query.Encode()
	var body io.Reader
	if payload != nil {
		body = bytes.NewReader(payload)
	}
	outgoing, err := http.NewRequestWithContext(ctx, request.Method, target.String(), body)
	if err != nil {
		return nil, false, err
	}
	for name, values := range request.Header {
		for _, value := range values {
			outgoing.Header.Add(name, value)
		}
	}
	if contentType != "" {
		outgoing.Header.Set("Content-Type", contentType)
	}
	if outgoing.Header.Get("Accept") == "" {
		outgoing.Header.Set("Accept", "application/json")
	}
	outgoing.Header.Set("User-Agent", c.agent)
	if c.token != "" {
		outgoing.Header.Set("Authorization", "Bearer "+c.token)
	}
	incoming, err := c.http.Do(outgoing)
	if err != nil {
		return nil, wrote.Load(), err
	}
	defer incoming.Body.Close()
	data, err := io.ReadAll(io.LimitReader(incoming.Body, maxBody+1))
	if err != nil {
		return nil, true, err
	}
	if len(data) > maxBody {
		return nil, true, fmt.Errorf("the response is larger than %d MiB", maxBody>>20)
	}
	return &Response{Status: incoming.StatusCode, Header: incoming.Header, Body: data}, true, nil
}

// transportError turns a failed round trip into an exit code. Exit 7: the
// server was not reached, or TLS failed. Exit 6: a mutation was sent and no
// response arrived, so nobody knows whether it took effect.
func (c *Client) transportError(method string, wrote bool, err error) error {
	message := c.Redact(err.Error())
	var certificate *tls.CertificateVerificationError
	var unknownAuthority x509.UnknownAuthorityError
	var hostname x509.HostnameError
	switch {
	case errors.As(err, &certificate), errors.As(err, &unknownAuthority), errors.As(err, &hostname):
		return exit.New(exit.Unreachable, "the server's TLS certificate was not accepted: %s", message).
			WithHint("for a private certificate authority pass --ca-file, or set ca-file in the context")
	case !isRead(method) && wrote:
		return exit.New(exit.Unknown, "the request was sent but no response arrived: %s", message).
			WithHint("the change may or may not have happened; check before sending it again")
	}
	// What is left can pass by itself: the server restarts, the network
	// comes back.
	failure := exit.New(exit.Unreachable, "cannot reach %s: %s", c.base.Host, message)
	var dns *net.DNSError
	if errors.As(err, &dns) {
		failure = exit.New(exit.Unreachable, "cannot resolve %s", c.base.Hostname())
	} else if errors.Is(err, context.DeadlineExceeded) {
		failure = exit.New(exit.Unreachable, "%s did not answer in time", c.base.Host)
	}
	failure.Away = true
	return failure
}

// Send performs a request and fails for every status that is not 2xx.
func (c *Client) Send(ctx context.Context, request Request) (*Response, error) {
	response, err := c.Do(ctx, request)
	if err != nil {
		return nil, err
	}
	if err := c.StatusError(request, response); err != nil {
		return nil, err
	}
	return response, nil
}

// JSON performs a request and decodes a 2xx body into out.
func (c *Client) JSON(ctx context.Context, request Request, out any) error {
	response, err := c.Send(ctx, request)
	if err != nil {
		return err
	}
	if out == nil {
		return nil
	}
	if err := json.Unmarshal(response.Body, out); err != nil {
		// A web server in front of the API answers a page for any path,
		// also while the API restarts behind it.
		failure := exit.New(exit.Failed, "%s %s did not answer JSON; is %s the API address?", request.Method, request.Path, c.base)
		failure.Away = true
		return failure
	}
	return nil
}
