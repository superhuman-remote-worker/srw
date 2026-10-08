package main

import (
	"net/http"
	"strings"
)

// The only routes the driver serves (gitprotocol-http(5), smart HTTP):
//
//	GET  /<connector>/<repository>/info/refs?service=git-upload-pack|git-receive-pack
//	POST /<connector>/<repository>/git-upload-pack
//	POST /<connector>/<repository>/git-receive-pack
//
// and Git LFS's paths, refused with a message. Everything else is a 404:
// the dumb protocol, web pages, the forge's API. The service a request
// uses is decided by its path; the "service" query parameter only says
// which advertisement a discovery GET asks for (a POST to git-receive-pack
// with service=git-upload-pack in its query is a push: Gogs CVE-2026-52810).

const (
	uploadPack  = "git-upload-pack"
	receivePack = "git-receive-pack"
)

type endpoint int

const (
	endpointRefs endpoint = iota + 1
	endpointRPC
	endpointLFS
)

type route struct {
	endpoint endpoint
	service  string // uploadPack or receivePack
	query    bool   // a POST that carried a query (refused once access is decided)
}

// writes reports whether the request is either form of a push.
func (r route) writes() bool { return r.service == receivePack }

// routeError is a refusal before anything is checked against a lease.
type routeError struct {
	status  int
	message string
}

const notServed = "not found: SRW's git swap driver serves only git's smart HTTP protocol for its connector's repository"

// parseRoute decides what a request is from its method, its exact path and,
// for discovery only, its exact query. The path is taken as sent: any
// escape, empty or dot segment is a 404, so nothing the driver checks
// differs from what it forwards (it forwards no part of the request path).
func parseRoute(r *http.Request, cfg *config) (route, *routeError) {
	raw := r.RequestURI
	if index := strings.IndexByte(raw, '?'); index >= 0 {
		raw = raw[:index]
	}
	path := r.URL.Path
	if raw != path || strings.ContainsAny(raw, "%\\") || !strings.HasPrefix(path, "/") {
		return route{}, &routeError{http.StatusNotFound, notServed}
	}
	segments := strings.Split(path[1:], "/")
	for _, segment := range segments {
		if !segmentShape.MatchString(segment) || segment == "." || segment == ".." {
			return route{}, &routeError{http.StatusNotFound, notServed}
		}
	}
	if len(segments) < 3 || segments[0] != cfg.connectorID {
		return route{}, &routeError{http.StatusNotFound, notServed}
	}
	for i := 2; i+1 < len(segments); i++ {
		if segments[i] == "info" && segments[i+1] == "lfs" {
			return route{endpoint: endpointLFS}, nil
		}
	}
	var found route
	var repository []string
	last := segments[len(segments)-1]
	switch {
	case last == uploadPack || last == receivePack:
		found = route{endpoint: endpointRPC, service: last}
		repository = segments[1 : len(segments)-1]
	case last == "refs" && segments[len(segments)-2] == "info":
		found = route{endpoint: endpointRefs}
		repository = segments[1 : len(segments)-2]
	default:
		return route{}, &routeError{http.StatusNotFound, notServed}
	}
	if len(repository) == 0 || strings.TrimSuffix(strings.Join(repository, "/"), ".git") != cfg.upstream.path {
		return route{}, &routeError{http.StatusNotFound, notServed}
	}
	switch found.endpoint {
	case endpointRefs:
		if r.Method != http.MethodGet {
			return route{}, &routeError{http.StatusMethodNotAllowed, "info/refs is fetched with GET"}
		}
		switch r.URL.RawQuery {
		case "service=" + uploadPack:
			found.service = uploadPack
		case "service=" + receivePack:
			found.service = receivePack
		default:
			// No service is the dumb protocol, which the driver does not serve.
			return route{}, &routeError{http.StatusForbidden, "only git's smart HTTP protocol is served: info/refs?service=git-upload-pack or git-receive-pack"}
		}
	case endpointRPC:
		if r.Method != http.MethodPost {
			return route{}, &routeError{http.StatusMethodNotAllowed, found.service + " is called with POST"}
		}
		found.query = r.URL.RawQuery != "" || strings.Contains(r.RequestURI, "?")
	}
	return found, nil
}
