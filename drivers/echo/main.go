// Command srw-driver-echo is a development service driver (connector drivers
// D5): the image behind srw.echo-service/v1, which a k3d gate uses to prove
// service-plane hosting. It is not a product driver.
//
// It serves HTTP on the srw-driver port (SRW_DRIVER_PORT):
//
//	GET  /           the request file's non-secret fields
//	GET  /healthz    200
//	POST /exchange   {"lease_token", "operation"}: calls the lease exchange
//	                 with the pod's sdi_ identity; answers the exchange's
//	                 status and fields, and the credential only as a SHA-256
//	GET  /probe?addr=HOST:PORT   whether a TCP connect from this pod completes
//	GET  /resolve?name=NAME      whether this pod can resolve a name
//	GET  /self       its uid, capabilities, no-new-privs and whether a
//	                 ServiceAccount token is mounted
//
// and a second listener on SRW_ECHO_EXTRA_PORT that nothing outside the pod
// may reach (the gate checks that only the named port is open). Never the
// identity or a credential in a response or a log line.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"time"
)

type server struct {
	request     map[string]any
	identity    string
	exchangeURL string
	client      *http.Client
	dialTimeout time.Duration
	resolver    *net.Resolver
}

func loadServer(getenv func(string) string) (*server, error) {
	requestPath := getenv("SRW_REQUEST_FILE")
	identityPath := getenv("SRW_DRIVER_IDENTITY_FILE")
	if requestPath == "" || identityPath == "" {
		return nil, fmt.Errorf("SRW_REQUEST_FILE and SRW_DRIVER_IDENTITY_FILE are required (run under the SRW shim)")
	}
	raw, err := os.ReadFile(requestPath)
	if err != nil {
		return nil, err
	}
	var request map[string]any
	if err := json.Unmarshal(raw, &request); err != nil || request == nil {
		return nil, fmt.Errorf("the request file is not a JSON object")
	}
	identity, err := os.ReadFile(identityPath)
	if err != nil {
		return nil, err
	}
	exchangeURL := getenv("SRW_EXCHANGE_URL")
	if exchangeURL == "" {
		exchange, _ := request["exchange"].(map[string]any)
		exchangeURL, _ = exchange["url"].(string)
	}
	return &server{
		request:     request,
		identity:    strings.TrimSpace(string(identity)),
		exchangeURL: strings.TrimRight(exchangeURL, "/"),
		client:      &http.Client{Timeout: 10 * time.Second},
		dialTimeout: 2 * time.Second,
		resolver:    net.DefaultResolver,
	}, nil
}

// publicRequest is the request without any secret: credential values become
// their key names, and the identity is never part of the request file.
func (s *server) publicRequest() map[string]any {
	out := map[string]any{}
	for key, value := range s.request {
		if key == "credentials" {
			names := []string{}
			if credentials, ok := value.(map[string]any); ok {
				for name := range credentials {
					names = append(names, name)
				}
			}
			out["credential_keys"] = names
			continue
		}
		out[key] = value
	}
	out["has_identity"] = strings.HasPrefix(s.identity, "sdi_")
	return out
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(body)
}

func (s *server) handleRoot(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	writeJSON(w, http.StatusOK, s.publicRequest())
}

func (s *server) handleExchange(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "POST only"})
		return
	}
	var body struct {
		LeaseToken string `json:"lease_token"`
		Operation  string `json:"operation"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 4096)).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid body"})
		return
	}
	if body.Operation == "" {
		body.Operation = "read"
	}
	payload, _ := json.Marshal(map[string]string{
		"lease_token": body.LeaseToken,
		"operation":   body.Operation,
	})
	request, err := http.NewRequestWithContext(r.Context(), http.MethodPost, s.exchangeURL+"/v1/leases/exchange", bytes.NewReader(payload))
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "bad exchange url"})
		return
	}
	request.Header.Set("Authorization", "Bearer "+s.identity)
	request.Header.Set("Content-Type", "application/json")
	response, err := s.client.Do(request)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "exchange unreachable", "detail": err.Error()})
		return
	}
	defer response.Body.Close()
	var answer map[string]any
	json.NewDecoder(io.LimitReader(response.Body, 64*1024)).Decode(&answer)
	out := map[string]any{"exchange_status": response.StatusCode, "cache_control": response.Header.Get("Cache-Control")}
	for key, value := range answer {
		if key == "credential" {
			text, _ := value.(string)
			digest := sha256.Sum256([]byte(text))
			out["credential_sha256"] = hex.EncodeToString(digest[:])
			continue
		}
		out[key] = value
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *server) handleProbe(w http.ResponseWriter, r *http.Request) {
	addr := r.URL.Query().Get("addr")
	if _, _, err := net.SplitHostPort(addr); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "addr is HOST:PORT"})
		return
	}
	conn, err := net.DialTimeout("tcp", addr, s.dialTimeout)
	if err != nil {
		writeJSON(w, http.StatusOK, map[string]any{"addr": addr, "reachable": false, "error": err.Error()})
		return
	}
	conn.Close()
	writeJSON(w, http.StatusOK, map[string]any{"addr": addr, "reachable": true})
}

func (s *server) handleResolve(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("name")
	if name == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "name is required"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	addresses, err := s.resolver.LookupHost(ctx, name)
	if err != nil {
		writeJSON(w, http.StatusOK, map[string]any{"name": name, "resolved": false, "error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"name": name, "resolved": true, "addresses": addresses})
}

// serviceAccountToken is where Kubernetes mounts a pod's token when it may.
const serviceAccountToken = "/var/run/secrets/kubernetes.io/serviceaccount/token"

// processFacts reads what the kernel says about this process: its user, its
// capabilities and whether it may gain privileges.
func processFacts(status string) map[string]string {
	facts := map[string]string{}
	for _, line := range strings.Split(status, "\n") {
		name, value, found := strings.Cut(line, ":")
		if !found {
			continue
		}
		switch name {
		case "Uid", "Gid", "CapInh", "CapPrm", "CapEff", "CapBnd", "CapAmb", "NoNewPrivs", "Seccomp":
			facts[name] = strings.Join(strings.Fields(value), " ")
		}
	}
	return facts
}

func (s *server) handleSelf(w http.ResponseWriter, r *http.Request) {
	status, err := os.ReadFile("/proc/self/status")
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	_, tokenErr := os.Stat(serviceAccountToken)
	writeJSON(w, http.StatusOK, map[string]any{
		"process":                processFacts(string(status)),
		"service_account_token":  tokenErr == nil,
		"identity_file_readable": s.identity != "",
	})
}

func (s *server) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handleRoot)
	mux.HandleFunc("/self", s.handleSelf)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("/exchange", s.handleExchange)
	mux.HandleFunc("/probe", s.handleProbe)
	mux.HandleFunc("/resolve", s.handleResolve)
	return mux
}

func main() {
	s, err := loadServer(os.Getenv)
	if err != nil {
		log.Fatalf("srw-driver-echo: %v", err)
	}
	port := os.Getenv("SRW_DRIVER_PORT")
	if port == "" {
		port = "8080"
	}
	extra := os.Getenv("SRW_ECHO_EXTRA_PORT")
	if extra == "" {
		extra = "9090"
	}
	go func() {
		// A port outside the srw-driver name: nothing outside the pod may
		// reach it.
		log.Printf("srw-driver-echo: extra listener on :%s", extra)
		err := http.ListenAndServe(":"+extra, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			writeJSON(w, http.StatusOK, map[string]string{"listener": "extra"})
		}))
		log.Printf("srw-driver-echo: extra listener stopped: %v", err)
	}()
	log.Printf("srw-driver-echo: serving %v on :%s", s.request["driver"], port)
	server := &http.Server{Addr: ":" + port, Handler: s.routes(), ReadHeaderTimeout: 10 * time.Second}
	log.Fatal(server.ListenAndServe())
}
