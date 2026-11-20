package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strings"

	"github.com/superhuman-remote-worker/srw/cli/internal/exit"
)

var missingScope = regexp.MustCompile(`^Insufficient token scope: requires (.+)$`)

// StatusError turns a response that is not 2xx into an error with its exit
// code, or returns nil. The texts follow what the server actually sends.
func (c *Client) StatusError(request Request, response *Response) error {
	status := response.Status
	if status >= 200 && status < 300 {
		return nil
	}
	if status >= 300 && status < 400 {
		// Never followed: the Authorization header would go along.
		return exit.Usagef("%s redirects to %s; the server address is not the API's",
			c.base, c.Redact(response.Header.Get("Location"))).
			WithHint("check the server URL (https, host and path)")
	}
	failure := &exit.Error{Code: codeFor(status), Status: status}
	if c.notTheAPI(request, response, failure) {
		return failure
	}
	detail := decodeDetail(response.Body)
	switch value := detail.(type) {
	case string:
		failure.Message = value
	case map[string]any:
		// A manifest issue: {code, message, document, path, source}.
		failure.ErrCode, _ = value["code"].(string)
		failure.Message, _ = value["message"].(string)
		failure.Path, _ = value["path"].(string)
		failure.File, _ = value["source"].(string)
		if number, ok := value["document"].(float64); ok {
			failure.Document = int(number)
		}
		if failure.Message == "" {
			failure.Message = http.StatusText(status)
		}
	case []any:
		// FastAPI's validation list. Print the field path and the message
		// only: detail[].input echoes the request body.
		failure.Message = validationMessage(value)
	}
	if failure.Message == "" {
		failure.Message = fmt.Sprintf("%s %s answered %d %s", request.Method, request.Path, status, http.StatusText(status))
	}
	failure.Message = c.Redact(failure.Message)
	switch {
	case status == http.StatusUnauthorized && failure.Message == "Invalid token":
		// The server cannot say which of the three it is.
		failure.Message = "the token is unknown, revoked or expired"
		failure.Hint = "create a new one under Settings → API Keys in the Cockpit, then run `srw login` or set SRW_TOKEN"
	case status == http.StatusForbidden && strings.HasSuffix(failure.Message, "personal access tokens cannot use this endpoint"):
		failure.Message = "this route is not available to tokens"
	case status == http.StatusForbidden:
		if match := missingScope.FindStringSubmatch(failure.Message); match != nil {
			failure.Hint = fmt.Sprintf("create a token that also has %s under Settings → API Keys", quoteAll(match[1]))
		}
	case status == http.StatusNotFound && failure.Message == "Not Found":
		// FastAPI's answer for a path it does not serve, as opposed to a
		// handler that did not find an object.
		failure.Message = fmt.Sprintf("this server does not have %s %s", request.Method, request.Path)
		if request.Known {
			failure.Message += "; it may be older than this srw"
		}
	}
	return failure
}

// notTheAPI recognises an answer that something in front of the API gave,
// not the API:
//
//   - a 404 or a gateway status (502, 503, 504) whose body is not a JSON
//     object; the API answers every error with one;
//   - one of Cloudflare's own codes, 520 to 530, which the API never sends;
//   - a Cloudflare error in JSON. Cloudflare answers a client that asks for
//     JSON with an RFC 9457 object for its 500, 502, 504 and 52x, and marks
//     it with "cloudflare_error": true. It has a "detail" like the API's.
//
// Such an answer says nothing about the object asked for. For a read, and
// for a change that cannot have reached the API, it is "not reached" (exit
// 7) and passes when the server is back. For a change that may have reached
// the API before the proxy gave up, the outcome is unknown (exit 6), as when
// an answer is lost: sending it again could do it twice.
func (c *Client) notTheAPI(request Request, response *Response, failure *exit.Error) bool {
	status := response.Status
	gateway := status == http.StatusBadGateway || status == http.StatusServiceUnavailable || status == http.StatusGatewayTimeout
	var object map[string]any
	isObject := json.Unmarshal(response.Body, &object) == nil && object != nil
	flagged, _ := object["cloudflare_error"].(bool)
	cloudflare := flagged || (status >= 520 && status <= 530)
	if !cloudflare {
		if !gateway && status != http.StatusNotFound {
			return false
		}
		// The API's own 503 passes too, but it carries the API's message.
		failure.Away = gateway
		if isObject {
			return false
		}
	}
	answer := strings.TrimSpace(fmt.Sprintf("%d %s", status, http.StatusText(status)))
	from := "something in front of it"
	if cloudflare {
		from = "Cloudflare in front of it"
		if title, _ := object["title"].(string); title != "" {
			answer = fmt.Sprintf("%d (%s)", status, c.Redact(title))
		}
	}
	if !isRead(request.Method) && mayHaveReachedTheAPI(status) {
		failure.Code, failure.Away = exit.Unknown, false
		failure.Message = fmt.Sprintf("%s %s got %s from %s; the request may have reached the API before that",
			request.Method, request.Path, answer, from)
		failure.Hint = "the change may or may not have happened; check before sending it again"
		return true
	}
	failure.Code, failure.Away = exit.Unreachable, true
	failure.Message = fmt.Sprintf("the API at %s is not answering: %s %s got %s from %s",
		c.base, request.Method, request.Path, answer, from)
	failure.Hint = "the server may be restarting; if this stays, check the server URL"
	return true
}

// mayHaveReachedTheAPI says whether a proxy's answer leaves open that the
// request got to the API. It did not when the proxy had no server to send it
// to (404, 503), nor for Cloudflare's codes for an origin it could not reach:
// 521 refused, 522 connection timed out, 523 unreachable, 525 and 526 TLS,
// 530. Everything else, a 502, a 504 or a 524 above all, may have come after
// the API started on the request.
func mayHaveReachedTheAPI(status int) bool {
	switch status {
	case http.StatusNotFound, http.StatusServiceUnavailable, 521, 522, 523, 525, 526, 530:
		return false
	}
	return true
}

func codeFor(status int) int {
	switch status {
	case http.StatusUnauthorized, http.StatusForbidden:
		return exit.Auth
	case http.StatusNotFound:
		return exit.NotFound
	case http.StatusConflict:
		return exit.Conflict
	}
	return exit.Failed
}

func decodeDetail(body []byte) any {
	var envelope struct {
		Detail any `json:"detail"`
	}
	if json.Unmarshal(body, &envelope) != nil {
		return nil
	}
	return envelope.Detail
}

func validationMessage(items []any) string {
	lines := make([]string, 0, len(items))
	for _, item := range items {
		entry, ok := item.(map[string]any)
		if !ok {
			continue
		}
		message, _ := entry["msg"].(string)
		var where []string
		if location, ok := entry["loc"].([]any); ok {
			for index, part := range location {
				// The first part is where the value was sent ("body", "query").
				if index == 0 && len(location) > 1 {
					continue
				}
				where = append(where, fmt.Sprint(part))
			}
		}
		if len(where) > 0 {
			lines = append(lines, strings.Join(where, ".")+": "+message)
		} else if message != "" {
			lines = append(lines, message)
		}
	}
	if len(lines) == 0 {
		return "the request was not accepted"
	}
	return "the request was not accepted: " + strings.Join(lines, "; ")
}

func quoteAll(list string) string {
	parts := strings.Split(list, ",")
	for index, part := range parts {
		parts[index] = "`" + strings.TrimSpace(part) + "`"
	}
	return strings.Join(parts, " and ")
}
