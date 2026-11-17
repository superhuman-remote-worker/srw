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
		failure.Hint = "check Settings → API Keys in the Cockpit, then run `srw login`"
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
