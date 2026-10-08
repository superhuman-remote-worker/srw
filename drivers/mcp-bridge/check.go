package main

import (
	"bytes"
	"errors"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
)

// The bridge forwards what the front checked. The front parses every
// message strictly (exact keys, no duplicate or unknown key, valid UTF-8,
// no batch) and forwards a body it re-encoded from what it checked. The
// bridge accepts a body only when the MCP Go SDK, which decodes it for the
// session and encodes it again for the process's stdin, reproduces exactly
// these bytes. So the process reads the bytes the front decided on, one
// message per line, and a top level in any other form (a key the SDK
// drops, a duplicate, an id it would renumber, whitespace) is refused
// instead of being read one way by the front and another way here.
//
// The bridge decides only on that top level (a request or an answer, its
// id, its method); params pass through byte for byte, so what a tool call
// names is the front's decision, on the same bytes the process reads. The
// bridge is no authorization boundary, and only the front reaches it: it
// serves a unix socket in a directory only the front's group may enter,
// never a port. A binding's process cannot reach it (nor send its binding
// header or its control routes), because each process runs as a user of
// its own: it cannot read another binding's environment or private
// directory, signal or trace another's process, or gain a privilege. What
// the processes still share is the pod: its network (a loopback port is
// open to every one of them, and the front's needs a lease), the image's
// read-only filesystem, /tmp and /dev/shm (sticky; their files are private
// by umask 077 and removed with their user), and the pod's CPU and memory.

var (
	errEmpty        = errors.New("the body is empty")
	errNotOneLine   = errors.New("a message is one line")
	errNotJSONRPC   = errors.New("the body is not one JSON-RPC message")
	errNotCanonical = errors.New("the message is not in the form the front forwards")
)

// checkMessage returns the message a body holds, if the body is exactly its
// encoding.
func checkMessage(body []byte) (jsonrpc.Message, error) {
	if len(body) == 0 {
		return nil, errEmpty
	}
	if bytes.ContainsAny(body, "\r\n") {
		return nil, errNotOneLine
	}
	message, err := jsonrpc.DecodeMessage(body)
	if err != nil {
		return nil, errNotJSONRPC
	}
	again, err := jsonrpc.EncodeMessage(message)
	if err != nil || !bytes.Equal(again, body) {
		return nil, errNotCanonical
	}
	return message, nil
}

// initializeCall reports whether a message is an initialize request, the
// only message that may open a session.
func initializeCall(message jsonrpc.Message) bool {
	request, ok := message.(*jsonrpc.Request)
	return ok && request.IsCall() && request.Method == "initialize"
}
