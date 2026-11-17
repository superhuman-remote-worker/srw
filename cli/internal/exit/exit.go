// Package exit defines the exit codes of srw and the error type that carries
// one. Scripts may rely on these codes; they are part of the contract.
package exit

import (
	"context"
	"errors"
	"fmt"
)

// Exit codes. 0 to 10 collide with no shell convention.
const (
	OK = 0
	// Failed: the operation failed (validation, a server error, anything
	// not listed below).
	Failed = 1
	// Usage: wrong usage, or a configuration the command cannot run with.
	Usage = 2
	// Auth: not authenticated or not allowed (401, 403).
	Auth = 3
	// NotFound: 404.
	NotFound = 4
	// Conflict: 409.
	Conflict = 5
	// Unknown: a mutation was sent and the response was lost.
	Unknown = 6
	// Unreachable: the server could not be reached, or TLS failed. A
	// request that never left is Unreachable, not Unknown.
	Unreachable = 7
	// WaitFailed: wait saw the job end failed or cancelled.
	WaitFailed = 8
	// WaitTimeout: wait timed out.
	WaitTimeout = 9
	// WaitNeedsPerson: wait saw the job stop for a person.
	WaitNeedsPerson = 10
	// Interrupted: Ctrl-C.
	Interrupted = 130
)

// Error is a failure with its exit code. Message is one line for a person.
// The remaining fields are the machine-readable error shape printed when a
// command's output is JSON.
type Error struct {
	Code    int
	Message string
	// Hint is a second line that says what to do about it.
	Hint string
	// Status is the HTTP status, 0 when no response arrived.
	Status int
	// ErrCode is the server's error code, when it sent one.
	ErrCode string
	// Document, Path and File locate a manifest error.
	Document int
	Path     string
	File     string
}

func (e *Error) Error() string { return e.Message }

// New returns an error with an exit code.
func New(code int, format string, args ...any) *Error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...)}
}

// Usagef returns a wrong-usage error.
func Usagef(format string, args ...any) *Error {
	return New(Usage, format, args...)
}

// WithHint returns the error with a second line.
func (e *Error) WithHint(format string, args ...any) *Error {
	e.Hint = fmt.Sprintf(format, args...)
	return e
}

// CodeOf maps any error to an exit code.
func CodeOf(err error) int {
	if err == nil {
		return OK
	}
	var coded *Error
	if errors.As(err, &coded) {
		return coded.Code
	}
	if errors.Is(err, context.Canceled) {
		return Interrupted
	}
	return Failed
}
