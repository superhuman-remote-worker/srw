# srw.gpt-live-sip/v1

A standing service driver for one phone-line connector. The orchestrator hands
it accepted-job session metadata; it claims a session lease, accepts the call,
holds the OpenAI sideband, journals events, and exposes the job-scoped HTTP/SSE
API. Credentials stay in the pod. Only the Go standard library is used.

Run `go test ./...`, `go test -race ./...`, and `go vet ./...` from this directory.
Build with `CGO_ENABLED=0 go build .`; the binary takes `serve` or `version`.
The request and identity files default to `/run/srw/request.json` and
`/run/srw/identity`. The four `SRW_*` overrides in the launcher contract are
supported. The exchange's credential response supplies the authoritative API
and WebSocket endpoints; connector `api_host`/`api_port` and catalog `model`
are parsed but do not override those endpoints or the handoff's model.

The tests use `httptest.Server` with a `net.Pipe` listener and a fake clock.
They exercise real HTTP parsing, connection hijacking, masked WebSocket frames,
and SSE without needing permission to open listening sockets. Production uses
ordinary TCP and verified TLS. Clipping fixtures were generated directly from
`contract.py` using Python and compare by Unicode code points. No test requires OpenAI or an SRW deployment.

## Implementation choices

- Admission has a shared 12-second deadline, including lease and credential
  requests. Its result is stored even if the initiating HTTP client disconnects.
  Accept itself is never retried. Exchange state writes happen in the background
  and are ordered before the session's journal rows.
- REST refusals preserve a bounded, validated error code but use a fixed message.
  Upstream REST messages can echo credentials or caller data. Transport failures
  have status `0`, code `transport_error`. Engine event messages remain in the
  specified journal/SSE shapes and never enter logs.
- An omitted voice defaults to `marin`, as in the Python client. An empty greeting
  or fallback suppresses that recorder append; the driver invents no call text.
- The 8 MiB WebSocket limit and 1 MiB exchange limit conflict for oversized
  individual journal rows. Such a row becomes a `kind: error` row with code
  `journal_event_too_large`, original kind, serialized byte size, and SHA-256
  digest. Its event ID is preserved (an ID larger than 1024 bytes gets a stable
  digest ID). The full event remains available through SSE. Rows are otherwise
  mapped without truncation; batches contain at most 50 rows and fit within
  1 MiB.
- SSE subscribers buffer 1024 events each, have a 10-second write deadline, and
  recheck their job lease every 30 seconds. Duplicate in-flight
  append event IDs return 400 instead of replacing another acknowledgement waiter.
  ID lines containing CR, LF, or NUL
  are omitted to preserve SSE framing; JSON still includes the original ID.
- The WebSocket client sends ping every 20 seconds and permits 60 seconds
  without a received frame before reconnecting. It rejects binary frames,
  unsolicited extensions, masked server frames, invalid UTF-8, and malformed
  fragmentation/control frames. Detach attempts a masked normal-close frame
  for at most 100 ms before closing the transport.
- Normal closed sessions keep renewing ownership until all events and the final
  state have been posted or finally refused. Lease loss stops all actions and
  writes without reporting an ended state. An active recorder that lost its
  lease may later be replaced through adoption, never by repeating accept.
  Retired recorders also expire from local memory after 15 minutes.
- SIGTERM cancels call actions, gives each journal up to 10 seconds to drain,
  and releases successfully drained leases. A failed drain is logged without
  call contents; its lease is left to expire for adoption. The process has a
  15-second total shutdown deadline and never hangs up during shutdown.
