"""A stdio MCP server for the stdio bridge's integration test (D5b).

It reads one JSON-RPC message per line, as a stdio server does (Python's
json, so a duplicate key's last value wins), logs every line exactly as it
read it to ``$FAKE_STDIO_LOG_DIR/<pid>.log``, and serves five tools:
``whoami`` (its process id, a digest of the credential in ``$MCP_TOKEN``,
its call count and whether any ``SRW_`` variable reached it),
``notes_read``, ``notes_write``, ``leak_credential`` (prints the credential
to stderr and answers with it) and ``crash``. Standard library only.
"""

from __future__ import annotations

import hashlib
import json
import os
import sys


def main() -> int:
    log_dir = os.environ.get("FAKE_STDIO_LOG_DIR", "")
    log = open(os.path.join(log_dir, f"{os.getpid()}.log"), "ab") if log_dir else None
    calls = 0

    def send(message: dict) -> None:
        sys.stdout.write(json.dumps(message) + "\n")
        sys.stdout.flush()

    def text(identifier, value: str) -> None:
        send(
            {
                "jsonrpc": "2.0",
                "id": identifier,
                "result": {"content": [{"type": "text", "text": value}]},
            }
        )

    for raw in sys.stdin.buffer:
        if log is not None:
            log.write(raw)
            log.flush()
        try:
            message = json.loads(raw)
        except ValueError:
            continue
        method, identifier = message.get("method"), message.get("id")
        if method == "initialize":
            send(
                {
                    "jsonrpc": "2.0",
                    "id": identifier,
                    "result": {
                        "protocolVersion": message["params"]["protocolVersion"],
                        "capabilities": {"tools": {}},
                        "serverInfo": {"name": "fake-stdio", "version": "1"},
                    },
                }
            )
        elif method == "ping":
            send({"jsonrpc": "2.0", "id": identifier, "result": {}})
        elif method == "tools/list":
            names = ("whoami", "notes_read", "notes_write", "leak_credential", "crash")
            send(
                {
                    "jsonrpc": "2.0",
                    "id": identifier,
                    "result": {
                        "tools": [
                            {"name": name, "inputSchema": {"type": "object"}}
                            for name in names
                        ]
                    },
                }
            )
        elif method == "tools/call":
            calls += 1
            name = (message.get("params") or {}).get("name")
            credential = os.environ.get("MCP_TOKEN", "")
            if name == "whoami":
                text(
                    identifier,
                    json.dumps(
                        {
                            "pid": os.getpid(),
                            "credential_sha256": hashlib.sha256(
                                credential.encode()
                            ).hexdigest()
                            if credential
                            else "",
                            "calls": calls,
                            "srw_env": sorted(
                                k for k in os.environ if k.startswith("SRW_")
                            ),
                        }
                    ),
                )
            elif name == "notes_read":
                text(identifier, "read")
            elif name == "notes_write":
                text(identifier, "called notes_write")
            elif name == "leak_credential":
                print(f"my credential is {credential}", file=sys.stderr, flush=True)
                text(identifier, f"I hold {credential}")
            elif name == "crash":
                os._exit(3)
            else:
                send(
                    {
                        "jsonrpc": "2.0",
                        "id": identifier,
                        "error": {"code": -32602, "message": f"Unknown tool: {name}"},
                    }
                )
        elif identifier is not None and method is not None:
            send(
                {
                    "jsonrpc": "2.0",
                    "id": identifier,
                    "error": {"code": -32601, "message": "Method not found"},
                }
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
