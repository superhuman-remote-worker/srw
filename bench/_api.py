"""Minimal orchestrator REST client for the bench suite (stdlib only).

Auth: SRW_TOKEN is sent both as X-MCP-Token and Bearer so either auth path
matches. Base URL: SRW_API_URL (default: a local orchestrator on
http://localhost:8085, e.g. through a kubectl port-forward).
"""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.parse
import urllib.request

DEFAULT_API = "http://localhost:8085"


class ApiError(RuntimeError):
    def __init__(self, status: int, body: str, url: str):
        super().__init__(f"HTTP {status} on {url}: {body[:300]}")
        self.status = status
        self.body = body


def _headers() -> dict:
    token = os.environ.get("SRW_TOKEN", "").strip()
    if not token:
        raise SystemExit("SRW_TOKEN is not set (X-MCP-Token for the orchestrator)")
    return {
        "X-MCP-Token": token,
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        # Some CDN bot filters (Cloudflare's 1010) block the default
        # Python-urllib signature; a client UA passes. If a WAF still blocks,
        # point SRW_API_URL at a kubectl port-forward instead.
        "User-Agent": "srw-bench/1.0 (+bench/README.md)",
    }


def base_url() -> str:
    return os.environ.get("SRW_API_URL", DEFAULT_API).rstrip("/")


def request(method: str, path: str, payload: dict | None = None,
            params: dict | None = None, timeout: int = 60):
    url = base_url() + path
    if params:
        url += "?" + urllib.parse.urlencode(params)
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(url, data=data, method=method, headers=_headers())
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode()
    except urllib.error.HTTPError as e:
        raise ApiError(e.code, e.read().decode(errors="replace"), url) from None
    return json.loads(body) if body else None


def get(path: str, **params):
    return request("GET", path, params=params or None)


def post(path: str, payload: dict):
    return request("POST", path, payload=payload)
