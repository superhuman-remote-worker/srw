"""The agent side of a cloud folder delivered with the ``oauth_token`` auth
kind (S5 of features/cloud_folders_on_virtual_sessions.md § Provider
neutrality): the per-call bearer fetched from the orchestrator's token route
under the claim lease and handed to rclone per spawn.

* ``CloudTokenSource`` caches a token until near its end, drops it when the
  remote refused it, and turns every fetch failure into a closed message;
* the rclone store adds the live credential to each spawn's environment only,
  and tries an operation once more after a 401 with a fresh bearer;
* the factory binds an ``oauth_token`` entry to a live credential (a refusing
  one without a token route) and a ``basic`` entry to none;
* the orchestrator client's fetcher calls the route with the live lease and
  the plan and never logs the token;
* the session wires a source per folder only with a client and a lease.
"""

from __future__ import annotations

import logging
import subprocess
from types import SimpleNamespace

import pytest

from agent.api import lease_context
from agent.api import orchestrator_client as client_module
from agent.api import persistent_session as ps_module
from agent.core.backends.cloud_token import (
    CloudTokenSource,
    CloudTokenUnavailable,
    unavailable_token_source,
)
from agent.core.backends.factory import cloud_remote_from_entry
from shared.runtime.core.backends import rclone as rclone_module
from shared.runtime.core.backends.rclone import RcloneObjectStore

TOKEN = "eyJ.first.token"
TOKEN_2 = "eyJ.second.token"


# ---------------------------------------------------------------------------
# The token source
# ---------------------------------------------------------------------------


class _Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


def test_the_source_caches_until_the_refresh_margin():
    clock = _Clock()
    tokens = iter([(TOKEN, 300.0), (TOKEN_2, 300.0)])
    calls: list[float] = []

    def fetch():
        calls.append(clock.now)
        return next(tokens)

    source = CloudTokenSource(fetch, refresh_margin=60.0, clock=clock)
    assert not source.cached
    assert source.config() == {"bearer_token": TOKEN}
    clock.now += 200.0
    assert source.config() == {"bearer_token": TOKEN}  # 100 s left: kept
    clock.now += 45.0
    assert source.config() == {"bearer_token": TOKEN_2}  # 55 s left: refreshed
    assert calls == [1000.0, 1245.0] and source.cached


def test_a_rejected_token_is_fetched_afresh_next_time():
    tokens = iter([(TOKEN, 300.0), (TOKEN_2, 300.0)])
    source = CloudTokenSource(lambda: next(tokens))
    assert source.config() == {"bearer_token": TOKEN}
    source.rejected()
    assert not source.cached
    assert source.config() == {"bearer_token": TOKEN_2}


def test_every_fetch_failure_is_a_closed_unavailable_message():
    def boom():
        raise ConnectionError("the body with a secret in it")

    with pytest.raises(CloudTokenUnavailable) as info:
        CloudTokenSource(boom).config()
    assert "secret" not in str(info.value)
    assert "ConnectionError" in str(info.value)
    with pytest.raises(CloudTokenUnavailable):
        CloudTokenSource(lambda: ("", 10.0)).config()
    with pytest.raises(CloudTokenUnavailable, match="no token route"):
        unavailable_token_source("no token route here").config()


# ---------------------------------------------------------------------------
# The rclone store
# ---------------------------------------------------------------------------


def _store(source=None):
    return RcloneObjectStore(
        remote_type="webdav",
        config={"url": "https://cloud.example/dav/", "vendor": "infinitescale"},
        root="",
        remote_name="cloud_oc",
        live_credential=source,
    )


def test_the_live_credential_is_in_the_spawn_environment_only(monkeypatch):
    source = CloudTokenSource(lambda: (TOKEN, 300.0))
    store = _store(source)
    env = store._spawn_env()
    assert env["RCLONE_CONFIG_CLOUD_OC_BEARER_TOKEN"] == TOKEN
    assert env["RCLONE_CONFIG_CLOUD_OC_TYPE"] == "webdav"
    assert "RCLONE_CONFIG_CLOUD_OC_BEARER_TOKEN" not in store._env_overlay
    assert "RCLONE_CONFIG_CLOUD_OC_BEARER_TOKEN" not in _store()._spawn_env()


def test_a_401_fetches_a_fresh_bearer_and_tries_once_more(monkeypatch):
    tokens = iter([(TOKEN, 300.0), (TOKEN_2, 300.0)])
    source = CloudTokenSource(lambda: next(tokens))
    store = _store(source)
    seen: list[str] = []
    answers = iter(
        [
            subprocess.CompletedProcess(
                ["rclone"], 1, stdout=b"", stderr=b"HTTP error 401 Unauthorized"
            ),
            subprocess.CompletedProcess(["rclone"], 0, stdout=b"[]", stderr=b""),
        ]
    )

    def run(args, **kwargs):
        seen.append(kwargs["env"]["RCLONE_CONFIG_CLOUD_OC_BEARER_TOKEN"])
        return next(answers)

    monkeypatch.setattr(rclone_module.subprocess, "run", run)
    proc = store._run(["lsjson", "cloud_oc:"])
    assert proc.returncode == 0
    assert seen == [TOKEN, TOKEN_2]


def test_without_a_live_credential_a_401_is_not_retried(monkeypatch):
    store = _store()
    runs: list[list[str]] = []

    def run(args, **kwargs):
        runs.append(list(args))
        return subprocess.CompletedProcess(
            ["rclone"], 1, stdout=b"", stderr=b"HTTP error 401 Unauthorized"
        )

    monkeypatch.setattr(rclone_module.subprocess, "run", run)
    assert store._run(["lsjson", "cloud_oc:"]).returncode == 1
    assert len(runs) == 1


# ---------------------------------------------------------------------------
# The factory
# ---------------------------------------------------------------------------


def _entry(auth, name="oc"):
    return {
        "name": name,
        "path": f"cloud/{name}",
        "mount_id": f"m-{name}",
        "mount_kind": "project",
        "access": "read_write",
        "rclone_spec": {
            "type": "webdav",
            "config": {"url": "https://cloud.example/dav/spaces/d/"},
            "root": "",
        },
        "auth": auth,
    }


def test_an_oauth_token_entry_binds_to_its_live_credential():
    source = CloudTokenSource(lambda: (TOKEN, 300.0))
    asked: list[str] = []

    def source_for(entry):
        asked.append(entry["mount_id"])
        return source

    remote = cloud_remote_from_entry(
        _entry({"type": "oauth_token"}), token_source_for=source_for
    )
    assert remote.path == "cloud/oc" and asked == ["m-oc"]
    assert remote.store._live_credential is source
    assert remote.store._spawn_env()["RCLONE_CONFIG_CLOUD_OC_BEARER_TOKEN"] == TOKEN


def test_without_a_token_route_the_folder_binds_with_a_refusing_source():
    remote = cloud_remote_from_entry(_entry({"type": "oauth_token"}))
    credential = remote.store._live_credential
    assert credential is not None
    with pytest.raises(CloudTokenUnavailable, match="no token route"):
        credential.config()
    basic = cloud_remote_from_entry(
        _entry({"type": "basic"}), token_source_for=lambda entry: None
    )
    assert basic.store._live_credential is None


# ---------------------------------------------------------------------------
# The orchestrator client's fetcher
# ---------------------------------------------------------------------------


class _Response:
    def __init__(self, status_code, body):
        self.status_code = status_code
        self._body = body

    def json(self):
        if isinstance(self._body, Exception):
            raise self._body
        return self._body


class _Client:
    calls: list[dict] = []
    answer: _Response = _Response(200, {"token": TOKEN, "expires_in": 240})

    def __init__(self, *, timeout, headers):
        self.headers = headers

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def get(self, url, params):
        _Client.calls.append({"url": url, "params": params, "headers": self.headers})
        return _Client.answer


def _lease():
    handle = lease_context.LeaseHandle()
    handle.update(
        "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb", 7, executor_id="pod-1", pod_uid="uid-1"
    )
    return handle


def _client():
    client = client_module.OrchestratorClient.__new__(client_module.OrchestratorClient)
    client.orchestrator_url = "http://srw-orchestrator:8085"
    return client


def test_the_fetcher_calls_the_route_with_the_live_lease_and_the_plan(
    monkeypatch, caplog
):
    _Client.calls = []
    _Client.answer = _Response(200, {"token": TOKEN, "expires_in": 240})
    monkeypatch.setattr(client_module.httpx, "Client", _Client)
    monkeypatch.setenv("MCP_INTERNAL_KEY", "internal-key")
    lease = _lease()
    fetch = _client().cloud_remote_token_fetcher(
        "thread-1", lease=lease, plan={"fingerprint": "fp-1", "incarnation": "inc-1"}
    )
    with caplog.at_level(logging.DEBUG):
        assert fetch("m-oc") == (TOKEN, 240.0)
    call = _Client.calls[0]
    assert call["url"] == (
        "http://srw-orchestrator:8085/internal/units/"
        "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb/cloud-token"
    )
    assert call["params"] == {
        "lease_token": 7,
        "pod_name": "pod-1",
        "pod_uid": "uid-1",
        "mount_id": "m-oc",
        "fingerprint": "fp-1",
        "incarnation": "inc-1",
    }
    assert call["headers"] == {"X-Internal-Key": "internal-key"}
    assert TOKEN not in caplog.text
    # A re-claim repoints the mutable handle; the next fetch follows it.
    lease.update(lease.unit_id, 8, executor_id="pod-1", pod_uid="uid-1")
    fetch("m-oc")
    assert _Client.calls[1]["params"]["lease_token"] == 8


def test_the_fetcher_turns_every_failure_into_a_closed_message(monkeypatch, caplog):
    monkeypatch.setattr(client_module.httpx, "Client", _Client)
    monkeypatch.setenv("MCP_INTERNAL_KEY", "internal-key")
    fetch = _client().cloud_remote_token_fetcher(
        "thread-1", lease=_lease(), plan={"fingerprint": "fp", "incarnation": "i"}
    )
    _Client.answer = _Response(403, {"detail": "Lease validation failed"})
    with caplog.at_level(logging.DEBUG), pytest.raises(CloudTokenUnavailable) as info:
        fetch("m-oc")
    assert str(info.value) == "token route answered 403"
    assert "Lease validation failed" not in caplog.text
    _Client.answer = _Response(200, {"token": "", "expires_in": 1})
    with pytest.raises(CloudTokenUnavailable, match="no token"):
        fetch("m-oc")
    _Client.answer = _Response(200, ValueError("no json"))
    with pytest.raises(CloudTokenUnavailable, match="no JSON"):
        fetch("m-oc")
    # No live lease (the handle was never pointed at a claim): nothing is asked.
    _Client.calls = []
    idle = _client().cloud_remote_token_fetcher(
        "thread-1", lease=lease_context.LeaseHandle(), plan={}
    )
    with pytest.raises(CloudTokenUnavailable, match="no live claim lease"):
        idle("m-oc")
    assert _Client.calls == []


# ---------------------------------------------------------------------------
# The session's wiring
# ---------------------------------------------------------------------------


def test_the_session_wires_a_source_per_folder_only_with_a_client_and_a_lease():
    asked: list = []

    def fetcher(thread_id, *, lease, plan):
        asked.append((thread_id, lease, plan))
        return lambda mount_id: (f"{TOKEN}:{mount_id}", 120.0)

    client = SimpleNamespace(cloud_remote_token_fetcher=fetcher)
    plan = {"fingerprint": "fp", "incarnation": "inc"}
    # No client, or a client without the fetcher, or no lease: no source.
    assert ps_module.cloud_token_source_factory(None, thread_id="t", plan=plan) is None
    assert (
        ps_module.cloud_token_source_factory(
            SimpleNamespace(), thread_id="t", plan=plan
        )
        is None
    )
    token = lease_context.current_lease.set(None)
    try:
        assert (
            ps_module.cloud_token_source_factory(client, thread_id="t", plan=plan)
            is None
        )
    finally:
        lease_context.current_lease.reset(token)
    handle = _lease()
    token = lease_context.current_lease.set(handle)
    try:
        source_for = ps_module.cloud_token_source_factory(
            client, thread_id="t", plan=plan
        )
    finally:
        lease_context.current_lease.reset(token)
    assert callable(source_for) and asked == [("t", handle, plan)]
    source = source_for({"mount_id": "m-oc", "name": "oc"})
    assert isinstance(source, CloudTokenSource)
    assert source.config() == {"bearer_token": f"{TOKEN}:m-oc"}
    assert source_for({"name": "outbox"}).config() == {
        "bearer_token": f"{TOKEN}:outbox"
    }
