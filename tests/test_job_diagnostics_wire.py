"""Wire contracts for per-job and per-thread operational diagnostics.

Characterized against the pre-extraction ``main.py`` handlers and ported onto
``orchestrator.routers.job_diagnostics`` unchanged. What is pinned here:

* **one shared log reader**, so a job log and a session log are filtered and
  id-scoped identically — a shared pod log must disaggregate the same way from
  either route;
* the **live file wins over the archive**, and the response says which it was;
* ``raw=true`` returns the *unfiltered, untailed* text, which for an archived
  pod log is only this job's (or session's) id-tagged lines, never the whole
  pooled pod log, and nothing when no line is tagged (N32);
* the shell-state proxy dials only a **freshly attested** recipient, and an
  agent-side mismatch stays a 409 rather than becoming a 502.

The pure helpers themselves are covered in ``tests/test_job_log_archive.py``.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from tests._mounted_router import mount_router


JOB_ID = "11111111-2222-3333-4444-555555555555"
THREAD_ID = "22222222-3333-4444-5555-666666666666"
OTHER_ID = "99999999-8888-7777-6666-555555555555"
AGENT_ID = "33333333-4444-4333-8333-777777777777"


def _wire(
    *,
    tmp_path=None,
    job=None,
    thread=None,
    blobs=None,
    audit_reader=None,
    prepare=None,
    job_gate=None,
    thread_gate=None,
):
    from orchestrator.routers.job_diagnostics import (
        JobDiagnosticsDependencies as RouteDeps,
    )
    from orchestrator.routers.job_diagnostics import router
    from orchestrator.services.job_diagnostics import (
        JobDiagnosticsDependencies as OpDeps,
    )

    store = SimpleNamespace()
    resolved_job = job if job is not None else {"id": JOB_ID, "context": {}}
    resolved_thread = (
        thread if thread is not None else {"id": THREAD_ID, "metadata": {}}
    )

    async def allow_job(_request, _store, _job_id):
        if job_gate is not None:
            return await job_gate(_request, _store, _job_id)
        return {"id": "owner"}, resolved_job

    async def allow_thread(_request, _store, _thread_id):
        if thread_gate is not None:
            return await thread_gate(_request, _store, _thread_id)
        return {"id": "owner"}, resolved_thread

    table = dict(blobs or {})
    ops = OpDeps(
        workspace=SimpleNamespace(base_path=tmp_path),
        snapshots=SimpleNamespace(
            get_blob=AsyncMock(side_effect=lambda key: table.get(key))
        ),
        audit_reader=audit_reader
        or SimpleNamespace(is_available=False, list_llm_requests=AsyncMock()),
        prepare_pinned_job_mutation_target=prepare or AsyncMock(return_value=None),
    )
    deps = RouteDeps(
        store=store,
        operations=ops,
        require_job_access=allow_job,
        require_thread_owner=allow_thread,
    )
    app = mount_router(
        router, factories={"job_diagnostics_dependencies_factory": lambda: deps}
    )
    return SimpleNamespace(client=TestClient(app), ops=ops)


def _write_job_log(tmp_path, text):
    logs = tmp_path / "logs"
    logs.mkdir(parents=True, exist_ok=True)
    (logs / f"job_{JOB_ID}.log").write_text(text)


# =============================================================================
# Job logs
# =============================================================================


def test_malformed_job_id_is_refused_before_the_gate():
    gate = AsyncMock()
    wire = _wire(job_gate=gate)
    resp = wire.client.get("/api/jobs/not-a-uuid/logs")
    assert resp.status_code == 400
    assert resp.json()["detail"] == "Invalid job_id format: not-a-uuid"
    gate.assert_not_awaited()


def test_live_log_file_wins_over_the_archive(tmp_path):
    _write_job_log(tmp_path, "line one\nline two\nline three")
    wire = _wire(
        tmp_path=tmp_path,
        job={"id": JOB_ID, "context": {"log_archive_keys": ["k"]}},
        blobs={"k": b"archived text"},
    )

    body = wire.client.get(f"/api/jobs/{JOB_ID}/logs").json()

    assert body["archived"] is False
    assert body["lines"] == ["line one", "line two", "line three"]
    assert body["total_lines"] == 3
    assert body["log_path"].endswith(f"logs/job_{JOB_ID}.log")
    wire.ops.snapshots.get_blob.assert_not_awaited()


def test_missing_log_and_no_archive_is_404(tmp_path):
    wire = _wire(tmp_path=tmp_path)
    resp = wire.client.get(f"/api/jobs/{JOB_ID}/logs")
    assert resp.status_code == 404
    assert resp.json()["detail"] == f"Log file not found for job {JOB_ID}"


def test_archived_log_is_scoped_to_this_job(tmp_path):
    text = "\n".join(
        [
            f'{{"level": "INFO", "job_id": "{JOB_ID}", "message": "mine"}}',
            f'{{"level": "INFO", "job_id": "{OTHER_ID}", "message": "not mine"}}',
        ]
    )
    wire = _wire(
        tmp_path=tmp_path,
        job={"id": JOB_ID, "context": {"log_archive_keys": ["k"]}},
        blobs={"k": text.encode()},
    )

    body = wire.client.get(f"/api/jobs/{JOB_ID}/logs").json()

    assert body["archived"] is True
    assert body["log_path"] is None
    assert len(body["lines"]) == 1
    assert OTHER_ID not in body["lines"][0]


def test_raw_live_log_returns_the_whole_unfiltered_text(tmp_path):
    text = "2026-07-15 10:00:01 - src.graph - INFO - fine\nsecond"
    _write_job_log(tmp_path, text)
    wire = _wire(tmp_path=tmp_path)

    resp = wire.client.get(
        f"/api/jobs/{JOB_ID}/logs", params={"raw": "true", "level": "ERROR"}
    )

    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/plain")
    assert resp.text == text  # the per-job file: untailed, unfiltered


def test_raw_archived_log_is_scoped_to_this_job(tmp_path):
    """N32: raw on a pooled pod log is this job's lines, not the whole log."""
    mine = f'{{"level": "INFO", "job_id": "{JOB_ID}", "message": "mine"}}'
    text = "\n".join(
        [mine, f'{{"level": "ERROR", "job_id": "{OTHER_ID}", "message": "other"}}']
    )
    wire = _wire(
        tmp_path=tmp_path,
        job={"id": JOB_ID, "context": {"log_archive_keys": ["k"]}},
        blobs={"k": text.encode()},
    )

    resp = wire.client.get(
        f"/api/jobs/{JOB_ID}/logs", params={"raw": "true", "level": "ERROR"}
    )

    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/plain")
    assert resp.text == mine  # scoped, but neither level-filtered nor tailed


def test_an_archived_log_with_no_line_for_this_job_serves_nothing(tmp_path):
    text = f'{{"job_id": "{OTHER_ID}", "message": "other"}}\nuntagged line'
    wire = _wire(
        tmp_path=tmp_path,
        job={"id": JOB_ID, "context": {"log_archive_keys": ["k"]}},
        blobs={"k": text.encode()},
    )

    raw = wire.client.get(f"/api/jobs/{JOB_ID}/logs", params={"raw": "true"})
    body = wire.client.get(f"/api/jobs/{JOB_ID}/logs").json()

    assert raw.status_code == 200
    assert raw.text == ""
    assert body["lines"] == []
    assert body["total_lines"] == 0


def test_level_and_grep_filters_are_reported(tmp_path):
    _write_job_log(
        tmp_path,
        "2026-07-15 10:00:00 - src.graph - ERROR - kaboom\n"
        "2026-07-15 10:00:01 - src.graph - INFO - fine",
    )
    wire = _wire(tmp_path=tmp_path)

    body = wire.client.get(f"/api/jobs/{JOB_ID}/logs", params={"level": "error"}).json()

    assert body["filtered"] is True
    assert body["lines"] == ["2026-07-15 10:00:00 - src.graph - ERROR - kaboom"]


@pytest.mark.parametrize("level", ["LOUD", "ERROR|INFO", ".*", "(A+)+$", "ERROR\\n"])
def test_an_invalid_level_is_a_400(tmp_path, level):
    _write_job_log(tmp_path, "anything")
    wire = _wire(tmp_path=tmp_path)
    resp = wire.client.get(f"/api/jobs/{JOB_ID}/logs", params={"level": level})
    assert resp.status_code == 400
    assert "Must be DEBUG, INFO, WARNING, or ERROR" in resp.json()["detail"]


def test_tail_keeps_the_last_n_lines_and_reports_the_full_total(tmp_path):
    _write_job_log(tmp_path, "\n".join(str(i) for i in range(50)))
    wire = _wire(tmp_path=tmp_path)
    body = wire.client.get(f"/api/jobs/{JOB_ID}/logs", params={"lines": 3}).json()
    assert body["lines"] == ["47", "48", "49"]
    assert body["total_lines"] == 50


@pytest.mark.parametrize("lines", [0, 1001])
def test_line_count_is_bounded(tmp_path, lines):
    wire = _wire(tmp_path=tmp_path)
    resp = wire.client.get(f"/api/jobs/{JOB_ID}/logs", params={"lines": lines})
    assert resp.status_code == 422


def test_an_unreadable_live_log_is_a_500(tmp_path):
    _write_job_log(tmp_path, "x")
    wire = _wire(tmp_path=tmp_path)
    with patch("pathlib.Path.read_text", side_effect=PermissionError("denied")):
        resp = wire.client.get(f"/api/jobs/{JOB_ID}/logs")
    assert resp.status_code == 500
    assert "Failed to read log file" in resp.json()["detail"]


def test_job_logs_refuse_a_caller_without_job_access(tmp_path):
    async def deny(_request, _store, _job_id):
        raise HTTPException(status_code=403, detail="Access denied")

    wire = _wire(tmp_path=tmp_path, job_gate=deny)
    assert wire.client.get(f"/api/jobs/{JOB_ID}/logs").status_code == 403


# =============================================================================
# Thread logs — archive only, same reader
# =============================================================================


def test_malformed_thread_id_is_refused_before_the_gate():
    gate = AsyncMock()
    wire = _wire(thread_gate=gate)
    resp = wire.client.get("/api/persistent/threads/nope/logs")
    assert resp.status_code == 400
    assert resp.json()["detail"] == "Invalid thread_id format: nope"
    gate.assert_not_awaited()


def test_a_session_with_no_archive_is_404():
    wire = _wire()
    resp = wire.client.get(f"/api/persistent/threads/{THREAD_ID}/logs")
    assert resp.status_code == 404
    assert "the agent pod may still" in resp.json()["detail"]


def test_thread_logs_are_always_reported_as_archived():
    lines = [f"{THREAD_ID} a", f"{THREAD_ID} b"]
    wire = _wire(
        thread={"id": THREAD_ID, "metadata": {"log_archive_keys": ["k"]}},
        blobs={"k": "\n".join(lines).encode()},
    )
    body = wire.client.get(f"/api/persistent/threads/{THREAD_ID}/logs").json()
    assert body["archived"] is True
    assert body["thread_id"] == THREAD_ID
    assert body["lines"] == lines


def test_raw_thread_log_is_scoped_to_this_session():
    mine = f'{{"thread_id": "{THREAD_ID}", "message": "mine"}}'
    text = "\n".join([mine, f'{{"thread_id": "{OTHER_ID}"}}', "untagged"])
    wire = _wire(
        thread={"id": THREAD_ID, "metadata": {"log_archive_keys": ["k"]}},
        blobs={"k": text.encode()},
    )
    resp = wire.client.get(
        f"/api/persistent/threads/{THREAD_ID}/logs", params={"raw": "true"}
    )
    assert resp.status_code == 200
    assert resp.text == mine


def test_thread_logs_use_the_same_scoping_and_filter_as_job_logs():
    text = "\n".join(
        [
            f'{{"level": "ERROR", "thread_id": "{THREAD_ID}", "message": "mine"}}',
            f'{{"level": "INFO", "thread_id": "{THREAD_ID}", "message": "quiet"}}',
            f'{{"level": "ERROR", "thread_id": "{OTHER_ID}", "message": "not mine"}}',
        ]
    )
    wire = _wire(
        thread={"id": THREAD_ID, "metadata": {"log_archive_keys": ["k"]}},
        blobs={"k": text.encode()},
    )

    body = wire.client.get(
        f"/api/persistent/threads/{THREAD_ID}/logs", params={"level": "ERROR"}
    ).json()

    assert body["filtered"] is True
    assert len(body["lines"]) == 1
    assert OTHER_ID not in body["lines"][0]


def test_thread_metadata_may_be_a_json_string():
    wire = _wire(
        thread={
            "id": THREAD_ID,
            "metadata": '{"log_archive_keys": ["k"]}',
        },
        blobs={"k": f"{THREAD_ID} stitched".encode()},
    )
    body = wire.client.get(f"/api/persistent/threads/{THREAD_ID}/logs").json()
    assert body["lines"] == [f"{THREAD_ID} stitched"]


def test_thread_logs_refuse_a_non_owner():
    async def deny(_request, _store, _thread_id):
        raise HTTPException(status_code=403, detail="Not your session")

    wire = _wire(thread_gate=deny)
    assert (
        wire.client.get(f"/api/persistent/threads/{THREAD_ID}/logs").status_code == 403
    )


# =============================================================================
# LLM requests
# =============================================================================


def test_llm_requests_are_503_without_an_audit_store():
    wire = _wire()
    resp = wire.client.get(f"/api/jobs/{JOB_ID}/llm-requests")
    assert resp.status_code == 503
    assert resp.json()["detail"] == "Audit store not available"


def test_llm_requests_validate_the_job_id_after_the_availability_check():
    """Order is characterized: availability first, then the id format."""
    reader = SimpleNamespace(is_available=True, list_llm_requests=AsyncMock())
    wire = _wire(audit_reader=reader)
    resp = wire.client.get("/api/jobs/not-a-uuid/llm-requests")
    assert resp.status_code == 400
    assert resp.json()["detail"] == "Invalid job_id format: not-a-uuid"
    reader.list_llm_requests.assert_not_awaited()


def test_llm_requests_forward_every_filter():
    reader = SimpleNamespace(
        is_available=True,
        list_llm_requests=AsyncMock(return_value={"requests": [], "total": 0}),
    )
    wire = _wire(audit_reader=reader)

    resp = wire.client.get(
        f"/api/jobs/{JOB_ID}/llm-requests",
        params={
            "limit": 5,
            "offset": 10,
            "call_type": "memory_extraction",
            "status": "error",
        },
    )

    assert resp.status_code == 200
    kwargs = reader.list_llm_requests.await_args.kwargs
    assert kwargs == {
        "limit": 5,
        "offset": 10,
        "call_type": "memory_extraction",
        "status": "error",
    }
    assert reader.list_llm_requests.await_args.args == (JOB_ID,)


def test_an_audit_store_failure_is_a_500():
    reader = SimpleNamespace(
        is_available=True,
        list_llm_requests=AsyncMock(side_effect=RuntimeError("store down")),
    )
    wire = _wire(audit_reader=reader)
    resp = wire.client.get(f"/api/jobs/{JOB_ID}/llm-requests")
    assert resp.status_code == 500
    assert resp.json()["detail"] == "store down"


def test_llm_requests_refuse_a_caller_without_job_access():
    async def deny(_request, _store, _job_id):
        raise HTTPException(status_code=403, detail="Access denied")

    reader = SimpleNamespace(is_available=True, list_llm_requests=AsyncMock())
    wire = _wire(audit_reader=reader, job_gate=deny)
    assert wire.client.get(f"/api/jobs/{JOB_ID}/llm-requests").status_code == 403
    reader.list_llm_requests.assert_not_awaited()


# =============================================================================
# Shell state
# =============================================================================


def test_shell_state_refuses_a_job_that_is_not_processing():
    wire = _wire(job={"id": JOB_ID, "status": "completed"})
    resp = wire.client.get(f"/api/jobs/{JOB_ID}/shell-state")
    assert resp.status_code == 400
    assert "Job is not processing" in resp.json()["detail"]


def test_shell_state_refuses_a_job_with_no_assigned_agent():
    wire = _wire(job={"id": JOB_ID, "status": "processing"})
    resp = wire.client.get(f"/api/jobs/{JOB_ID}/shell-state")
    assert resp.status_code == 400
    assert resp.json()["detail"] == "Job has no assigned agent"


def test_shell_state_never_dials_an_unattested_recipient():
    prepare = AsyncMock(return_value=None)
    wire = _wire(
        job={"id": JOB_ID, "status": "processing", "assigned_agent_id": AGENT_ID},
        prepare=prepare,
    )
    with patch("orchestrator.services.job_diagnostics.httpx.AsyncClient") as client:
        resp = wire.client.get(f"/api/jobs/{JOB_ID}/shell-state")
    assert resp.status_code == 409
    assert resp.json()["detail"] == {"code": "pinned_recipient_unavailable"}
    client.assert_not_called()
    prepare.assert_awaited_once_with(
        agent_id=AGENT_ID, job_id=JOB_ID, require_idle=False
    )


def _target(status=200, payload=None, text=""):
    recipient = MagicMock()
    recipient.model_dump.return_value = {"expected_agent_id": AGENT_ID}
    target = SimpleNamespace(
        agent={"pod_ip": "10.0.0.9", "pod_port": 8001}, recipient=recipient
    )
    response = SimpleNamespace(
        status_code=status, text=text, json=lambda: payload or {"tabs": []}
    )
    posts = []

    class _Client:
        def __init__(self, *a, **k):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def post(self, url, json):
            posts.append((url, json))
            return response

    return target, _Client, posts


def test_shell_state_posts_the_recipient_envelope_to_the_exact_pod():
    target, client_cls, posts = _target(payload={"tabs": [{"name": "work"}]})
    wire = _wire(
        job={"id": JOB_ID, "status": "processing", "assigned_agent_id": AGENT_ID},
        prepare=AsyncMock(return_value=target),
    )
    with patch("orchestrator.services.job_diagnostics.httpx.AsyncClient", client_cls):
        resp = wire.client.get(f"/api/jobs/{JOB_ID}/shell-state")
    assert resp.status_code == 200
    assert resp.json() == {"tabs": [{"name": "work"}]}
    assert posts == [
        ("http://10.0.0.9:8001/system/shell-state", {"expected_agent_id": AGENT_ID})
    ]


def test_an_agent_side_recipient_mismatch_stays_a_409():
    target, client_cls, _ = _target(status=409, text="mismatch")
    wire = _wire(
        job={"id": JOB_ID, "status": "processing", "assigned_agent_id": AGENT_ID},
        prepare=AsyncMock(return_value=target),
    )
    with patch("orchestrator.services.job_diagnostics.httpx.AsyncClient", client_cls):
        resp = wire.client.get(f"/api/jobs/{JOB_ID}/shell-state")
    assert resp.status_code == 409
    assert resp.json()["detail"] == {"code": "pinned_recipient_mismatch"}


def test_any_other_agent_status_is_a_502():
    target, client_cls, _ = _target(status=500, text="boom")
    wire = _wire(
        job={"id": JOB_ID, "status": "processing", "assigned_agent_id": AGENT_ID},
        prepare=AsyncMock(return_value=target),
    )
    with patch("orchestrator.services.job_diagnostics.httpx.AsyncClient", client_cls):
        resp = wire.client.get(f"/api/jobs/{JOB_ID}/shell-state")
    assert resp.status_code == 502
    assert "Agent returned 500" in resp.json()["detail"]


def test_a_transport_failure_is_a_502():
    import httpx

    target, _client_cls, _ = _target()

    class _Failing:
        def __init__(self, *a, **k):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def post(self, *a, **k):
            raise httpx.ConnectError("no route")

    wire = _wire(
        job={"id": JOB_ID, "status": "processing", "assigned_agent_id": AGENT_ID},
        prepare=AsyncMock(return_value=target),
    )
    with patch("orchestrator.services.job_diagnostics.httpx.AsyncClient", _Failing):
        resp = wire.client.get(f"/api/jobs/{JOB_ID}/shell-state")
    assert resp.status_code == 502
    assert "Failed to connect to agent" in resp.json()["detail"]


def test_shell_state_refuses_a_caller_without_job_access():
    async def deny(_request, _store, _job_id):
        raise HTTPException(status_code=403, detail="Access denied")

    prepare = AsyncMock()
    wire = _wire(job_gate=deny, prepare=prepare)
    assert wire.client.get(f"/api/jobs/{JOB_ID}/shell-state").status_code == 403
    prepare.assert_not_awaited()


# =============================================================================
# Per-application dependency isolation
# =============================================================================


def test_each_application_resolves_its_own_archive_store():
    first = _wire(
        thread={"id": THREAD_ID, "metadata": {"log_archive_keys": ["k"]}},
        blobs={"k": b"x"},
    )
    second = _wire()
    first.client.get(f"/api/persistent/threads/{THREAD_ID}/logs")
    first.ops.snapshots.get_blob.assert_awaited_once()
    second.ops.snapshots.get_blob.assert_not_awaited()
