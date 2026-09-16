"""Session wake on job completion — Phase 1.

Covers the delivery half (knowledge-base/knowledge/features/session_wake_on_job_completion.md):
enqueue guards, the claim/settle contract, live inject vs the durable branch,
the liveness predicate, and the payload. The DB is faked at the methods the
service touches; the SQL guards themselves (per-status dedup, SKIP LOCKED
disjointness, re-claim past the visibility timeout, the backstop arm) are
Postgres semantics and are exercised against a real server, not mocked here.

Two properties are worth stating because getting either wrong reintroduces the
bug the feature exists to remove:

* A wake that cannot be delivered must NOT be marked sent — it goes back for
  retry, or is buried as 'dead' so the operator sees it. Silently consuming it
  is indistinguishable from never having fired.
* A duplicate delivery is a visible message in the user's transcript plus a
  paid LLM turn, so the claim has to come before the send and a failure to
  settle must leave the row re-claimable rather than double-sent.
"""

from __future__ import annotations

import ast
import asyncio
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from orchestrator.services import session_wake
from shared.pinned_session_identity import PinnedSessionBinding

# Captured at import, before the autouse fixture replaces it with a mock — the
# two tests below need the REAL implementation.
_REAL_NOTIFY_OWNER = session_wake._notify_owner

JOB_ID = "3f2a1b8c-0000-4000-8000-000000000001"
THREAD_ID = "aa11bb22-0000-4000-8000-000000000002"
AGENT_ID = "cc33dd44-0000-4000-8000-000000000003"
RUNTIME_GENERATION = "dd44ee55-0000-4000-8000-000000000004"
ATTACH_TOKEN = "ee55ff66-0000-4000-8000-000000000005"
POD_UID = "ff660077-0000-4000-8000-000000000006"


def test_wake_service_has_no_direct_pod_ip_injection() -> None:
    """Kubernetes wakes must enter the durable inbox, never a raw Pod IP."""

    repository = Path(__file__).resolve().parents[1]
    calls: list[tuple[str, int]] = []
    # R1.B07 moved both sanctioned callers out of `main`: the conference hold
    # stand-by notice and the Legate one-liner. The guard follows them rather
    # than narrowing — the property is "exactly these two, each with the exact
    # DB recheck, and none inside the wake service itself".
    for relative in (
        "src/orchestrator/services/session_wake.py",
        "src/orchestrator/main.py",
        "src/orchestrator/services/officer_conference.py",
        "src/orchestrator/services/officer_notices.py",
    ):
        tree = ast.parse((repository / relative).read_text())
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            function = node.func
            name = (
                function.id
                if isinstance(function, ast.Name)
                else function.attr
                if isinstance(function, ast.Attribute)
                else ""
            )
            if name != "_inject_live":
                continue
            calls.append((relative, node.lineno))
            assert any(keyword.arg == "db" for keyword in node.keywords), (
                f"{relative}:{node.lineno} bypasses the exact DB recheck"
            )
    assert all(
        relative != "src/orchestrator/services/session_wake.py" for relative, _ in calls
    )
    assert len(calls) == 2


# --------------------------------------------------------------------------
# Fakes
# --------------------------------------------------------------------------


class _FakeResponse:
    def __init__(self, status_code: int, text: str = ""):
        self.status_code = status_code
        self.text = text

    def json(self):
        if self.status_code in {200, 202}:
            return {"delivery_state": _FakeAsyncClient.next_delivery_state}
        return {}


class _FakeAsyncClient:
    """Records POSTs; returns a scripted status (or raises a scripted error)."""

    posts: list = []
    next_status = 200
    next_delivery_state = "admitted"
    raises: Exception | None = None
    on_enter = None

    def __init__(self, *a, **kw):
        self.init_kwargs = kw

    async def __aenter__(self):
        if _FakeAsyncClient.on_enter is not None:
            _FakeAsyncClient.on_enter()
        return self

    async def __aexit__(self, *a):
        return False

    async def post(self, url, json=None, headers=None):
        _FakeAsyncClient.posts.append((url, json))
        if _FakeAsyncClient.raises is not None:
            raise _FakeAsyncClient.raises
        return _FakeResponse(_FakeAsyncClient.next_status)


def _claim_row(**over) -> dict:
    row = {
        "id": JOB_ID,
        "created_by_thread_id": THREAD_ID,
        "status": "completed",
        "wake_attempts": 1,
        "user_id": "u",
        "project_id": None,
        "description": "Explore a warm-neutral theme for the marketing site",
        "expert_id": None,
        "config_name": "worker_base",
        "freeze_data": None,
        "error_message": None,
    }
    row.update(over)
    return row


def _thread(**over) -> dict:
    t = {
        "id": THREAD_ID,
        "user_id": "u",
        "status": "active",
        "agent_id": AGENT_ID,
        "title": "Theme work",
        "execution_lane": "pinned",
        "runtime_generation": RUNTIME_GENERATION,
        "runtime_attach_token": ATTACH_TOKEN,
        "runtime_retirement_token": None,
        "metadata": {},
    }
    t.update(over)
    return t


def _agent(**over) -> dict:
    a = {
        "id": AGENT_ID,
        "thread_id": THREAD_ID,
        "status": "session",
        "pod_ip": "10.1.2.3",
        "pod_port": 8001,
        "hostname": "srw-agent-wake",
        "pod_uid": POD_UID,
    }
    a.update(over)
    return a


def _binding(thread: dict | None, agent: dict | None) -> PinnedSessionBinding | None:
    if (
        thread is None
        or agent is None
        or thread.get("execution_lane") != "pinned"
        or thread.get("status")
        not in {"created", "active", "awaiting_user", "suspended"}
        or thread.get("runtime_retirement_token") is not None
        or str(thread.get("agent_id") or "") != str(agent.get("id") or "")
        or str(agent.get("thread_id") or "") != str(thread.get("id") or "")
    ):
        return None
    try:
        return PinnedSessionBinding.from_mapping(
            {
                "thread_id": thread.get("id"),
                "runtime_generation": thread.get("runtime_generation"),
                "agent_id": agent.get("id"),
                "runtime_attach_token": thread.get("runtime_attach_token"),
                "agent_hostname": agent.get("hostname"),
                "pod_namespace": "srw",
                "pod_uid": agent.get("pod_uid"),
                "pod_ip": agent.get("pod_ip"),
                "pod_port": agent.get("pod_port"),
                "agent_status": agent.get("status"),
            }
        )
    except (TypeError, ValueError, AttributeError):
        return None


def _db(*, claimed=None, thread=None, agent=None) -> SimpleNamespace:
    save_thread_message = AsyncMock(
        return_value={
            "transcript_inserted": True,
            "thread_id": THREAD_ID,
            "state": "persisted",
        }
    )
    return SimpleNamespace(
        claim_pending_job_wakes=AsyncMock(
            return_value=list(claimed) if claimed is not None else []
        ),
        finish_job_wake=AsyncMock(return_value=True),
        release_job_wake=AsyncMock(return_value="pending"),
        defer_job_wake_for_input=AsyncMock(return_value=True),
        assign_job_wake_delivery=AsyncMock(return_value=True),
        mark_job_wake_pending=AsyncMock(return_value=True),
        get_thread=AsyncMock(return_value=thread),
        get_agent=AsyncMock(return_value=agent),
        pinned_thread_agent_is_reciprocal=AsyncMock(return_value=True),
        get_pinned_session_binding=AsyncMock(return_value=_binding(thread, agent)),
        get_expert_by_id=AsyncMock(return_value=None),
        save_thread_message=save_thread_message,
        persist_thread_input_delivery=save_thread_message,
        get_thread_job_counts=AsyncMock(
            return_value={"total": 0, "finished": 0, "running": 0}
        ),
        close_message_routes_for_terminal_jobs=AsyncMock(return_value=[]),
    )


@pytest.fixture(autouse=True)
def _reset_http(monkeypatch):
    _FakeAsyncClient.posts = []
    _FakeAsyncClient.next_status = 200
    _FakeAsyncClient.next_delivery_state = "admitted"
    _FakeAsyncClient.raises = None
    _FakeAsyncClient.on_enter = None
    monkeypatch.setattr(session_wake, "_notify_owner", AsyncMock())


# --------------------------------------------------------------------------
# Enqueue
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_maybe_wake_session_ignores_non_terminal_status():
    db = _db()
    assert await session_wake.maybe_wake_session(db, JOB_ID, "processing") is False
    db.mark_job_wake_pending.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status", ["completed", "failed", "cancelled", "pending_review"]
)
async def test_maybe_wake_session_enqueues_every_terminal_status(status):
    db = _db()
    assert await session_wake.maybe_wake_session(db, JOB_ID, status) is True
    db.mark_job_wake_pending.assert_awaited_once_with(JOB_ID, status)


@pytest.mark.asyncio
async def test_maybe_wake_session_never_raises_into_the_completion_path():
    """A completion must not fail because a notification could not be enqueued."""
    db = _db()
    db.mark_job_wake_pending = AsyncMock(side_effect=RuntimeError("db down"))
    assert await session_wake.maybe_wake_session(db, JOB_ID, "completed") is False


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["completed", "failed", "cancelled"])
async def test_terminal_transition_auto_closes_open_message_routes(status):
    """The auto-close ruling: every hooked terminal path (this choke point)
    closes the job's still-open worker-message routes so they stop showing
    as "open" in sitreps/pending counts after the job is dead."""
    db = _db()
    assert await session_wake.maybe_wake_session(db, JOB_ID, status) is True
    db.close_message_routes_for_terminal_jobs.assert_awaited_once_with(JOB_ID)


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["pending_review", "paused", "processing"])
async def test_non_final_statuses_do_not_close_message_routes(status):
    """pending_review and paused jobs can still resume and answer their open
    question — their routes must survive."""
    db = _db()
    await session_wake.maybe_wake_session(db, JOB_ID, status)
    db.close_message_routes_for_terminal_jobs.assert_not_awaited()


@pytest.mark.asyncio
async def test_route_auto_close_failure_does_not_cost_the_wake():
    """Fail-open: a broken close must neither raise into the completion path
    nor swallow the session wake itself."""
    db = _db()
    db.close_message_routes_for_terminal_jobs = AsyncMock(
        side_effect=RuntimeError("db down")
    )
    assert await session_wake.maybe_wake_session(db, JOB_ID, "cancelled") is True
    db.mark_job_wake_pending.assert_awaited_once_with(JOB_ID, "cancelled")


# --------------------------------------------------------------------------
# Live delivery
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_live_kubernetes_session_uses_durable_role_event():
    db = _db(claimed=[_claim_row()], thread=_thread(), agent=_agent())

    assert await session_wake.drain_pending_wakes(db) == 0

    assert _FakeAsyncClient.posts == []
    kwargs = db.persist_thread_input_delivery.await_args.kwargs
    assert kwargs["thread_id"] == THREAD_ID
    assert kwargs["role"] == "event"
    assert kwargs["content"].startswith("[JOB_FINISHED]")
    db.defer_job_wake_for_input.assert_awaited_once_with(JOB_ID)
    db.finish_job_wake.assert_not_awaited()


@pytest.mark.asyncio
async def test_blocked_outcome_uses_one_truthful_officer_dedup_identity():
    row = _claim_row(
        status="cancelled",
        completion_outcome_kind="blocked_undelivered",
        project_id="project-1",
    )
    thread = _thread(metadata={"config_override": {"officer": {"enabled": True}}})
    db = _db(claimed=[row], thread=thread, agent=_agent())
    db.enqueue_session_wake_event = AsyncMock(return_value=True)

    assert await session_wake.drain_pending_wakes(db) == 1

    db.enqueue_session_wake_event.assert_awaited_once()
    kwargs = db.enqueue_session_wake_event.await_args.kwargs
    assert kwargs["dedup_key"] == f"{JOB_ID[:8]}:blocked_undelivered"
    assert kwargs["payload"]["status"] == "blocked_undelivered"
    db.finish_job_wake.assert_awaited_once_with(JOB_ID, "cancelled")
    assert _FakeAsyncClient.posts == []


@pytest.mark.asyncio
async def test_completion_hook_and_outbox_share_blocked_officer_dedup_key(
    monkeypatch,
):
    db = _db()
    db.get_job = AsyncMock(
        return_value={
            **_claim_row(
                status="cancelled",
                completion_outcome_kind="blocked_undelivered",
                project_id="project-1",
            )
        }
    )
    db.route_project_officer_job_transition = AsyncMock(return_value={"enqueued": True})
    monkeypatch.setattr(session_wake, "kick_event_drain", lambda _db: None)

    assert await session_wake._notify_project_officer_of_job(db, JOB_ID, "cancelled")
    kwargs = db.route_project_officer_job_transition.await_args.kwargs
    assert kwargs["status"] == "blocked_undelivered"
    assert kwargs["dedup_key"] == f"{JOB_ID[:8]}:blocked_undelivered"


@pytest.mark.asyncio
async def test_kubernetes_wake_opens_no_http_client():
    db = _db(claimed=[_claim_row()], thread=_thread(), agent=_agent())
    await session_wake.drain_pending_wakes(db)
    assert _FakeAsyncClient.posts == []


@pytest.mark.asyncio
async def test_queued_receipt_stays_retryable_until_provider_admission():
    row = _claim_row()
    db = _db(claimed=[row], thread=_thread(), agent=_agent())
    db.persist_thread_input_delivery.side_effect = [
        {"thread_id": THREAD_ID, "state": "queued", "transcript_inserted": True},
        {"thread_id": THREAD_ID, "state": "admitted", "transcript_inserted": False},
    ]

    assert await session_wake.drain_pending_wakes(db) == 0
    db.defer_job_wake_for_input.assert_awaited_once_with(JOB_ID)
    db.finish_job_wake.assert_not_awaited()

    assert await session_wake.drain_pending_wakes(db) == 1
    db.finish_job_wake.assert_awaited_once_with(JOB_ID, "completed")
    calls = db.persist_thread_input_delivery.await_args_list
    assert calls[0].kwargs["delivery_id"] == calls[1].kwargs["delivery_id"]


@pytest.mark.asyncio
async def test_pod_port_is_irrelevant_to_durable_wake():
    db = _db(claimed=[_claim_row()], thread=_thread(), agent=_agent(pod_port=None))
    await session_wake.drain_pending_wakes(db)
    assert _FakeAsyncClient.posts == []
    db.persist_thread_input_delivery.assert_awaited_once()


# --------------------------------------------------------------------------
# Liveness predicate
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_durable_wake_does_not_probe_a_recyclable_pod_ip(monkeypatch):
    probe = AsyncMock(return_value=False)
    db = _db(claimed=[_claim_row()], thread=_thread(), agent=_agent())

    assert await session_wake.drain_pending_wakes(db) == 0

    assert _FakeAsyncClient.posts == []
    db.save_thread_message.assert_awaited_once()
    assert db.save_thread_message.await_args.kwargs["role"] == "event"
    db.finish_job_wake.assert_not_awaited()
    db.defer_job_wake_for_input.assert_awaited_once_with(JOB_ID)
    probe.assert_not_awaited()


@pytest.mark.parametrize(
    "changed_binding",
    [
        replace(_binding(_thread(), _agent()), agent_hostname="successor-agent"),
        replace(_binding(_thread(), _agent()), pod_uid="successor-pod-uid"),
        replace(_binding(_thread(), _agent()), pod_ip="10.9.8.7"),
        replace(_binding(_thread(), _agent()), pod_port=9001),
        replace(
            _binding(_thread(), _agent()),
            agent_id="11111111-2222-4333-8444-555555555555",
        ),
        replace(
            _binding(_thread(), _agent()),
            runtime_attach_token="99999999-8888-4777-8666-555555555555",
        ),
    ],
    ids=["hostname", "pod_uid", "pod_ip", "pod_port", "agent_id", "attach"],
)
@pytest.mark.asyncio
async def test_post_probe_binding_change_never_reaches_live_input(changed_binding):
    """A stale ready Pod cannot receive a wake after any DB target rotation."""

    thread = _thread()
    agent = _agent()
    original = _binding(thread, agent)
    assert original is not None
    db = _db(claimed=[_claim_row()], thread=thread, agent=agent)
    db.get_pinned_session_binding.side_effect = [original, changed_binding]

    assert await session_wake.drain_pending_wakes(db) == 0

    assert _FakeAsyncClient.posts == []
    db.save_thread_message.assert_awaited_once()
    db.finish_job_wake.assert_not_awaited()


@pytest.mark.asyncio
async def test_live_status_transition_still_uses_durable_inbox():
    thread = _thread()
    agent = _agent(status="ready")
    original = _binding(thread, agent)
    assert original is not None
    db = _db(claimed=[_claim_row()], thread=thread, agent=agent)
    db.get_pinned_session_binding.side_effect = [
        original,
        replace(original, agent_status="working"),
        replace(original, agent_status="working"),
    ]

    assert await session_wake.drain_pending_wakes(db) == 0
    assert _FakeAsyncClient.posts == []
    db.persist_thread_input_delivery.assert_awaited_once()


@pytest.mark.parametrize(
    "changed_binding",
    [
        replace(_binding(_thread(), _agent()), pod_uid="successor-pod-uid"),
        replace(_binding(_thread(), _agent()), agent_status="offline"),
    ],
    ids=["pod_uid", "offline_status"],
)
@pytest.mark.asyncio
async def test_client_entry_binding_rotation_never_reaches_old_target(
    changed_binding,
):
    """Client setup is an await boundary, so exact DB authority follows it."""

    thread = _thread()
    agent = _agent()
    original = _binding(thread, agent)
    assert original is not None
    rotated = {"value": False}
    db = _db(claimed=[_claim_row()], thread=thread, agent=agent)

    async def _current_binding(*_args, **_kwargs):
        return changed_binding if rotated["value"] else original

    db.get_pinned_session_binding.side_effect = _current_binding
    _FakeAsyncClient.on_enter = lambda: rotated.__setitem__("value", True)

    assert await session_wake.drain_pending_wakes(db) == 0
    assert _FakeAsyncClient.posts == []
    db.save_thread_message.assert_awaited_once()


@pytest.mark.asyncio
async def test_post_probe_offline_status_never_reaches_live_input():
    thread = _thread()
    agent = _agent(status="ready")
    original = _binding(thread, agent)
    assert original is not None
    db = _db(claimed=[_claim_row()], thread=thread, agent=agent)
    db.get_pinned_session_binding.side_effect = [
        original,
        replace(original, agent_status="offline"),
    ]

    assert await session_wake.drain_pending_wakes(db) == 0
    assert _FakeAsyncClient.posts == []
    db.save_thread_message.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "agent_over",
    [
        {"status": "offline"},
        {"status": "booting"},
        {"pod_ip": None},
    ],
)
async def test_unusable_agent_states_take_the_durable_branch(agent_over):
    db = _db(claimed=[_claim_row()], thread=_thread(), agent=_agent(**agent_over))
    assert await session_wake.drain_pending_wakes(db) == 0
    assert _FakeAsyncClient.posts == []
    db.save_thread_message.assert_awaited_once()


@pytest.mark.asyncio
async def test_delivery_does_not_self_heal_a_stale_binding():
    """/connection clears a dead agent_id as a user-driven repair. A background
    delivery must not mutate session bindings on the way past."""
    db = _db(claimed=[_claim_row()], thread=_thread(), agent=_agent(status="offline"))
    db.update_thread_agent = AsyncMock()
    await session_wake.drain_pending_wakes(db)
    db.update_thread_agent.assert_not_awaited()


# --------------------------------------------------------------------------
# Durable branch
# --------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["suspended", "ended", "awaiting_user", "idle"])
async def test_non_live_threads_get_a_durable_row_and_no_restore(status):
    """'ended' is deliberately included: an active thread whose pod dies is
    marked 'ended', not 'suspended', and ended threads are user-resumable —
    skipping them would silently drop completions for a supported case."""
    db = _db(claimed=[_claim_row()], thread=_thread(status=status, agent_id=None))
    db.restore_thread_workspace = AsyncMock()

    assert await session_wake.drain_pending_wakes(db) == 0

    db.save_thread_message.assert_awaited_once()
    kwargs = db.save_thread_message.await_args.kwargs
    assert kwargs["thread_id"] == THREAD_ID
    assert kwargs["role"] == "event"
    assert "[JOB_FINISHED]" in kwargs["content"]
    # Phase 1 explicitly never resumes a suspended pod — that is the resume-OOM
    # surface and it belongs to Phase 2.
    db.restore_thread_workspace.assert_not_awaited()


@pytest.mark.asyncio
async def test_durable_branch_notifies_the_owner(monkeypatch):
    notify = AsyncMock()
    monkeypatch.setattr(session_wake, "_notify_owner", notify)
    db = _db(claimed=[_claim_row()], thread=_thread(agent_id=None))
    await session_wake.drain_pending_wakes(db)
    notify.assert_awaited_once()


@pytest.mark.asyncio
async def test_owner_notification_records_a_session_wake_row(monkeypatch):
    """The owner's half of the durable branch is a ``session_wake`` feed row
    (unified notification system): addressed to the thread owner, deduped per
    (thread, job) so a re-claimed wake never files twice, with the session
    thread as its source so the cockpit can deep-link back. Which channel
    reaches the owner — and at what address — is the notification system's
    business; nothing here resolves an email. (The old dispatch() path handed
    the email leg an empty address and silently dropped the notice — live-gate
    regression, 2026-07-27; a feed row cannot be dropped that way.)"""
    recorded = {}

    class _Svc:
        async def record(self, **kw):
            recorded.update(kw)
            return SimpleNamespace(notification_id="n-1", inserted=True)

    monkeypatch.setitem(
        __import__("sys").modules,
        "orchestrator.services.notification_service",
        type("M", (), {"notification_service": _Svc()}),
    )
    db = _db(claimed=[_claim_row()], thread=_thread(agent_id=None))
    monkeypatch.setattr(session_wake, "_notify_owner", _REAL_NOTIFY_OWNER)

    assert await session_wake.drain_pending_wakes(db) == 0

    assert recorded["recipient_id"] == "u"
    assert recorded["category"] == "session_wake"
    assert recorded["dedup_key"] == f"session_wake:{THREAD_ID}:{JOB_ID}"
    assert recorded["source_kind"] == "thread"
    assert recorded["source_id"] == THREAD_ID
    assert recorded["action_params"] == {"thread_id": THREAD_ID, "job_id": JOB_ID}
    assert recorded["payload"]["job_id"] == JOB_ID
    assert recorded["payload"]["status"] == "completed"
    assert recorded["payload"]["title"] == "Theme work"
    assert JOB_ID[:8] in recorded["subject"]


@pytest.mark.asyncio
async def test_owner_without_an_email_still_gets_the_row(monkeypatch):
    """An owner with no address on file is NOT skipped at this layer: the feed
    row is the durable half and lands in-app regardless. Suppressing the email
    leg for an addressless recipient is the notification system's job — the
    wake never looks the user up."""
    calls = []

    class _Svc:
        async def record(self, **kw):
            calls.append(kw)
            return SimpleNamespace(notification_id="n-1", inserted=True)

    monkeypatch.setitem(
        __import__("sys").modules,
        "orchestrator.services.notification_service",
        type("M", (), {"notification_service": _Svc()}),
    )
    db = _db(claimed=[_claim_row()], thread=_thread(agent_id=None))
    db.get_user = AsyncMock(return_value={"id": "u", "email": None})
    monkeypatch.setattr(session_wake, "_notify_owner", _REAL_NOTIFY_OWNER)

    assert await session_wake.drain_pending_wakes(db) == 0
    assert len(calls) == 1
    assert calls[0]["recipient_id"] == "u"
    db.get_user.assert_not_awaited()


@pytest.mark.asyncio
async def test_durable_kubernetes_delivery_notifies_the_owner(monkeypatch):
    notify = AsyncMock()
    monkeypatch.setattr(session_wake, "_notify_owner", notify)
    db = _db(claimed=[_claim_row()], thread=_thread(), agent=_agent())
    await session_wake.drain_pending_wakes(db)
    notify.assert_awaited_once()


@pytest.mark.asyncio
async def test_failed_notification_does_not_undeliver_the_notice(monkeypatch):
    monkeypatch.setattr(
        session_wake, "_notify_owner", AsyncMock(side_effect=RuntimeError("smtp down"))
    )
    db = _db(claimed=[_claim_row()], thread=_thread(agent_id=None))
    assert await session_wake.drain_pending_wakes(db) == 0
    db.finish_job_wake.assert_not_awaited()
    db.defer_job_wake_for_input.assert_awaited_once_with(JOB_ID)


@pytest.mark.asyncio
async def test_durable_retry_finishes_only_after_provider_admission():
    db = _db(claimed=[_claim_row()], thread=_thread(agent_id=None))
    db.persist_thread_input_delivery = AsyncMock(
        return_value={
            "transcript_inserted": False,
            "thread_id": THREAD_ID,
            "state": "admitted",
        }
    )

    assert await session_wake.drain_pending_wakes(db) == 1

    db.finish_job_wake.assert_awaited_once_with(JOB_ID, "completed")
    db.defer_job_wake_for_input.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("disposition", ["historical", "superseded"])
async def test_rewound_delivery_receipt_closes_wake_without_new_execution(disposition):
    db = _db(claimed=[_claim_row()], thread=_thread(agent_id=None))
    db.persist_thread_input_delivery = AsyncMock(
        return_value={
            "transcript_inserted": False,
            "thread_id": THREAD_ID,
            "state": "persisted",
            "execution_disposition": disposition,
        }
    )

    assert await session_wake.drain_pending_wakes(db) == 1
    db.finish_job_wake.assert_awaited_once_with(JOB_ID, "completed")
    db.defer_job_wake_for_input.assert_not_awaited()


@pytest.mark.asyncio
async def test_wrong_thread_delivery_receipt_cannot_settle_wake():
    db = _db(claimed=[_claim_row()], thread=_thread(agent_id=None))
    db.persist_thread_input_delivery = AsyncMock(
        return_value={
            "transcript_inserted": True,
            "thread_id": "ffffffff-ffff-4fff-8fff-ffffffffffff",
            "state": "admitted",
        }
    )

    assert await session_wake.drain_pending_wakes(db) == 0
    db.finish_job_wake.assert_not_awaited()
    db.release_job_wake.assert_awaited_once_with(
        JOB_ID,
        max_attempts=session_wake.MAX_ATTEMPTS,
    )


# --------------------------------------------------------------------------
# Settle contract
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_no_thread_backref_consumes_the_claim_without_delivering():
    """ON DELETE SET NULL nulled the backref — nobody to wake, and re-claiming
    forever would starve real wakes behind it in the ORDER BY."""
    db = _db(claimed=[_claim_row(created_by_thread_id=None)])
    assert await session_wake.drain_pending_wakes(db) == 1
    db.get_thread.assert_not_awaited()
    db.finish_job_wake.assert_awaited_once()
    db.release_job_wake.assert_not_awaited()


@pytest.mark.asyncio
async def test_durable_write_failure_releases_for_retry():
    db = _db(claimed=[_claim_row()], thread=_thread(agent_id=None))
    db.persist_thread_input_delivery = AsyncMock(
        side_effect=RuntimeError("write failed")
    )

    assert await session_wake.drain_pending_wakes(db) == 0

    db.finish_job_wake.assert_not_awaited()
    db.release_job_wake.assert_awaited_once_with(
        JOB_ID, max_attempts=session_wake.MAX_ATTEMPTS
    )


@pytest.mark.asyncio
async def test_thread_lookup_failure_releases_rather_than_consuming():
    db = _db(claimed=[_claim_row()])
    db.get_thread = AsyncMock(side_effect=RuntimeError("db blip"))
    assert await session_wake.drain_pending_wakes(db) == 0
    db.release_job_wake.assert_awaited_once()


@pytest.mark.asyncio
async def test_vanished_thread_consumes_the_claim():
    db = _db(claimed=[_claim_row()], thread=None)
    assert await session_wake.drain_pending_wakes(db) == 1
    db.finish_job_wake.assert_awaited_once()


@pytest.mark.asyncio
async def test_hard_delete_retirement_wins_the_late_finish_cas():
    """A pre-delete claim may retain its old thread projection. Once delivery
    resolves, the finish CAS must report that deletion already retired it and
    the drain must not count an undeliverable wake as delivered."""
    db = _db(claimed=[_claim_row()], thread=None)
    db.finish_job_wake = AsyncMock(return_value=False)

    assert await session_wake.drain_pending_wakes(db) == 0

    db.finish_job_wake.assert_awaited_once_with(JOB_ID, "completed")
    db.release_job_wake.assert_not_awaited()


@pytest.mark.asyncio
async def test_one_bad_row_does_not_abort_the_rest_of_the_batch():
    rows = [
        _claim_row(id="j1", created_by_thread_id=None),
        _claim_row(id="j2"),
    ]
    db = _db(claimed=rows, thread=_thread(agent_id=None))
    db.persist_thread_input_delivery = AsyncMock(
        side_effect=[RuntimeError("boom"), {"transcript_inserted": True}]
    )
    # j1 short-circuits (no backref), j2's durable write raises then releases.
    await session_wake.drain_pending_wakes(db)
    assert db.finish_job_wake.await_count == 1  # j1 only
    assert db.release_job_wake.await_count == 1  # j2


@pytest.mark.asyncio
async def test_a_batch_is_delivered_concurrently_within_a_cap(monkeypatch):
    """A claim belongs to this drain only for the visibility window. Serial
    delivery of a fan-out into dead pods (~12s each) would overrun it and let
    another replica re-claim a row still being sent — the exact duplicate the
    claim exists to prevent. The cap keeps a burst from opening one socket per
    dead pod at once."""
    monkeypatch.setattr(session_wake, "_DELIVER_CONCURRENCY", 3)
    inflight = 0
    peak = 0

    async def _slow(db, row):
        nonlocal inflight, peak
        inflight += 1
        peak = max(peak, inflight)
        await asyncio.sleep(0.01)
        inflight -= 1
        return True

    monkeypatch.setattr(session_wake, "_deliver", _slow)
    db = _db(claimed=[_claim_row(id=f"j{i}") for i in range(9)])

    assert await session_wake.drain_pending_wakes(db) == 9
    assert peak > 1, "deliveries ran serially"
    assert peak <= 3, f"concurrency cap exceeded (peak={peak})"


@pytest.mark.asyncio
async def test_a_settle_failure_is_not_counted_as_delivered(monkeypatch):
    """Re-delivering is the lesser evil against marking a wake sent that never
    arrived — so a failed settle leaves the row 'sending' for the timeout to
    re-claim, and must not inflate the delivered count."""
    db = _db(claimed=[_claim_row()], thread=_thread(), agent=_agent())
    db.finish_job_wake = AsyncMock(side_effect=RuntimeError("settle failed"))

    assert await session_wake.drain_pending_wakes(db) == 0


@pytest.mark.asyncio
async def test_claim_failure_is_swallowed():
    db = _db()
    db.claim_pending_job_wakes = AsyncMock(side_effect=RuntimeError("db down"))
    assert await session_wake.drain_pending_wakes(db) == 0


@pytest.mark.asyncio
async def test_the_claim_carries_the_visibility_timeout():
    db = _db()
    await session_wake.drain_pending_wakes(db)
    db.claim_pending_job_wakes.assert_awaited_once_with(
        limit=session_wake.CLAIM_BATCH,
        visibility_timeout_seconds=session_wake.VISIBILITY_TIMEOUT_SECONDS,
    )


# --------------------------------------------------------------------------
# Payload
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_payload_carries_pointers_not_the_result():
    """Inlining output would make every wake expensive and defeat delegating."""
    row = _claim_row(
        freeze_data={
            "summary": "Three theme directions explored.",
            "confidence": 85,
            "deliverables": ["a.md", "b.md", "c.md", "d.md"],
        }
    )
    db = _db(claimed=[row], thread=_thread(agent_id=None))
    await session_wake.drain_pending_wakes(db)

    text = db.save_thread_message.await_args.kwargs["content"]
    assert "4 deliverables" in text
    assert "get_job" in text
    assert "a.md" not in text  # names of the files are output, not a pointer
    assert "Confidence: 85" in text
    assert "Three theme directions explored." in text


@pytest.mark.asyncio
async def test_payload_names_the_task_so_fanned_out_jobs_are_distinguishable():
    db = _db(claimed=[_claim_row()], thread=_thread(agent_id=None))
    await session_wake.drain_pending_wakes(db)
    text = db.save_thread_message.await_args.kwargs["content"]
    assert "- Task: Explore a warm-neutral theme for the marketing site" in text


@pytest.mark.asyncio
async def test_payload_parses_freeze_data_delivered_as_a_json_string():
    """asyncpg hands JSONB back as a raw string; a naive .get() would silently
    drop the summary."""
    db = _db(
        claimed=[_claim_row(freeze_data='{"summary": "from json", "confidence": 40}')],
        thread=_thread(agent_id=None),
    )
    await session_wake.drain_pending_wakes(db)
    text = db.save_thread_message.await_args.kwargs["content"]
    assert "from json" in text and "Confidence: 40" in text


@pytest.mark.asyncio
async def test_payload_carries_the_sibling_set():
    """Saves the agent a list_jobs round-trip on every wake."""
    db = _db(claimed=[_claim_row()], thread=_thread(agent_id=None))
    db.get_thread_job_counts = AsyncMock(
        return_value={
            "total": 3,
            "finished": 1,
            "running": 1,
            "failed": 1,
            "cancelled": 0,
        }
    )
    await session_wake.drain_pending_wakes(db)
    text = db.save_thread_message.await_args.kwargs["content"]
    assert "1 of 3 finished" in text
    assert "1 still running" in text and "1 failed" in text


@pytest.mark.asyncio
async def test_sibling_line_omitted_for_a_lone_job():
    db = _db(claimed=[_claim_row()], thread=_thread(agent_id=None))
    db.get_thread_job_counts = AsyncMock(
        return_value={"total": 1, "finished": 1, "running": 0}
    )
    await session_wake.drain_pending_wakes(db)
    assert "outstanding jobs" not in db.save_thread_message.await_args.kwargs["content"]


@pytest.mark.asyncio
async def test_expert_name_is_used_when_the_job_ran_a_db_expert():
    db = _db(claimed=[_claim_row(expert_id="e1")], thread=_thread(agent_id=None))
    db.get_expert_by_id = AsyncMock(return_value={"name": "designer"})
    await session_wake.drain_pending_wakes(db)
    assert "expert: designer" in db.save_thread_message.await_args.kwargs["content"]


@pytest.mark.asyncio
async def test_error_message_surfaces_only_for_failures():
    db = _db(
        claimed=[_claim_row(status="failed", error_message="OOMKilled")],
        thread=_thread(agent_id=None),
    )
    await session_wake.drain_pending_wakes(db)
    text = db.save_thread_message.await_args.kwargs["content"]
    assert "- Error: OOMKilled" in text
    assert "- Status: failed" in text


@pytest.mark.asyncio
async def test_payload_survives_a_counts_lookup_failure():
    db = _db(claimed=[_claim_row()], thread=_thread(agent_id=None))
    db.get_thread_job_counts = AsyncMock(side_effect=RuntimeError("nope"))
    assert await session_wake.drain_pending_wakes(db) == 0


# --------------------------------------------------------------------------
# Sweeper
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_sweeper_drains_then_exits_on_shutdown(monkeypatch):
    monkeypatch.setattr(session_wake, "TICK_SECONDS", 0.01)
    drained = AsyncMock(return_value=0)
    monkeypatch.setattr(session_wake, "drain_pending_wakes", drained)
    shutdown = asyncio.Event()

    task = asyncio.create_task(session_wake.session_wake_sweeper_loop(_db(), shutdown))
    await asyncio.sleep(0.05)
    shutdown.set()
    await asyncio.wait_for(task, timeout=2)

    assert drained.await_count >= 1


@pytest.mark.asyncio
async def test_sweeper_tick_survives_a_raising_drain(monkeypatch):
    monkeypatch.setattr(session_wake, "TICK_SECONDS", 0.01)
    calls = []

    async def _boom(db, **kw):
        calls.append(1)
        raise RuntimeError("tick blew up")

    monkeypatch.setattr(session_wake, "drain_pending_wakes", _boom)
    shutdown = asyncio.Event()
    task = asyncio.create_task(session_wake.session_wake_sweeper_loop(_db(), shutdown))
    await asyncio.sleep(0.05)
    shutdown.set()
    await asyncio.wait_for(task, timeout=2)

    assert len(calls) >= 2, "a raising tick must not kill the loop"
