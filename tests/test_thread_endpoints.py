"""Tests for orchestrator thread endpoints (persistent agent system).

Tests replicate endpoint logic directly rather than importing from
orchestrator.main, which requires database and service dependencies
that aren't available in the test environment.

Covers sections 5.1–5.13 of persistent_agent_tests.md:
  - POST /api/agents/threads (agent-facing)
  - GET  /api/agents/threads/{thread_id}/workspace
  - POST /api/agents/threads/{thread_id}/upgrade-to-vm
  - POST /api/agents/threads/{thread_id}/messages
  - POST /api/persistent/threads (user-facing, auth)
  - GET  /api/persistent/threads
  - GET  /api/persistent/threads/{thread_id}
  - DELETE /api/persistent/threads/{thread_id}
  - GET  /api/persistent/threads/{thread_id}/messages
  - GET  /api/persistent/threads/{thread_id}/ide
  - WS   /ws/persistent/{thread_id} (proxy)
  - _inspect_session_event()
  - _inspect_browser_event()
"""

import asyncio
import json
import os
from unittest.mock import AsyncMock, MagicMock, PropertyMock, patch
from uuid import UUID, uuid4

import pytest

from orchestrator.services.workspace_lifecycle import WorkspaceOwner


# =============================================================================
# Replicated endpoint logic — mirrors orchestrator/main.py handlers
# =============================================================================


async def agent_create_thread(
    db,
    gitea,
    provisioner,
    *,
    config_name="persistent_defaults",
    permission_mode="read_only",
    title="Local Session",
    inject_credentials=None,
    inject_env_credentials=None,
):
    """5.1: POST /api/agents/threads — mirrors orchestrator/main.py handler.

    Includes the system-default chat/auxiliary/embedding pin injection
    that lands a config_override in thread metadata. Without this, the
    standalone agent boots against its YAML default and 401s on
    api.openai.com (see knowledge-base/knowledge/hardcoded_model_defaults.md).
    """
    thread_id = await db.create_thread(
        user_id=None,
        config_name=config_name,
        permission_mode=permission_mode,
        title=title,
    )

    config_override = {}
    chat_model = await db.resolve_default_for_capability("chat")
    if chat_model:
        llm_section = {"model": chat_model}
        if inject_credentials:
            await inject_credentials(
                section=llm_section,
                model_id=chat_model,
                user_id=None,
                resolved_keys=None,
            )
        config_override["llm"] = llm_section
    aux_model = await db.resolve_default_for_capability("auxiliary")
    if aux_model:
        aux_section = {"model": aux_model}
        if inject_credentials:
            await inject_credentials(
                section=aux_section,
                model_id=aux_model,
                user_id=None,
                resolved_keys=None,
            )
        config_override["auxiliary"] = aux_section
    embedding_model = await db.resolve_default_for_capability("embedding")
    if embedding_model:
        env_keys = {"EMBEDDING_MODEL": embedding_model}
        if inject_env_credentials:
            await inject_env_credentials(
                env_keys=env_keys,
                prefix="EMBEDDING",
                model_id=embedding_model,
                user_id=None,
                resolved_keys=None,
            )
        config_override["env_keys"] = env_keys
    if config_override:
        await db.merge_thread_metadata_config_override(thread_id, config_override)

    if gitea.is_initialized:
        repo_name = f"thread-{thread_id[:8]}"
        git_remote_url = await gitea.create_repo(repo_name, intent_marker=str(uuid4()))
        if git_remote_url:
            await db.merge_thread_workspace_context(
                thread_id, {"git_remote_url": git_remote_url, "repo_name": repo_name}
            )
    if provisioner.is_available:
        asyncio.create_task(
            provisioner.create_workspace(WorkspaceOwner.session(thread_id))
        )
    return {"thread_id": thread_id, "status": "created"}


def parse_workspace_metadata(thread):
    """5.2: GET /api/agents/threads/{thread_id}/workspace — parse + structure."""
    if not thread:
        return None  # caller raises 404
    metadata = thread.get("metadata") or {}
    if isinstance(metadata, str):
        try:
            metadata = json.loads(metadata)
        except (json.JSONDecodeError, TypeError):
            metadata = {}
    ws = metadata.get("workspace_container") or {}
    vm = metadata.get("vm") or {}
    return {
        "status": ws.get("status", "none"),
        "pod_ip": ws.get("pod_ip"),
        "pod_name": ws.get("pod_name"),
        "namespace": ws.get("namespace"),
        "git_remote_url": ws.get("git_remote_url"),
        "vm_status": vm.get("status"),
        "vm_ssh_host": vm.get("ssh_host"),
        "vm_ssh_port": vm.get("ssh_port"),
        "vm_name": vm.get("vm_name"),
        "config_override": metadata.get("config_override"),
        "project_ids": metadata.get("project_ids"),
    }


async def agent_upgrade_to_vm(db, vm_provisioner, thread_id, grant_gate=None):
    """5.3: POST /api/agents/threads/{thread_id}/upgrade-to-vm

    ``grant_gate`` mirrors the shared Sec-1 authorization gate
    (``_enforce_workspace_upgrade_grants``), now run on this endpoint too
    (Phase 2: it was previously ungated). Async ``(thread, target_tier) -> None``
    raising ``_GateDenied`` on refusal, run BEFORE provisioning (fail-closed).
    """
    thread = await db.get_thread(thread_id)
    if not thread:
        return {"error": 404, "detail": "Thread not found"}
    if grant_gate is not None:
        try:
            await grant_gate(thread, "vm")
        except _GateDenied as exc:
            return {"error": 403, "detail": str(exc)}
    if not vm_provisioner.is_available:
        return {"error": 503, "detail": "VM provisioning not available"}

    metadata = thread.get("metadata") or {}
    if isinstance(metadata, str):
        try:
            metadata = json.loads(metadata)
        except (json.JSONDecodeError, TypeError):
            metadata = {}
    vm_ctx = metadata.get("vm") or {}
    if vm_ctx.get("status") in ("provisioning", "created", "ready"):
        return {
            "status": vm_ctx["status"],
            "thread_id": thread_id,
            "message": "VM already provisioned or in progress",
        }

    ok = await vm_provisioner.create_thread_vm(
        thread_id=thread_id,
        agent_config=thread.get("config_name", "defaults"),
    )
    if not ok:
        return {"error": 500, "detail": "Failed to request VM provisioning"}

    return {
        "status": "provisioning",
        "thread_id": thread_id,
        "vm_provisioner_mode": vm_provisioner.mode,
    }


class _GateDenied(Exception):
    """Stand-in for the orchestrator's HTTPException(403) raised by the shared
    Sec-1 grant gate (_enforce_workspace_upgrade_grants)."""


async def agent_upgrade_to_workspace(
    db,
    provisioner,
    thread_id,
    target_tier="sandbox",
    grant_gate=None,
    vm_provisioner=None,
):
    """5.3b: POST /api/agents/threads/{thread_id}/upgrade-to-workspace
    (workspace_tier_upgrade.md §4.2 S2 + S4 / Phase 2) — mirrors orchestrator/main.py.

    Provisions a real container for a lite thread (virtual/none -> sandbox).
    Idempotent on an in-flight/ready container. ``vm`` is delegated to the
    operator-gated upgrade-to-vm path (Phase 2), which runs the same grant gate.
    ``grant_gate`` mirrors the shared Sec-1 authorization gate
    (``_enforce_workspace_upgrade_grants``): an async ``(thread, target_tier) ->
    None`` that raises ``_GateDenied`` on refusal, run BEFORE provisioning
    (fail-closed).
    """
    target_tier = target_tier or "sandbox"
    if target_tier not in ("sandbox", "vm"):
        return {"error": 400, "detail": f"unsupported target_tier {target_tier!r}"}

    # vm delegates to the operator-gated VM path (which runs the same gate,
    # provisions the VM, and records metadata.vm). Mirrors the real endpoint's
    # `return await agent_upgrade_thread_to_vm(request, thread_id)`.
    if target_tier == "vm":
        return await agent_upgrade_to_vm(
            db, vm_provisioner, thread_id, grant_gate=grant_gate
        )

    thread = await db.get_thread(thread_id)
    if not thread:
        return {"error": 404, "detail": "Thread not found"}

    if grant_gate is not None:
        try:
            await grant_gate(thread, target_tier)
        except _GateDenied as exc:
            return {"error": 403, "detail": str(exc)}

    if not (provisioner.is_available and provisioner.in_cluster):
        return {
            "error": 503,
            "detail": "Workspace container provisioning not available",
        }

    metadata = thread.get("metadata") or {}
    if isinstance(metadata, str):
        try:
            metadata = json.loads(metadata)
        except (json.JSONDecodeError, TypeError):
            metadata = {}
    wc = metadata.get("workspace_container") or {}
    if wc.get("status") in ("pending", "creating", "created", "ready"):
        return {
            "status": wc["status"],
            "thread_id": thread_id,
            "target_tier": "sandbox",
            "message": "Workspace container already provisioned or in progress",
        }

    await db.merge_thread_workspace_context(thread_id, {"status": "pending"})
    await provisioner.create_workspace(WorkspaceOwner.session(thread_id))

    return {
        "status": "provisioning",
        "thread_id": thread_id,
        "target_tier": "sandbox",
    }


async def agent_save_message(
    db, thread_id, role, content=None, tool_calls=None, turn_number=None
):
    """5.4: POST /api/agents/threads/{thread_id}/messages"""
    message_id = await db.save_thread_message(
        thread_id=thread_id,
        role=role,
        content=content,
        tool_calls=tool_calls,
        turn_number=turn_number,
    )
    return {"message_id": message_id, "status": "saved"}


def check_thread_ownership(thread, user_id):
    """Shared ownership check for auth-required endpoints (5.7, 5.8, 5.9, 5.10)."""
    if not thread:
        return 404
    if thread.get("user_id") and str(thread["user_id"]) != str(user_id):
        return 403
    return 200


async def end_thread(
    db,
    thread,
    user_id,
    container_provisioner,
    vm_provisioner,
    snapshot_service,
    gitea_client,
    thread_id,
):
    """5.8: DELETE /api/persistent/threads/{thread_id}"""
    status = check_thread_ownership(thread, user_id)
    if status != 200:
        return {"error": status}

    metadata = thread.get("metadata") or {}
    if isinstance(metadata, str):
        try:
            metadata = json.loads(metadata)
        except (json.JSONDecodeError, TypeError):
            metadata = {}
    ws_ctx = metadata.get("workspace_container") or {}

    # Snapshot
    pod_ip = ws_ctx.get("pod_ip")
    if snapshot_service.is_available and pod_ip and ws_ctx.get("status") == "ready":
        await snapshot_service.capture_vm_snapshot(
            job_id=thread_id,
            ssh_host=pod_ip,
            ssh_port=22,
            source_type="pod",
            entity_type="threads",
        )

    # Container cleanup
    if container_provisioner.is_available:
        await container_provisioner.delete_workspace(WorkspaceOwner.session(thread_id))

    # VM cleanup
    vm_ctx = metadata.get("vm") or {}
    if vm_ctx.get("status") in ("provisioning", "created", "ready"):
        if vm_provisioner.is_available:
            await vm_provisioner.delete_thread_vm(thread_id)

    # Gitea cleanup
    repo_name = ws_ctx.get("repo_name")
    if repo_name and gitea_client.is_initialized:
        await gitea_client.delete_repo(repo_name)

    await db.end_thread(thread_id)
    return {"status": "ended"}


def get_thread_ide_status(thread, metadata=None):
    """5.10: GET /api/persistent/threads/{thread_id}/ide — logic only."""
    if metadata is None:
        metadata = thread.get("metadata") or {}
        if isinstance(metadata, str):
            try:
                metadata = json.loads(metadata)
            except (ValueError, TypeError):
                metadata = {}

    ws_ctx = metadata.get("workspace_container", {})
    repo_name = ws_ctx.get("repo_name")
    gitea_url = None
    if repo_name:
        gitea_base = os.environ.get("GITEA_URL", "").rstrip("/")
        gitea_user = os.environ.get("GITEA_ADMIN_USER", "srw")
        if gitea_base:
            gitea_url = f"{gitea_base}/{gitea_user}/{repo_name}"

    vm_ctx = metadata.get("vm", {})

    # VM takes precedence
    if vm_ctx.get("status") == "ready":
        ssh_host = vm_ctx.get("ssh_host") or vm_ctx.get("pod_ip")
        if ssh_host:
            proxy_base = os.environ.get("IDE_PROXY_BASE_URL", "http://localhost:8085")
            return {
                "status": "active",
                "code_server_url": f"{proxy_base}/api/ide/tid-1/proxy/?folder=/home/agent-host/workspace",
                "source": "live_vm",
                "gitea_url": gitea_url,
            }

    # Container
    if ws_ctx.get("status") == "ready" and ws_ctx.get("pod_ip"):
        proxy_base = os.environ.get("IDE_PROXY_BASE_URL", "http://localhost:8085")
        return {
            "status": "active",
            "code_server_url": f"{proxy_base}/api/ide/tid-1/proxy/?folder=/home/agent-host/workspace",
            "source": "live_workspace",
            "gitea_url": gitea_url,
        }

    # Provisioning
    if ws_ctx.get("status") == "provisioning" or vm_ctx.get("status") == "provisioning":
        return {"status": "restoring", "code_server_url": None, "gitea_url": gitea_url}

    return {"status": "unavailable", "code_server_url": None, "gitea_url": gitea_url}


async def ws_proxy_resolve(db, ws_suspension, thread_id):
    """5.11: WS /ws/persistent/{thread_id} — resolution + validation logic."""
    thread = await db.get_thread(thread_id)
    if not thread:
        return {"close_code": 4404, "reason": "Thread not found"}

    metadata = thread.get("metadata") or {}
    if isinstance(metadata, str):
        try:
            metadata = json.loads(metadata)
        except (json.JSONDecodeError, TypeError):
            metadata = {}
    ws_ctx = metadata.get("workspace_container") or {}

    if ws_ctx.get("status") == "suspended" and ws_suspension.is_enabled:
        ok = await ws_suspension.restore_thread_workspace(thread_id)
        if not ok:
            return {"close_code": 4503, "reason": "Failed to restore workspace"}

    if not thread.get("agent_id"):
        return {"close_code": 4404, "reason": "Thread not active or no agent assigned"}

    agent = await db.get_agent(str(thread["agent_id"]))
    if not agent:
        return {"close_code": 4503, "reason": "Agent not available"}

    pod_ip = agent.get("pod_ip")
    pod_port = agent.get("pod_port", 8001)
    if not pod_ip:
        return {"close_code": 4503, "reason": "Agent has no IP"}

    return {"upstream_url": f"ws://{pod_ip}:{pod_port}/ws/chat"}


# 5.12 / 5.13: Notification helpers
_SESSION_NOTIFY_METHODS = {"permission.request", "vm_upgrade.needed", "ready"}
_SESSION_RESOLVE_METHODS = {"approve", "deny"}


def inspect_session_event(
    raw, thread_id, user_id, thread_title, config_name, broadcast_fn
):
    """5.12: _inspect_session_event"""
    if not user_id:
        return
    try:
        event = json.loads(raw)
        method = event.get("method")
        if method not in _SESSION_NOTIFY_METHODS:
            return

        params = event.get("params") or {}
        type_map = {
            "permission.request": "session.permission_request",
            "vm_upgrade.needed": "session.vm_upgrade",
            "ready": "session.waiting",
        }
        broadcast_fn(
            user_id=user_id,
            event_type=type_map[method],
            data={
                "thread_id": thread_id,
                "title": thread_title,
                "config_name": config_name,
                "tool": params.get("tool"),
                "args": params.get("args"),
                "reason": params.get("reason"),
                "command": params.get("command"),
            },
        )
    except (json.JSONDecodeError, Exception):
        pass


def inspect_browser_event(raw, thread_id, user_id, broadcast_fn):
    """5.13: _inspect_browser_event"""
    if not user_id:
        return
    try:
        event = json.loads(raw)
        method = event.get("method")
        if method not in _SESSION_RESOLVE_METHODS:
            return
        broadcast_fn(
            user_id=user_id,
            event_type="session.resolved",
            data={"thread_id": thread_id, "resolution": method},
        )
    except (json.JSONDecodeError, Exception):
        pass


# =============================================================================
# Fixtures
# =============================================================================


def _mock_db():
    db = AsyncMock()
    db.create_thread = AsyncMock(return_value="aaaaaaaa-1111-2222-3333-444444444444")
    db.get_thread = AsyncMock(return_value=None)
    db.get_agent = AsyncMock(return_value=None)
    db.save_thread_message = AsyncMock(return_value="msg-uuid-1")
    db.merge_thread_workspace_context = AsyncMock()
    db.merge_thread_metadata_config_override = AsyncMock()
    db.end_thread = AsyncMock()
    db.list_threads = AsyncMock(return_value=[])
    db.get_thread_messages_history = AsyncMock(return_value=[])
    db.get_thread_message_count = AsyncMock(return_value=0)
    db.acquire = MagicMock()
    # Pin resolution defaults to "no system pin set" so existing tests
    # that don't care about the override stay green.
    db.resolve_default_for_capability = AsyncMock(return_value=None)
    return db


def _mock_gitea(initialized=False):
    g = AsyncMock()
    type(g).is_initialized = PropertyMock(return_value=initialized)
    g.create_repo = AsyncMock(return_value="http://gitea:3000/srw/thread-aaaaaaaa.git")
    g.delete_repo = AsyncMock()
    return g


def _mock_provisioner(available=False, in_cluster=None):
    p = AsyncMock()
    type(p).is_available = PropertyMock(return_value=available)
    # Defaults to tracking is_available unless overridden (the upgrade-to-
    # workspace gate requires both is_available AND in_cluster).
    type(p).in_cluster = PropertyMock(
        return_value=available if in_cluster is None else in_cluster
    )
    p.create_workspace = AsyncMock(return_value=True)
    p.delete_workspace = AsyncMock()
    return p


def _mock_vm_provisioner(available=False, mode="nats"):
    v = AsyncMock()
    type(v).is_available = PropertyMock(return_value=available)
    type(v).mode = PropertyMock(return_value=mode)
    v.create_thread_vm = AsyncMock(return_value=True)
    v.delete_thread_vm = AsyncMock()
    return v


def _mock_snapshot(available=False):
    s = AsyncMock()
    type(s).is_available = PropertyMock(return_value=available)
    s.capture_vm_snapshot = AsyncMock()
    return s


def _mock_suspension(enabled=False):
    s = AsyncMock()
    type(s).is_enabled = PropertyMock(return_value=enabled)
    s.restore_thread_workspace = AsyncMock(return_value=True)
    return s


def _make_thread(
    thread_id="aaaaaaaa-1111-2222-3333-444444444444",
    user_id=None,
    agent_id=None,
    config_name="persistent_defaults",
    title="Test Session",
    metadata=None,
    status="active",
):
    return {
        "id": thread_id,
        "user_id": user_id,
        "agent_id": agent_id,
        "config_name": config_name,
        "title": title,
        "metadata": metadata or {},
        "status": status,
    }


# =============================================================================
# 5.1: POST /api/agents/threads
# =============================================================================


class TestAgentCreateThread:
    """Tests for POST /api/agents/threads (agent-facing, no auth)."""

    @pytest.mark.asyncio
    async def test_creates_thread_with_null_user(self):
        db = _mock_db()
        gitea = _mock_gitea(initialized=False)
        prov = _mock_provisioner(available=False)

        result = await agent_create_thread(db, gitea, prov)

        db.create_thread.assert_awaited_once_with(
            user_id=None,
            config_name="persistent_defaults",
            permission_mode="read_only",
            title="Local Session",
        )
        assert result == {
            "thread_id": "aaaaaaaa-1111-2222-3333-444444444444",
            "status": "created",
        }

    @pytest.mark.asyncio
    async def test_creates_gitea_repo_when_initialized(self):
        db = _mock_db()
        gitea = _mock_gitea(initialized=True)
        prov = _mock_provisioner(available=False)

        await agent_create_thread(db, gitea, prov)

        assert gitea.create_repo.await_args.args == ("thread-aaaaaaaa",)
        UUID(gitea.create_repo.await_args.kwargs["intent_marker"])
        db.merge_thread_workspace_context.assert_awaited_once_with(
            "aaaaaaaa-1111-2222-3333-444444444444",
            {
                "git_remote_url": "http://gitea:3000/srw/thread-aaaaaaaa.git",
                "repo_name": "thread-aaaaaaaa",
            },
        )

    @pytest.mark.asyncio
    async def test_skips_gitea_when_not_initialized(self):
        db = _mock_db()
        gitea = _mock_gitea(initialized=False)
        prov = _mock_provisioner(available=False)

        await agent_create_thread(db, gitea, prov)

        gitea.create_repo.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_triggers_container_provisioning_when_available(self):
        db = _mock_db()
        gitea = _mock_gitea(initialized=False)
        prov = _mock_provisioner(available=True)

        await agent_create_thread(db, gitea, prov)

        # Give the background task a chance to be scheduled
        await asyncio.sleep(0)
        prov.create_workspace.assert_awaited_once_with(
            WorkspaceOwner.session("aaaaaaaa-1111-2222-3333-444444444444")
        )

    @pytest.mark.asyncio
    async def test_skips_container_when_not_available(self):
        db = _mock_db()
        gitea = _mock_gitea(initialized=False)
        prov = _mock_provisioner(available=False)

        await agent_create_thread(db, gitea, prov)

        await asyncio.sleep(0)
        prov.create_workspace.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_returns_500_on_db_exception(self):
        db = _mock_db()
        db.create_thread = AsyncMock(side_effect=RuntimeError("DB down"))
        gitea = _mock_gitea()
        prov = _mock_provisioner()

        with pytest.raises(RuntimeError, match="DB down"):
            await agent_create_thread(db, gitea, prov)

    @pytest.mark.asyncio
    async def test_skips_override_when_no_pins_set(self):
        """Without system pins, no config_override is persisted. The
        readiness gate is the gatekeeper that prevents thread create
        before pins are set; if the operator somehow gets here without
        pins, the agent will boot on YAML defaults — but that's a
        separate bug, not our problem to mask. Pin: don't fabricate
        an override out of nothing."""
        db = _mock_db()
        # Default mock returns None for every capability — no pins set.
        gitea = _mock_gitea(initialized=False)
        prov = _mock_provisioner(available=False)

        await agent_create_thread(db, gitea, prov)

        db.merge_thread_metadata_config_override.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_persists_chat_auxiliary_embedding_overrides(self):
        """When all three system pins are set, the override carries chat
        + auxiliary + embedding sections so the agent doesn't fall
        through to YAML defaults at attach. This is the fix for the
        "Untitled Session" + missing-memory + embedding-401 bug
        documented in knowledge-base/knowledge/hardcoded_model_defaults.md."""
        db = _mock_db()

        async def _resolve(cap):
            return {
                "chat": "chat-model",
                "auxiliary": "aux-model",
                "embedding": "emb-model",
            }.get(cap)

        db.resolve_default_for_capability = AsyncMock(side_effect=_resolve)

        async def fake_inject(*, section, model_id, user_id, resolved_keys):
            section["base_url"] = f"https://endpoint/{model_id}/v1"
            section["api_key"] = "k"

        async def fake_inject_env(
            *, env_keys, prefix, model_id, user_id, resolved_keys
        ):
            env_keys[f"{prefix}_BASE_URL"] = "https://e/v1"
            env_keys[f"{prefix}_API_KEY"] = "k"

        gitea = _mock_gitea(initialized=False)
        prov = _mock_provisioner(available=False)

        await agent_create_thread(
            db,
            gitea,
            prov,
            inject_credentials=fake_inject,
            inject_env_credentials=fake_inject_env,
        )

        db.merge_thread_metadata_config_override.assert_awaited_once()
        call_args = db.merge_thread_metadata_config_override.await_args
        thread_id, override = call_args.args
        assert thread_id == "aaaaaaaa-1111-2222-3333-444444444444"
        assert override["llm"]["model"] == "chat-model"
        assert override["llm"]["base_url"] == "https://endpoint/chat-model/v1"
        assert override["auxiliary"]["model"] == "aux-model"
        assert override["auxiliary"]["base_url"] == "https://endpoint/aux-model/v1"
        assert override["env_keys"]["EMBEDDING_MODEL"] == "emb-model"
        assert override["env_keys"]["EMBEDDING_BASE_URL"] == "https://e/v1"

    @pytest.mark.asyncio
    async def test_persists_partial_override_when_only_chat_pinned(self):
        """If only chat is pinned (no auxiliary, no embedding), the
        override carries chat alone. The readiness gate would normally
        block thread create in this state, but if it doesn't (test
        environment, future relaxation), the override should at least
        carry what IS set rather than nothing."""
        db = _mock_db()

        async def _resolve(cap):
            return "chat-only" if cap == "chat" else None

        db.resolve_default_for_capability = AsyncMock(side_effect=_resolve)
        gitea = _mock_gitea(initialized=False)
        prov = _mock_provisioner(available=False)

        await agent_create_thread(db, gitea, prov)

        db.merge_thread_metadata_config_override.assert_awaited_once()
        _, override = db.merge_thread_metadata_config_override.await_args.args
        assert override["llm"]["model"] == "chat-only"
        assert "auxiliary" not in override
        assert "env_keys" not in override


# =============================================================================
# 5.2: GET /api/agents/threads/{thread_id}/workspace
# =============================================================================


class TestAgentGetThreadWorkspace:
    """Tests for GET /api/agents/threads/{thread_id}/workspace."""

    def test_returns_none_when_thread_not_found(self):
        assert parse_workspace_metadata(None) is None

    def test_returns_default_status_none_when_no_workspace(self):
        thread = _make_thread(metadata={})
        result = parse_workspace_metadata(thread)
        assert result["status"] == "none"
        assert result["pod_ip"] is None

    def test_parses_workspace_container_fields(self):
        thread = _make_thread(
            metadata={
                "workspace_container": {
                    "status": "ready",
                    "pod_ip": "10.0.0.50",
                    "pod_name": "ws-pod-1",
                    "namespace": "agent-vms",
                    "git_remote_url": "http://gitea/repo.git",
                }
            }
        )
        result = parse_workspace_metadata(thread)
        assert result["status"] == "ready"
        assert result["pod_ip"] == "10.0.0.50"
        assert result["pod_name"] == "ws-pod-1"
        assert result["namespace"] == "agent-vms"
        assert result["git_remote_url"] == "http://gitea/repo.git"

    def test_parses_vm_fields(self):
        thread = _make_thread(
            metadata={
                "vm": {
                    "status": "ready",
                    "ssh_host": "192.168.1.10",
                    "ssh_port": 2222,
                    "vm_name": "agent-vm-1",
                }
            }
        )
        result = parse_workspace_metadata(thread)
        assert result["vm_status"] == "ready"
        assert result["vm_ssh_host"] == "192.168.1.10"
        assert result["vm_ssh_port"] == 2222
        assert result["vm_name"] == "agent-vm-1"

    def test_handles_json_string_metadata(self):
        """Legacy format: metadata stored as JSON string instead of dict."""
        metadata_str = json.dumps(
            {"workspace_container": {"status": "ready", "pod_ip": "10.0.0.1"}}
        )
        thread = _make_thread(metadata=metadata_str)
        result = parse_workspace_metadata(thread)
        assert result["status"] == "ready"
        assert result["pod_ip"] == "10.0.0.1"

    def test_handles_invalid_json_string_metadata(self):
        thread = _make_thread(metadata="not-valid-json{")
        result = parse_workspace_metadata(thread)
        assert result["status"] == "none"

    def test_returns_config_override_and_project_ids(self):
        thread = _make_thread(
            metadata={
                "config_override": {"llm": {"model": "gpt-4o"}},
                "project_ids": ["proj-1", "proj-2"],
            }
        )
        result = parse_workspace_metadata(thread)
        assert result["config_override"] == {"llm": {"model": "gpt-4o"}}
        assert result["project_ids"] == ["proj-1", "proj-2"]


def resolve_cloud_sync_fork(
    *,
    thread: dict,
    metadata: dict,
    cloud_mount_cfg,
    mount_rows: list,
    driver: str,
    cloud_up: bool,
    build_agent_cloud_sync,
):
    """5.2 cloud_sync fork — mirrors ``agent_get_thread_workspace``
    (orchestrator/main.py), including:

    - B8 F2: a ``protected_cloud`` thread whose ``cloud_mount_cfg`` resolved
      to None (flag off, VM tier, or a refused/absent grant) must NOT fall
      into the live rclone/session-folder ``cloud_sync`` fallback — that
      would hand a protected thread a live write path.
    - B10 F-C1: ``cloud_sync_degraded`` drops the ``nc_session_folder`` veto
      for a protected thread — a protected thread with no resolved mount is
      ALWAYS degraded, even when a (now-unused) legacy ``nc_session_folder``
      happens to still be set on the row. Previously a protected thread with
      a stale ``nc_session_folder`` reported ``cloud_sync_degraded=False``
      despite having no live cloud access at all.
    - B10 F-C1: also returns the ``protected_cloud`` response key the
      endpoint now surfaces, so the agent can gate its own legacy shim on
      the same signal.

    ``driver`` stands in for ``_cloud_workspace_driver()`` so this stays
    hermetic (no env coupling).
    """
    if cloud_mount_cfg:
        cloud_sync_cfg = None
    elif metadata.get("protected_cloud"):
        cloud_sync_cfg = None
    elif driver == "rclone_mount":
        cloud_sync_cfg = build_agent_cloud_sync(thread, mount_rows=[])
    else:
        cloud_sync_cfg = build_agent_cloud_sync(thread, mount_rows=mount_rows)
    protected_cloud = bool(metadata.get("protected_cloud"))
    cloud_sync_degraded = bool(
        cloud_up
        and not cloud_mount_cfg
        and not cloud_sync_cfg
        and (protected_cloud or not thread.get("nc_session_folder"))
    )
    return cloud_sync_cfg, cloud_sync_degraded, protected_cloud


class TestResolveCloudSyncFork:
    """B8 review F2: a protected_cloud thread whose cloud_mount_cfg resolved
    to None must get NO live cloud_sync fallback, and must be flagged
    degraded so the agent surfaces the same degraded-cloud state it shows
    for a failed initial pull."""

    def test_protected_thread_with_no_mount_skips_live_fallback_and_is_degraded(self):
        thread = {"id": "t1"}
        build_sync = MagicMock()
        cloud_sync_cfg, degraded, protected_cloud = resolve_cloud_sync_fork(
            thread=thread,
            metadata={"protected_cloud": True},
            cloud_mount_cfg=None,
            mount_rows=[{"backend_id": "nextcloud", "cloud_handle": "handle::Proj"}],
            driver="sync",
            cloud_up=True,
            build_agent_cloud_sync=build_sync,
        )
        assert cloud_sync_cfg is None
        assert degraded is True
        assert protected_cloud is True
        build_sync.assert_not_called()

    def test_protected_thread_with_no_mount_skips_rclone_fallback_too(self):
        """Same guard must hold even when CLOUD_WORKSPACE_DRIVER=rclone_mount,
        which for an unprotected thread would otherwise take the rclone-driver
        branch (empty mount_rows session-folder fallback)."""
        thread = {"id": "t1"}
        build_sync = MagicMock()
        cloud_sync_cfg, degraded, protected_cloud = resolve_cloud_sync_fork(
            thread=thread,
            metadata={"protected_cloud": True},
            cloud_mount_cfg=None,
            mount_rows=[],
            driver="rclone_mount",
            cloud_up=True,
            build_agent_cloud_sync=build_sync,
        )
        assert cloud_sync_cfg is None
        assert degraded is True
        assert protected_cloud is True
        build_sync.assert_not_called()

    def test_protected_thread_with_resolved_mount_is_not_degraded(self):
        thread = {"id": "t1"}
        build_sync = MagicMock()
        cloud_sync_cfg, degraded, protected_cloud = resolve_cloud_sync_fork(
            thread=thread,
            metadata={"protected_cloud": True},
            cloud_mount_cfg={"driver": "rclone", "protected": True},
            mount_rows=[],
            driver="sync",
            cloud_up=True,
            build_agent_cloud_sync=build_sync,
        )
        assert cloud_sync_cfg is None
        assert degraded is False
        assert protected_cloud is True
        build_sync.assert_not_called()

    def test_non_protected_thread_still_gets_live_fallback(self):
        """Regression guard: the new elif must not swallow the ordinary
        (non-protected) fallback path."""
        thread = {"id": "t1"}
        sentinel = {"kind": "session_folder"}
        build_sync = MagicMock(return_value=sentinel)
        cloud_sync_cfg, degraded, protected_cloud = resolve_cloud_sync_fork(
            thread=thread,
            metadata={},
            cloud_mount_cfg=None,
            mount_rows=[],
            driver="sync",
            cloud_up=True,
            build_agent_cloud_sync=build_sync,
        )
        assert cloud_sync_cfg is sentinel
        assert degraded is False
        assert protected_cloud is False
        build_sync.assert_called_once_with(thread, mount_rows=[])

    def test_protected_thread_with_nc_session_folder_set_is_still_degraded(self):
        """B10 F-C1: the exact bug being fixed. A protected thread with no
        resolved mount, but a still-set legacy ``nc_session_folder`` (e.g. a
        thread created before the protected-mode refactor, or one whose
        session-folder provisioning raced ahead of the protected marker),
        must NOT get to hide behind the nc_session_folder veto — it is
        degraded (no live cloud access at all) regardless."""
        thread = {"id": "t1", "nc_session_folder": "Sessions/thread-1"}
        build_sync = MagicMock()
        cloud_sync_cfg, degraded, protected_cloud = resolve_cloud_sync_fork(
            thread=thread,
            metadata={"protected_cloud": True},
            cloud_mount_cfg=None,
            mount_rows=[],
            driver="sync",
            cloud_up=True,
            build_agent_cloud_sync=build_sync,
        )
        assert cloud_sync_cfg is None
        assert degraded is True
        assert protected_cloud is True
        build_sync.assert_not_called()

    def test_non_protected_thread_with_nc_session_folder_set_is_not_degraded(self):
        """Regression guard: the dropped nc_session_folder veto is
        protected-only — an ordinary thread with a working legacy session
        folder (and no cloud_sync resolved, hypothetically) is unaffected."""
        thread = {"id": "t1", "nc_session_folder": "Sessions/thread-1"}
        build_sync = MagicMock(return_value=None)
        cloud_sync_cfg, degraded, protected_cloud = resolve_cloud_sync_fork(
            thread=thread,
            metadata={},
            cloud_mount_cfg=None,
            mount_rows=[],
            driver="sync",
            cloud_up=True,
            build_agent_cloud_sync=build_sync,
        )
        assert degraded is False
        assert protected_cloud is False


# =============================================================================
# 5.3: POST /api/agents/threads/{thread_id}/upgrade-to-vm
# =============================================================================


class TestAgentUpgradeToVm:
    """Tests for POST /api/agents/threads/{thread_id}/upgrade-to-vm."""

    @pytest.mark.asyncio
    async def test_returns_404_when_thread_not_found(self):
        db = _mock_db()
        db.get_thread = AsyncMock(return_value=None)
        vm = _mock_vm_provisioner(available=True)

        result = await agent_upgrade_to_vm(db, vm, "tid-1")
        assert result["error"] == 404

    @pytest.mark.asyncio
    async def test_returns_503_when_vm_not_available(self):
        db = _mock_db()
        db.get_thread = AsyncMock(return_value=_make_thread())
        vm = _mock_vm_provisioner(available=False)

        result = await agent_upgrade_to_vm(db, vm, "tid-1")
        assert result["error"] == 503

    @pytest.mark.asyncio
    async def test_returns_existing_status_when_vm_already_provisioned(self):
        thread = _make_thread(metadata={"vm": {"status": "ready"}})
        db = _mock_db()
        db.get_thread = AsyncMock(return_value=thread)
        vm = _mock_vm_provisioner(available=True)

        result = await agent_upgrade_to_vm(db, vm, "tid-1")
        assert result["status"] == "ready"
        assert result["message"] == "VM already provisioned or in progress"
        vm.create_thread_vm.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_returns_existing_status_for_provisioning(self):
        thread = _make_thread(metadata={"vm": {"status": "provisioning"}})
        db = _mock_db()
        db.get_thread = AsyncMock(return_value=thread)
        vm = _mock_vm_provisioner(available=True)

        result = await agent_upgrade_to_vm(db, vm, "tid-1")
        assert result["status"] == "provisioning"

    @pytest.mark.asyncio
    async def test_creates_vm_for_new_request(self):
        thread = _make_thread(metadata={})
        db = _mock_db()
        db.get_thread = AsyncMock(return_value=thread)
        vm = _mock_vm_provisioner(available=True, mode="nats")

        result = await agent_upgrade_to_vm(db, vm, "tid-1")

        assert result["status"] == "provisioning"
        assert result["vm_provisioner_mode"] == "nats"
        vm.create_thread_vm.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_returns_500_when_provisioner_fails(self):
        thread = _make_thread(metadata={})
        db = _mock_db()
        db.get_thread = AsyncMock(return_value=thread)
        vm = _mock_vm_provisioner(available=True)
        vm.create_thread_vm = AsyncMock(return_value=False)

        result = await agent_upgrade_to_vm(db, vm, "tid-1")
        assert result["error"] == 500

    @pytest.mark.asyncio
    async def test_grant_gate_refusal_returns_403_before_provisioning(self):
        # Phase 2 (⚠️ gap closed): this endpoint now runs the shared Sec-1 gate.
        # A refusal is a 403 BEFORE the availability check or provisioning.
        thread = _make_thread(metadata={})
        db = _mock_db()
        db.get_thread = AsyncMock(return_value=thread)
        vm = _mock_vm_provisioner(available=True)

        async def _deny(_thread, _tier):
            raise _GateDenied("User is not permitted to use VM workspaces")

        result = await agent_upgrade_to_vm(db, vm, "tid-1", grant_gate=_deny)
        assert result["error"] == 403
        vm.create_thread_vm.assert_not_awaited()


# =============================================================================
# 5.3b: POST /api/agents/threads/{thread_id}/upgrade-to-workspace (S2)
# =============================================================================


class TestAgentUpgradeToWorkspace:
    """Tests for POST /api/agents/threads/{thread_id}/upgrade-to-workspace —
    lite -> sandbox container provisioning (workspace_tier_upgrade.md §4.2 S2).
    """

    @pytest.mark.asyncio
    async def test_returns_400_for_unknown_tier(self):
        # Only sandbox|vm are valid targets; anything else is a 400.
        db = _mock_db()
        prov = _mock_provisioner(available=True, in_cluster=True)
        result = await agent_upgrade_to_workspace(
            db, prov, "tid-1", target_tier="bogus"
        )
        assert result["error"] == 400
        prov.create_workspace.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_vm_tier_delegates_to_vm_path(self):
        # Phase 2: vm delegates to the VM provisioner (not the container path),
        # records nothing on the container, and reports provisioning.
        thread = _make_thread(metadata={})
        db = _mock_db()
        db.get_thread = AsyncMock(return_value=thread)
        prov = _mock_provisioner(available=True, in_cluster=True)
        vm = _mock_vm_provisioner(available=True, mode="nats")
        result = await agent_upgrade_to_workspace(
            db, prov, "tid-1", target_tier="vm", vm_provisioner=vm
        )
        assert result["status"] == "provisioning"
        assert result["vm_provisioner_mode"] == "nats"
        vm.create_thread_vm.assert_awaited_once()
        # The container provisioner is untouched on a vm upgrade.
        prov.create_workspace.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_vm_tier_refused_by_grant_gate_returns_403(self):
        # Phase 2: an owner without can_use_vm / vm_workspace is refused 403 and
        # no VM is provisioned (the gate runs inside the delegated vm path).
        thread = _make_thread(metadata={})
        db = _mock_db()
        db.get_thread = AsyncMock(return_value=thread)
        prov = _mock_provisioner(available=True, in_cluster=True)
        vm = _mock_vm_provisioner(available=True)

        async def _deny(_thread, _tier):
            raise _GateDenied("User is not permitted to use VM workspaces")

        result = await agent_upgrade_to_workspace(
            db, prov, "tid-1", target_tier="vm", vm_provisioner=vm, grant_gate=_deny
        )
        assert result["error"] == 403
        vm.create_thread_vm.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_returns_404_when_thread_not_found(self):
        db = _mock_db()
        db.get_thread = AsyncMock(return_value=None)
        prov = _mock_provisioner(available=True, in_cluster=True)
        result = await agent_upgrade_to_workspace(db, prov, "tid-1")
        assert result["error"] == 404

    @pytest.mark.asyncio
    async def test_returns_503_when_not_in_cluster(self):
        # Provisioner available but out-of-cluster (e.g. dev compose) → no pod.
        db = _mock_db()
        db.get_thread = AsyncMock(return_value=_make_thread())
        prov = _mock_provisioner(available=True, in_cluster=False)
        result = await agent_upgrade_to_workspace(db, prov, "tid-1")
        assert result["error"] == 503
        prov.create_workspace.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_returns_503_when_provisioner_unavailable(self):
        db = _mock_db()
        db.get_thread = AsyncMock(return_value=_make_thread())
        prov = _mock_provisioner(available=False)
        result = await agent_upgrade_to_workspace(db, prov, "tid-1")
        assert result["error"] == 503

    @pytest.mark.asyncio
    async def test_idempotent_when_already_provisioning(self):
        thread = _make_thread(metadata={"workspace_container": {"status": "pending"}})
        db = _mock_db()
        db.get_thread = AsyncMock(return_value=thread)
        prov = _mock_provisioner(available=True, in_cluster=True)
        result = await agent_upgrade_to_workspace(db, prov, "tid-1")
        assert result["status"] == "pending"
        assert (
            result["message"]
            == "Workspace container already provisioned or in progress"
        )
        # No duplicate provisioning, no status churn.
        prov.create_workspace.assert_not_awaited()
        db.merge_thread_workspace_context.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_idempotent_when_already_ready(self):
        thread = _make_thread(metadata={"workspace_container": {"status": "ready"}})
        db = _mock_db()
        db.get_thread = AsyncMock(return_value=thread)
        prov = _mock_provisioner(available=True, in_cluster=True)
        result = await agent_upgrade_to_workspace(db, prov, "tid-1")
        assert result["status"] == "ready"
        prov.create_workspace.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_provisions_for_new_request(self):
        thread = _make_thread(metadata={})
        db = _mock_db()
        db.get_thread = AsyncMock(return_value=thread)
        prov = _mock_provisioner(available=True, in_cluster=True)
        result = await agent_upgrade_to_workspace(db, prov, "tid-1")
        assert result["status"] == "provisioning"
        assert result["target_tier"] == "sandbox"
        db.merge_thread_workspace_context.assert_awaited_once_with(
            "tid-1", {"status": "pending"}
        )
        prov.create_workspace.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_defaults_to_sandbox_when_tier_omitted(self):
        thread = _make_thread(metadata={})
        db = _mock_db()
        db.get_thread = AsyncMock(return_value=thread)
        prov = _mock_provisioner(available=True, in_cluster=True)
        result = await agent_upgrade_to_workspace(db, prov, "tid-1")
        assert result["status"] == "provisioning"

    @pytest.mark.asyncio
    async def test_grant_gate_refusal_returns_403_before_provisioning(self):
        # Sec-1 (S4): a shell-restricted owner (or vm without vm_workspace) is
        # refused 403 and nothing provisions.
        thread = _make_thread(metadata={})
        db = _mock_db()
        db.get_thread = AsyncMock(return_value=thread)
        prov = _mock_provisioner(available=True, in_cluster=True)

        async def _deny(_thread, _tier):
            raise _GateDenied("config exceeds your capability grants: shell_tools")

        result = await agent_upgrade_to_workspace(db, prov, "tid-1", grant_gate=_deny)
        assert result["error"] == 403
        assert "shell_tools" in result["detail"]
        prov.create_workspace.assert_not_awaited()
        db.merge_thread_workspace_context.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_grant_gate_pass_provisions(self):
        # A default (ungated) sandbox upgrade passes the gate and provisions.
        thread = _make_thread(metadata={})
        db = _mock_db()
        db.get_thread = AsyncMock(return_value=thread)
        prov = _mock_provisioner(available=True, in_cluster=True)

        async def _allow(_thread, _tier):
            return None

        result = await agent_upgrade_to_workspace(db, prov, "tid-1", grant_gate=_allow)
        assert result["status"] == "provisioning"
        prov.create_workspace.assert_awaited_once()


# =============================================================================
# 5.4: POST /api/agents/threads/{thread_id}/messages
# =============================================================================


class TestAgentSaveMessage:
    """Tests for POST /api/agents/threads/{thread_id}/messages."""

    @pytest.mark.asyncio
    async def test_saves_message_and_returns_id(self):
        db = _mock_db()
        result = await agent_save_message(db, "tid-1", "assistant", "hello", None, 1)

        assert result == {"message_id": "msg-uuid-1", "status": "saved"}
        db.save_thread_message.assert_awaited_once_with(
            thread_id="tid-1",
            role="assistant",
            content="hello",
            tool_calls=None,
            turn_number=1,
        )

    @pytest.mark.asyncio
    async def test_returns_500_on_db_exception(self):
        db = _mock_db()
        db.save_thread_message = AsyncMock(side_effect=RuntimeError("DB error"))

        with pytest.raises(RuntimeError, match="DB error"):
            await agent_save_message(db, "tid-1", "user", "hi")


# =============================================================================
# 5.5: POST /api/persistent/threads (user-facing, auth)
# =============================================================================


class TestUserCreateThread:
    """Tests for POST /api/persistent/threads (auth required)."""

    def test_user_model_setting_goes_to_llm_key(self):
        """User settings model → config_override['llm']['model']."""
        settings = {"persistent_agent": {"model": "gpt-4o"}}
        config_override = {}
        pa = settings.get("persistent_agent", {})
        if pa.get("model"):
            config_override["llm"] = {"model": pa["model"]}
        assert config_override == {"llm": {"model": "gpt-4o"}}

    def test_user_permission_mode_goes_to_interactive_key(self):
        """User settings permission_mode → config_override['interactive']['permission_mode']."""
        settings = {"persistent_agent": {"permission_mode": "autonomous"}}
        config_override = {}
        pa = settings.get("persistent_agent", {})
        if pa.get("permission_mode"):
            config_override["interactive"] = {"permission_mode": pa["permission_mode"]}
        assert config_override == {"interactive": {"permission_mode": "autonomous"}}

    def test_idle_timeout_nested_under_interactive(self):
        settings = {"persistent_agent": {"idle_timeout_minutes": 30}}
        config_override = {}
        pa = settings.get("persistent_agent", {})
        if pa.get("idle_timeout_minutes"):
            config_override.setdefault("interactive", {})["idle_timeout_minutes"] = 30
        assert config_override == {"interactive": {"idle_timeout_minutes": 30}}

    def test_request_model_overrides_user_default(self):
        """Per-session model override takes priority over user defaults."""
        config_override = {"llm": {"model": "gpt-4o"}}  # from user settings
        request_model = "claude-opus-4-6"
        if request_model:
            config_override.setdefault("llm", {})["model"] = request_model
        assert config_override["llm"]["model"] == "claude-opus-4-6"

    def test_request_permission_mode_overrides_user_default(self):
        """Per-session permission mode override takes priority over user defaults.

        Regression: the request's permission_mode used to be written only to
        the threads.permission_mode column (which the agent never reads) and
        never bridged into config_override.interactive — so a non-default pick
        in the New Session form was dropped and every session booted
        "read_only". The agent reads config.interactive.permission_mode, so the
        per-session choice must land there, exactly like the model bridge above.
        """
        # User default already applied to config_override (main.py:12146).
        config_override = {"interactive": {"permission_mode": "read_only"}}
        request_permission_mode = "autonomous"
        if request_permission_mode:
            config_override.setdefault("interactive", {})["permission_mode"] = (
                request_permission_mode
            )
        assert config_override["interactive"]["permission_mode"] == "autonomous"
        # Column stays in sync with the resolved config_override value.
        effective = (config_override.get("interactive") or {}).get(
            "permission_mode"
        ) or "read_only"
        assert effective == "autonomous"

    def test_omitted_permission_mode_keeps_user_default(self):
        """Omitting permission_mode (None) preserves the user's saved default."""
        config_override = {"interactive": {"permission_mode": "auto_accept"}}
        request_permission_mode = None
        if request_permission_mode:  # pragma: no cover - documents the None path
            config_override.setdefault("interactive", {})["permission_mode"] = (
                request_permission_mode
            )
        assert config_override["interactive"]["permission_mode"] == "auto_accept"

    def test_project_ids_normalization(self):
        """Legacy project_id → [project_id] normalization."""
        # New format
        assert ["p1", "p2"] == (["p1", "p2"] or [])
        # Legacy single project_id
        project_id = "p1"
        project_ids = None
        effective = project_ids or ([project_id] if project_id else [])
        assert effective == ["p1"]
        # Neither
        effective = None or ([] if not None else [])
        assert effective == []


# =============================================================================
# 5.6: GET /api/persistent/threads
# =============================================================================


class TestListThreads:
    """Tests for GET /api/persistent/threads."""

    @pytest.mark.asyncio
    async def test_returns_threads_for_user(self):
        db = _mock_db()
        db.list_threads = AsyncMock(return_value=[{"id": "t1"}, {"id": "t2"}])

        threads = await db.list_threads(user_id="user-1", project_id=None, status=None)
        assert len(threads) == 2

    @pytest.mark.asyncio
    async def test_supports_project_id_filter(self):
        db = _mock_db()
        await db.list_threads(user_id="user-1", project_id="proj-1", status=None)
        db.list_threads.assert_awaited_once_with(
            user_id="user-1", project_id="proj-1", status=None
        )

    @pytest.mark.asyncio
    async def test_supports_status_filter(self):
        db = _mock_db()
        await db.list_threads(user_id="user-1", project_id=None, status="active")
        db.list_threads.assert_awaited_once_with(
            user_id="user-1", project_id=None, status="active"
        )


# =============================================================================
# 5.7: GET /api/persistent/threads/{thread_id}
# =============================================================================


class TestGetThread:
    """Tests for GET /api/persistent/threads/{thread_id}."""

    def test_returns_404_when_not_found(self):
        assert check_thread_ownership(None, "user-1") == 404

    def test_returns_403_for_non_owner(self):
        thread = _make_thread(user_id="user-2")
        assert check_thread_ownership(thread, "user-1") == 403

    def test_returns_200_for_owner(self):
        thread = _make_thread(user_id="user-1")
        assert check_thread_ownership(thread, "user-1") == 200

    def test_returns_200_when_user_id_null(self):
        """Threads with NULL user_id are visible to all users."""
        thread = _make_thread(user_id=None)
        assert check_thread_ownership(thread, "user-1") == 200


# =============================================================================
# 5.8: DELETE /api/persistent/threads/{thread_id}
# =============================================================================


class TestEndThread:
    """Tests for DELETE /api/persistent/threads/{thread_id}."""

    @pytest.mark.asyncio
    async def test_returns_404_when_not_found(self):
        result = await end_thread(
            _mock_db(),
            None,
            "user-1",
            _mock_provisioner(),
            _mock_vm_provisioner(),
            _mock_snapshot(),
            _mock_gitea(),
            "tid-1",
        )
        assert result["error"] == 404

    @pytest.mark.asyncio
    async def test_returns_403_for_non_owner(self):
        thread = _make_thread(user_id="user-2")
        result = await end_thread(
            _mock_db(),
            thread,
            "user-1",
            _mock_provisioner(),
            _mock_vm_provisioner(),
            _mock_snapshot(),
            _mock_gitea(),
            "tid-1",
        )
        assert result["error"] == 403

    @pytest.mark.asyncio
    async def test_captures_snapshot_when_ready_and_available(self):
        thread = _make_thread(
            user_id="user-1",
            metadata={"workspace_container": {"status": "ready", "pod_ip": "10.0.0.5"}},
        )
        snap = _mock_snapshot(available=True)
        result = await end_thread(
            _mock_db(),
            thread,
            "user-1",
            _mock_provisioner(),
            _mock_vm_provisioner(),
            snap,
            _mock_gitea(),
            "tid-1",
        )
        assert result["status"] == "ended"
        snap.capture_vm_snapshot.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_skips_snapshot_when_not_ready(self):
        thread = _make_thread(
            user_id="user-1",
            metadata={"workspace_container": {"status": "provisioning"}},
        )
        snap = _mock_snapshot(available=True)
        await end_thread(
            _mock_db(),
            thread,
            "user-1",
            _mock_provisioner(),
            _mock_vm_provisioner(),
            snap,
            _mock_gitea(),
            "tid-1",
        )
        snap.capture_vm_snapshot.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_deletes_container_when_available(self):
        thread = _make_thread(user_id="user-1")
        prov = _mock_provisioner(available=True)
        await end_thread(
            _mock_db(),
            thread,
            "user-1",
            prov,
            _mock_vm_provisioner(),
            _mock_snapshot(),
            _mock_gitea(),
            "tid-1",
        )
        prov.delete_workspace.assert_awaited_once_with(WorkspaceOwner.session("tid-1"))

    @pytest.mark.asyncio
    async def test_deletes_vm_when_provisioned(self):
        thread = _make_thread(
            user_id="user-1",
            metadata={"vm": {"status": "ready"}},
        )
        vm = _mock_vm_provisioner(available=True)
        await end_thread(
            _mock_db(),
            thread,
            "user-1",
            _mock_provisioner(),
            vm,
            _mock_snapshot(),
            _mock_gitea(),
            "tid-1",
        )
        vm.delete_thread_vm.assert_awaited_once_with("tid-1")

    @pytest.mark.asyncio
    async def test_skips_vm_delete_when_no_vm(self):
        thread = _make_thread(user_id="user-1", metadata={})
        vm = _mock_vm_provisioner(available=True)
        await end_thread(
            _mock_db(),
            thread,
            "user-1",
            _mock_provisioner(),
            vm,
            _mock_snapshot(),
            _mock_gitea(),
            "tid-1",
        )
        vm.delete_thread_vm.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_deletes_gitea_repo_when_present(self):
        thread = _make_thread(
            user_id="user-1",
            metadata={"workspace_container": {"repo_name": "thread-aaaa"}},
        )
        gitea = _mock_gitea(initialized=True)
        await end_thread(
            _mock_db(),
            thread,
            "user-1",
            _mock_provisioner(),
            _mock_vm_provisioner(),
            _mock_snapshot(),
            gitea,
            "tid-1",
        )
        gitea.delete_repo.assert_awaited_once_with("thread-aaaa")

    @pytest.mark.asyncio
    async def test_calls_end_thread_on_db(self):
        thread = _make_thread(user_id="user-1")
        db = _mock_db()
        await end_thread(
            db,
            thread,
            "user-1",
            _mock_provisioner(),
            _mock_vm_provisioner(),
            _mock_snapshot(),
            _mock_gitea(),
            "tid-1",
        )
        db.end_thread.assert_awaited_once_with("tid-1")


# =============================================================================
# 5.9: GET /api/persistent/threads/{thread_id}/messages
# =============================================================================


class TestGetThreadMessages:
    """Tests for GET /api/persistent/threads/{thread_id}/messages."""

    def test_limit_capped_at_500(self):
        """min(limit, 500) caps at 500."""
        assert min(1000, 500) == 500
        assert min(200, 500) == 200
        assert min(500, 500) == 500

    @pytest.mark.asyncio
    async def test_returns_messages_with_total(self):
        db = _mock_db()
        db.get_thread_messages_history = AsyncMock(return_value=[{"id": "m1"}])
        db.get_thread_message_count = AsyncMock(return_value=42)

        messages = await db.get_thread_messages_history(
            thread_id="tid-1", limit=200, offset=0
        )
        total = await db.get_thread_message_count("tid-1")

        result = {"messages": messages, "total": total, "thread_id": "tid-1"}
        assert result["total"] == 42
        assert len(result["messages"]) == 1


# =============================================================================
# 5.10: GET /api/persistent/threads/{thread_id}/ide
# =============================================================================


class TestGetThreadIdeStatus:
    """Tests for GET /api/persistent/threads/{thread_id}/ide."""

    def test_vm_ready_returns_active_live_vm(self):
        """VM ready → active, source=live_vm."""
        thread = _make_thread()
        metadata = {
            "vm": {"status": "ready", "ssh_host": "10.0.0.99"},
            "workspace_container": {"repo_name": "thread-aaaa"},
        }
        with patch.dict(
            os.environ, {"GITEA_URL": "http://gitea:3000", "GITEA_ADMIN_USER": "srw"}
        ):
            result = get_thread_ide_status(thread, metadata)
        assert result["status"] == "active"
        assert result["source"] == "live_vm"
        assert result["gitea_url"] == "http://gitea:3000/srw/thread-aaaa"

    def test_container_ready_returns_active_live_workspace(self):
        """Container ready → active, source=live_workspace."""
        thread = _make_thread()
        metadata = {
            "workspace_container": {"status": "ready", "pod_ip": "10.0.0.50"},
        }
        result = get_thread_ide_status(thread, metadata)
        assert result["status"] == "active"
        assert result["source"] == "live_workspace"

    def test_vm_takes_precedence_over_container(self):
        """When both VM and container ready, VM wins."""
        thread = _make_thread()
        metadata = {
            "vm": {"status": "ready", "ssh_host": "10.0.0.99"},
            "workspace_container": {"status": "ready", "pod_ip": "10.0.0.50"},
        }
        result = get_thread_ide_status(thread, metadata)
        assert result["source"] == "live_vm"

    def test_provisioning_returns_restoring(self):
        """Provisioning → restoring."""
        thread = _make_thread()
        result = get_thread_ide_status(
            thread, {"workspace_container": {"status": "provisioning"}}
        )
        assert result["status"] == "restoring"
        assert result["code_server_url"] is None

    def test_no_workspace_returns_unavailable(self):
        """No workspace → unavailable."""
        thread = _make_thread()
        result = get_thread_ide_status(thread, {})
        assert result["status"] == "unavailable"

    def test_gitea_url_none_when_no_repo(self):
        """gitea_url is None when no repo_name."""
        thread = _make_thread()
        result = get_thread_ide_status(thread, {})
        assert result["gitea_url"] is None

    def test_gitea_url_none_when_no_gitea_env(self):
        """gitea_url is None when GITEA_URL not set."""
        thread = _make_thread()
        metadata = {"workspace_container": {"repo_name": "thread-aaaa"}}
        with patch.dict(os.environ, {}, clear=True):
            os.environ.pop("GITEA_URL", None)
            result = get_thread_ide_status(thread, metadata)
        assert result["gitea_url"] is None


# =============================================================================
# 5.11: WS /ws/persistent/{thread_id} (proxy resolution)
# =============================================================================


class TestWsProxyResolve:
    """Tests for WS /ws/persistent/{thread_id} resolution logic."""

    @pytest.mark.asyncio
    async def test_closes_4404_when_thread_not_found(self):
        db = _mock_db()
        db.get_thread = AsyncMock(return_value=None)
        result = await ws_proxy_resolve(db, _mock_suspension(), "tid-1")
        assert result["close_code"] == 4404

    @pytest.mark.asyncio
    async def test_restores_suspended_workspace(self):
        thread = _make_thread(
            agent_id="agent-1",
            metadata={"workspace_container": {"status": "suspended"}},
        )
        db = _mock_db()
        db.get_thread = AsyncMock(return_value=thread)
        db.get_agent = AsyncMock(return_value={"pod_ip": "10.0.0.5", "pod_port": 8001})
        susp = _mock_suspension(enabled=True)

        result = await ws_proxy_resolve(db, susp, "tid-1")

        susp.restore_thread_workspace.assert_awaited_once_with("tid-1")
        assert "upstream_url" in result

    @pytest.mark.asyncio
    async def test_closes_4503_when_restoration_fails(self):
        thread = _make_thread(
            agent_id="agent-1",
            metadata={"workspace_container": {"status": "suspended"}},
        )
        db = _mock_db()
        db.get_thread = AsyncMock(return_value=thread)
        susp = _mock_suspension(enabled=True)
        susp.restore_thread_workspace = AsyncMock(return_value=False)

        result = await ws_proxy_resolve(db, susp, "tid-1")
        assert result["close_code"] == 4503

    @pytest.mark.asyncio
    async def test_closes_4404_when_no_agent_id(self):
        thread = _make_thread(agent_id=None)
        db = _mock_db()
        db.get_thread = AsyncMock(return_value=thread)

        result = await ws_proxy_resolve(db, _mock_suspension(), "tid-1")
        assert result["close_code"] == 4404

    @pytest.mark.asyncio
    async def test_closes_4503_when_agent_not_found(self):
        thread = _make_thread(agent_id="agent-1")
        db = _mock_db()
        db.get_thread = AsyncMock(return_value=thread)
        db.get_agent = AsyncMock(return_value=None)

        result = await ws_proxy_resolve(db, _mock_suspension(), "tid-1")
        assert result["close_code"] == 4503

    @pytest.mark.asyncio
    async def test_closes_4503_when_agent_has_no_ip(self):
        thread = _make_thread(agent_id="agent-1")
        db = _mock_db()
        db.get_thread = AsyncMock(return_value=thread)
        db.get_agent = AsyncMock(return_value={"pod_ip": None, "pod_port": 8001})

        result = await ws_proxy_resolve(db, _mock_suspension(), "tid-1")
        assert result["close_code"] == 4503

    @pytest.mark.asyncio
    async def test_returns_upstream_url_on_success(self):
        thread = _make_thread(agent_id="agent-1")
        db = _mock_db()
        db.get_thread = AsyncMock(return_value=thread)
        db.get_agent = AsyncMock(return_value={"pod_ip": "10.0.0.5", "pod_port": 8001})

        result = await ws_proxy_resolve(db, _mock_suspension(), "tid-1")
        assert result["upstream_url"] == "ws://10.0.0.5:8001/ws/chat"

    @pytest.mark.asyncio
    async def test_uses_default_pod_port_8001(self):
        thread = _make_thread(agent_id="agent-1")
        db = _mock_db()
        db.get_thread = AsyncMock(return_value=thread)
        db.get_agent = AsyncMock(return_value={"pod_ip": "10.0.0.5"})

        result = await ws_proxy_resolve(db, _mock_suspension(), "tid-1")
        assert result["upstream_url"] == "ws://10.0.0.5:8001/ws/chat"


# =============================================================================
# 5.12: _inspect_session_event()
# =============================================================================


class TestInspectSessionEvent:
    """Tests for _inspect_session_event notification helper."""

    def test_returns_immediately_when_user_id_falsy(self):
        broadcast = MagicMock()
        inspect_session_event(
            '{"method": "permission.request"}',
            "tid-1",
            "",
            "Title",
            "persistent_defaults",
            broadcast,
        )
        broadcast.assert_not_called()

    def test_broadcasts_permission_request(self):
        broadcast = MagicMock()
        raw = json.dumps(
            {
                "method": "permission.request",
                "params": {
                    "tool": "run_command",
                    "args": {"cmd": "ls"},
                    "reason": "needs shell",
                },
            }
        )
        inspect_session_event(
            raw, "tid-1", "user-1", "My Session", "persistent_defaults", broadcast
        )
        broadcast.assert_called_once()
        call_kwargs = broadcast.call_args[1]
        assert call_kwargs["event_type"] == "session.permission_request"
        assert call_kwargs["data"]["tool"] == "run_command"
        assert call_kwargs["data"]["thread_id"] == "tid-1"
        assert call_kwargs["data"]["title"] == "My Session"

    def test_broadcasts_vm_upgrade_needed(self):
        broadcast = MagicMock()
        raw = json.dumps(
            {
                "method": "vm_upgrade.needed",
                "params": {"command": "sudo apt install vim"},
            }
        )
        inspect_session_event(raw, "tid-1", "user-1", "Title", None, broadcast)
        call_kwargs = broadcast.call_args[1]
        assert call_kwargs["event_type"] == "session.vm_upgrade"
        assert call_kwargs["data"]["command"] == "sudo apt install vim"

    def test_broadcasts_ready_as_session_waiting(self):
        broadcast = MagicMock()
        raw = json.dumps({"method": "ready"})
        inspect_session_event(
            raw, "tid-1", "user-1", "Title", "persistent_defaults", broadcast
        )
        call_kwargs = broadcast.call_args[1]
        assert call_kwargs["event_type"] == "session.waiting"

    def test_ignores_non_notify_methods(self):
        broadcast = MagicMock()
        raw = json.dumps({"method": "text.delta"})
        inspect_session_event(
            raw, "tid-1", "user-1", "Title", "persistent_defaults", broadcast
        )
        broadcast.assert_not_called()

    def test_ignores_invalid_json(self):
        broadcast = MagicMock()
        inspect_session_event(
            "not-json{", "tid-1", "user-1", "Title", "persistent_defaults", broadcast
        )
        broadcast.assert_not_called()

    def test_handles_missing_params_gracefully(self):
        """params defaults to {} when absent."""
        broadcast = MagicMock()
        raw = json.dumps({"method": "permission.request"})
        inspect_session_event(
            raw, "tid-1", "user-1", "Title", "persistent_defaults", broadcast
        )
        call_kwargs = broadcast.call_args[1]
        assert call_kwargs["data"]["tool"] is None
        assert call_kwargs["data"]["args"] is None


# =============================================================================
# 5.13: _inspect_browser_event()
# =============================================================================


class TestInspectBrowserEvent:
    """Tests for _inspect_browser_event notification helper."""

    def test_returns_immediately_when_user_id_falsy(self):
        broadcast = MagicMock()
        inspect_browser_event('{"method": "approve"}', "tid-1", "", broadcast)
        broadcast.assert_not_called()

    def test_broadcasts_approve_as_session_resolved(self):
        broadcast = MagicMock()
        raw = json.dumps({"method": "approve"})
        inspect_browser_event(raw, "tid-1", "user-1", broadcast)
        broadcast.assert_called_once()
        call_kwargs = broadcast.call_args[1]
        assert call_kwargs["event_type"] == "session.resolved"
        assert call_kwargs["data"]["resolution"] == "approve"
        assert call_kwargs["data"]["thread_id"] == "tid-1"

    def test_broadcasts_deny_as_session_resolved(self):
        broadcast = MagicMock()
        raw = json.dumps({"method": "deny"})
        inspect_browser_event(raw, "tid-1", "user-1", broadcast)
        call_kwargs = broadcast.call_args[1]
        assert call_kwargs["data"]["resolution"] == "deny"

    def test_ignores_non_resolve_methods(self):
        broadcast = MagicMock()
        raw = json.dumps({"method": "text.delta"})
        inspect_browser_event(raw, "tid-1", "user-1", broadcast)
        broadcast.assert_not_called()

    def test_ignores_invalid_json(self):
        broadcast = MagicMock()
        inspect_browser_event("not{json", "tid-1", "user-1", broadcast)
        broadcast.assert_not_called()

    def test_silently_ignores_exceptions(self):
        """Any exception is swallowed — no propagation."""

        def bad_broadcast(**kwargs):
            raise RuntimeError("broadcast exploded")

        # Should not raise
        inspect_browser_event('{"method": "approve"}', "tid-1", "user-1", bad_broadcast)
