"""Tests for orchestrator/database/postgres.py thread methods.

Covers section 6 of persistent_agent_tests.md:
  6.1  create_thread
  6.2  get_thread
  6.3  list_threads
  6.4  end_thread
  6.5  update_thread_status
  6.6  update_thread_agent
  6.7  save_thread_message
  6.8  get_thread_messages_history
  6.9  get_thread_message_count
  6.10 update_thread_tokens
  6.11 merge_thread_workspace_context / merge_thread_vm_context

Mocks the asyncpg connection pool to test method logic without a live DB.
"""

import json
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest

from orchestrator.database.postgres import PostgresDB


# =============================================================================
# Fixtures
# =============================================================================


def _make_db_with_conn(mock_conn):
    """Create a PostgresDB instance with acquire() yielding mock_conn."""
    db = PostgresDB.__new__(PostgresDB)
    db._pool = MagicMock()
    db._connection_string = "test"
    db._queries = {}

    @asynccontextmanager
    async def mock_acquire():
        yield mock_conn

    db.acquire = mock_acquire
    return db


def _mock_conn():
    """Create a mock asyncpg connection."""
    conn = AsyncMock()
    transaction = MagicMock()
    transaction.__aenter__ = AsyncMock(return_value=transaction)
    transaction.__aexit__ = AsyncMock(return_value=False)
    conn.transaction = MagicMock(return_value=transaction)
    return conn


@pytest.mark.asyncio
async def test_pinned_workspace_resume_accepts_exact_soft_retired_pvc_shell():
    thread_id = UUID("aaaaaaaa-1111-4222-8333-444444444444")
    current_generation = UUID("11111111-1111-4111-8111-111111111111")
    retired_generation = UUID("22222222-2222-4222-8222-222222222222")
    attempt = UUID("33333333-3333-4333-8333-333333333333")
    pvc_uid = UUID("44444444-4444-4444-8444-444444444444")
    endpoint_generation = UUID("55555555-5555-4555-8555-555555555555")
    namespace = "srw-test"
    pod_name = f"ws-thread-{str(thread_id)[:12]}"
    pvc_name = f"pvc-{pod_name}"
    service_name = pod_name
    workspace = {
        "status": "deleted",
        "provisioner": "k8s",
        "namespace": namespace,
        "pod_name": pod_name,
        "host": f"{pod_name}.{namespace}.svc.cluster.local",
        "port": 30022,
        "pod_ip": None,
        "ide_host": None,
        "ide_port": None,
        "_runtime_incarnation": None,
        "_canvas_workspace_generation": str(endpoint_generation),
    }
    binding = {
        "kind": "remote",
        "backing_id": f"k8s-pvc:{namespace}:{pvc_uid}",
        "generation": str(endpoint_generation),
        "ssh_host_key_fingerprint": "SHA256:" + ("A" * 43),
    }
    thread = {
        "status": "created",
        "execution_lane": "pinned",
        "runtime_generation": current_generation,
        "runtime_retirement_token": None,
        "agent_id": None,
        "runtime_attach_token": None,
        "metadata": {
            "workspace_container": workspace,
            "_workspace_binding": binding,
        },
    }
    intent = {
        "attempt_id": attempt,
        "runtime_generation": current_generation,
        "pod_name": pod_name,
        "pvc_name": pvc_name,
        "seed_configmap_name": None,
        "service_name": service_name,
    }
    conn = _mock_conn()
    conn.fetch = AsyncMock(return_value=[])
    conn.fetchrow = AsyncMock(
        side_effect=[
            thread,
            None,
            {"runtime_generation": str(retired_generation)},
            intent,
        ]
    )
    conn.execute = AsyncMock(return_value="UPDATE 1")
    db = _make_db_with_conn(conn)

    result = await db.reserve_pinned_thread_workspace_provision_intent(
        str(thread_id),
        expected_runtime_generation=str(current_generation),
        expected_agent_id=None,
        expected_attach_token=None,
        expected_workspace_context=workspace,
        expected_binding_context=binding,
        attempt_id=str(attempt),
        namespace=namespace,
        pod_name=pod_name,
        pvc_name=pvc_name,
        seed_configmap_name=None,
        service_name=service_name,
        retained_service_uid=None,
        network_tier="internet-only",
        manifest_fingerprint="a" * 64,
    )

    assert result == intent
    pending_metadata = json.loads(conn.execute.await_args.args[4])
    pending = pending_metadata["workspace_container"]
    assert pending["status"] == "pending"
    assert pending["_workspace_provision_generation"] == str(current_generation)
    assert pending["_runtime_incarnation"] is None
    assert pending["pod_name"] is None
    proof_call = conn.fetchrow.await_args_list[2]
    proof_sql = " ".join(proof_call.args[0].split())
    assert "JOIN thread_runtime_retirement_outcomes AS outcome" in proof_sql
    assert "intent.runtime_generation <> $6::uuid" in proof_sql
    assert "outcome.permanent = false" in proof_sql
    assert "outcome.outcome = 'settled'" in proof_sql
    assert proof_call.args[6] == current_generation


@pytest.mark.asyncio
async def test_complete_pinned_workspace_intent_decodes_jsonb_previous_binding_text():
    """Real asyncpg returns this JSONB column as text in the deployed pool."""

    thread_id = "aaaaaaaa-1111-4222-8333-444444444444"
    generation = UUID("11111111-1111-4111-8111-111111111111")
    attempt = UUID("22222222-2222-4222-8222-222222222222")
    agent_id = UUID("33333333-3333-4333-8333-333333333333")
    attach_token = UUID("44444444-4444-4444-8444-444444444444")
    pod_uid = UUID("55555555-5555-4555-8555-555555555555")
    pvc_uid = UUID("66666666-6666-4666-8666-666666666666")
    seed_uid = UUID("77777777-7777-4777-8777-777777777777")
    service_uid = UUID("88888888-8888-4888-8888-888888888888")
    metadata = {
        "workspace_container": {
            "status": "pending",
            "_workspace_provision_attempt": str(attempt),
            "_workspace_provision_generation": str(generation),
        }
    }
    thread = {
        "execution_lane": "pinned",
        "status": "created",
        "runtime_generation": generation,
        "runtime_retirement_token": None,
        "agent_id": agent_id,
        "runtime_attach_token": attach_token,
        "metadata": json.dumps(metadata),
    }
    intent = {
        "status": "planned",
        "runtime_generation": generation,
        "pod_uid": pod_uid,
        "pvc_uid": pvc_uid,
        "pvc_name": "pvc-workspace",
        "seed_configmap_uid": seed_uid,
        "seed_configmap_name": "seed-workspace",
        "service_uid": service_uid,
        "service_name": "workspace-service",
        "created_agent_id": agent_id,
        "created_attach_token": attach_token,
        "previous_binding": "{}",
        "retained_binding_generation": None,
        "retained_pvc_uid": None,
        "namespace": "srw-test",
        "pod_name": "workspace-pod",
    }
    conn = _mock_conn()
    conn.fetchrow = AsyncMock(side_effect=[thread, intent])
    conn.execute = AsyncMock(side_effect=["UPDATE 1", "UPDATE 1"])
    db = _make_db_with_conn(conn)

    result = await db.complete_pinned_thread_workspace_provision_intent(
        thread_id,
        expected_runtime_generation=str(generation),
        attempt_id=str(attempt),
        expected_pod_uid=str(pod_uid),
        expected_pvc_uid=str(pvc_uid),
        expected_seed_configmap_uid=str(seed_uid),
        expected_service_uid=str(service_uid),
        pod_ip="10.42.0.9",
        ssh_host_key_fingerprint="SHA256:" + ("A" * 43),
    )

    assert result is not None
    assert result["runtime_incarnation"] == str(pod_uid)
    assert result["backing_id"] == f"k8s-pvc:srw-test:{pvc_uid}"
    assert result["host"] == "workspace-service.srw-test.svc.cluster.local"


class TestStatelessRetirementAcknowledgementSQL:
    THREAD_ID = "aaaaaaaa-1111-4222-8333-444444444444"
    GENERATION = "11111111-1111-4111-8111-111111111111"
    RUNTIME = "22222222-2222-4222-8222-222222222222"
    FINGERPRINT = "SHA256:" + ("A" * 43)

    @staticmethod
    def _normalized_fetchval_sql(conn) -> str:
        return " ".join(conn.fetchval.await_args.args[0].split())

    @pytest.mark.asyncio
    async def test_resident_ack_creates_protocol_provenance_key(self):
        conn = _mock_conn()
        conn.fetchval = AsyncMock(return_value=UUID(self.THREAD_ID))
        db = _make_db_with_conn(conn)

        assert await db.acknowledge_stateless_thread_resident_retirement(
            self.THREAD_ID,
            terminal_token=8,
            workspace_generation=self.GENERATION,
            endpoint_generation=self.GENERATION,
            runtime_incarnation=self.RUNTIME,
            host_key_fingerprint=self.FINGERPRINT,
            proof={"rclone_mounts": 1},
        )

        sql = self._normalized_fetchval_sql(conn)
        assert (
            "'{_stateless_claim_retirement,residents_retired_by}', "
            "'\"protocol\"'::jsonb, true" in sql
        )

    @pytest.mark.asyncio
    async def test_shell_ack_creates_protocol_provenance_key(self):
        conn = _mock_conn()
        conn.fetchval = AsyncMock(return_value=UUID(self.THREAD_ID))
        db = _make_db_with_conn(conn)

        assert await db.acknowledge_stateless_thread_shell_retirement(
            self.THREAD_ID,
            terminal_token=8,
            workspace_generation=self.GENERATION,
            endpoint_generation=self.GENERATION,
            runtime_incarnation=self.RUNTIME,
            host_key_fingerprint=self.FINGERPRINT,
        )

        sql = self._normalized_fetchval_sql(conn)
        assert (
            "'{_stateless_claim_retirement,remote_retired_by}', "
            "'\"protocol\"'::jsonb, true" in sql
        )

    @pytest.mark.asyncio
    async def test_exact_terminal_ack_creates_both_provenance_keys(self):
        conn = _mock_conn()
        conn.fetchval = AsyncMock(return_value=UUID(self.THREAD_ID))
        db = _make_db_with_conn(conn)

        assert await db.acknowledge_stateless_thread_shell_absent(
            self.THREAD_ID,
            terminal_token=8,
            runtime_incarnation=self.RUNTIME,
        )

        sql = self._normalized_fetchval_sql(conn)
        assert (
            "'{_stateless_claim_retirement,residents_retired_by}', "
            "'\"workspace_runtime_terminal\"'::jsonb, true" in sql
        )
        assert (
            "'{_stateless_claim_retirement,remote_retired_by}', "
            "'\"workspace_runtime_terminal\"'::jsonb, true" in sql
        )


# =============================================================================
# 6.1: create_thread
# =============================================================================


class TestCreateThread:
    """Tests for create_thread method."""

    @pytest.fixture(autouse=True)
    def capture_boundary(self, monkeypatch):
        # This class verifies INSERT shape; snapshot transactions and delivery
        # have their own production-schema and capture tests.
        monkeypatch.setattr(
            "orchestrator.services.manifest_execution_snapshot.capture_execution",
            AsyncMock(),
        )

    @pytest.mark.asyncio
    async def test_returns_uuid_string(self):
        conn = _mock_conn()
        conn.fetchrow = AsyncMock(
            return_value={"id": UUID("aaaaaaaa-1111-2222-3333-444444444444")}
        )
        db = _make_db_with_conn(conn)

        result = await db.create_thread()
        assert result == "aaaaaaaa-1111-2222-3333-444444444444"
        assert isinstance(result, str)

    @pytest.mark.asyncio
    async def test_passes_all_params_to_query(self):
        user_id = "11111111-1111-4111-8111-111111111111"
        project_id = "22222222-2222-4222-8222-222222222222"
        conn = _mock_conn()
        conn.fetchrow = AsyncMock(
            return_value={"id": UUID("aaaaaaaa-1111-2222-3333-444444444444")}
        )
        db = _make_db_with_conn(conn)

        await db.create_thread(
            user_id=user_id,
            project_id=project_id,
            config_name="persistent_defaults",
            permission_mode="autonomous",
            narration_mode="silent",
            title="My Session",
        )

        call_args = conn.fetchrow.call_args
        # Positional args after the SQL query
        assert call_args[0][1] == user_id  # user_id
        assert call_args[0][2] == project_id  # project_id
        assert call_args[0][3] == "persistent_defaults"  # config_name
        assert call_args[0][4] == "autonomous"  # permission_mode
        assert call_args[0][5] == "silent"  # narration_mode
        assert call_args[0][6] == "My Session"  # title

    @pytest.mark.asyncio
    async def test_optional_params_default_none(self):
        conn = _mock_conn()
        conn.fetchrow = AsyncMock(
            return_value={"id": UUID("aaaaaaaa-1111-2222-3333-444444444444")}
        )
        db = _make_db_with_conn(conn)

        await db.create_thread()

        call_args = conn.fetchrow.call_args
        assert call_args[0][1] is None  # user_id
        assert call_args[0][2] is None  # project_id

    @pytest.mark.asyncio
    async def test_initial_event_is_inserted_in_the_creation_transaction(self):
        conn = _mock_conn()
        conn.fetchrow = AsyncMock(
            return_value={"id": UUID("aaaaaaaa-1111-2222-3333-444444444444")}
        )
        db = _make_db_with_conn(conn)

        metadata = {
            "review_delivery": {
                "datasource_id": "repo-1",
                "branch": "feature/review",
            }
        }
        await db.create_thread(
            initial_metadata=metadata,
            initial_event="Review job abc on branch feature/review",
        )

        stored = json.loads(conn.fetchrow.await_args.args[7])
        assert stored["review_delivery"] == metadata["review_delivery"]
        event_call = next(
            call
            for call in conn.execute.await_args_list
            if "INSERT INTO thread_messages" in call.args[0]
        )
        assert "'event'" in event_call.args[0]
        assert event_call.args[1] == UUID("aaaaaaaa-1111-2222-3333-444444444444")
        assert event_call.args[2] == "Review job abc on branch feature/review"
        conn.transaction.return_value.__aenter__.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_default_values(self):
        conn = _mock_conn()
        conn.fetchrow = AsyncMock(
            return_value={"id": UUID("aaaaaaaa-1111-2222-3333-444444444444")}
        )
        db = _make_db_with_conn(conn)

        await db.create_thread()

        call_args = conn.fetchrow.call_args
        assert call_args[0][3] == "session_base"  # config_name
        assert call_args[0][4] == "supervised"  # permission_mode
        assert call_args[0][5] == "auto"  # narration_mode
        assert call_args[0][6] == "Untitled Session"  # title
        assert call_args[0][8] == "pinned"  # execution_lane

    @pytest.mark.asyncio
    async def test_process_zero_receipt_is_stripped_at_common_create_funnel(self):
        conn = _mock_conn()
        conn.fetchrow = AsyncMock(
            return_value={"id": UUID("aaaaaaaa-1111-2222-3333-444444444444")}
        )
        db = _make_db_with_conn(conn)

        await db.create_thread(
            execution_lane="stateless",
            initial_metadata={
                "config_override": {"workspace": {"backend": "virtual"}},
                "_stateless_workspace_process_zero_observation": {
                    "runtime_incarnation": ("22222222-2222-4222-8222-222222222222"),
                    "observed_at": "2026-08-26T12:00:00+00:00",
                },
            },
        )

        stored = json.loads(conn.fetchrow.await_args.args[7])
        assert "_stateless_workspace_process_zero_observation" not in stored

    @pytest.mark.asyncio
    async def test_explicit_stateless_lane_is_written_in_creation_insert(self):
        conn = _mock_conn()
        conn.fetchrow = AsyncMock(
            return_value={"id": UUID("aaaaaaaa-1111-2222-3333-444444444444")}
        )
        db = _make_db_with_conn(conn)

        await db.create_thread(
            execution_lane="stateless",
            initial_metadata={"config_override": {"workspace": {"backend": "virtual"}}},
        )

        sql, *params = conn.fetchrow.call_args.args
        assert "metadata, execution_lane" in sql
        assert "$7::jsonb, $8" in sql
        assert params[7] == "stateless"
        stored = json.loads(params[6])
        assert stored["config_override"]["workspace"]["backend"] == "virtual"

    @pytest.mark.asyncio
    async def test_stateless_sandbox_insert_atomically_contains_fresh_nonce(self):
        generation = "11111111-2222-4333-8444-555555555555"
        initial_metadata = {
            "config_override": {"workspace": {"backend": "sandbox"}},
            "workspace_container": {
                "status": "pending",
                "provisioner": "k8s",
                "_runtime_creation": {
                    "generation": generation,
                    "mode": "create",
                    "attempted": False,
                    "replaces_uid": None,
                },
            },
        }
        conn = _mock_conn()
        conn.fetchrow = AsyncMock(
            return_value={"id": UUID("aaaaaaaa-1111-2222-3333-444444444444")}
        )
        db = _make_db_with_conn(conn)

        await db.create_thread(
            execution_lane="stateless",
            initial_metadata=initial_metadata,
        )

        stored = json.loads(conn.fetchrow.await_args.args[7])
        # The creation generation is server-owned: a caller-supplied value is
        # replaced with a fresh nonce in the same insert, and nothing else in
        # the projection moves.
        stored_workspace = stored["workspace_container"]
        stored_marker = stored_workspace["_runtime_creation"]
        assert stored_marker["generation"] != generation
        assert str(UUID(stored_marker["generation"])) == stored_marker["generation"]
        assert {k: v for k, v in stored_marker.items() if k != "generation"} == {
            "mode": "create",
            "attempted": False,
            "replaces_uid": None,
        }
        assert {
            k: v for k, v in stored_workspace.items() if k != "_runtime_creation"
        } == {
            k: v
            for k, v in initial_metadata["workspace_container"].items()
            if k != "_runtime_creation"
        }
        assert stored["datasource_ids"] == []

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "initial_metadata",
        [
            {"config_override": {"workspace": {"backend": "sandbox"}}},
            {
                "config_override": {"workspace": {"backend": "virtual"}},
                "workspace_container": {"status": "pending", "provisioner": "k8s"},
            },
            {"config_override": {"workspace": {"backend": "none"}, "officer": []}},
            {},
        ],
        ids=[
            "sandbox-missing-nonce",
            "virtual-physical-context",
            "malformed-pinned-class",
            "missing-backend",
        ],
    )
    async def test_stateless_insert_refuses_unclassified_or_ambiguous_metadata(
        self, initial_metadata
    ):
        conn = _mock_conn()
        db = _make_db_with_conn(conn)

        with pytest.raises(ValueError, match="initial stateless session metadata"):
            await db.create_thread(
                execution_lane="stateless",
                initial_metadata=initial_metadata,
            )

        conn.fetchrow.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_unknown_execution_lane_fails_before_database_access(self):
        conn = _mock_conn()
        db = _make_db_with_conn(conn)

        with pytest.raises(ValueError, match="Unsupported thread execution lane"):
            await db.create_thread(execution_lane="future-lane")

        conn.fetchrow.assert_not_awaited()


# =============================================================================
# 6.2: get_thread
# =============================================================================


class TestGetThread:
    """Tests for get_thread method."""

    TID = "5833c729-c0cd-496f-9a40-e9b811ae0ced"

    @pytest.mark.asyncio
    async def test_returns_dict_for_existing_thread(self):
        row = {"id": self.TID, "user_id": "user-1", "status": "active"}
        conn = _mock_conn()
        conn.fetchrow = AsyncMock(return_value=row)
        db = _make_db_with_conn(conn)

        result = await db.get_thread(self.TID)
        assert result == {"id": self.TID, "user_id": "user-1", "status": "active"}

    @pytest.mark.asyncio
    async def test_returns_none_for_nonexistent(self):
        conn = _mock_conn()
        conn.fetchrow = AsyncMock(return_value=None)
        db = _make_db_with_conn(conn)

        result = await db.get_thread("cccccccc-0000-4000-8000-000000000000")
        assert result is None

    @pytest.mark.asyncio
    async def test_malformed_id_is_none_not_dataerror(self):
        """The id is user-supplied path input on every thread route; an
        8-char prefix used to hit the uuid bind and 500. It must resolve to
        None (the caller's 404) without ever touching the pool.
        knowledge-base/knowledge/issues/session_turn_end_cloud_push_blocks_queued_input.md"""
        conn = _mock_conn()
        conn.fetchrow = AsyncMock()
        db = _make_db_with_conn(conn)

        assert await db.get_thread("5833c729") is None
        assert await db.get_thread("not-a-uuid") is None
        conn.fetchrow.assert_not_awaited()


# =============================================================================
# 6.3: list_threads
# =============================================================================


class TestListThreads:
    """Tests for list_threads method."""

    @pytest.mark.asyncio
    async def test_no_filters_returns_all(self):
        conn = _mock_conn()
        conn.fetch = AsyncMock(return_value=[{"id": "t1"}, {"id": "t2"}])
        db = _make_db_with_conn(conn)

        result = await db.list_threads()
        assert len(result) == 2
        # No caller filter — only the always-on session gate (0206: subagent
        # child rows share the table and must never reach the sessions page).
        sql = " ".join(conn.fetch.call_args[0][0].split())
        assert "WHERE kind = 'session' ORDER BY" in sql
        assert conn.fetch.call_args[0][1:] == ()
        assert "ORDER BY created_at DESC" in sql
        assert "LIMIT 50" in sql

    @pytest.mark.asyncio
    async def test_user_id_filter_includes_null(self):
        """user_id filter includes threads with matching user_id AND user_id IS NULL."""
        conn = _mock_conn()
        conn.fetch = AsyncMock(return_value=[])
        db = _make_db_with_conn(conn)

        await db.list_threads(user_id="user-1")
        sql = " ".join(conn.fetch.call_args[0][0].split())
        assert "user_id = $1 OR user_id IS NULL" in sql

    @pytest.mark.asyncio
    async def test_project_id_filter(self):
        conn = _mock_conn()
        conn.fetch = AsyncMock(return_value=[])
        db = _make_db_with_conn(conn)

        await db.list_threads(project_id="proj-1")
        sql = " ".join(conn.fetch.call_args[0][0].split())
        assert "project_id = $1" in sql

    @pytest.mark.asyncio
    async def test_status_filter(self):
        conn = _mock_conn()
        conn.fetch = AsyncMock(return_value=[])
        db = _make_db_with_conn(conn)

        await db.list_threads(status="ended")
        sql = " ".join(conn.fetch.call_args[0][0].split())
        assert "status = $1" in sql

    @pytest.mark.asyncio
    async def test_multiple_filters_combined_with_and(self):
        conn = _mock_conn()
        conn.fetch = AsyncMock(return_value=[])
        db = _make_db_with_conn(conn)

        await db.list_threads(user_id="u1", project_id="p1", status="active")
        sql = " ".join(conn.fetch.call_args[0][0].split())
        assert "AND" in sql

    @pytest.mark.asyncio
    async def test_capped_at_50(self):
        conn = _mock_conn()
        conn.fetch = AsyncMock(return_value=[])
        db = _make_db_with_conn(conn)

        await db.list_threads()
        sql = " ".join(conn.fetch.call_args[0][0].split())
        assert "LIMIT 50" in sql


# =============================================================================
# 6.4: end_thread
# =============================================================================


def _proven_soft_retirement_metadata(*, retain_runtime: bool) -> dict:
    generation = "11111111-1111-4111-8111-111111111111"
    runtime = "22222222-2222-4222-8222-222222222222"
    fingerprint = "SHA256:" + ("A" * 43)
    ack = {
        "kind": "protocol",
        "terminal_token": 8,
        "workspace_generation": generation,
        "endpoint_generation": generation,
        "runtime_incarnation": runtime,
        "host_key_fingerprint": fingerprint,
    }
    return {
        "_stateless_workspace_retirement_pending": True,
        "_stateless_claim_retirement": {
            "terminal_token": 8,
            "claimant_quiesced": True,
            "shell_retirement_required": True,
            "resident_cleanup_required": True,
            "residents_retired": True,
            "residents_retired_by": "protocol",
            "remote_retired": True,
            "remote_retired_by": "protocol",
            "permanent": False,
            "workspace_generation": generation,
            "endpoint_generation": generation,
            "runtime_incarnation": runtime,
            "host_key_fingerprint": fingerprint,
        },
        "_stateless_resident_retirement_ack": dict(ack),
        "_stateless_shell_retirement_ack": dict(ack),
        "_workspace_binding": {
            "kind": "remote",
            "generation": generation,
            "backing_id": "k8s-pod:agent-workspaces:pod-uid",
            "ssh_host_key_fingerprint": fingerprint,
        },
        "workspace_container": {
            "status": "deleted",
            "provisioner": "k8s",
            "_canvas_workspace_generation": generation,
            "_runtime_incarnation": runtime if retain_runtime else None,
            "_snapshot_restore_required": True,
        },
    }


class TestStatelessWorkspaceCreationAuthority:
    THREAD_ID = "aaaaaaaa-1111-4222-8333-444444444444"
    GENERATION = "11111111-2222-4333-8444-555555555555"

    @pytest.mark.asyncio
    async def test_periodic_prepare_never_arms_markerless_physical_row(self):
        conn = _mock_conn()
        conn.fetchrow = AsyncMock(
            return_value={
                "status": "created",
                "execution_lane": "stateless",
                # 0197/0198 canonicalise the thread's own runtime generation
                # before any creation authority is considered.
                "runtime_generation": self.GENERATION,
                "metadata": {
                    "workspace_container": {
                        "status": "pending",
                        "provisioner": "k8s",
                    }
                },
            }
        )
        db = _make_db_with_conn(conn)

        result = await db.prepare_stateless_thread_workspace_creation(
            self.THREAD_ID,
            proposed_generation=self.GENERATION,
            mode="create",
        )

        assert result == {
            "state": "blocked",
            "reason": "workspace creation authority was not pre-armed",
        }
        conn.execute.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_db_boundary_refuses_creation_marker_with_extra_fields(self):
        marker = {
            "generation": self.GENERATION,
            "mode": "create",
            "attempted": False,
            "replaces_uid": None,
            "future": False,
        }
        conn = _mock_conn()
        conn.fetchrow = AsyncMock(
            return_value={
                "status": "created",
                "execution_lane": "stateless",
                "runtime_generation": self.GENERATION,
                "metadata": {
                    "workspace_container": {
                        "status": "pending",
                        "provisioner": "k8s",
                        "_runtime_creation": marker,
                    }
                },
            }
        )
        db = _make_db_with_conn(conn)

        result = await db.prepare_stateless_thread_workspace_creation(
            self.THREAD_ID,
            proposed_generation=self.GENERATION,
            mode="create",
        )

        assert result["state"] == "blocked"
        assert "malformed" in result["reason"]
        conn.execute.assert_not_awaited()


class TestEndThread:
    """Tests for end_thread method."""

    @pytest.mark.asyncio
    async def test_sets_status_ended(self):
        conn = _mock_conn()
        db = _make_db_with_conn(conn)

        await db.end_thread("tid-1")

        sql = " ".join(conn.execute.call_args[0][0].split())
        assert "status = 'ended'" in sql
        assert "ended_at = CURRENT_TIMESTAMP" in sql
        assert "control_admission_agent_id = NULL" in sql
        assert conn.execute.call_args[0][1] == "tid-1"

    @pytest.mark.asyncio
    async def test_does_not_error_on_nonexistent_thread(self):
        """UPDATE affects 0 rows — no exception."""
        conn = _mock_conn()
        db = _make_db_with_conn(conn)

        # Should not raise
        await db.end_thread("nonexistent")

    @pytest.mark.asyncio
    @pytest.mark.parametrize("permanent", [False, True])
    async def test_queued_token_zero_lite_end_mints_terminal_generation(
        self, permanent
    ):
        metadata = {
            "config_override": {"workspace": {"backend": "virtual"}},
            "workspace_container": {},
            "_workspace_binding": {
                "generation": "11111111-1111-4111-8111-111111111111",
                "kind": "virtual",
                "backing_id": "rclone:threads/tid-1",
                "ssh_host_key_fingerprint": None,
            },
        }
        queue = {
            "unit_kind": "session_turn",
            "state": "queued",
            "lease_token": 0,
            "leased_by": None,
            "last_leased_by": None,
            "leased_until": None,
            "attempts_since_completion": 0,
            "input_seq": 4,
            "consumed_seq": None,
            "control_input_seq": 0,
            "control_consumed_seq": 0,
            "interrupt_admission_lease_token": None,
            "interrupt_admission_turn_id": None,
        }
        conn = _mock_conn()
        conn.fetchrow = AsyncMock(
            side_effect=[
                {
                    "id": "tid-1",
                    "status": "active",
                    "execution_lane": "stateless",
                    "metadata": metadata,
                },
                queue,
                {"lease_token": 1, "attempts_since_completion": 0},
                {"id": "tid-1", "kind": "session", "execution_lane": "stateless"},
            ]
        )
        conn.fetch = AsyncMock(return_value=[])
        conn.fetchval = AsyncMock(side_effect=[False, False, 1, "tid-1"])
        db = _make_db_with_conn(conn)

        closed = await db.begin_stateless_thread_workspace_retirement(
            "tid-1", force=True, permanent=permanent
        )

        assert closed["state"] == "closed"
        assert closed["terminal_token"] == 1
        assert closed["shell_retirement_required"] is False
        assert closed["resident_cleanup_required"] is False
        marker = json.loads(conn.fetchval.await_args_list[-1].args[2])
        authority = (
            marker["_stateless_claim_retirement"]
            if ("_stateless_claim_retirement" in marker)
            else marker
        )
        # The UPDATE receives the authority object itself, not a whole metadata
        # document; retain compatibility with a future composed SQL helper.
        assert authority["terminal_token"] == 1
        assert authority["permanent"] is permanent
        if not permanent:
            pending_metadata = {
                **metadata,
                "_stateless_workspace_retirement_pending": True,
                "_stateless_claim_retirement": authority,
            }
            finish_conn = _mock_conn()
            finish_conn.fetchrow = AsyncMock(
                side_effect=[
                    {
                        "status": "ended",
                        "execution_lane": "stateless",
                        "metadata": pending_metadata,
                    },
                    {
                        "unit_kind": "session_turn",
                        "state": "done",
                        "lease_token": 1,
                    },
                    {
                        "id": "tid-1",
                        "kind": "session",
                        "execution_lane": "stateless",
                    },
                ]
            )
            finish_conn.fetch = AsyncMock(return_value=[])
            finish_conn.fetchval = AsyncMock(return_value="tid-1")
            finish_db = _make_db_with_conn(finish_conn)

            assert (
                await finish_db.finish_stateless_thread_workspace_retirement("tid-1")
                is True
            )
            settled_metadata = json.loads(finish_conn.fetchval.await_args.args[2])
            assert (
                settled_metadata["_stateless_workspace_retirement_settled"][
                    "terminal_token"
                ]
                == 1
            )

    @pytest.mark.asyncio
    async def test_force_end_token_zero_retires_legacy_unbound_permission(
        self, monkeypatch
    ):
        from shared import session_permission_retirement

        metadata = {
            "config_override": {"workspace": {"backend": "virtual"}},
            "workspace_container": {},
            "_workspace_binding": {
                "generation": "11111111-1111-4111-8111-111111111111",
                "kind": "virtual",
                "backing_id": "rclone:threads/tid-1",
                "ssh_host_key_fingerprint": None,
            },
        }
        queue = {
            "unit_kind": "session_turn",
            "state": "queued",
            "lease_token": 0,
            "leased_by": None,
            "last_leased_by": None,
            "leased_until": None,
            "attempts_since_completion": 0,
            "input_seq": None,
            "consumed_seq": None,
            "control_input_seq": 0,
            "control_consumed_seq": 0,
            "interrupt_admission_lease_token": None,
            "interrupt_admission_turn_id": None,
        }
        conn = _mock_conn()
        conn.fetchrow = AsyncMock(
            side_effect=[
                {
                    "id": "tid-1",
                    "status": "active",
                    "execution_lane": "stateless",
                    "metadata": metadata,
                },
                queue,
                {"lease_token": 1, "attempts_since_completion": 0},
                {"id": "tid-1", "kind": "session", "execution_lane": "stateless"},
            ]
        )
        conn.fetch = AsyncMock(return_value=[])
        conn.fetchval = AsyncMock(side_effect=[False, True, 1, "tid-1"])
        retirement = MagicMock(epoch_bumped=True, count=1)
        retire = AsyncMock(return_value=retirement)
        monkeypatch.setattr(
            session_permission_retirement,
            "retire_stale_stateless_permissions",
            retire,
        )
        db = _make_db_with_conn(conn)

        closed = await db.begin_stateless_thread_workspace_retirement(
            "tid-1", force=True
        )

        assert closed["state"] == "closed"
        assert closed["terminal_token"] == 1
        retire.assert_awaited_once_with(
            conn,
            thread_id="tid-1",
            retired_lease_token=0,
            successor_lease_token=1,
            reason="force_end",
            epoch_already_bumped=False,
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize("permanent", [False, True])
    async def test_never_input_physical_end_creates_terminal_queue_authority(
        self, permanent
    ):
        generation = "11111111-1111-4111-8111-111111111111"
        runtime = "22222222-2222-4222-8222-222222222222"
        metadata = {
            "config_override": {"workspace": {"backend": "sandbox"}},
            "workspace_container": {
                "status": "ready",
                "provisioner": "k8s",
                "_canvas_workspace_generation": generation,
                "_runtime_incarnation": runtime,
            },
            "_workspace_binding": {
                "generation": generation,
                "kind": "remote",
                "backing_id": "k8s-pod:agent-workspaces:pod-uid",
                "ssh_host_key_fingerprint": "SHA256:" + ("A" * 43),
            },
        }
        synthetic = {
            "unit_kind": "session_turn",
            "state": "queued",
            "lease_token": 0,
            "leased_by": None,
            "last_leased_by": None,
            "leased_until": None,
            "attempts_since_completion": 0,
            "input_seq": None,
            "consumed_seq": None,
            "control_input_seq": 0,
            "control_consumed_seq": 0,
            "interrupt_admission_lease_token": None,
            "interrupt_admission_turn_id": None,
        }
        conn = _mock_conn()
        conn.fetchrow = AsyncMock(
            side_effect=[
                {
                    "id": "tid-1",
                    "status": "created",
                    "execution_lane": "stateless",
                    "metadata": metadata,
                },
                None,
                synthetic,
                {"lease_token": 1, "attempts_since_completion": 0},
                {"id": "tid-1", "kind": "session", "execution_lane": "stateless"},
            ]
        )
        conn.fetch = AsyncMock(return_value=[])
        conn.fetchval = AsyncMock(side_effect=[False, False, 1, "tid-1"])
        db = _make_db_with_conn(conn)

        closed = await db.begin_stateless_thread_workspace_retirement(
            "tid-1", force=True, permanent=permanent
        )

        assert closed["terminal_token"] == 1
        assert closed["shell_retirement_required"] is True
        assert closed["resident_cleanup_required"] is True
        assert "INSERT INTO run_queue" in conn.fetchrow.await_args_list[2].args[0]

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("backing_id", "snapshot_captured", "permanent", "succeeds"),
        [
            (None, False, False, False),
            (None, False, True, False),
            ("k8s-pvc:agent-workspaces:pvc-uid", False, False, False),
            ("k8s-pvc:agent-workspaces:pvc-uid", False, True, False),
            ("k8s-pod:agent-workspaces:pod-uid", False, False, False),
            ("k8s-pod:agent-workspaces:pod-uid", True, False, False),
            ("k8s-pod:agent-workspaces:pod-uid", False, True, False),
        ],
    )
    async def test_missing_runtime_absence_proof_preserves_emptydir_bytes(
        self,
        backing_id,
        snapshot_captured,
        permanent,
        succeeds,
    ):
        metadata = {
            "config_override": {"workspace": {"backend": "sandbox"}},
            "workspace_container": {
                "status": "deleted",
                "provisioner": "k8s",
                "_runtime_incarnation": None,
                "_snapshot_restore_required": snapshot_captured,
            },
            "_workspace_binding": {
                "kind": "remote",
                "backing_id": backing_id,
            },
        }
        conn = _mock_conn()
        conn.fetchrow = AsyncMock(
            side_effect=[
                {
                    "id": "tid-1",
                    "status": "created",
                    "execution_lane": "stateless",
                    "metadata": metadata,
                },
                None,
            ]
        )
        conn.fetchval = AsyncMock(side_effect=[False, False, "tid-1"])
        db = _make_db_with_conn(conn)

        if succeeds:
            result = await db.begin_stateless_thread_workspace_retirement(
                "tid-1",
                force=True,
                permanent=permanent,
                workspace_absence_proven=True,
            )
        else:
            with pytest.raises(RuntimeError, match="absence proof is unsupported"):
                await db.begin_stateless_thread_workspace_retirement(
                    "tid-1",
                    force=True,
                    permanent=permanent,
                    workspace_absence_proven=True,
                )
        assert not any(
            "INSERT INTO run_queue" in call.args[0]
            for call in conn.fetchrow.await_args_list
        )
        if succeeds:
            assert result["state"] == "closed"
            assert result["terminal_token"] == 0
            assert result["workspace_absence_proven"] is True
            assert result["shell_retirement_required"] is False
            assert result["resident_cleanup_required"] is False
            marker = json.loads(conn.fetchval.await_args_list[-1].args[2])
            assert marker["workspace_absence_proven"] is True
        else:
            assert conn.fetchval.await_count == 2

    @pytest.mark.asyncio
    async def test_missing_runtime_without_preflight_never_mutates_lifecycle(self):
        metadata = {
            "config_override": {"workspace": {"backend": "sandbox"}},
            "workspace_container": {
                "status": "created",
                "provisioner": "k8s",
                "_runtime_incarnation": None,
                "_snapshot_restore_required": False,
            },
            "_workspace_binding": {"kind": "remote", "backing_id": None},
        }
        conn = _mock_conn()
        conn.fetchrow = AsyncMock(
            side_effect=[
                {
                    "id": "tid-1",
                    "status": "created",
                    "execution_lane": "stateless",
                    "metadata": metadata,
                },
                None,
            ]
        )
        conn.fetchval = AsyncMock(side_effect=[False, False])
        db = _make_db_with_conn(conn)

        result = await db.begin_stateless_thread_workspace_retirement(
            "tid-1", force=True
        )

        assert result == {"state": "needs_runtime_preflight"}
        assert conn.fetchval.await_count == 2
        assert not any(
            "INSERT INTO run_queue" in call.args[0]
            for call in conn.fetchrow.await_args_list
        )

    @pytest.mark.asyncio
    async def test_settled_lite_end_revives_pending_input_without_token_reuse(self):
        metadata = {
            "_stateless_workspace_retirement_settled": {
                "terminal_token": 1,
                "cleanup_complete": True,
                "permanent": False,
                "backing_id": "rclone:threads/tid-1",
                "runtime_incarnation": None,
                "snapshot_restore_required": False,
            },
            "workspace_container": {},
        }
        conn = _mock_conn()
        conn.fetchrow = AsyncMock(
            side_effect=[
                {
                    "status": "ended",
                    "execution_lane": "stateless",
                    "runtime_generation": None,
                    "runtime_retirement_token": None,
                    "metadata": metadata,
                },
                {"unit_kind": "session_turn"},
            ]
        )
        conn.fetchval = AsyncMock(side_effect=[12, "tid-1", "tid-1"])
        db = _make_db_with_conn(conn)

        assert await db.resume_thread("tid-1") is True
        pending_sql = " ".join(conn.fetchval.await_args_list[0].args[0].split())
        assert "FROM thread_input_deliveries" in pending_sql
        queue_sql = " ".join(conn.fetchval.await_args_list[1].args[0].split())
        assert "input_seq > COALESCE(consumed_seq, -1)" in queue_sql
        assert "THEN 'queued' ELSE 'done'" in queue_sql
        assert conn.fetchval.await_args_list[1].args[1:] == ("tid-1", 12)

    @pytest.mark.asyncio
    async def test_resume_clears_agent_and_control_capability(self):
        conn = _mock_conn()
        conn.fetchrow = AsyncMock(
            return_value={
                "status": "ended",
                "execution_lane": "pinned",
                "runtime_generation": None,
                "runtime_retirement_token": None,
                "metadata": {},
            }
        )
        conn.fetchval = AsyncMock(return_value="tid-1")
        db = _make_db_with_conn(conn)

        assert await db.resume_thread("tid-1") is True

        sql = " ".join(conn.fetchval.call_args[0][0].split())
        assert "status = 'created'" in sql
        assert "agent_id = NULL" in sql
        assert "control_admission_agent_id = NULL" in sql

    @pytest.mark.asyncio
    async def test_resume_refuses_ownerless_historical_thread(self):
        conn = _mock_conn()
        conn.fetchrow = AsyncMock(
            return_value={
                "status": "ended",
                "user_id": None,
                "execution_lane": "pinned",
                "runtime_generation": None,
                "runtime_retirement_token": None,
                "metadata": {},
            }
        )
        db = _make_db_with_conn(conn)

        assert await db.resume_thread("tid-1") is False
        conn.fetchval.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_stateless_retirement_marker_fences_resume_until_settled(self):
        pending_conn = _mock_conn()
        pending_conn.fetchrow = AsyncMock(
            return_value={
                "status": "ended",
                "execution_lane": "stateless",
                "runtime_generation": None,
                "runtime_retirement_token": None,
                "metadata": {"_stateless_workspace_retirement_pending": True},
            }
        )
        pending_db = _make_db_with_conn(pending_conn)

        assert await pending_db.resume_thread("tid-1") is False
        pending_conn.fetchval.assert_not_awaited()

        settled_conn = _mock_conn()
        settled_conn.fetchrow = AsyncMock(
            side_effect=[
                {
                    "status": "ended",
                    "execution_lane": "stateless",
                    "runtime_generation": None,
                    "runtime_retirement_token": None,
                    "metadata": {
                        "_stateless_workspace_retirement_settled": {
                            "terminal_token": 7,
                            "cleanup_complete": True,
                            "permanent": False,
                            "backing_id": None,
                            "runtime_incarnation": None,
                            "snapshot_restore_required": False,
                        }
                    },
                },
                {"unit_kind": "session_turn"},
            ]
        )
        settled_conn.fetchval = AsyncMock(side_effect=[None, "tid-1", "tid-1"])
        settled_db = _make_db_with_conn(settled_conn)

        assert await settled_db.resume_thread("tid-1") is True
        update_call = settled_conn.fetchval.await_args_list[-1]
        update_sql = " ".join(update_call.args[0].split())
        assert "metadata = $2::jsonb" in update_sql
        stored = json.loads(update_call.args[2])
        assert "_stateless_workspace_retirement_settled" not in stored

    @pytest.mark.asyncio
    @pytest.mark.parametrize("malformed", [None, False, 0, "", [], {}])
    async def test_stateless_malformed_retirement_marker_never_revives_queue(
        self, malformed
    ):
        conn = _mock_conn()
        conn.fetchrow = AsyncMock(
            return_value={
                "status": "ended",
                "execution_lane": "stateless",
                "runtime_generation": None,
                "runtime_retirement_token": None,
                "metadata": {"_stateless_workspace_retirement_pending": malformed},
            }
        )
        db = _make_db_with_conn(conn)

        assert await db.resume_thread("tid-1") is False
        conn.fetchval.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_permanent_settled_intent_never_revives_queue(self):
        conn = _mock_conn()
        conn.fetchrow = AsyncMock(
            return_value={
                "status": "ended",
                "execution_lane": "stateless",
                "runtime_generation": None,
                "runtime_retirement_token": None,
                "metadata": {
                    "_stateless_workspace_retirement_settled": {
                        "terminal_token": 7,
                        "cleanup_complete": True,
                        "permanent": True,
                        "backing_id": None,
                        "runtime_incarnation": None,
                        "snapshot_restore_required": False,
                    }
                },
            }
        )
        db = _make_db_with_conn(conn)

        assert await db.resume_thread("tid-1") is False
        assert conn.fetchrow.await_count == 1
        conn.fetchval.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_finish_soft_retirement_requires_old_runtime_uid_cleared(self):
        conn = _mock_conn()
        conn.fetchrow = AsyncMock(
            side_effect=[
                {
                    "status": "ended",
                    "execution_lane": "stateless",
                    "metadata": _proven_soft_retirement_metadata(retain_runtime=True),
                },
                {"unit_kind": "session_turn", "state": "done", "lease_token": 8},
            ]
        )
        db = _make_db_with_conn(conn)

        assert await db.finish_stateless_thread_workspace_retirement("tid-1") is False
        conn.fetchval.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_finish_soft_retirement_publishes_settled_proof_after_uid_clear(
        self,
    ):
        conn = _mock_conn()
        conn.fetchrow = AsyncMock(
            side_effect=[
                {
                    "status": "ended",
                    "execution_lane": "stateless",
                    "metadata": _proven_soft_retirement_metadata(retain_runtime=False),
                },
                {"unit_kind": "session_turn", "state": "done", "lease_token": 8},
                {"id": "tid-1", "kind": "session", "execution_lane": "stateless"},
            ]
        )
        conn.fetch = AsyncMock(return_value=[])
        conn.fetchval = AsyncMock(return_value="tid-1")
        db = _make_db_with_conn(conn)

        assert await db.finish_stateless_thread_workspace_retirement("tid-1") is True
        stored = json.loads(conn.fetchval.await_args.args[2])
        settled = stored["_stateless_workspace_retirement_settled"]
        assert settled["terminal_token"] == 8
        assert settled["snapshot_restore_required"] is True
        assert "_stateless_workspace_retirement_pending" not in stored
        # A resumable End keeps the PVC on purpose. Say so, rather than leaving
        # a row whose only workspace statement is the cleanup projection's
        # "deleted" — which is true of the pod and false of the volume.
        assert settled["permanent"] is False
        assert stored["workspace_container"]["volume_reclaimed"] is False

    @pytest.mark.asyncio
    async def test_finish_refuses_to_erase_in_progress_creation_authority(self):
        metadata = _proven_soft_retirement_metadata(retain_runtime=False)
        metadata["workspace_container"]["_runtime_creation"] = {
            "generation": "33333333-3333-4333-8333-333333333333",
            "mode": "restore",
            "attempted": True,
            "replaces_uid": None,
        }
        conn = _mock_conn()
        conn.fetchrow = AsyncMock(
            side_effect=[
                {
                    "status": "ended",
                    "execution_lane": "stateless",
                    "runtime_generation": None,
                    "runtime_retirement_token": None,
                    "metadata": metadata,
                },
                {"unit_kind": "session_turn", "state": "done", "lease_token": 8},
            ]
        )
        db = _make_db_with_conn(conn)

        assert not await db.finish_stateless_thread_workspace_retirement("tid-1")
        conn.fetchval.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("backing_id", "snapshot_required", "mode"),
        [
            ("k8s-pod:agent-workspaces:old-uid", True, "restore"),
            ("k8s-pvc:agent-workspaces:pvc-uid", False, "create"),
        ],
    )
    async def test_resume_prearms_fresh_physical_creation_authority(
        self, backing_id, snapshot_required, mode
    ):
        metadata = {
            "_stateless_workspace_retirement_settled": {
                "terminal_token": 8,
                "cleanup_complete": True,
                "permanent": False,
                "backing_id": backing_id,
                "runtime_incarnation": "22222222-2222-4222-8222-222222222222",
                "snapshot_restore_required": snapshot_required,
            },
            "workspace_container": {
                "status": "deleted",
                "provisioner": "k8s",
                "_runtime_incarnation": None,
                "_snapshot_restore_required": snapshot_required,
            },
        }
        conn = _mock_conn()
        conn.fetchrow = AsyncMock(
            side_effect=[
                {
                    "status": "ended",
                    "execution_lane": "stateless",
                    "runtime_generation": None,
                    "runtime_retirement_token": None,
                    "metadata": metadata,
                },
                {"unit_kind": "session_turn"},
            ]
        )
        # Resume rotates the generation, then binds the creation marker to that
        # exact generation in a second statement (0197/0198).
        resumed_generation = "33333333-3333-4333-8333-333333333333"
        conn.fetchval = AsyncMock(
            side_effect=[None, "tid-1", resumed_generation, "tid-1"]
        )
        db = _make_db_with_conn(conn)

        assert await db.resume_thread("tid-1")

        stored = json.loads(conn.fetchval.await_args_list[-1].args[2])
        assert "_stateless_workspace_retirement_settled" not in stored
        marker = stored["workspace_container"]["_runtime_creation"]
        assert marker["generation"] == resumed_generation
        assert marker["mode"] == mode
        assert marker["attempted"] is False
        assert marker["replaces_uid"] is None
        assert str(UUID(marker["generation"])) == marker["generation"]

    @pytest.mark.asyncio
    async def test_resume_refuses_settled_emptydir_with_stale_runtime_uid(self):
        metadata = {
            "_stateless_workspace_retirement_settled": {
                "terminal_token": 8,
                "cleanup_complete": True,
                "permanent": False,
                "backing_id": "k8s-pod:agent-workspaces:pod-uid",
                "runtime_incarnation": "old-runtime",
                "snapshot_restore_required": True,
            },
            "workspace_container": {
                "status": "deleted",
                "provisioner": "k8s",
                "_runtime_incarnation": "old-runtime",
                "_snapshot_restore_required": True,
            },
        }
        conn = _mock_conn()
        conn.fetchrow = AsyncMock(
            return_value={
                "status": "ended",
                "execution_lane": "stateless",
                "runtime_generation": None,
                "runtime_retirement_token": None,
                "metadata": metadata,
            }
        )
        db = _make_db_with_conn(conn)

        assert await db.resume_thread("tid-1") is False
        conn.fetchval.assert_not_awaited()


class TestInactiveThreadControlCapability:
    """Reapers nominate exact owners; the retirement funnel closes admission."""

    @pytest.mark.asyncio
    async def test_orphan_end_returns_exact_retirement_candidates(self):
        conn = _mock_conn()
        conn.fetch = AsyncMock(return_value=[])
        db = _make_db_with_conn(conn)

        await db.mark_orphaned_threads_ended()

        sql = " ".join(conn.fetch.call_args[0][0].split())
        assert "SELECT thread.id, thread.status, thread.runtime_generation" in sql
        assert "thread.agent_id, thread.runtime_attach_token" in sql
        assert "thread.status IN ('created', 'active')" in sql
        assert "thread.runtime_retirement_token IS NULL" in sql
        assert "agent.status = 'offline'" in sql
        assert "UPDATE threads" not in sql

    @pytest.mark.asyncio
    async def test_orphan_suspend_returns_exact_retirement_candidates(self):
        conn = _mock_conn()
        conn.fetch = AsyncMock(return_value=[])
        db = _make_db_with_conn(conn)

        await db.mark_orphaned_threads_suspended()

        sql = " ".join(conn.fetch.call_args[0][0].split())
        assert "SELECT thread.id, thread.status, thread.runtime_generation" in sql
        assert "thread.agent_id, thread.runtime_attach_token" in sql
        assert "thread.status IN ('awaiting_user', 'suspended')" in sql
        assert "thread.runtime_retirement_token IS NULL" in sql
        assert "agent.status = 'offline'" in sql
        assert "UPDATE threads" not in sql


# =============================================================================
# 6.5: update_thread_status
# =============================================================================


class TestUpdateThreadStatus:
    """Tests for update_thread_status method."""

    @pytest.mark.asyncio
    async def test_updates_status_and_last_activity(self):
        conn = _mock_conn()
        db = _make_db_with_conn(conn)

        await db.update_thread_status("tid-1", "idle")

        sql = " ".join(conn.execute.call_args[0][0].split())
        assert "status = $2::text" in sql
        assert "last_activity = CURRENT_TIMESTAMP" in sql
        # $2 is cast in every comparison: an uncast $2 next to the ::text one
        # made asyncpg's prepare refuse the statement (AmbiguousParameterError).
        assert "WHEN $2::text IN ('ended', 'suspended') THEN NULL" in sql
        assert conn.execute.call_args[0][1] == "tid-1"
        assert conn.execute.call_args[0][2] == "idle"


class TestAgentDeletionControlCapability:
    """Agent-row deletion cannot strand its non-FK control credential."""

    @staticmethod
    def _transactional_conn(*responses: str):
        conn = _mock_conn()
        conn.execute = AsyncMock(side_effect=responses)
        tx = AsyncMock()
        tx.__aenter__.return_value = None
        tx.__aexit__.return_value = False
        conn.transaction = MagicMock(return_value=tx)
        return conn

    @pytest.mark.asyncio
    async def test_delete_agent_closes_exact_capability_before_delete(self):
        conn = self._transactional_conn("UPDATE 1", "DELETE 1")
        conn.fetchval = AsyncMock(return_value=False)
        db = _make_db_with_conn(conn)

        deleted = await db.delete_agent("11111111-1111-4111-8111-111111111111")

        assert deleted is True
        assert "SELECT EXISTS" in conn.fetchval.await_args.args[0]
        assert conn.execute.await_count == 2
        assert (
            "control_admission_agent_id = NULL"
            in conn.execute.await_args_list[0].args[0]
        )
        assert (
            "agent_id = $1 OR control_admission_agent_id = $1"
            in (conn.execute.await_args_list[0].args[0])
        )
        assert "DELETE FROM agents" in conn.execute.await_args_list[1].args[0]

    @pytest.mark.asyncio
    async def test_offline_gc_closes_capabilities_in_same_transaction(self):
        conn = self._transactional_conn("UPDATE 2", "DELETE 2")
        conn.fetch = AsyncMock(
            return_value=[
                {"id": UUID("11111111-1111-4111-8111-111111111111")},
                {"id": UUID("22222222-2222-4222-8222-222222222222")},
            ]
        )
        db = _make_db_with_conn(conn)

        deleted = await db.gc_offline_agents(retention_hours=24)

        assert deleted == 2
        assert conn.execute.await_count == 2
        close_sql = conn.execute.await_args_list[0].args[0]
        delete_sql = conn.execute.await_args_list[1].args[0]
        assert "control_admission_agent_id = NULL" in close_sql
        assert "agent_id = ANY($1::uuid[])" in close_sql
        assert "ANY($1::uuid[])" in close_sql
        assert "DELETE FROM agents" in delete_sql
        assert "status = 'offline'" in delete_sql


# =============================================================================
# 6.6: update_thread_agent
# =============================================================================


class TestUpdateThreadAgent:
    """Tests for update_thread_agent method."""

    @pytest.mark.asyncio
    async def test_binds_agent_id(self):
        conn = _mock_conn()
        db = _make_db_with_conn(conn)

        await db.update_thread_agent("tid-1", "agent-42")

        sql = " ".join(conn.execute.call_args[0][0].split())
        assert "agent_id = $2" in sql
        assert conn.execute.call_args[0][1] == "tid-1"
        assert conn.execute.call_args[0][2] == "agent-42"


# =============================================================================
# 6.7: save_thread_message
# =============================================================================


class TestSaveThreadMessage:
    """Tests for save_thread_message method."""

    @pytest.mark.asyncio
    async def test_rejects_legacy_writer_for_stateless_thread(self):
        conn = _mock_conn()
        conn.fetchrow = AsyncMock(return_value={"execution_lane": "stateless"})
        db = _make_db_with_conn(conn)

        with pytest.raises(
            RuntimeError,
            match="legacy message writer is unavailable for stateless threads",
        ):
            await db.save_thread_message("tid-1", "user", "hello")

        assert conn.fetchrow.await_count == 1

    @pytest.mark.asyncio
    async def test_returns_uuid_string(self):
        conn = _mock_conn()
        conn.fetchrow = AsyncMock(
            return_value={"id": UUID("bbbbbbbb-1111-2222-3333-444444444444")}
        )
        db = _make_db_with_conn(conn)

        result = await db.save_thread_message("tid-1", "user", "hello")
        assert result == "bbbbbbbb-1111-2222-3333-444444444444"

    @pytest.mark.asyncio
    async def test_tool_calls_serialized_to_json(self):
        conn = _mock_conn()
        conn.fetchrow = AsyncMock(
            return_value={"id": UUID("bbbbbbbb-1111-2222-3333-444444444444")}
        )
        db = _make_db_with_conn(conn)

        tool_calls = [{"name": "search", "args": {"q": "test"}}]
        await db.save_thread_message("tid-1", "assistant", None, tool_calls=tool_calls)

        insert_args = conn.fetchrow.call_args[0]
        # tool_calls is 4th positional param after SQL
        serialized = insert_args[4]
        assert json.loads(serialized) == tool_calls

    @pytest.mark.asyncio
    async def test_tool_calls_none_becomes_sql_null(self):
        conn = _mock_conn()
        conn.fetchrow = AsyncMock(
            return_value={"id": UUID("bbbbbbbb-1111-2222-3333-444444444444")}
        )
        db = _make_db_with_conn(conn)

        await db.save_thread_message("tid-1", "user", "hi", tool_calls=None)

        insert_args = conn.fetchrow.call_args[0]
        assert insert_args[4] is None  # tool_calls

    @pytest.mark.asyncio
    async def test_updates_thread_last_activity_and_turns(self):
        conn = _mock_conn()
        conn.fetchrow = AsyncMock(
            return_value={"id": UUID("bbbbbbbb-1111-2222-3333-444444444444")}
        )
        db = _make_db_with_conn(conn)

        await db.save_thread_message("tid-1", "user", "hi", turn_number=5)

        # Second call should be the UPDATE
        update_call = conn.execute.call_args
        sql = " ".join(update_call[0][0].split())
        assert "last_activity = CURRENT_TIMESTAMP" in sql
        assert "GREATEST(total_turns" in sql
        assert update_call[0][2] == 5  # turn_number


# =============================================================================
# 6.8: get_thread_messages_history
# =============================================================================


class TestGetThreadMessagesHistory:
    """Tests for get_thread_messages_history method."""

    @pytest.mark.asyncio
    async def test_returns_messages_asc_order(self):
        conn = _mock_conn()
        conn.fetch = AsyncMock(return_value=[])
        db = _make_db_with_conn(conn)

        await db.get_thread_messages_history("tid-1")
        sql = " ".join(conn.fetch.call_args[0][0].split())
        assert "ORDER BY created_at ASC" in sql

    @pytest.mark.asyncio
    async def test_supports_limit_and_offset(self):
        conn = _mock_conn()
        conn.fetch = AsyncMock(return_value=[])
        db = _make_db_with_conn(conn)

        await db.get_thread_messages_history("tid-1", limit=50, offset=100)
        args = conn.fetch.call_args[0]
        assert args[2] == 50  # limit
        assert args[3] == 100  # offset

    @pytest.mark.asyncio
    async def test_deserializes_tool_calls_json(self):
        ts = datetime(2026, 3, 30, 10, 0, 0, tzinfo=timezone.utc)
        row = {
            "id": UUID("cccccccc-1111-2222-3333-444444444444"),
            "role": "assistant",
            "content": None,
            "tool_calls": '[{"name": "search"}]',
            "turn_number": 1,
            "metrics": None,
            "tool_call_id": None,
            "thinking": None,
            "created_at": ts,
        }
        conn = _mock_conn()
        conn.fetch = AsyncMock(return_value=[row])
        db = _make_db_with_conn(conn)

        result = await db.get_thread_messages_history("tid-1")
        assert result[0]["tool_calls"] == [{"name": "search"}]

    @pytest.mark.asyncio
    async def test_null_tool_calls_becomes_none(self):
        row = {
            "id": UUID("cccccccc-1111-2222-3333-444444444444"),
            "role": "user",
            "content": "hi",
            "tool_calls": None,
            "turn_number": 1,
            "metrics": None,
            "tool_call_id": None,
            "thinking": None,
            "created_at": datetime(2026, 3, 30, tzinfo=timezone.utc),
        }
        conn = _mock_conn()
        conn.fetch = AsyncMock(return_value=[row])
        db = _make_db_with_conn(conn)

        result = await db.get_thread_messages_history("tid-1")
        assert result[0]["tool_calls"] is None

    @pytest.mark.asyncio
    async def test_formats_created_at_as_iso_string(self):
        ts = datetime(2026, 3, 30, 10, 0, 0, tzinfo=timezone.utc)
        row = {
            "id": UUID("cccccccc-1111-2222-3333-444444444444"),
            "role": "user",
            "content": "hi",
            "tool_calls": None,
            "turn_number": 1,
            "metrics": None,
            "tool_call_id": None,
            "thinking": None,
            "created_at": ts,
        }
        conn = _mock_conn()
        conn.fetch = AsyncMock(return_value=[row])
        db = _make_db_with_conn(conn)

        result = await db.get_thread_messages_history("tid-1")
        assert result[0]["created_at"] == ts.isoformat()

    @pytest.mark.asyncio
    async def test_null_created_at_becomes_none(self):
        row = {
            "id": UUID("cccccccc-1111-2222-3333-444444444444"),
            "role": "user",
            "content": "hi",
            "tool_calls": None,
            "turn_number": 1,
            "metrics": None,
            "tool_call_id": None,
            "thinking": None,
            "created_at": None,
        }
        conn = _mock_conn()
        conn.fetch = AsyncMock(return_value=[row])
        db = _make_db_with_conn(conn)

        result = await db.get_thread_messages_history("tid-1")
        assert result[0]["created_at"] is None

    @pytest.mark.asyncio
    async def test_result_dict_keys(self):
        row = {
            "id": UUID("cccccccc-1111-2222-3333-444444444444"),
            "role": "user",
            "content": "hi",
            "tool_calls": None,
            "turn_number": 3,
            "metrics": None,
            "tool_call_id": None,
            "thinking": None,
            "created_at": datetime(2026, 3, 30, tzinfo=timezone.utc),
        }
        conn = _mock_conn()
        conn.fetch = AsyncMock(return_value=[row])
        db = _make_db_with_conn(conn)

        result = await db.get_thread_messages_history("tid-1")
        assert set(result[0].keys()) == {
            "id",
            "role",
            "content",
            "tool_calls",
            "turn_number",
            "metrics",
            "tool_call_id",
            "thinking",
            "created_at",
        }

    @pytest.mark.asyncio
    async def test_full_load_omits_limit_clause(self):
        """No limit ⇒ load the entire conversation (the Bug #1 fix)."""
        conn = _mock_conn()
        conn.fetch = AsyncMock(return_value=[])
        db = _make_db_with_conn(conn)

        await db.get_thread_messages_history("tid-1")
        sql = " ".join(conn.fetch.call_args[0][0].split())
        assert "LIMIT" not in sql
        # query + thread_id only — no limit/offset bound params
        assert len(conn.fetch.call_args[0]) == 2


# =============================================================================
# get_thread_messages_page (cursor paging)
# =============================================================================


class TestGetThreadMessagesPage:
    """Tests for the cursor-paged get_thread_messages_page method."""

    def _row(self, mid: str, ts):
        return {
            "id": UUID(mid),
            "role": "user",
            "content": "hi",
            "tool_calls": None,
            "turn_number": 1,
            "metrics": None,
            "tool_call_id": None,
            "thinking": None,
            "created_at": ts,
        }

    @pytest.mark.asyncio
    async def test_before_inclusive_desc_reversed_to_asc(self):
        t_old = datetime(2026, 3, 30, 10, 0, 0, tzinfo=timezone.utc)
        t_new = datetime(2026, 3, 30, 11, 0, 0, tzinfo=timezone.utc)
        conn = _mock_conn()
        # DB returns DESC (newest first); the method must reverse to ascending.
        conn.fetch = AsyncMock(
            return_value=[
                self._row("cccccccc-1111-2222-3333-444444444401", t_new),
                self._row("cccccccc-1111-2222-3333-444444444402", t_old),
            ]
        )
        db = _make_db_with_conn(conn)

        cursor = datetime(2026, 3, 30, 12, 0, 0, tzinfo=timezone.utc)
        messages, has_more = await db.get_thread_messages_page(
            "tid-1", before=cursor, limit=50
        )
        sql = " ".join(conn.fetch.call_args[0][0].split())
        assert "created_at <= $2" in sql
        assert "ORDER BY created_at DESC" in sql
        assert has_more is False
        assert messages[0]["created_at"] == t_old.isoformat()
        assert messages[1]["created_at"] == t_new.isoformat()

    @pytest.mark.asyncio
    async def test_before_has_more_drops_probe_row(self):
        ts = datetime(2026, 3, 30, 10, 0, 0, tzinfo=timezone.utc)
        rows = [
            self._row("cccccccc-0000-0000-0000-00000000000%d" % i, ts) for i in range(3)
        ]
        conn = _mock_conn()
        conn.fetch = AsyncMock(return_value=rows)
        db = _make_db_with_conn(conn)

        messages, has_more = await db.get_thread_messages_page(
            "tid-1", before=ts, limit=2
        )
        sql = " ".join(conn.fetch.call_args[0][0].split())
        assert "LIMIT $3" in sql  # limit + 1 probe
        assert conn.fetch.call_args[0][3] == 3  # the limit+1 bound
        assert has_more is True
        assert len(messages) == 2

    @pytest.mark.asyncio
    async def test_after_inclusive_asc_not_reversed(self):
        t_a = datetime(2026, 3, 30, 10, 0, 0, tzinfo=timezone.utc)
        t_b = datetime(2026, 3, 30, 11, 0, 0, tzinfo=timezone.utc)
        conn = _mock_conn()
        conn.fetch = AsyncMock(
            return_value=[
                self._row("cccccccc-1111-2222-3333-44444444440a", t_a),
                self._row("cccccccc-1111-2222-3333-44444444440b", t_b),
            ]
        )
        db = _make_db_with_conn(conn)

        messages, has_more = await db.get_thread_messages_page(
            "tid-1", after=t_a, limit=50
        )
        sql = " ".join(conn.fetch.call_args[0][0].split())
        assert "created_at >= $2" in sql
        assert "ORDER BY created_at ASC" in sql
        assert has_more is False
        assert messages[0]["created_at"] == t_a.isoformat()
        assert messages[1]["created_at"] == t_b.isoformat()


# =============================================================================
# 6.9: get_thread_message_count
# =============================================================================


class TestGetThreadMessageCount:
    """Tests for get_thread_message_count method."""

    @pytest.mark.asyncio
    async def test_returns_integer_count(self):
        conn = _mock_conn()
        conn.fetchval = AsyncMock(return_value=42)
        db = _make_db_with_conn(conn)

        result = await db.get_thread_message_count("tid-1")
        assert result == 42

    @pytest.mark.asyncio
    async def test_returns_zero_for_empty(self):
        conn = _mock_conn()
        conn.fetchval = AsyncMock(return_value=0)
        db = _make_db_with_conn(conn)

        result = await db.get_thread_message_count("tid-1")
        assert result == 0


# =============================================================================
# 6.10: update_thread_tokens
# =============================================================================


class TestUpdateThreadTokens:
    """Tests for update_thread_tokens method."""

    @pytest.mark.asyncio
    async def test_increments_tokens(self):
        conn = _mock_conn()
        db = _make_db_with_conn(conn)

        await db.update_thread_tokens("tid-1", 1500)

        sql = " ".join(conn.execute.call_args[0][0].split())
        assert "total_tokens = total_tokens + $2" in sql
        assert conn.execute.call_args[0][1] == "tid-1"
        assert conn.execute.call_args[0][2] == 1500


# =============================================================================
# 6.11: merge_thread_workspace_context / merge_thread_vm_context
# =============================================================================


class TestMergeThreadWorkspaceContext:
    """Tests for merge_thread_workspace_context method."""

    @pytest.mark.asyncio
    async def test_returns_true_on_update(self):
        conn = _mock_conn()
        conn.execute = AsyncMock(return_value="UPDATE 1")
        db = _make_db_with_conn(conn)

        result = await db.merge_thread_workspace_context(
            "aaaaaaaa-1111-2222-3333-444444444444",
            {"pod_ip": "10.0.0.5"},
        )
        assert result is True

    @pytest.mark.asyncio
    async def test_returns_false_when_not_found(self):
        conn = _mock_conn()
        conn.execute = AsyncMock(return_value="UPDATE 0")
        db = _make_db_with_conn(conn)

        result = await db.merge_thread_workspace_context(
            "aaaaaaaa-1111-2222-3333-444444444444",
            {"pod_ip": "10.0.0.5"},
        )
        assert result is False

    @pytest.mark.asyncio
    async def test_returns_false_for_invalid_uuid(self):
        conn = _mock_conn()
        db = _make_db_with_conn(conn)

        result = await db.merge_thread_workspace_context(
            "not-a-uuid", {"pod_ip": "10.0.0.5"}
        )
        assert result is False
        conn.execute.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_merges_into_workspace_container_path(self):
        conn = _mock_conn()
        conn.execute = AsyncMock(return_value="UPDATE 1")
        db = _make_db_with_conn(conn)

        await db.merge_thread_workspace_context(
            "aaaaaaaa-1111-2222-3333-444444444444",
            {"status": "ready"},
        )
        sql = conn.execute.call_args[0][0]
        assert "workspace_container" in sql
        # First param is the JSON-serialized updates
        json_param = conn.execute.call_args[0][1]
        assert json.loads(json_param) == {"status": "ready"}


class TestStatelessWorkspaceProcessZeroObservation:
    THREAD_ID = "aaaaaaaa-1111-4222-8333-444444444444"
    RUNTIME = "22222222-2222-4222-8222-222222222222"

    @staticmethod
    def _row(*, runtime: str, observation=None):
        metadata = {
            "workspace_container": {
                "provisioner": "k8s",
                "_runtime_incarnation": runtime,
            }
        }
        if observation is not None:
            metadata["_stateless_workspace_process_zero_observation"] = observation
        return {"execution_lane": "stateless", "metadata": metadata}

    @pytest.mark.asyncio
    async def test_record_is_exact_uid_bound_and_idempotent(self):
        conn = _mock_conn()
        conn.fetchrow = AsyncMock(return_value=self._row(runtime=self.RUNTIME))
        conn.execute = AsyncMock(side_effect=["UPDATE 1", "INSERT 0 1"])
        db = _make_db_with_conn(conn)

        assert await db.record_stateless_thread_workspace_process_zero(
            self.THREAD_ID,
            runtime_incarnation=self.RUNTIME,
        )
        payload = json.loads(conn.execute.await_args_list[0].args[2])
        assert payload["workspace_container"]["status"] == "retiring_process_zero"
        receipt_insert = conn.execute.await_args_list[1]
        assert receipt_insert.args[2] == self.RUNTIME

        conn.reset_mock()
        conn.fetchrow = AsyncMock(return_value=self._row(runtime=self.RUNTIME))
        conn.execute = AsyncMock(side_effect=["UPDATE 1", "INSERT 0 0"])
        assert await db.record_stateless_thread_workspace_process_zero(
            self.THREAD_ID,
            runtime_incarnation=self.RUNTIME,
        )
        assert conn.execute.await_count == 2

    @pytest.mark.asyncio
    async def test_record_refuses_runtime_drift(self):
        conn = _mock_conn()
        conn.fetchrow = AsyncMock(
            return_value=self._row(runtime="33333333-3333-4333-8333-333333333333")
        )
        db = _make_db_with_conn(conn)

        assert not await db.record_stateless_thread_workspace_process_zero(
            self.THREAD_ID,
            runtime_incarnation=self.RUNTIME,
        )
        conn.execute.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_read_requires_receipt_and_current_runtime_match(self):
        receipt = {
            "runtime_incarnation": self.RUNTIME,
            "observed_at": datetime.now(timezone.utc).isoformat(),
        }
        conn = _mock_conn()
        conn.fetchrow = AsyncMock(
            return_value=self._row(runtime=self.RUNTIME, observation=receipt)
        )
        conn.fetchval = AsyncMock(
            side_effect=lambda _query, _thread_id, runtime: runtime == self.RUNTIME
        )
        db = _make_db_with_conn(conn)

        assert (
            await db.get_stateless_thread_workspace_process_zero(
                self.THREAD_ID,
                expected_runtime_incarnation=self.RUNTIME,
            )
            == self.RUNTIME
        )

        conn.fetchrow = AsyncMock(
            return_value=self._row(
                runtime="33333333-3333-4333-8333-333333333333",
                observation=receipt,
            )
        )
        assert (
            await db.get_stateless_thread_workspace_process_zero(self.THREAD_ID) is None
        )


class TestMergeThreadVmContext:
    """Tests for merge_thread_vm_context method."""

    @pytest.mark.asyncio
    async def test_returns_true_on_update(self):
        conn = _mock_conn()
        conn.execute = AsyncMock(return_value="UPDATE 1")
        db = _make_db_with_conn(conn)

        result = await db.merge_thread_vm_context(
            "aaaaaaaa-1111-2222-3333-444444444444",
            {"status": "ready", "ssh_host": "10.0.0.99"},
        )
        assert result is True

    @pytest.mark.asyncio
    async def test_returns_false_for_invalid_uuid(self):
        conn = _mock_conn()
        db = _make_db_with_conn(conn)

        result = await db.merge_thread_vm_context("bad-uuid", {"status": "ready"})
        assert result is False

    @pytest.mark.asyncio
    async def test_merges_into_vm_path(self):
        conn = _mock_conn()
        conn.execute = AsyncMock(return_value="UPDATE 1")
        db = _make_db_with_conn(conn)

        await db.merge_thread_vm_context(
            "aaaaaaaa-1111-2222-3333-444444444444",
            {"ssh_port": 2222},
        )
        sql = conn.execute.call_args[0][0]
        assert "'{vm}'" in sql
        json_param = conn.execute.call_args[0][1]
        assert json.loads(json_param) == {"ssh_port": 2222}
