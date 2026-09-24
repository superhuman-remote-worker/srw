"""R1.B12 lane A — characterization of the live thread-config commit core.

``apply_thread_config_update_locked`` is the validate → authorize → enrich →
persist → audit core shared by the internal agent PATCH and the owner-facing
PATCH. It runs inside ``thread_configuration_transaction`` after the
``SELECT … FOR UPDATE`` read and the managed-runtime generation fence.

This suite pinned what the core did while it still lived in
``orchestrator.main`` — status codes and detail payloads of every refusal,
the exact collaborator calls and their order, what is persisted (redacted)
versus returned (enriched), and that a refusal persists and audits nothing —
and runs unchanged against the moved function in
``services/thread_config_update``.

Only the ``apply_locked`` fixture knows where the core lives and how its
collaborators reach it; it was the one fixture switched by the move. The
pure policy helpers the core uses (tool/officer/delegation
validators, the strict protected marker, the stateless workspace gate,
redaction, the change summary) run for real; everything with an effect is a
fake that records into one ordered ledger.
"""

from __future__ import annotations

import contextlib
import functools
import json
import uuid
from dataclasses import dataclass
from typing import Any, Awaitable, Callable

import pytest
from fastapi import HTTPException

from orchestrator.database.postgres import DatasourcePolicyConflictError
from orchestrator.services import thread_config_update as tcu
from shared.credential_connectors import CredentialConnectorAttachedError

pytestmark = pytest.mark.asyncio

THREAD = "11111111-2222-4333-8444-555555555555"
OWNER = "aaaaaaaa-1111-4111-8111-111111111111"
PROJECT = "99999999-9999-4999-8999-999999999999"
LINKED_PROJECT = "88888888-8888-4888-8888-888888888888"
OTHER_THREAD = "77777777-7777-4777-8777-777777777777"
DS_A = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa1"
DS_B = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbb2"
DS_C = "cccccccc-cccc-4ccc-8ccc-ccccccccccc3"

REQUEST = object()
ACTOR = {"id": OWNER, "email": "owner@example.test", "is_admin": False}
OWNER_ROW = {"id": OWNER, "is_admin": False, "is_approved": True}
RESOLVED_KEYS = {"openai": "sk-resolved"}
PROVENANCE = {"origin": "explicit", "stamp": "fake-provenance"}
FLIP = {"tools": {"sql": ["sql_query", "sql_execute"]}}
GENERIC_403 = "One or more selected connectors are unavailable"
THREAD_NOT_FOUND = "Thread not found"


# --------------------------------------------------------------------------- #
# Recording fakes
# --------------------------------------------------------------------------- #


def _snapshot(value: Any) -> Any:
    """Copy containers so a call records what the collaborator SAW then.

    The core mutates ``config_override`` after handing it to collaborators
    (the grant check receives the fragment itself when no connector is
    selected, and enrichment replaces ``llm`` afterwards). Non-container
    objects keep their identity so ``request``/``store``/``conn`` can be
    asserted with ``is``.
    """
    if isinstance(value, dict):
        return {key: _snapshot(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_snapshot(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_snapshot(item) for item in value)
    if isinstance(value, set):
        return set(value)
    return value


class Fakes:
    """Every effectful collaborator of the commit core, in one ledger.

    ``results[name]`` is what a call returns, ``effects[name]`` (when set)
    computes it from the real arguments, and ``errors[name]`` makes it raise.
    Calls land in ``calls`` as ``(name, args, kwargs)`` snapshots.
    """

    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []
        self.results: dict[str, Any] = {
            "store.get_project_officer": None,
            "store.get_datasource_policy_rows": [],
            "store.get_user": dict(OWNER_ROW),
            "store.resolve_datasources_for_thread": [
                {"type": "postgresql", "name": "PG"}
            ],
            "store.resolve_api_keys_for_job": dict(RESOLVED_KEYS),
            "store.merge_thread_config_override": True,
            "store.set_thread_datasource_ids": True,
            "store.refresh_session_execution": {"delivery_override": {}},
            "conn.fetchrow": None,
            "thread_project_ids": [PROJECT, LINKED_PROJECT],
            "build_datasource_tool_override": FLIP,
            "datasource_selection_provenance": PROVENANCE,
            "enforce_session_create_grants": None,
            "inject_model_credentials": None,
            "log_security_event": None,
            "transaction.enter": None,
            "transaction.exit": None,
        }
        self.effects: dict[str, Callable[..., Any]] = {
            # Authorization answers with the canonical selection and one
            # policy revision per connector, like the real service.
            "authorize_thread_datasource_selection": (
                lambda _user, ids, **_kw: (
                    [str(uuid.UUID(str(v))) for v in ids],
                    {str(uuid.UUID(str(v))): 3 for v in ids},
                )
            ),
        }
        self.errors: dict[str, BaseException] = {}
        self.conn = _Conn(self)
        self.store = _Store(self)

    # -- ledger ------------------------------------------------------------ #

    def invoke(self, name: str, args: tuple[Any, ...], kwargs: dict[str, Any]):
        self.calls.append((name, _snapshot(args), _snapshot(kwargs)))
        if name in self.errors:
            raise self.errors[name]
        if name in self.effects:
            return self.effects[name](*args, **kwargs)
        return self.results[name]

    def names(self) -> list[str]:
        return [name for name, _args, _kwargs in self.calls]

    def only(self, name: str) -> tuple[tuple[Any, ...], dict[str, Any]]:
        matches = [(a, k) for n, a, k in self.calls if n == name]
        assert len(matches) == 1, (name, self.names())
        return matches[0]

    # -- collaborator callables ------------------------------------------- #

    async def thread_project_ids(self, *args: Any, **kwargs: Any) -> Any:
        return self.invoke("thread_project_ids", args, kwargs)

    async def authorize_thread_datasource_selection(
        self, *args: Any, **kwargs: Any
    ) -> Any:
        return self.invoke("authorize_thread_datasource_selection", args, kwargs)

    def build_datasource_tool_override(self, *args: Any, **kwargs: Any) -> Any:
        return self.invoke("build_datasource_tool_override", args, kwargs)

    async def datasource_selection_provenance(self, *args: Any, **kwargs: Any) -> Any:
        return self.invoke("datasource_selection_provenance", args, kwargs)

    async def enforce_session_create_grants(self, *args: Any, **kwargs: Any) -> Any:
        return self.invoke("enforce_session_create_grants", args, kwargs)

    async def inject_model_credentials(self, *args: Any, **kwargs: Any) -> Any:
        return self.invoke("inject_model_credentials", args, kwargs)

    async def log_security_event(self, *args: Any, **kwargs: Any) -> Any:
        return self.invoke("log_security_event", args, kwargs)


class _Conn:
    def __init__(self, fakes: Fakes) -> None:
        self._fakes = fakes

    async def fetchrow(self, *args: Any, **kwargs: Any) -> Any:
        return self._fakes.invoke("conn.fetchrow", args, kwargs)


class _Store:
    """Only the store methods the core (and, for the ordering test, the
    transaction wrapper around it) may use — any other call is an
    ``AttributeError``."""

    def __init__(self, fakes: Fakes) -> None:
        self._fakes = fakes

    async def get_project_officer(self, *args: Any, **kwargs: Any) -> Any:
        return self._fakes.invoke("store.get_project_officer", args, kwargs)

    async def get_datasource_policy_rows(self, *args: Any, **kwargs: Any) -> Any:
        return self._fakes.invoke("store.get_datasource_policy_rows", args, kwargs)

    async def get_user(self, *args: Any, **kwargs: Any) -> Any:
        return self._fakes.invoke("store.get_user", args, kwargs)

    async def resolve_datasources_for_thread(self, *args: Any, **kwargs: Any) -> Any:
        return self._fakes.invoke("store.resolve_datasources_for_thread", args, kwargs)

    async def resolve_api_keys_for_job(self, *args: Any, **kwargs: Any) -> Any:
        return self._fakes.invoke("store.resolve_api_keys_for_job", args, kwargs)

    async def merge_thread_config_override(self, *args: Any, **kwargs: Any) -> Any:
        return self._fakes.invoke("store.merge_thread_config_override", args, kwargs)

    async def set_thread_datasource_ids(self, *args: Any, **kwargs: Any) -> Any:
        return self._fakes.invoke("store.set_thread_datasource_ids", args, kwargs)

    async def refresh_session_execution(self, *args: Any, **kwargs: Any) -> Any:
        return self._fakes.invoke("store.refresh_session_execution", args, kwargs)

    def thread_configuration_transaction(self, thread_id: str) -> Any:
        fakes = self._fakes

        @contextlib.asynccontextmanager
        async def _scope():
            fakes.invoke("transaction.enter", (thread_id,), {})
            try:
                yield fakes.conn
            finally:
                fakes.invoke("transaction.exit", (thread_id,), {})

        return _scope()


async def _never(*_args: Any, **_kwargs: Any) -> Any:
    raise AssertionError("not a collaborator of the live thread-config commit")


@dataclass(frozen=True)
class BoundCore:
    """The commit core bound to one set of fakes.

    Calling it has the core's signature. ``dependencies`` is the
    ``ThreadConfigUpdateDependencies`` the transaction wrapper
    (``apply_thread_config_update``) needs to reach the same core.
    """

    core: Callable[..., Awaitable[tuple[dict[str, Any], list[str] | None]]]
    dependencies: tcu.ThreadConfigUpdateDependencies

    async def __call__(
        self,
        thread_id: str,
        thread_row: dict[str, Any] | None,
        config_override: dict[str, Any],
        datasource_ids: list[str] | None,
        *,
        request: Any,
        actor: dict[str, Any] | None,
    ) -> tuple[dict[str, Any], list[str] | None]:
        return await self.core(
            thread_id,
            thread_row,
            config_override,
            datasource_ids,
            request=request,
            actor=actor,
        )


# --------------------------------------------------------------------------- #
# THE seam: the only place that knows where the core lives.
# --------------------------------------------------------------------------- #


@pytest.fixture
def apply_locked() -> Callable[[Fakes], BoundCore]:
    """Bind ``fakes`` to the commit core, wherever it lives.

    R1.B12: the core is ``thread_config_update.apply_thread_config_update_locked``
    and every effectful collaborator arrives through
    ``ThreadConfigUpdateDependencies``, so the fakes are the dependencies.
    """

    def bind(fakes: Fakes) -> BoundCore:
        dependencies = tcu.ThreadConfigUpdateDependencies(
            store=fakes.store,
            vm_provisioner=None,
            container_provisioner=None,
            recovery_store=None,
            enforce_workspace_upgrade_grants=_never,
            require_internal=_never,
            require_thread_owner=_never,
            thread_project_ids=fakes.thread_project_ids,
            authorize_thread_datasource_selection=(
                fakes.authorize_thread_datasource_selection
            ),
            build_datasource_tool_override=fakes.build_datasource_tool_override,
            datasource_selection_provenance=fakes.datasource_selection_provenance,
            enforce_session_create_grants=fakes.enforce_session_create_grants,
            inject_model_credentials=fakes.inject_model_credentials,
            log_security_event=fakes.log_security_event,
        )
        core = functools.partial(
            tcu.apply_thread_config_update_locked, dependencies=dependencies
        )
        return BoundCore(core, dependencies)

    return bind


@pytest.fixture
def fakes() -> Fakes:
    return Fakes()


@pytest.fixture
def apply(apply_locked, fakes) -> BoundCore:
    return apply_locked(fakes)


# --------------------------------------------------------------------------- #
# Rows
# --------------------------------------------------------------------------- #


def pinned_row(**over: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "id": THREAD,
        "user_id": OWNER,
        "project_id": None,
        "execution_lane": "pinned",
        "metadata": {"config_override": {"workspace": {"backend": "sandbox"}}},
    }
    row.update(over)
    return row


def stateless_row(backend: str = "virtual", **over: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "id": THREAD,
        "user_id": None,
        "project_id": None,
        "execution_lane": "stateless",
        "metadata": {
            "config_override": {
                "workspace": {"backend": backend},
                "officer": {"enabled": False, "conference": False},
            }
        },
    }
    row.update(over)
    return row


def with_selection(row: dict[str, Any], ids: list[str]) -> dict[str, Any]:
    metadata = dict(row["metadata"])
    metadata["datasource_ids"] = list(ids)
    return {**row, "metadata": metadata}


SELECTION_TAIL = [
    "store.resolve_datasources_for_thread",
    "build_datasource_tool_override",
    "datasource_selection_provenance",
]
PERSIST_AND_AUDIT = ["store.merge_thread_config_override", "log_security_event"]


async def _refused(apply: BoundCore, *args: Any, **kwargs: Any) -> HTTPException:
    with pytest.raises(HTTPException) as exc:
        await apply(*args, **kwargs)
    return exc.value


# --------------------------------------------------------------------------- #
# Protected-cloud marker
# --------------------------------------------------------------------------- #


class TestProtectedCloudMarker:
    @pytest.mark.parametrize(
        "fragment",
        [
            {"workspace": {"backend": "sandbox"}},
            {"workspace": {}},
            {"officer": {"enabled": False}},
            {"officer": {}, "llm": {"model": "m-1"}},
        ],
    )
    async def test_live_marker_refuses_runtime_class_blocks_before_any_effect(
        self, apply, fakes, fragment
    ):
        row = pinned_row(
            project_id=PROJECT,
            metadata={
                "protected_cloud": True,
                "config_override": {"workspace": {"backend": "sandbox"}},
                "datasource_ids": [DS_A],
            },
        )
        error = await _refused(
            apply, THREAD, row, fragment, [DS_B], request=REQUEST, actor=ACTOR
        )
        assert error.status_code == 409
        assert error.detail == {
            "code": "protected_cloud_runtime_class_fixed",
            "message": (
                "Protected cloud sessions cannot change workspace tier or Officer mode."
            ),
        }
        assert fakes.calls == []

    @pytest.mark.parametrize(
        "metadata",
        ["{not json", [], {"protected_cloud": 1}, {"protected_cloud": "true"}],
    )
    async def test_malformed_marker_refuses_every_edit_before_any_effect(
        self, apply, fakes, metadata
    ):
        error = await _refused(
            apply,
            THREAD,
            pinned_row(metadata=metadata),
            {"llm": {"temperature": 0.1}},
            None,
            request=REQUEST,
            actor=None,
        )
        assert error.status_code == 409
        assert error.detail == {
            "code": "protected_cloud_malformed",
            "message": "Protected cloud session state is invalid.",
        }
        assert fakes.calls == []

    async def test_live_marker_admits_ordinary_settings(self, apply, fakes):
        row = pinned_row(metadata={"protected_cloud": True})
        result = await apply(
            THREAD,
            row,
            {"llm": {"temperature": 0.2}},
            None,
            request=REQUEST,
            actor=None,
        )
        assert result == ({"llm": {"temperature": 0.2}}, None)
        assert fakes.names() == ["enforce_session_create_grants", *PERSIST_AND_AUDIT]

    async def test_explicit_false_marker_is_an_ordinary_pinned_row(self, apply, fakes):
        """The pinned lane has no tier gate here: a workspace block passes."""
        row = pinned_row(
            metadata={"protected_cloud": False, "config_override": {}},
        )
        result = await apply(
            THREAD,
            row,
            {"workspace": {"backend": "vm"}},
            None,
            request=REQUEST,
            actor=None,
        )
        assert result == ({"workspace": {"backend": "vm"}}, None)
        assert fakes.names() == ["enforce_session_create_grants", *PERSIST_AND_AUDIT]

    async def test_absent_row_is_unprotected(self, apply, fakes):
        result = await apply(
            THREAD,
            None,
            {"officer": {"enabled": True}},
            None,
            request=REQUEST,
            actor=None,
        )
        assert result == ({"officer": {"enabled": True}}, None)
        assert fakes.names() == PERSIST_AND_AUDIT


# --------------------------------------------------------------------------- #
# Officer Post ownership
# --------------------------------------------------------------------------- #


class TestOfficerPostOwnership:
    @pytest.mark.parametrize("post_thread", [THREAD, uuid.UUID(THREAD)])
    async def test_the_commissioned_officer_thread_cannot_edit_its_block(
        self, apply, fakes, post_thread
    ):
        fakes.results["store.get_project_officer"] = {"thread_id": post_thread}
        error = await _refused(
            apply,
            THREAD,
            pinned_row(project_id=uuid.UUID(PROJECT)),
            {"officer": {"enabled": True}},
            [DS_A],
            request=REQUEST,
            actor=ACTOR,
        )
        assert error.status_code == 409
        assert error.detail == (
            "The commissioned officer block is owned by the Officer Post; use "
            "the project Officer Post endpoint so durable and runtime "
            "configuration change atomically."
        )
        assert fakes.calls == [("store.get_project_officer", (PROJECT,), {})]

    @pytest.mark.parametrize(
        "post", [None, {}, {"thread_id": None}, {"thread_id": OTHER_THREAD}]
    )
    async def test_a_post_that_is_not_this_thread_does_not_own_the_block(
        self, apply, fakes, post
    ):
        """On the pinned lane the officer block is persisted as sent — the
        create-time validator only runs on the stateless lane."""
        fakes.results["store.get_project_officer"] = post
        result = await apply(
            THREAD,
            pinned_row(project_id=PROJECT),
            {"officer": {"enabled": "true", "bogus": 1}},
            None,
            request=REQUEST,
            actor=None,
        )
        assert result == ({"officer": {"enabled": "true", "bogus": 1}}, None)
        assert fakes.names() == [
            "store.get_project_officer",
            "enforce_session_create_grants",
            *PERSIST_AND_AUDIT,
        ]
        args, _ = fakes.only("store.merge_thread_config_override")
        assert args == (THREAD, {"officer": {"enabled": "true", "bogus": 1}})

    async def test_no_project_means_no_post_lookup(self, apply, fakes):
        await apply(
            THREAD,
            pinned_row(project_id=None),
            {"officer": {"enabled": False}},
            None,
            request=REQUEST,
            actor=None,
        )
        assert "store.get_project_officer" not in fakes.names()

    async def test_only_an_officer_block_consults_the_post(self, apply, fakes):
        await apply(
            THREAD,
            pinned_row(project_id=PROJECT),
            {"llm": {"temperature": 0.3}},
            None,
            request=REQUEST,
            actor=None,
        )
        assert "store.get_project_officer" not in fakes.names()


# --------------------------------------------------------------------------- #
# Stateless lane
# --------------------------------------------------------------------------- #


class TestStatelessLane:
    async def test_a_drifted_row_is_refused_before_any_effect(self, apply, fakes):
        error = await _refused(
            apply,
            THREAD,
            stateless_row(backend="vm"),
            {"llm": {"temperature": 0.1}},
            [DS_A],
            request=REQUEST,
            actor=None,
        )
        assert error.status_code == 409
        assert error.detail == (
            "Stateless execution requires an attested Kubernetes sandbox or a "
            "supported lite workspace (virtual/none); this session's workspace "
            "is unavailable (declared_backend_unsupported)"
        )
        assert fakes.calls == []

    async def test_the_officer_block_is_normalized_before_it_persists(
        self, apply, fakes
    ):
        result = await apply(
            THREAD,
            stateless_row(),
            {"officer": {"enabled": "false", "conference": 0}},
            None,
            request=REQUEST,
            actor=None,
        )
        normalized = {"officer": {"enabled": False, "conference": False}}
        assert result == (normalized, None)
        assert fakes.names() == PERSIST_AND_AUDIT
        assert fakes.only("store.merge_thread_config_override")[0] == (
            THREAD,
            normalized,
        )

    async def test_an_empty_officer_block_persists_as_an_empty_mapping(
        self, apply, fakes
    ):
        result = await apply(
            THREAD, stateless_row(), {"officer": {}}, None, request=REQUEST, actor=None
        )
        assert result == ({"officer": {}}, None)
        _, audit = fakes.only("log_security_event")
        assert audit["detail"] == "keys=officer"

    async def test_an_unknown_officer_key_is_400_before_any_effect(self, apply, fakes):
        error = await _refused(
            apply,
            THREAD,
            stateless_row(),
            {"officer": {"bogus": 1}},
            None,
            request=REQUEST,
            actor=None,
        )
        assert error.status_code == 400
        assert error.detail == "Unknown officer override keys: ['bogus']"
        assert fakes.calls == []

    @pytest.mark.parametrize(
        "fragment, reason",
        [
            (
                {"officer": {"enabled": True}},
                "officer sessions still use the pinned watchdog and wake drain",
            ),
            (
                {"officer": {"conference": "true"}},
                "conference sessions still use pinned lifecycle wakes",
            ),
        ],
    )
    async def test_a_pinned_only_session_class_is_refused(
        self, apply, fakes, fragment, reason
    ):
        error = await _refused(
            apply,
            THREAD,
            stateless_row(),
            fragment,
            None,
            request=REQUEST,
            actor=None,
        )
        assert error.status_code == 409
        assert error.detail == (
            f"A stateless session cannot enable pinned-only lifecycle behavior ({reason})"
        )
        assert fakes.calls == []

    @pytest.mark.parametrize(
        "workspace",
        [{"backend": "sandbox"}, {"backend": None}, "virtual", None, ["virtual"]],
    )
    async def test_a_workspace_tier_change_is_refused(self, apply, fakes, workspace):
        error = await _refused(
            apply,
            THREAD,
            stateless_row(),
            {"workspace": workspace},
            None,
            request=REQUEST,
            actor=None,
        )
        assert error.status_code == 409
        assert error.detail == (
            "A stateless session cannot change its workspace tier through "
            "generic config mutation"
        )
        assert fakes.calls == []

    @pytest.mark.parametrize(
        "workspace", [{"backend": "virtual"}, {}, {"idle_minutes": 5}]
    )
    async def test_same_tier_workspace_tuning_is_allowed(self, apply, fakes, workspace):
        result = await apply(
            THREAD,
            stateless_row(),
            {"workspace": workspace},
            None,
            request=REQUEST,
            actor=None,
        )
        assert result == ({"workspace": workspace}, None)
        assert fakes.names() == PERSIST_AND_AUDIT


# --------------------------------------------------------------------------- #
# tools / delegation validation
# --------------------------------------------------------------------------- #


class TestToolsAndDelegation:
    async def test_accepted_tool_groups_are_kept(self, apply, fakes):
        tools = {"delegation": ["delegate_agent"], "research": [], "sql": []}
        result = await apply(
            THREAD,
            pinned_row(user_id=None),
            {"tools": dict(tools)},
            None,
            request=REQUEST,
            actor=None,
        )
        assert result == ({"tools": tools}, None)
        assert fakes.only("store.merge_thread_config_override")[0] == (
            THREAD,
            {"tools": tools},
        )

    async def test_an_empty_tools_request_is_dropped(self, apply, fakes):
        result = await apply(
            THREAD,
            pinned_row(user_id=None),
            {"tools": {}},
            None,
            request=REQUEST,
            actor=None,
        )
        assert result == ({}, None)
        assert fakes.only("store.merge_thread_config_override")[0] == (THREAD, {})
        assert fakes.only("log_security_event")[1]["detail"] == "empty"

    @pytest.mark.parametrize(
        "tools, detail",
        [
            (
                {"canvas": ["run_command"]},
                "tools.canvas: 'run_command' is in tools.shell. A tool list may "
                "only name tools of its own category — the loader resolves a "
                "name against the global registry, not against the key it "
                "arrived under, so a foreign name would bind the foreign tool.",
            ),
            (
                "x",
                "tools: expected an object keyed by tool category; got str ('x').",
            ),
        ],
    )
    async def test_invalid_tools_are_a_400_before_any_effect(
        self, apply, fakes, tools, detail
    ):
        error = await _refused(
            apply,
            THREAD,
            pinned_row(),
            {"tools": tools, "delegation": "not-an-object"},
            [DS_A],
            request=REQUEST,
            actor=ACTOR,
        )
        assert error.status_code == 400
        assert error.detail == detail
        assert fakes.calls == []

    async def test_the_delegation_block_is_validated_and_legacy_keys_drop(
        self, apply, fakes
    ):
        result = await apply(
            THREAD,
            pinned_row(user_id=None),
            {"delegation": {"enabled": True, "mode": "light", "max_concurrent": 2}},
            None,
            request=REQUEST,
            actor=None,
        )
        accepted = {"delegation": {"enabled": True, "max_concurrent": 2}}
        assert result == (accepted, None)
        assert fakes.only("store.merge_thread_config_override")[0] == (
            THREAD,
            accepted,
        )

    @pytest.mark.parametrize("delegation", [{}, {"mode": "light", "max_depth": 2}])
    async def test_an_empty_delegation_block_is_dropped(self, apply, fakes, delegation):
        result = await apply(
            THREAD,
            pinned_row(user_id=None),
            {"delegation": delegation},
            None,
            request=REQUEST,
            actor=None,
        )
        assert result == ({}, None)
        assert fakes.only("log_security_event")[1]["detail"] == "empty"

    @pytest.mark.parametrize(
        "delegation, detail",
        [
            (
                {"enabled": "yes"},
                "config_override.delegation.enabled must be a boolean",
            ),
            ("on", "config_override.delegation must be an object"),
            (
                {"max_concurrent": 0},
                "config_override.delegation.max_concurrent must be a positive integer",
            ),
            (
                {"surprise": 1},
                "config_override.delegation.surprise is not a session delegation setting",
            ),
        ],
    )
    async def test_malformed_delegation_is_a_400_before_any_effect(
        self, apply, fakes, delegation, detail
    ):
        error = await _refused(
            apply,
            THREAD,
            pinned_row(),
            {"delegation": delegation},
            [DS_A],
            request=REQUEST,
            actor=ACTOR,
        )
        assert error.status_code == 400
        assert error.detail == detail
        assert fakes.calls == []


# --------------------------------------------------------------------------- #
# Live datasource selection
# --------------------------------------------------------------------------- #


class TestDatasourceSelection:
    async def test_a_selection_without_a_row_is_404_before_any_effect(
        self, apply, fakes
    ):
        error = await _refused(apply, THREAD, None, {}, [], request=REQUEST, actor=None)
        assert error.status_code == 404
        assert error.detail == THREAD_NOT_FOUND
        assert fakes.calls == []

    @pytest.mark.parametrize("ids", [["not-a-uuid"], [DS_A, "x"], [None]])
    async def test_a_non_uuid_selection_is_the_generic_403(self, apply, fakes, ids):
        error = await _refused(
            apply, THREAD, pinned_row(), {}, ids, request=REQUEST, actor=ACTOR
        )
        assert error.status_code == 403
        assert error.detail == GENERIC_403
        assert fakes.calls == []

    async def test_removing_a_credential_connector_is_409(self, apply, fakes):
        fakes.results["store.get_datasource_policy_rows"] = [
            {"id": DS_B, "type": "credentials"}
        ]
        error = await _refused(
            apply,
            THREAD,
            with_selection(pinned_row(), [DS_A, DS_B]),
            {},
            [DS_A],
            request=REQUEST,
            actor=ACTOR,
        )
        assert error.status_code == 409
        assert error.detail == (
            "Credential connectors stay attached for the lifetime of the session"
        )
        assert fakes.calls == [("store.get_datasource_policy_rows", ([DS_B],), {})]

    async def test_removing_an_ordinary_connector_proceeds(self, apply, fakes):
        fakes.results["store.get_datasource_policy_rows"] = [
            {"id": DS_B, "type": "postgresql"}
        ]
        await apply(
            THREAD,
            with_selection(pinned_row(), [DS_A, DS_B]),
            {},
            [DS_A],
            request=REQUEST,
            actor=ACTOR,
        )
        assert fakes.names() == [
            "store.get_datasource_policy_rows",
            "thread_project_ids",
            "store.get_user",
            "authorize_thread_datasource_selection",
            *SELECTION_TAIL,
            "enforce_session_create_grants",
            "store.merge_thread_config_override",
            "store.set_thread_datasource_ids",
            "log_security_event",
        ]

    async def test_an_additive_selection_does_not_consult_policy_rows(
        self, apply, fakes
    ):
        await apply(
            THREAD,
            with_selection(pinned_row(), [DS_A]),
            {},
            [DS_A, DS_B],
            request=REQUEST,
            actor=ACTOR,
        )
        assert "store.get_datasource_policy_rows" not in fakes.names()

    async def test_a_missing_owner_is_the_generic_403(self, apply, fakes):
        fakes.results["store.get_user"] = None
        error = await _refused(
            apply,
            THREAD,
            pinned_row(user_id=uuid.UUID(OWNER)),
            {"llm": {"model": "m-1"}},
            [DS_A],
            request=REQUEST,
            actor=ACTOR,
        )
        assert error.status_code == 403
        assert error.detail == GENERIC_403
        assert fakes.calls == [
            ("thread_project_ids", (THREAD,), {}),
            ("store.get_user", (OWNER,), {}),
        ]

    async def test_the_owner_path_authorizes_as_the_owner_against_the_current_backend(
        self, apply, fakes
    ):
        await apply(
            THREAD,
            pinned_row(user_id=uuid.UUID(OWNER), project_id=PROJECT),
            {},
            [DS_A.upper(), DS_B],
            request=REQUEST,
            actor=ACTOR,
        )
        args, kwargs = fakes.only("authorize_thread_datasource_selection")
        # The requested ids reach authorization as sent (str() only); the
        # canonical form is used only for the removal diff.
        assert args == (OWNER_ROW, [DS_A.upper(), DS_B])
        assert kwargs == {
            "workspace_backend": "sandbox",
            "target_project_ids": [PROJECT, LINKED_PROJECT],
            "effective_work_owner_id": OWNER,
        }
        assert fakes.only("thread_project_ids") == ((THREAD,), {})

    async def test_the_ownerless_path_may_narrow_its_materialized_selection(
        self, apply, fakes
    ):
        row = with_selection(pinned_row(user_id=None), [DS_A, DS_B])
        result = await apply(THREAD, row, {}, [DS_A], request=REQUEST, actor=None)
        assert fakes.names() == [
            "store.get_datasource_policy_rows",
            "thread_project_ids",
            "authorize_thread_datasource_selection",
            *SELECTION_TAIL,
            "store.merge_thread_config_override",
            "store.set_thread_datasource_ids",
            "log_security_event",
        ]
        args, kwargs = fakes.only("authorize_thread_datasource_selection")
        assert args == (None, [DS_A])
        assert kwargs == {
            "workspace_backend": "sandbox",
            "target_project_ids": [PROJECT, LINKED_PROJECT],
            "trusted_system_inheritance": True,
        }
        assert result == ({"tools": FLIP["tools"]}, [DS_A])

    async def test_the_ownerless_path_reads_json_string_metadata(self, apply, fakes):
        row = pinned_row(
            user_id=None,
            metadata=json.dumps(
                {
                    "config_override": {"workspace": {"backend": "virtual"}},
                    "datasource_ids": [DS_A],
                }
            ),
        )
        await apply(THREAD, row, {}, [DS_A], request=REQUEST, actor=None)
        args, kwargs = fakes.only("authorize_thread_datasource_selection")
        assert args == (None, [DS_A])
        assert kwargs["workspace_backend"] == "virtual"
        assert "store.get_datasource_policy_rows" not in fakes.names()

    @pytest.mark.parametrize(
        "persisted, requested",
        [([DS_A], [DS_C]), ([], [DS_A]), (["garbage"], [DS_A])],
    )
    async def test_the_ownerless_path_cannot_add_a_connector(
        self, apply, fakes, persisted, requested
    ):
        row = with_selection(pinned_row(user_id=None), persisted)
        error = await _refused(
            apply, THREAD, row, {}, requested, request=REQUEST, actor=None
        )
        assert error.status_code == 403
        assert error.detail == GENERIC_403
        expected = ["thread_project_ids"]
        if persisted:
            expected.insert(0, "store.get_datasource_policy_rows")
        assert fakes.names() == expected

    async def test_the_flip_wins_over_request_tools_in_the_grant_fragment(
        self, apply, fakes
    ):
        fakes.results["build_datasource_tool_override"] = {
            "tools": {"sql": ["sql_query", "sql_execute"]},
            "ignored": True,
        }
        fragment = {"tools": {"sql": [], "research": []}, "llm": {"temperature": 0.3}}
        result = await apply(
            THREAD,
            pinned_row(project_id=PROJECT),
            fragment,
            [DS_A],
            request=REQUEST,
            actor=ACTOR,
        )
        grant_fragment = {
            "tools": {"sql": ["sql_query", "sql_execute"], "research": []},
            "llm": {"temperature": 0.3},
        }
        assert fakes.only("enforce_session_create_grants") == (
            (grant_fragment,),
            {"user_id": OWNER, "project_ids": [PROJECT]},
        )
        assert fakes.only("store.resolve_datasources_for_thread") == (
            (),
            {"datasource_ids": [DS_A], "project_ids": [PROJECT, LINKED_PROJECT]},
        )
        assert fakes.only("build_datasource_tool_override") == (
            ([{"type": "postgresql", "name": "PG"}], None),
            {},
        )
        # The flip never persists; it rides only the returned fragment.
        assert fakes.only("store.merge_thread_config_override")[0] == (
            THREAD,
            {"tools": {"sql": [], "research": []}, "llm": {"temperature": 0.3}},
        )
        assert result == (grant_fragment, [DS_A])
        assert fragment == {
            "tools": {"sql": [], "research": []},
            "llm": {"temperature": 0.3},
        }

    async def test_a_selection_always_stamps_tools_on_the_returned_fragment(
        self, apply, fakes
    ):
        fakes.results["build_datasource_tool_override"] = {}
        result = await apply(
            THREAD,
            pinned_row(),
            {"llm": {"temperature": 0.3}},
            [],
            request=REQUEST,
            actor=ACTOR,
        )
        assert result == ({"llm": {"temperature": 0.3}, "tools": {}}, [])
        assert fakes.only("enforce_session_create_grants")[0] == (
            {"llm": {"temperature": 0.3}, "tools": {}},
        )
        assert fakes.only("store.merge_thread_config_override")[0] == (
            THREAD,
            {"llm": {"temperature": 0.3}},
        )

    @pytest.mark.parametrize(
        "actor, creation_path",
        [(None, "live_thread_internal"), (ACTOR, "live_thread_rest")],
    )
    async def test_provenance_names_the_creation_path_and_rides_the_selection_write(
        self, apply, fakes, actor, creation_path
    ):
        fakes.effects["authorize_thread_datasource_selection"] = lambda *_a, **_k: (
            [DS_B],
            {DS_B: 7},
        )
        result = await apply(
            THREAD,
            pinned_row(user_id=uuid.UUID(OWNER)),
            {},
            [DS_A],
            request=REQUEST,
            actor=actor,
        )
        assert fakes.only("datasource_selection_provenance") == (
            (),
            {
                "datasource_ids": [DS_B],
                "policy_revisions": {DS_B: 7},
                "origin": "explicit",
                "effective_work_owner_id": OWNER,
                "actor": actor,
                "project_ids": [PROJECT, LINKED_PROJECT],
                "creation_path": creation_path,
            },
        )
        assert fakes.only("store.set_thread_datasource_ids") == (
            (THREAD, [DS_B]),
            {
                "datasource_policy_revisions": {DS_B: 7},
                "datasource_selection_provenance": PROVENANCE,
            },
        )
        # The selected ids are authorization's answer, not the request.
        assert result[1] == [DS_B]

    async def test_ownerless_provenance_has_no_effective_owner(self, apply, fakes):
        row = with_selection(pinned_row(user_id=None), [DS_A])
        await apply(THREAD, row, {}, [DS_A], request=REQUEST, actor=None)
        _, kwargs = fakes.only("datasource_selection_provenance")
        assert kwargs["effective_work_owner_id"] is None
        assert kwargs["creation_path"] == "live_thread_internal"


# --------------------------------------------------------------------------- #
# Grant enforcement
# --------------------------------------------------------------------------- #


class TestGrantEnforcement:
    @pytest.mark.parametrize(
        "project_id, project_ids",
        [(uuid.UUID(PROJECT), [PROJECT]), (None, [])],
    )
    async def test_an_owned_thread_checks_its_owners_grants(
        self, apply, fakes, project_id, project_ids
    ):
        await apply(
            THREAD,
            pinned_row(user_id=uuid.UUID(OWNER), project_id=project_id),
            {"llm": {"temperature": 0.5}},
            None,
            request=REQUEST,
            actor=ACTOR,
        )
        assert fakes.only("enforce_session_create_grants") == (
            ({"llm": {"temperature": 0.5}},),
            {"user_id": OWNER, "project_ids": project_ids},
        )

    async def test_an_ownerless_thread_has_no_grants_to_check(self, apply, fakes):
        await apply(
            THREAD,
            pinned_row(user_id=None, project_id=PROJECT),
            {"llm": {"temperature": 0.5}},
            None,
            request=REQUEST,
            actor=None,
        )
        assert "enforce_session_create_grants" not in fakes.names()

    @pytest.mark.parametrize("datasource_ids", [None, [DS_A]])
    async def test_a_grant_denial_persists_and_audits_nothing(
        self, apply, fakes, datasource_ids
    ):
        denial = HTTPException(status_code=422, detail="denied")
        fakes.errors["enforce_session_create_grants"] = denial
        with pytest.raises(HTTPException) as exc:
            await apply(
                THREAD,
                pinned_row(),
                {"llm": {"model": "m-1"}},
                datasource_ids,
                request=REQUEST,
                actor=ACTOR,
            )
        assert exc.value is denial
        assert fakes.names()[-1] == "enforce_session_create_grants"
        for effect in (
            "store.resolve_api_keys_for_job",
            "inject_model_credentials",
            "store.merge_thread_config_override",
            "store.set_thread_datasource_ids",
            "log_security_event",
        ):
            assert effect not in fakes.names()


# --------------------------------------------------------------------------- #
# Model swap enrichment
# --------------------------------------------------------------------------- #


def _inject_transport(*, section, model_id, user_id, resolved_keys):
    section["api_key"] = "sk-injected"
    section["base_url"] = "https://models.example/v1"


class TestModelEnrichment:
    async def test_a_model_swap_resolves_and_injects_its_transport(self, apply, fakes):
        fakes.effects["inject_model_credentials"] = _inject_transport
        caller_llm = {"model": "m-1", "temperature": 0.2}
        fragment = {"llm": caller_llm}
        result = await apply(
            THREAD,
            pinned_row(user_id=uuid.UUID(OWNER), project_id=uuid.UUID(PROJECT)),
            fragment,
            None,
            request=REQUEST,
            actor=ACTOR,
        )
        assert fakes.names() == [
            "enforce_session_create_grants",
            "store.resolve_api_keys_for_job",
            "inject_model_credentials",
            *PERSIST_AND_AUDIT,
        ]
        # The grant check saw the fragment before enrichment.
        assert fakes.only("enforce_session_create_grants")[0] == (
            {"llm": {"model": "m-1", "temperature": 0.2}},
        )
        assert fakes.only("store.resolve_api_keys_for_job") == (
            (),
            {"user_id": OWNER, "project_id": PROJECT},
        )
        assert fakes.only("inject_model_credentials") == (
            (),
            {
                "section": {"model": "m-1", "temperature": 0.2},
                "model_id": "m-1",
                "user_id": OWNER,
                "resolved_keys": RESOLVED_KEYS,
            },
        )
        enriched = {
            "model": "m-1",
            "temperature": 0.2,
            "api_key": "sk-injected",
            "base_url": "https://models.example/v1",
            "provider": None,
        }
        assert result == ({"llm": enriched}, None)
        # Persisted WITHOUT the secret; the None sentinels stay.
        assert fakes.only("store.merge_thread_config_override")[0] == (
            THREAD,
            {
                "llm": {
                    "model": "m-1",
                    "temperature": 0.2,
                    "base_url": "https://models.example/v1",
                    "provider": None,
                }
            },
        )
        # The caller's nested mapping is copied, the top level is rebound.
        assert caller_llm == {"model": "m-1", "temperature": 0.2}
        assert result[0] is fragment

    async def test_unresolved_transport_keys_become_explicit_none(self, apply, fakes):
        result = await apply(
            THREAD,
            pinned_row(),
            {"llm": {"model": "m-1"}},
            None,
            request=REQUEST,
            actor=None,
        )
        assert result == (
            {
                "llm": {
                    "model": "m-1",
                    "provider": None,
                    "base_url": None,
                    "api_key": None,
                }
            },
            None,
        )
        assert fakes.only("store.merge_thread_config_override")[0] == (
            THREAD,
            {"llm": {"model": "m-1", "provider": None, "base_url": None}},
        )

    async def test_an_ownerless_row_resolves_with_no_owner_or_project(
        self, apply, fakes
    ):
        await apply(
            THREAD,
            pinned_row(user_id=None, project_id=None),
            {"llm": {"model": "m-1"}},
            None,
            request=REQUEST,
            actor=None,
        )
        assert fakes.only("store.resolve_api_keys_for_job") == (
            (),
            {"user_id": None, "project_id": None},
        )
        assert fakes.only("inject_model_credentials")[1]["user_id"] is None

    @pytest.mark.parametrize(
        "llm", [{"temperature": 0.1}, {"model": ""}, {"model": None}, {}]
    )
    async def test_no_model_means_no_enrichment(self, apply, fakes, llm):
        result = await apply(
            THREAD,
            pinned_row(user_id=None),
            {"llm": dict(llm)},
            None,
            request=REQUEST,
            actor=None,
        )
        assert result == ({"llm": llm}, None)
        assert fakes.names() == PERSIST_AND_AUDIT

    async def test_no_row_means_no_enrichment(self, apply, fakes):
        result = await apply(
            THREAD, None, {"llm": {"model": "m-1"}}, None, request=REQUEST, actor=None
        )
        assert result == ({"llm": {"model": "m-1"}}, None)
        assert fakes.names() == PERSIST_AND_AUDIT


# --------------------------------------------------------------------------- #
# Persistence
# --------------------------------------------------------------------------- #


class TestPersistence:
    async def test_the_persisted_fragment_is_redacted_the_returned_one_is_not(
        self, apply, fakes
    ):
        result = await apply(
            THREAD,
            pinned_row(user_id=None),
            {"llm": {"api_key": "sk-caller", "temperature": 0.1}},
            None,
            request=REQUEST,
            actor=None,
        )
        assert fakes.only("store.merge_thread_config_override") == (
            (THREAD, {"llm": {"temperature": 0.1}}),
            {},
        )
        assert result == ({"llm": {"api_key": "sk-caller", "temperature": 0.1}}, None)

    @pytest.mark.parametrize("merged", [False, None, 0])
    @pytest.mark.parametrize("datasource_ids", [None, [DS_A]])
    async def test_a_merge_miss_is_404_and_nothing_follows(
        self, apply, fakes, merged, datasource_ids
    ):
        fakes.results["store.merge_thread_config_override"] = merged
        error = await _refused(
            apply,
            THREAD,
            pinned_row(),
            {"llm": {"temperature": 0.1}},
            datasource_ids,
            request=REQUEST,
            actor=ACTOR,
        )
        assert error.status_code == 404
        assert error.detail == THREAD_NOT_FOUND
        assert fakes.names()[-1] == "store.merge_thread_config_override"
        assert "store.set_thread_datasource_ids" not in fakes.names()
        assert "log_security_event" not in fakes.names()

    @pytest.mark.parametrize(
        "raised, detail",
        [
            (CredentialConnectorAttachedError("credential still attached"), None),
            (
                DatasourcePolicyConflictError("revision moved"),
                "Connector policy changed while updating the session; retry the request",
            ),
        ],
    )
    async def test_a_selection_write_conflict_is_409_and_not_audited(
        self, apply, fakes, raised, detail
    ):
        fakes.errors["store.set_thread_datasource_ids"] = raised
        error = await _refused(
            apply,
            THREAD,
            pinned_row(),
            {"llm": {"temperature": 0.1}},
            [DS_A],
            request=REQUEST,
            actor=ACTOR,
        )
        assert error.status_code == 409
        assert error.detail == (detail if detail is not None else str(raised))
        assert error.__cause__ is raised
        # The config merge already ran; the selection write is the last effect.
        assert fakes.names()[-2:] == [
            "store.merge_thread_config_override",
            "store.set_thread_datasource_ids",
        ]
        assert "log_security_event" not in fakes.names()

    @pytest.mark.parametrize("updated", [False, None])
    async def test_a_selection_write_miss_is_404(self, apply, fakes, updated):
        fakes.results["store.set_thread_datasource_ids"] = updated
        error = await _refused(
            apply, THREAD, pinned_row(), {}, [DS_A], request=REQUEST, actor=ACTOR
        )
        assert error.status_code == 404
        assert error.detail == THREAD_NOT_FOUND
        assert "log_security_event" not in fakes.names()

    async def test_no_selection_means_no_selection_write(self, apply, fakes):
        await apply(
            THREAD,
            pinned_row(),
            {"llm": {"temperature": 0.1}},
            None,
            request=REQUEST,
            actor=ACTOR,
        )
        assert "store.set_thread_datasource_ids" not in fakes.names()
        assert "thread_project_ids" not in fakes.names()


# --------------------------------------------------------------------------- #
# Audit
# --------------------------------------------------------------------------- #


class TestConfigChangeAudit:
    @pytest.mark.parametrize("actor", [ACTOR, None])
    async def test_the_audit_is_the_last_effect_and_names_key_paths_only(
        self, apply, fakes, actor
    ):
        fakes.effects["inject_model_credentials"] = _inject_transport
        await apply(
            THREAD,
            pinned_row(),
            {"llm": {"model": "m-1"}, "delegation": {"mode": "light"}},
            [DS_A],
            request=REQUEST,
            actor=actor,
        )
        assert fakes.names()[-3:] == [
            "store.merge_thread_config_override",
            "store.set_thread_datasource_ids",
            "log_security_event",
        ]
        args, kwargs = fakes.only("log_security_event")
        assert len(args) == 1 and args[0] is fakes.store
        assert kwargs == {
            "resource_type": "thread",
            "event_type": "session_config_updated",
            "user": actor,
            "resource_id": THREAD,
            # Computed PRE-enrichment and after validation dropped the empty
            # delegation block: no api_key/base_url/provider paths.
            "detail": "keys=llm.model datasource_ids=1",
            "request": REQUEST,
        }
        assert kwargs["request"] is REQUEST

    async def test_an_audit_failure_propagates_after_everything_persisted(
        self, apply, fakes
    ):
        """The core does not guard the audit call; the real writer never
        raises, so a raising writer surfaces unchanged."""
        failure = RuntimeError("audit down")
        fakes.errors["log_security_event"] = failure
        with pytest.raises(RuntimeError) as exc:
            await apply(THREAD, pinned_row(), {}, [DS_A], request=REQUEST, actor=ACTOR)
        assert exc.value is failure
        assert "store.set_thread_datasource_ids" in fakes.names()


# --------------------------------------------------------------------------- #
# Return value
# --------------------------------------------------------------------------- #


class TestReturnValue:
    async def test_without_a_selection_the_callers_fragment_is_returned_mutated(
        self, apply, fakes
    ):
        fragment = {"tools": {}, "delegation": {"enabled": False}}
        result = await apply(
            THREAD, pinned_row(), fragment, None, request=REQUEST, actor=ACTOR
        )
        assert result[0] is fragment
        assert fragment == {"delegation": {"enabled": False}}
        assert result[1] is None

    async def test_with_a_selection_a_new_fragment_carries_the_grant_tools(
        self, apply, fakes
    ):
        fragment = {"tools": {"research": []}}
        result = await apply(
            THREAD, pinned_row(), fragment, [DS_A], request=REQUEST, actor=ACTOR
        )
        assert result[0] is not fragment
        assert result[0] == {
            "tools": {"research": [], "sql": ["sql_query", "sql_execute"]}
        }
        assert fragment == {"tools": {"research": []}}
        assert result[1] == [DS_A]


# --------------------------------------------------------------------------- #
# The transaction wrapper around the core
# --------------------------------------------------------------------------- #


class TestTransactionOrdering:
    async def test_the_core_runs_inside_the_locked_transaction(self, apply, fakes):
        locked_row = pinned_row(project_id=PROJECT)
        fakes.results["conn.fetchrow"] = locked_row
        fakes.effects["store.refresh_session_execution"] = (
            lambda _thread_id, *, conn, config_override: {
                "delivery_override": {"delivered": dict(config_override)}
            }
        )
        result = await tcu.apply_thread_config_update(
            THREAD,
            locked_row,
            {"llm": {"temperature": 0.4}},
            [DS_A],
            request=REQUEST,
            actor=ACTOR,
            dependencies=apply.dependencies,
        )
        assert fakes.names() == [
            "transaction.enter",
            "conn.fetchrow",
            "thread_project_ids",
            "store.get_user",
            "authorize_thread_datasource_selection",
            *SELECTION_TAIL,
            "enforce_session_create_grants",
            "store.merge_thread_config_override",
            "store.set_thread_datasource_ids",
            "log_security_event",
            "store.refresh_session_execution",
            "transaction.exit",
        ]
        assert fakes.only("conn.fetchrow") == (
            ("SELECT * FROM threads WHERE id=$1 FOR UPDATE", uuid.UUID(THREAD)),
            {},
        )
        core_override = {"llm": {"temperature": 0.4}, "tools": FLIP["tools"]}
        args, kwargs = fakes.only("store.refresh_session_execution")
        assert args == (THREAD,)
        assert kwargs["conn"] is fakes.conn
        assert kwargs["config_override"] == core_override
        assert result == ({"delivered": core_override}, [DS_A])

    async def test_the_core_reads_the_locked_row_not_the_callers(self, apply, fakes):
        """The caller's row only feeds the runtime-ownership check; the core
        sees ``dict(current)`` from ``FOR UPDATE``."""
        fakes.results["conn.fetchrow"] = pinned_row(user_id=None)
        await tcu.apply_thread_config_update(
            THREAD,
            pinned_row(user_id=OWNER),
            {"llm": {"temperature": 0.4}},
            None,
            request=REQUEST,
            actor=None,
            dependencies=apply.dependencies,
        )
        assert fakes.names() == [
            "transaction.enter",
            "conn.fetchrow",
            *PERSIST_AND_AUDIT,
            "store.refresh_session_execution",
            "transaction.exit",
        ]

    async def test_a_core_refusal_leaves_the_transaction_without_a_refresh(
        self, apply, fakes
    ):
        fakes.results["conn.fetchrow"] = stateless_row()
        with pytest.raises(HTTPException) as exc:
            await tcu.apply_thread_config_update(
                THREAD,
                stateless_row(),
                {"workspace": {"backend": "sandbox"}},
                None,
                request=REQUEST,
                actor=None,
                dependencies=apply.dependencies,
            )
        assert exc.value.status_code == 409
        assert fakes.names() == [
            "transaction.enter",
            "conn.fetchrow",
            "transaction.exit",
        ]
