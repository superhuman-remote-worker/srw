"""A saved Expert's ``$extends`` may restate its base, never contradict it.

A DB Expert's base is ``spec.runtime.config.config_name`` (``harness_config_name``
on the row): ``resolve_config`` loads that base and merges the private fragment
onto it, so a ``$extends`` INSIDE the fragment is never followed. On dev an
Expert saved with ``config_name: worker_base`` and ``config: {$extends:
receptionist}`` resolved on the plain worker base (no live voice driver, the
generic worker tools) and nothing warned. Every save path now refuses that
contradiction with a 422 naming the fix; a ``$extends`` that agrees (the shape
account copies and every shipped manifest carry) stays allowed, resolves
exactly as before, and no longer rides into the resolved config as a literal.
"""

from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
import yaml
from fastapi import HTTPException

from orchestrator.schemas.expert_catalog import (
    ExpertCreate,
    ExpertDefaultForkRequest,
    ExpertUpdate,
)
from orchestrator.services import expert_authoring as expert_authoring_module
from orchestrator.services.config_resolver import resolve_config
from orchestrator.services.expert_authoring import ExpertAuthoringService
from orchestrator.services.expert_catalog import ExpertCatalogService
from orchestrator.services.expert_catalog_contracts import ExpertWritePolicy
from orchestrator.services.manifest_experts import refuse_contradicting_spec_extends
from orchestrator.services.manifest_resolution import LiveManifestResolver
from orchestrator.services.manifest_store import resource_key
from orchestrator.services.session_tool_policy import with_validated_tool_overrides
from shared.runtime.core.expert_resolution import (
    build_expert_config,
    extends_contradiction,
    same_config_base,
)

USER = "00000000-0000-0000-0000-0000000000a1"
EXPERT = "00000000-0000-0000-0000-0000000000e1"
CONFIG_DIR = Path(__file__).resolve().parents[1] / "config"

#: The dev incident, verbatim in shape.
DEV_REFUSAL = (
    "config.$extends names receptionist but the expert's base "
    "(spec.runtime.config.config_name) is worker_base; "
    "set config_name: receptionist to extend it."
)


def _runtime(private: dict) -> dict:
    return {"adapter": "srw/v1", "config": deepcopy(private)}


def _expert_doc(private: dict, *, name="desk", expert_type=None) -> dict:
    metadata = {"name": name, "scope": {"kind": "Account", "name": USER}}
    if expert_type:
        metadata["annotations"] = {"srw.io/expert-type": expert_type}
    return {
        "apiVersion": "srw/v1alpha1",
        "kind": "Expert",
        "metadata": metadata,
        "spec": {"runtime": _runtime(private)},
    }


INCIDENT = {
    "config_name": "worker_base",
    "asset_name": "receptionist",
    "config": {"$extends": "receptionist", "display_name": "Front desk"},
}
AGREEING = {
    "config_name": "worker_base",
    "config": {"$extends": "worker_base", "display_name": "Copy"},
}


# --- The comparison: the resolver's view of "one base" ----------------------


class TestSameBase:
    @pytest.mark.parametrize(
        "left,right",
        [
            ("worker_base", "worker_base"),
            # The resolver re-roots every chain root onto the role's overlay.
            ("worker_base", "session_base"),
            ("session_base", "expert_base"),
            ("subagent_base", "worker_base"),
            # Public aliases and the overlay spelling canonicalise to a root.
            ("default", "worker_base"),
            ("overlays/session", "worker_base"),
            ("persistent_default", "expert_base"),
            # One installed expert, two selectors.
            ("receptionist", "receptionist"),
            ("receptionist", "experts/receptionist"),
        ],
    )
    def test_same_base(self, left, right):
        assert same_config_base(left, right)
        assert same_config_base(right, left)

    @pytest.mark.parametrize(
        "left,right",
        [
            ("receptionist", "worker_base"),
            ("scholar", "session_base"),
            ("receptionist", "scholar"),
            ("experts/receptionist", "experts/scholar"),
        ],
    )
    def test_different_base(self, left, right):
        assert not same_config_base(left, right)
        assert not same_config_base(right, left)

    def test_the_dev_incident_message(self):
        assert (
            extends_contradiction(INCIDENT["config"], INCIDENT["config_name"])
            == DEV_REFUSAL
        )

    def test_absent_base_is_the_roles_root(self):
        assert extends_contradiction({"$extends": "worker_base"}, None) is None
        assert (
            extends_contradiction(
                {"$extends": "worker_base"}, None, expert_type="session"
            )
            is None
        )
        assert "is session_base;" in extends_contradiction(
            {"$extends": "scholar"}, None, expert_type="session"
        )

    @pytest.mark.parametrize(
        "fragment", [{}, {"llm": {}}, {"$extends": None}, {"$extends": ""}, None, []]
    )
    def test_no_claim_is_no_contradiction(self, fragment):
        assert extends_contradiction(fragment, "receptionist") is None

    def test_every_shipped_manifest_agrees_with_itself(self):
        paths = sorted(CONFIG_DIR.glob("experts/*/config.yaml")) + sorted(
            CONFIG_DIR.glob("subagents/*/config.yaml")
        )
        assert paths
        for path in paths:
            document = yaml.safe_load(path.read_text(encoding="utf-8"))
            assert "$extends" in document["spec"]["runtime"]["config"]["config"], path
            refuse_contradicting_spec_extends(document["spec"])


# --- Resolution: unchanged for agreeing rows, no literal key ----------------


def _row(private: dict, *, expert_type="worker") -> dict:
    return {
        "id": EXPERT,
        "name": "desk",
        "expert_type": expert_type,
        "config": deepcopy(private.get("config", {})),
        "prompts": {},
        "harness_config_name": private.get("config_name"),
        "harness_asset_name": private.get("asset_name"),
        "harness_config_layers": deepcopy(private.get("layers", [])),
    }


def _resolve(row: dict, *, role="worker") -> tuple[dict, dict]:
    capture: dict = {}
    blob = resolve_config(
        base_config_name="worker_base",
        expert_row=row,
        expert_type=role,
        capture=capture,
    )
    blob.pop("resolved_at", None)
    return blob, capture["merged_fragment"]


def _without_extends(private: dict) -> dict:
    stripped = deepcopy(private)
    stripped["config"].pop("$extends", None)
    for layer in stripped.get("layers", []):
        layer.pop("$extends", None)
    return stripped


class TestResolution:
    @pytest.mark.parametrize(
        "private,role",
        [
            (AGREEING, "worker"),
            (AGREEING, "session"),
            (
                {
                    "config_name": "session_base",
                    "config": {"$extends": "persistent_default"},
                },
                "session",
            ),
            (
                {
                    "config_name": "receptionist",
                    "config": {"$extends": "experts/receptionist"},
                    "layers": [{"$extends": "receptionist", "icon": "call"}],
                },
                "worker",
            ),
        ],
    )
    def test_an_agreeing_extends_resolves_as_without_it(self, private, role):
        blob, policy = _resolve(_row(private), role=role)
        assert "$extends" not in repr(blob)
        assert "$extends" not in policy
        assert (blob, policy) == _resolve(_row(_without_extends(private)), role=role)

    def test_the_base_is_still_config_name_never_the_fragment(self):
        """A stored contradiction keeps resolving where it always did."""
        blob, policy = _resolve(_row(INCIDENT))
        assert "$extends" not in repr(blob)
        assert (blob, policy) == _resolve(_row(_without_extends(INCIDENT)))

    def test_build_expert_config_drops_the_key_and_never_mutates_the_row(self):
        row = _row(
            {
                "config_name": "worker_base",
                "config": {"$extends": "worker_base", "icon": "a"},
                "layers": [{"$extends": "worker_base", "color": "#000000"}],
            }
        )
        before = deepcopy(row)
        merged, _ = build_expert_config({"agent_id": "base"}, row)
        assert merged == {"agent_id": "base", "icon": "a", "color": "#000000"}
        assert row == before


# --- Save path 1: manifest apply (documents, Project inline, Job inline) ----


def _resolver() -> LiveManifestResolver:
    authority = SimpleNamespace(
        account={"kind": "Account", "name": USER},
        new_projects=set(),
        scope=AsyncMock(side_effect=lambda value, write=False: deepcopy(value)),
        resource=AsyncMock(),
    )
    store = SimpleNamespace(by_name=AsyncMock(return_value=None))
    return LiveManifestResolver(store, authority)


class TestManifestApply:
    @pytest.mark.asyncio
    async def test_an_applied_expert_contradiction_is_refused(self):
        with pytest.raises(HTTPException) as exc:
            await _resolver().prepare([_expert_doc(INCIDENT)])
        assert exc.value.status_code == 422
        assert exc.value.detail == DEV_REFUSAL

    @pytest.mark.asyncio
    async def test_a_contradicting_private_layer_is_refused(self):
        private = {
            "config_name": "receptionist",
            "config": {},
            "layers": [{"$extends": "scholar"}],
        }
        with pytest.raises(HTTPException) as exc:
            await _resolver().prepare([_expert_doc(private)])
        assert exc.value.status_code == 422
        assert exc.value.detail.startswith("layers[0].$extends names scholar but")
        assert "is receptionist; set config_name: scholar" in exc.value.detail

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "private",
        [
            AGREEING,
            {"config_name": "worker_base", "config": {"$extends": "session_base"}},
            {"config": {"$extends": "default"}},
            {"config_name": "receptionist", "config": {"display_name": "Desk"}},
            {
                "config_name": "receptionist",
                "asset_name": "receptionist",
                "config": {"$extends": "receptionist"},
            },
        ],
    )
    async def test_an_agreeing_or_absent_extends_is_applied(self, private):
        doc = _expert_doc(private)
        resolver = await _resolver().prepare([doc])
        prepared = resolver.prepared[resource_key(resolver.documents[0])]
        assert prepared["resolved"]["spec"]["runtime"]["config"] == private

    @pytest.mark.asyncio
    async def test_the_roles_root_names_the_absent_base(self):
        doc = _expert_doc({"config": {"$extends": "scholar"}}, expert_type="session")
        with pytest.raises(HTTPException) as exc:
            await _resolver().prepare([doc])
        assert "config_name) is session_base; set config_name: scholar" in (
            exc.value.detail
        )

    @pytest.mark.asyncio
    async def test_a_projects_inline_expert_is_refused(self):
        project = {
            "apiVersion": "srw/v1alpha1",
            "kind": "Project",
            "metadata": {"name": "front", "scope": {"kind": "Account", "name": USER}},
            "spec": {
                "resources": {
                    "experts": {"desk": {"inline": {"runtime": _runtime(INCIDENT)}}}
                }
            },
        }
        with pytest.raises(HTTPException) as exc:
            await _resolver().prepare([project])
        assert exc.value.status_code == 422
        assert exc.value.detail == DEV_REFUSAL

    @pytest.mark.asyncio
    async def test_a_jobs_inline_expert_is_refused(self):
        with pytest.raises(HTTPException) as exc:
            await _resolver().selection(
                "Expert",
                {"inline": {"runtime": _runtime(INCIDENT)}},
                {"kind": "Account", "name": USER},
                [],
            )
        assert exc.value.status_code == 422
        assert exc.value.detail == DEV_REFUSAL

    def test_another_adapter_is_never_read(self):
        refuse_contradicting_spec_extends(
            {"runtime": {"image": "example/custom:1", "config": INCIDENT}}
        )


# --- Save path 2: inline Experts on job/session creation --------------------


def _admit(private: dict, *, role="worker") -> dict:
    from orchestrator.schemas.inline_expert import InlineExpertSelection
    from orchestrator.services.inline_expert import admit_inline_expert

    selection = InlineExpertSelection.model_validate(
        {"inline": {"runtime": _runtime(private)}}
    )
    return admit_inline_expert(
        selection.inline, role=role, trusted_image="srw-agent:installed"
    )


class TestInlineExpert:
    def test_a_contradiction_is_refused(self):
        with pytest.raises(HTTPException) as exc:
            _admit(INCIDENT)
        assert exc.value.status_code == 422
        assert exc.value.detail == DEV_REFUSAL

    def test_without_config_name_the_roles_base_is_compared(self):
        with pytest.raises(HTTPException) as exc:
            _admit({"config": {"$extends": "receptionist"}}, role="session")
        assert "config_name) is session_base;" in exc.value.detail
        assert _admit({"config": {"$extends": "worker_base"}}, role="session")

    def test_an_agreeing_extends_is_admitted(self):
        row = _admit(AGREEING)
        assert row["harness_config_name"] == "worker_base"
        assert row["config"]["display_name"] == "Copy"


# --- Save path 3: the experts REST API (create/import/update/copy/fork) -----


def _validate_fragment(config):
    """The real fragment gate, bound to the real tool-vocabulary gate."""
    owner = SimpleNamespace(
        deps=SimpleNamespace(
            with_validated_tool_overrides=with_validated_tool_overrides
        )
    )
    return ExpertCatalogService.validate_expert_fragment(owner, config)


def _stored(private: dict, *, expert_type="worker") -> dict:
    return {
        **_row(private, expert_type=expert_type),
        "owner_id": USER,
        "display_name": "Desk",
        "description": None,
        "icon": "smart_toy",
        "color": "#6B7280",
        "tags": [expert_type],
        "managed_key": None,
        "harness_adapter": "srw/v1",
    }


@pytest.fixture
def rest():
    store = SimpleNamespace(
        create_expert=AsyncMock(return_value={"id": "created"}),
        update_expert=AsyncMock(return_value={"id": EXPERT}),
        get_expert_by_id=AsyncMock(),
        get_expert_visible_by_id=AsyncMock(),
        fork_and_set_user_expert_default=AsyncMock(return_value=_stored(AGREEING)),
    )
    catalog = SimpleNamespace(
        validate_expert_fragment=_validate_fragment,
        require_visible_roster_refs=AsyncMock(),
        bundled_expert_source=AsyncMock(return_value=None),
        deps=SimpleNamespace(
            looks_like_uuid=lambda value: True,
            visible_project_ids=AsyncMock(return_value="all"),
        ),
    )
    service = ExpertAuthoringService(
        store=store,
        catalog=catalog,
        resolve_default_models=AsyncMock(),
        prefetch_roster_refs=AsyncMock(),
    )
    policy = ExpertWritePolicy(
        enforce_save=AsyncMock(),
        enforce_save_prelude=AsyncMock(),
        strip_save_grants=AsyncMock(side_effect=lambda config, **_: (config, [])),
    )
    user = {"id": USER, "is_admin": False}
    return SimpleNamespace(service=service, store=store, policy=policy, user=user)


def _create_body(config, *, expert_type="worker"):
    return ExpertCreate(
        name="desk", display_name="Desk", expert_type=expert_type, config=config
    )


class TestRestApi:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("route", ["create_expert", "import_expert"])
    async def test_create_and_import_refuse_a_contradiction(self, rest, route):
        with pytest.raises(HTTPException) as exc:
            await getattr(rest.service, route)(
                _create_body({"$extends": "receptionist"}),
                user=rest.user,
                write_policy=rest.policy,
            )
        assert exc.value.status_code == 422
        assert exc.value.detail == DEV_REFUSAL
        rest.store.create_expert.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("route", ["create_expert", "import_expert"])
    async def test_create_and_import_keep_an_agreeing_extends(self, rest, route):
        await getattr(rest.service, route)(
            _create_body({"$extends": "worker_base"}, expert_type="session"),
            user=rest.user,
            write_policy=rest.policy,
        )
        assert rest.store.create_expert.await_args.kwargs["config"] == {
            "$extends": "worker_base"
        }

    @pytest.mark.asyncio
    async def test_update_compares_with_the_rows_own_base(self, rest):
        rest.store.get_expert_by_id.return_value = _stored(
            {"config_name": "receptionist", "config": {}}
        )
        with pytest.raises(HTTPException) as exc:
            await rest.service.update_expert(
                EXPERT,
                ExpertUpdate(config={"$extends": "worker_base"}),
                user=rest.user,
                write_policy=rest.policy,
            )
        assert exc.value.status_code == 422
        assert exc.value.detail == (
            "config.$extends names worker_base but the expert's base "
            "(spec.runtime.config.config_name) is receptionist; "
            "set config_name: worker_base to extend it."
        )
        rest.store.update_expert.assert_not_awaited()

        await rest.service.update_expert(
            EXPERT,
            ExpertUpdate(config={"$extends": "experts/receptionist"}),
            user=rest.user,
            write_policy=rest.policy,
        )
        rest.store.update_expert.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_a_metadata_edit_of_a_stored_contradiction_still_saves(self, rest):
        rest.store.get_expert_by_id.return_value = _stored(INCIDENT)
        await rest.service.update_expert(
            EXPERT,
            ExpertUpdate(display_name="Renamed"),
            user=rest.user,
            write_policy=rest.policy,
        )
        rest.store.update_expert.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_a_copy_refuses_to_carry_a_contradiction_forward(self, rest):
        rest.store.get_expert_visible_by_id.return_value = _stored(INCIDENT)
        with pytest.raises(HTTPException) as exc:
            await rest.service.duplicate_expert(
                EXPERT, user=rest.user, write_policy=rest.policy
            )
        assert exc.value.detail == DEV_REFUSAL
        rest.store.create_expert.assert_not_awaited()

        rest.store.get_expert_visible_by_id.return_value = _stored(AGREEING)
        await rest.service.duplicate_expert(
            EXPERT, user=rest.user, write_policy=rest.policy
        )
        kwargs = rest.store.create_expert.await_args.kwargs
        assert kwargs["srw_config_name"] == "worker_base"
        assert kwargs["config"]["$extends"] == "worker_base"

    @pytest.mark.asyncio
    async def test_a_personal_default_fork_refuses_a_contradiction(
        self, rest, monkeypatch
    ):
        monkeypatch.setattr(
            expert_authoring_module,
            "personal_defaults_allowed",
            AsyncMock(return_value=True),
        )
        resolve_root = AsyncMock(return_value=SimpleNamespace(expert=_stored(INCIDENT)))
        monkeypatch.setattr(
            expert_authoring_module, "resolve_root_expert", resolve_root
        )
        with pytest.raises(HTTPException) as exc:
            await rest.service.fork_my_expert_default(
                "worker",
                ExpertDefaultForkRequest(expert_id=EXPERT),
                user=rest.user,
                write_policy=rest.policy,
            )
        assert exc.value.detail == DEV_REFUSAL
        rest.store.fork_and_set_user_expert_default.assert_not_awaited()

        resolve_root.return_value = SimpleNamespace(expert=_stored(AGREEING))
        await rest.service.fork_my_expert_default(
            "worker",
            ExpertDefaultForkRequest(expert_id=EXPERT),
            user=rest.user,
            write_policy=rest.policy,
        )
        rest.store.fork_and_set_user_expert_default.assert_awaited_once()
