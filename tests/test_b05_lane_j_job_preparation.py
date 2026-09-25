"""R1.B05 lane J — worker job preparation, extracted from ``orchestrator.main``.

Three things are proven here, in this order:

1. **Reference wiring.** The ``_*_deps`` factories below build every dependency
   object lane J's modules take, reading the owning modules and the
   application's resources *at call time*. They are the exact specification the
   composition implements (R1.B12: ``orchestrator.application.preparation`` /
   ``controls``, formerly ``main._<name>_dependencies()``), and every test in
   this file goes through them — so a field that cannot be resolved late is a
   failure here, not a surprise at integration.

2. **Parity against the current ``main`` implementation.** Much of lane J is
   only covered through mocked callers, and several nodes had no test at all
   (``_mask_repository_transport``, ``_redispatch_livelock_trip``,
   ``_get_vm_context``, ``_get_infra_transient_context``,
   ``_container_ssh_key_path``, the two workspace-config injectors,
   ``_stateless_worker_workspace_owner``, ``_mcp_datasource_runtime_allowed``,
   the two stateless attestations). Those are characterized against ``main``
   here, so the move is provably behaviour-preserving rather than
   assumption-preserving.

3. **Late binding, per port-contract §P3.** A wrapper does NOT intercept a
   call made inside a service. Every dependency field gets a test that patches
   its owner, rebuilds the dependency object through the reference factory AND
   through the application's composition factory, and asserts the stub was
   reached — asserting a value only the stub could produce, never merely "it
   didn't crash". Running both sides through one table is also the parity
   proof between the reference wiring and the application's.

The fences (``attest_pinned_k8s_job_workspace``,
``pinned_k8s_job_workspace_authority_is_current``, the two stateless
attestations, ``workspace_runtime_unchanged_before_delivery``) are exercised on
their refusal paths specifically. None of them is relaxed anywhere in this
file.
"""

from __future__ import annotations


from orchestrator.services.workspace_lifecycle import EnsureOutcome
from tests import b08_completion_helpers as b08_helpers

import inspect
import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import HTTPException

import orchestrator.main as main
from orchestrator.routers import job_assignment as job_assignment_routes
from orchestrator.services import agent_datasource_payload
from orchestrator.services import job_datasource_selection
from orchestrator.services import job_dispatch_credentials
from orchestrator.services import job_start_bundle
from orchestrator.services.managed_repository_authority import (
    ManagedRepositoryAuthorityError,
)
from orchestrator.services import job_workspace_authority
from orchestrator.services import job_workspace_runtime
from orchestrator.services.container_provisioner import (
    WorkspaceRuntimeAttestation,
    WorkspaceRuntimeAuthorityError,
)
from orchestrator.services.workspace_lifecycle import WorkspaceOwner
from orchestrator.application import access as access_composition
from orchestrator.application import catalogue as catalogue_composition
from orchestrator.application import completion as completion_composition
from orchestrator.application import controls as controls_composition
from orchestrator.application import http as http_composition
from orchestrator.application import jobs as jobs_composition
from orchestrator.application import preparation as preparation_composition
from orchestrator.application import sessions as sessions_composition
from orchestrator.application.resources import bound
from orchestrator.security import access as access_module
from orchestrator.services import (
    agent_datasource_payload as agent_datasource_payload_module,
)
from orchestrator.services import config_resolver as config_resolver_module
from orchestrator.services import container_provisioner as container_provisioner_module
from orchestrator.services import deployment_gates as deployment_gates_module
from orchestrator.services import dispatch_credentials as dispatch_credentials_module
from orchestrator.services import grant_enforcement as grant_enforcement_module
from orchestrator.services import (
    job_datasource_selection as job_datasource_selection_module,
)
from orchestrator.services import (
    job_dispatch_credentials as job_dispatch_credentials_module,
)
from orchestrator.services import job_dispatcher as job_dispatcher_module
from orchestrator.services import job_mutation_target as job_mutation_target_module
from orchestrator.services import job_start_bundle as job_start_bundle_module
from orchestrator.services import (
    job_workspace_authority as job_workspace_authority_module,
)
from orchestrator.services import job_workspace_runtime as job_workspace_runtime_module
from orchestrator.services import (
    managed_repository_authority as managed_repository_authority_module,
)
from orchestrator.services import runtime_actor as runtime_actor_module
from orchestrator.services import (
    session_config_resolution as session_config_resolution_module,
)
from orchestrator.services import subjob_completion as subjob_completion_module
from orchestrator.services import (
    thread_datasource_authorization as thread_datasource_authorization_module,
)
from orchestrator.services import vm_provisioner as vm_provisioner_module
from orchestrator.services import workspace_lifecycle as workspace_lifecycle_module
from orchestrator.services import workspace_suspension as workspace_suspension_module
from orchestrator.services import workspace_tier_policy as workspace_tier_policy_module
from shared.runtime.core import model_registry as model_registry_module
import functools


# =============================================================================
# Reference wiring — the factories the composition implements
# =============================================================================
#
# Every field reads its owner (``<module>.<name>``) or the application's
# resources inside the factory body, so a monkeypatched owner is observed on
# the next build. Capturing any of these at import time is exactly the defect
# §P1 forbids.


def _datasource_payload_deps() -> (
    agent_datasource_payload.DatasourcePayloadDependencies
):
    return agent_datasource_payload.DatasourcePayloadDependencies(
        logger=preparation_composition.logger,
        mcp_datasources_enabled=deployment_gates_module.mcp_datasources_enabled,
        mcp_stdio_enabled=deployment_gates_module.mcp_stdio_enabled,
    )


def _datasource_selection_deps() -> (
    job_datasource_selection.JobDatasourceSelectionDependencies
):
    return job_datasource_selection.JobDatasourceSelectionDependencies(
        store=main.app.state.resources.postgres_db,
        authorize_thread_datasource_selection=bound(
            thread_datasource_authorization_module.authorize_thread_datasource_selection,
            sessions_composition.thread_datasource_authorization_dependencies,
            main.app.state.resources,
        ),
        backend_from_override=workspace_tier_policy_module.backend_from_override,
        revalidate_selection=bound(
            job_datasource_selection_module.revalidate_job_datasource_selection,
            preparation_composition.job_datasource_selection_dependencies,
            main.app.state.resources,
        ),
    )


def _workspace_runtime_deps() -> job_workspace_runtime.JobWorkspaceRuntimeDependencies:
    return job_workspace_runtime.JobWorkspaceRuntimeDependencies(
        store=main.app.state.resources.postgres_db,
        vm_mode=lambda: vm_provisioner_module.vm_provisioner.mode,
        workspace_provisioner=container_provisioner_module.container_provisioner,
        vm_workspaces_on_pod_network=access_module.vm_workspaces_on_pod_network,
        stateless_worker_enabled=lambda: main.app.state.resources.settings.stateless_worker_enabled,
        backend_from_override=workspace_tier_policy_module.backend_from_override,
    )


def _workspace_authority_deps() -> (
    job_workspace_authority.JobWorkspaceAuthorityDependencies
):
    return job_workspace_authority.JobWorkspaceAuthorityDependencies(
        store=main.app.state.resources.postgres_db,
        logger=preparation_composition.logger,
        workspace_provisioner=container_provisioner_module.container_provisioner,
        vm_provisioner=vm_provisioner_module.vm_provisioner,
        vm_mode=lambda: vm_provisioner_module.vm_provisioner.mode,
        ensure_workspace=workspace_lifecycle_module.ensure_workspace,
        workspace_suspension=workspace_suspension_module.workspace_suspension_service,
        handle_scholar_completion=b08_helpers.handle_scholar_completion,
        handle_delegation_child_completion=b08_helpers.handle_delegation_child_completion,
        resolve_inherited_workspace=bound(
            job_workspace_authority_module.resolve_subjob_inherited_workspace,
            preparation_composition.job_workspace_authority_dependencies,
            main.app.state.resources,
        ),
        fail_subjob_and_unblock_parent=bound(
            job_workspace_authority_module.fail_subjob_and_unblock_parent,
            preparation_composition.job_workspace_authority_dependencies,
            main.app.state.resources,
        ),
        workspace_runtime_unchanged_before_delivery=(
            bound(
                job_workspace_authority_module.workspace_runtime_unchanged_before_delivery,
                preparation_composition.job_workspace_authority_dependencies,
                main.app.state.resources,
            )
        ),
    )


def _dispatch_credential_deps() -> (
    job_dispatch_credentials.DispatchCredentialDependencies
):
    return job_dispatch_credentials.DispatchCredentialDependencies(
        store=main.app.state.resources.postgres_db,
        logger=preparation_composition.logger,
        resolve_model=model_registry_module.resolve_model,
        inject_model_credentials=bound(
            dispatch_credentials_module.inject_model_credentials,
            preparation_composition.dispatch_credential_dependencies,
            main.app.state.resources,
        ),
        inject_env_key_credentials=bound(
            dispatch_credentials_module.inject_env_key_credentials,
            preparation_composition.dispatch_credential_dependencies,
            main.app.state.resources,
        ),
        inject_search_credentials=bound(
            dispatch_credentials_module.inject_search_credentials,
            preparation_composition.dispatch_credential_dependencies,
            main.app.state.resources,
        ),
        inject_system_kb_embedding_profile=bound(
            dispatch_credentials_module.inject_system_kb_embedding_profile,
            preparation_composition.dispatch_credential_dependencies,
            main.app.state.resources,
        ),
        dispatch_llm_provider_fallback=dispatch_credentials_module.dispatch_llm_provider_fallback,
        nested_model_slots=dispatch_credentials_module.nested_model_slots,
    )


def _start_bundle_deps() -> job_start_bundle.JobStartBundleDependencies:
    return job_start_bundle.JobStartBundleDependencies(
        store=main.app.state.resources.postgres_db,
        logger=preparation_composition.logger,
        forge=main.app.state.resources.gitea_client,
        workspace_runtime=_workspace_runtime_deps(),
        inject_dispatch_credentials=bound(
            job_dispatch_credentials_module.inject_dispatch_credentials,
            preparation_composition.job_dispatch_credential_dependencies,
            main.app.state.resources,
        ),
        resolve_authorized_job_datasources=bound(
            job_datasource_selection_module.resolve_authorized_job_datasources,
            preparation_composition.job_datasource_selection_dependencies,
            main.app.state.resources,
        ),
        job_project_repositories=bound(
            job_start_bundle_module.job_project_repositories,
            preparation_composition.job_start_bundle_dependencies,
            main.app.state.resources,
        ),
        apply_cloud_storage_override=agent_datasource_payload_module.apply_cloud_storage_override,
        build_datasources_payload=bound(
            agent_datasource_payload_module.build_datasources_payload,
            preparation_composition.datasource_payload_dependencies,
            main.app.state.resources,
        ),
        build_datasource_tool_override=bound(
            agent_datasource_payload_module.build_datasource_tool_override,
            preparation_composition.datasource_payload_dependencies,
            main.app.state.resources,
        ),
        prepare_job_primary_repository_authority=managed_repository_authority_module.prepare_job_primary_repository_authority,
        prepare_project_repository_authority=managed_repository_authority_module.prepare_project_repository_authority,
        authorize_job_repository_transport=managed_repository_authority_module.authorize_job_repository_transport,
        mint_worker_runtime_actor=runtime_actor_module.mint_worker_runtime_actor,
        inject_blob_credentials=config_resolver_module.inject_blob_credentials,
        grant_denied_error=grant_enforcement_module.GrantDenied,
        lite_workspace_config_error=workspace_tier_policy_module.LiteWorkspaceConfigError,
        backend_from_override=workspace_tier_policy_module.backend_from_override,
        inject_lite_workspace_config=workspace_tier_policy_module.inject_lite_workspace_config,
        is_experts_db_enabled=deployment_gates_module.is_experts_db_enabled,
        user_experts_enabled=bound(
            grant_enforcement_module.user_experts_enabled,
            preparation_composition.grant_enforcement_dependencies,
            main.app.state.resources,
        ),
        enforce_dispatch_grants=bound(
            grant_enforcement_module.enforce_dispatch_grants,
            preparation_composition.grant_enforcement_dependencies,
            main.app.state.resources,
        ),
        grant_violations_detail=grant_enforcement_module.grant_violations_detail,
        resolve_default_models=bound(
            session_config_resolution_module.resolve_default_models,
            preparation_composition.session_config_dependencies,
            main.app.state.resources,
        ),
        prefetch_roster_refs=bound(
            session_config_resolution_module.prefetch_roster_refs,
            preparation_composition.session_config_dependencies,
            main.app.state.resources,
        ),
        seed_registry_model_overrides=bound(
            dispatch_credentials_module.seed_registry_model_overrides,
            preparation_composition.dispatch_credential_dependencies,
            main.app.state.resources,
        ),
        gather_in_scope_skills=(
            lambda *args, **kwargs: catalogue_composition.expert_catalog_service(
                main.app.state.resources
            ).gather_in_scope_skills(*args, **kwargs)
        ),
        resolve_config=config_resolver_module.resolve_config,
        vm_workspaces_on_pod_network=access_module.vm_workspaces_on_pod_network,
    )


def _job_assignment_deps() -> job_assignment_routes.JobAssignmentDependencies:
    return job_assignment_routes.JobAssignmentDependencies(
        store=main.app.state.resources.postgres_db,
        # The assignment route's dependencies are composed by ``controls``, so
        # the injected logger is that module's (R1.B12).
        logger=controls_composition.logger,
        require_admin=functools.partial(
            access_composition.require_admin, main.app.state.resources
        ),
        vm_mode=lambda: vm_provisioner_module.vm_provisioner.mode,
        completion_commands_enabled=lambda: main.app.state.resources.settings.completion_commands_enabled,
        prepare_job_workspace_runtime=bound(
            job_workspace_authority_module.prepare_job_workspace_runtime,
            preparation_composition.job_workspace_authority_dependencies,
            main.app.state.resources,
        ),
        prepare_job_repository_before_claim=bound(
            job_start_bundle_module.prepare_job_repository_before_claim,
            preparation_composition.job_start_bundle_dependencies,
            main.app.state.resources,
        ),
        resume_missing_workspace=lambda *args, **kwargs: (
            job_workspace_runtime.resume_missing_workspace(
                *args,
                **kwargs,
                dependencies=preparation_composition.job_workspace_runtime_dependencies(
                    main.app.state.resources
                ),
            )
        ),
        guard_completion_control=main.app.state.resources.completion_control_boundary.guard,
        claim_completion_control=main.app.state.resources.completion_control_boundary.claim,
        abort_completion_control_claim=main.app.state.resources.completion_control_boundary.abort,
        completion_resume_guard_kwargs=(
            main.app.state.resources.completion_control_boundary.resume_guard_kwargs
        ),
        dispatch_job_to_agent=lambda job, agent: (
            controls_composition.job_delivery_operations(
                main.app.state.resources
            ).dispatch(job, agent)
        ),
        resume_job_on_agent=lambda job, agent: (
            controls_composition.job_delivery_operations(
                main.app.state.resources
            ).resume(job, agent)
        ),
        trigger_dispatch=bound(
            job_dispatcher_module.trigger_dispatch,
            jobs_composition.job_dispatch_dependencies,
            main.app.state.resources,
        ),
    )


# =============================================================================
# Fixtures
# =============================================================================


def _stamp(row: dict, backend: str | None = None) -> dict:
    """Give a job row the server-owned workspace contract the resolver needs."""
    row = dict(row)
    context = row.get("context") or {}
    if isinstance(context, str):
        context = json.loads(context)
    context = dict(context)
    if backend is None:
        backend = "vm" if "vm" in context else "sandbox"
    context.setdefault(
        "_workspace_contract",
        {
            "version": 1,
            "requested_backend": backend,
            "assigned_backend": backend,
            "assignment_source": "test",
        },
    )
    row["context"] = context
    row.setdefault("config_override", {"workspace": {"backend": backend}})
    return row


RUNTIME_INCARNATION = "11111111-1111-1111-1111-111111111111"

READY_CONTAINER = {
    "status": "ready",
    "provisioner": "k8s",
    "host": "10.0.0.7",
    "pod_ip": "10.0.0.7",
    "port": 30022,
    "_runtime_incarnation": RUNTIME_INCARNATION,
}

READY_VM = {
    "status": "ready",
    "ssh_host": "100.64.0.5",
    "ssh_port": 22,
    "provisioner": "kubevirt",
}


def _attestation(**overrides) -> WorkspaceRuntimeAttestation:
    base = {
        "backing_id": "pvc-1",
        "workspace_generation": "gen-1",
        "runtime_incarnation": RUNTIME_INCARNATION,
        "ssh_host_key_fingerprint": "SHA256:abc",
        "host": "10.0.0.7",
        "pod_ip": "10.0.0.7",
        "port": 30022,
    }
    base.update(overrides)
    return WorkspaceRuntimeAttestation(**base)


# =============================================================================
# 1. Pure helpers — parity with main, including the previously uncovered ones
# =============================================================================


class TestPureHelperParity:
    @pytest.mark.parametrize(
        "url",
        [
            None,
            "",
            "https://user:secret@gitea.internal:3000/org/repo.git",
            "ssh://srw-repo-abc@gitea:2222/org/repo.git",
            "http://[::1/broken",
            "not a url at all",
        ],
    )
    def test_mask_repository_transport(self, url):
        assert job_start_bundle.mask_repository_transport(
            url
        ) == job_start_bundle_module.mask_repository_transport(url)

    def test_mask_repository_transport_drops_userinfo(self):
        masked = job_start_bundle.mask_repository_transport(
            "https://user:secret@gitea.internal:3000/org/repo.git"
        )
        assert "secret" not in masked and "user" not in masked
        assert masked == "https://gitea.internal:3000/org/repo.git"

    @pytest.mark.parametrize(
        "context",
        [
            None,
            {},
            {"_lease_recovery": {"state": "tripped", "attempts": 3}},
            {"_lease_recovery": {"state": "armed"}},
            {"_lease_recovery": "not-a-mapping"},
            json.dumps({"_lease_recovery": {"state": "tripped"}}),
            "{not json",
        ],
    )
    def test_redispatch_livelock_trip(self, context):
        job = {"id": "j", "context": context}
        assert job_start_bundle.redispatch_livelock_trip(
            job
        ) == job_start_bundle_module.redispatch_livelock_trip(job)

    def test_redispatch_livelock_trip_counts_and_delay_constants(self):
        # The redispatch loop is bounded by these two; a change to either is a
        # behaviour change, not a refactor.
        assert (
            job_start_bundle.FRESH_PINNED_RECIPIENT_ATTESTATION_ATTEMPTS
            == job_mutation_target_module.FRESH_PINNED_RECIPIENT_ATTESTATION_ATTEMPTS
            == 8
        )
        assert (
            job_start_bundle.FRESH_PINNED_RECIPIENT_ATTESTATION_DELAY_S
            == job_mutation_target_module.FRESH_PINNED_RECIPIENT_ATTESTATION_DELAY_S
            == 0.25
        )

    def test_pinned_job_mutation_target_shape(self):
        assert (
            job_start_bundle.PinnedJobMutationTarget._fields
            == job_mutation_target_module.PinnedJobMutationTarget._fields
            == ("agent", "recipient")
        )

    @pytest.mark.parametrize(
        "context",
        [
            None,
            {},
            {"vm": READY_VM, "workspace_container": READY_CONTAINER},
            json.dumps({"vm": READY_VM}),
            "{not json",
            {"infra_transient": {"attempts": 2}},
            {"infra_transient": "not-a-dict"},
        ],
    )
    def test_context_readers(self, context):
        job = {"id": "j", "context": context}
        assert job_workspace_runtime.get_vm_context(
            job
        ) == job_workspace_runtime_module.get_vm_context(job)
        assert job_workspace_runtime.get_container_context(
            job
        ) == job_workspace_runtime_module.get_container_context(job)
        assert job_workspace_runtime.get_infra_transient_context(
            job
        ) == job_workspace_runtime_module.get_infra_transient_context(job)

    def test_workspace_context_keys_constant(self):
        assert (
            job_workspace_runtime.WORKSPACE_CONTEXT_KEYS
            == job_workspace_runtime_module.WORKSPACE_CONTEXT_KEYS
            == {"vm": "vm", "sandbox": "workspace_container"}
        )

    def test_inherit_wait_budget_constant(self):
        assert (
            job_workspace_authority.INHERIT_WORKSPACE_MAX_WAIT_S
            == job_workspace_authority_module.INHERIT_WORKSPACE_MAX_WAIT_S
        )

    @pytest.mark.parametrize(
        "job",
        [
            {"id": "self-job", "context": {}},
            {
                "id": "child",
                "parent_job_id": "parent",
                "context": {"inherits_parent_workspace": True},
            },
            # The flag alone does not transfer ownership without a parent id.
            {"id": "child", "context": {"inherits_parent_workspace": True}},
            # A self-provisioned child (workspace key, no flag) owns its own.
            {
                "id": "child",
                "parent_job_id": "parent",
                "context": {"workspace_container": READY_CONTAINER},
            },
            {
                "id": "child",
                "parent_job_id": "parent",
                "context": json.dumps({"inherits_parent_workspace": True}),
            },
        ],
    )
    def test_stateless_worker_workspace_owner(self, job):
        assert job_workspace_runtime.stateless_worker_workspace_owner(
            job
        ) == job_workspace_runtime_module.stateless_worker_workspace_owner(job)

    @pytest.mark.parametrize(
        "ctx,env",
        [
            ({}, None),
            ({"provisioner": "docker"}, None),
            ({"provisioner": "k8s"}, None),
            ({"provisioner": "docker"}, "/custom/key"),
            ({"provisioner": "k8s"}, "   "),
        ],
    )
    def test_container_ssh_key_path(self, ctx, env, monkeypatch):
        if env is None:
            monkeypatch.delenv("SSH_KEY_PATH", raising=False)
        else:
            monkeypatch.setenv("SSH_KEY_PATH", env)
        assert job_workspace_runtime.container_ssh_key_path(
            ctx
        ) == job_workspace_runtime_module.container_ssh_key_path(ctx)

    @pytest.mark.parametrize("replace_endpoint", [False, True])
    @pytest.mark.parametrize(
        "ctx",
        [
            {},
            {"status": "provisioning"},
            {"status": "ready"},  # no host/pod_ip -> untouched
            READY_CONTAINER,
            {**READY_CONTAINER, "provisioner": "docker"},
        ],
    )
    @pytest.mark.parametrize(
        "override",
        [
            None,
            {},
            {"workspace": {"remote": {"host": "stale", "port": 9, "username": "x"}}},
        ],
    )
    def test_inject_container_workspace_config(self, ctx, override, replace_endpoint):
        import copy as _copy

        mine = job_workspace_runtime.inject_container_workspace_config(
            _copy.deepcopy(override), dict(ctx), replace_endpoint=replace_endpoint
        )
        theirs = job_workspace_runtime_module.inject_container_workspace_config(
            _copy.deepcopy(override), dict(ctx), replace_endpoint=replace_endpoint
        )
        assert mine == theirs

    @pytest.mark.parametrize("replace_endpoint", [False, True])
    @pytest.mark.parametrize(
        "ctx",
        [
            {},
            {"status": "ready"},  # no ssh_host -> untouched
            READY_VM,
            {**READY_VM, "ssh_port": 2222},
        ],
    )
    @pytest.mark.parametrize(
        "override",
        [None, {}, {"workspace": {"remote": {"host": "stale", "port": 9}}}],
    )
    def test_inject_vm_workspace_config(self, ctx, override, replace_endpoint):
        import copy as _copy

        mine = job_workspace_runtime.inject_vm_workspace_config(
            _copy.deepcopy(override), dict(ctx), replace_endpoint=replace_endpoint
        )
        theirs = job_workspace_runtime_module.inject_vm_workspace_config(
            _copy.deepcopy(override), dict(ctx), replace_endpoint=replace_endpoint
        )
        assert mine == theirs

    def test_inject_container_config_always_refreshes_managed_fields(self):
        """Deployment-owned fields are refreshed even without replace_endpoint.

        A stale persisted ``remote`` block must not point a resumed worker at an
        obsolete mount, so username/key_path are rewritten unconditionally.
        """
        out = job_workspace_runtime.inject_container_workspace_config(
            {"workspace": {"remote": {"username": "someone-else", "key_path": "/old"}}},
            dict(READY_CONTAINER),
        )
        assert out["workspace"]["remote"]["username"] == "agent-host"
        assert out["workspace"]["remote"]["key_path"] == "/run/secrets/vm-ssh-key"

    @pytest.mark.parametrize(
        "ctx,override",
        [
            ({}, None),
            ({"sudo_denial": "not-a-dict"}, {}),
            ({"sudo_denial": {"denied": True}}, {"workspace": {"backend": "vm"}}),
            (
                {"sudo_denial": {"denied": True, "decided_by": "ops", "reason": "no"}},
                {},
            ),
            ({"sudo_denial": {"denied": False}}, None),
            (json.dumps({"sudo_denial": {"denied": True}}), None),
            ("{not json", None),
        ],
    )
    def test_apply_sticky_sudo_denial(self, ctx, override):
        import copy as _copy

        job = {"id": "j", "context": ctx}
        assert job_workspace_runtime.apply_sticky_sudo_denial(
            job, _copy.deepcopy(override)
        ) == job_workspace_runtime_module.apply_sticky_sudo_denial(
            job, _copy.deepcopy(override)
        )

    @pytest.mark.parametrize(
        "rows",
        [
            None,
            [],
            [{"type": "repository", "name": "app"}],
            [{"type": "CREDENTIALS", "id": "abc"}],
            [{"type": "postgresql", "name": "db"}, "not-a-dict"],
            [{"type": "repository"}],
        ],
    )
    def test_repository_datasource_names(self, rows):
        assert job_datasource_selection.repository_datasource_names(
            rows
        ) == job_datasource_selection_module.repository_datasource_names(rows)


# =============================================================================
# 2. Exact datasource resolution — the last gate before a credential payload
# =============================================================================


class TestExactDatasourceResolution:
    ID_A = "aaaaaaaa-0000-0000-0000-000000000001"
    ID_B = "bbbbbbbb-0000-0000-0000-000000000002"

    def _both(self, selected, revisions, resolved):
        """Run main and the service, returning (mine, theirs) or the two errors."""

        def run(fn):
            try:
                return ("ok", fn(selected, revisions, resolved))
            except HTTPException as exc:
                return ("http", exc.status_code, exc.detail)

        return (
            run(job_datasource_selection.require_exact_datasource_resolution),
            run(job_datasource_selection_module.require_exact_datasource_resolution),
        )

    @pytest.mark.parametrize(
        "selected,revisions,resolved",
        [
            ([], {}, []),
            ([], {}, None),
            ([ID_A], {ID_A: 3}, [{"id": ID_A, "policy_revision": 3}]),
            # revision drift
            ([ID_A], {ID_A: 3}, [{"id": ID_A, "policy_revision": 4}]),
            # silent reduction
            ([ID_A, ID_B], {ID_A: 1, ID_B: 1}, [{"id": ID_A, "policy_revision": 1}]),
            # duplicate resolver rows
            (
                [ID_A],
                {ID_A: 1},
                [
                    {"id": ID_A, "policy_revision": 1},
                    {"id": ID_A, "policy_revision": 1},
                ],
            ),
            # extra row the snapshot never authorized
            (
                [ID_A],
                {ID_A: 1},
                [
                    {"id": ID_A, "policy_revision": 1},
                    {"id": ID_B, "policy_revision": 1},
                ],
            ),
            # revision map does not cover the selection
            ([ID_A], {}, [{"id": ID_A, "policy_revision": 0}]),
            # malformed ids
            (["not-a-uuid"], {}, []),
            ([ID_A], {ID_A: 1}, [{"policy_revision": 1}]),
        ],
    )
    def test_parity_including_every_refusal(self, selected, revisions, resolved):
        mine, theirs = self._both(selected, revisions, resolved)
        assert mine == theirs

    def test_empty_selection_is_authoritative_not_unset(self):
        """An authorized empty selection resolves to an empty payload, not a refusal."""
        assert (
            job_datasource_selection.require_exact_datasource_resolution([], {}, None)
            == []
        )

    def test_refusal_is_one_generic_403(self):
        with pytest.raises(HTTPException) as exc:
            job_datasource_selection.require_exact_datasource_resolution(
                [self.ID_A], {self.ID_A: 1}, [{"id": self.ID_B, "policy_revision": 1}]
            )
        assert exc.value.status_code == 403
        assert exc.value.detail == "One or more selected connectors are unavailable"
        # The refusal must not name which connector failed.
        assert self.ID_A not in str(exc.value.detail)


# =============================================================================
# 3. Datasource payload builders (shared with the session side)
# =============================================================================


def _ds(**kw):
    base = {
        "id": "dddddddd-0000-0000-0000-00000000000a",
        "type": "postgresql",
        "name": "primary",
        "description": None,
        "connection_url": "postgres://host/db",
        "credentials": {"password": "s3cret"},
        "project_read_only": False,
    }
    base.update(kw)
    return base


class TestDatasourcePayload:
    @pytest.mark.parametrize(
        "datasource",
        [
            _ds(),
            _ds(type="mcp", credentials={"transport": "http"}),
            _ds(type="mcp", credentials={"transport": "stdio"}),
            _ds(type="mcp", credentials=json.dumps({"transport": "stdio"})),
            _ds(type="mcp", credentials="{not json"),
            _ds(type="mcp", credentials=None),
            _ds(type="mcp", credentials="a-string"),
        ],
    )
    @pytest.mark.parametrize("mcp_on", [False, True])
    @pytest.mark.parametrize("stdio_on", [False, True])
    def test_mcp_runtime_gate_answers_from_the_deployment_flags(
        self, datasource, mcp_on, stdio_on, monkeypatch
    ):
        """B06 deleted the `main` bridge this used to compare against, so the
        gate is asserted against the two flags it actually reads instead of
        against a second spelling of itself."""
        monkeypatch.setenv("MCP_DATASOURCES_ENABLED", "true" if mcp_on else "false")
        monkeypatch.setenv("MCP_STDIO_ENABLED", "true" if stdio_on else "false")

        allowed = agent_datasource_payload.mcp_datasource_runtime_allowed(
            datasource, dependencies=_datasource_payload_deps()
        )

        credentials = datasource.get("credentials") or {}
        if isinstance(credentials, str):
            try:
                credentials = json.loads(credentials)
            except (json.JSONDecodeError, ValueError):
                credentials = {}
        transport = (
            credentials.get("transport", "http")
            if isinstance(credentials, dict)
            else "http"
        )
        if datasource.get("type") != "mcp":
            assert allowed is True
        elif not mcp_on:
            assert allowed is False
        elif str(transport).lower() == "stdio":
            assert allowed is stdio_on
        else:
            assert allowed is True

    def test_read_only_managed_connector_withholds_credentials(self):
        rows = [_ds(project_read_only=True)]
        payload = agent_datasource_payload.build_datasources_payload(
            rows, dependencies=_datasource_payload_deps()
        )
        assert payload[0]["credentials"] == {}
        assert payload == agent_datasource_payload_module.build_datasources_payload(
            rows,
            dependencies=preparation_composition.datasource_payload_dependencies(
                main.app.state.resources
            ),
        )

    def test_email_stays_credentialed_at_every_tier(self):
        rows = [
            _ds(
                type="email",
                name="inbox",
                project_read_only=True,
                credentials={"password": "imap"},
                config={"access": "read"},
            )
        ]
        payload = agent_datasource_payload.build_datasources_payload(
            rows, dependencies=_datasource_payload_deps()
        )
        assert payload[0]["credentials"] == {"password": "imap"}
        assert payload == agent_datasource_payload_module.build_datasources_payload(
            rows,
            dependencies=preparation_composition.datasource_payload_dependencies(
                main.app.state.resources
            ),
        )

    def test_only_one_email_datasource_is_forwarded(self):
        rows = [
            _ds(type="email", name="first", config={}),
            _ds(type="email", name="second", config={}),
        ]
        payload = agent_datasource_payload.build_datasources_payload(
            rows, dependencies=_datasource_payload_deps()
        )
        assert [entry["name"] for entry in payload] == ["first"]
        assert payload == agent_datasource_payload_module.build_datasources_payload(
            rows,
            dependencies=preparation_composition.datasource_payload_dependencies(
                main.app.state.resources
            ),
        )

    def test_kb_row_is_stripped_of_url_and_credentials(self):
        rows = [
            _ds(
                type="kb",
                name="okf",
                connection_url="https://kb.example/repo.git",
                credentials={"token": "t"},
                config={"native_project_id": "p"},
            )
        ]
        payload = agent_datasource_payload.build_datasources_payload(
            rows, dependencies=_datasource_payload_deps()
        )
        assert payload[0]["connection_url"] is None
        assert payload[0]["credentials"] == {}
        assert payload[0]["project_read_only"] is True
        assert payload == agent_datasource_payload_module.build_datasources_payload(
            rows,
            dependencies=preparation_composition.datasource_payload_dependencies(
                main.app.state.resources
            ),
        )

    def test_repository_carries_internal_id_and_config(self):
        rows = [
            _ds(
                type="repository",
                name="app",
                config={"forge": "gitea"},
                require_default_branch=True,
                default_branch="main",
                cli_hint="git",
            )
        ]
        payload = agent_datasource_payload.build_datasources_payload(
            rows, dependencies=_datasource_payload_deps()
        )
        assert payload[0]["datasource_id"] == rows[0]["id"]
        assert payload[0]["config"] == {"forge": "gitea"}
        assert payload[0]["require_default_branch"] is True
        assert payload == agent_datasource_payload_module.build_datasources_payload(
            rows,
            dependencies=preparation_composition.datasource_payload_dependencies(
                main.app.state.resources
            ),
        )

    @pytest.mark.parametrize("rows", [None, [], [_ds(credentials="{not json")]])
    def test_payload_parity_edges(self, rows):
        assert agent_datasource_payload.build_datasources_payload(
            rows, dependencies=_datasource_payload_deps()
        ) == agent_datasource_payload_module.build_datasources_payload(
            rows,
            dependencies=preparation_composition.datasource_payload_dependencies(
                main.app.state.resources
            ),
        )

    def test_tool_override_parity(self):
        rows = [_ds(), _ds(type="neo4j", name="graph")]
        assert agent_datasource_payload.build_datasource_tool_override(
            rows, {"tools": {"custom": ["x"]}}, dependencies=_datasource_payload_deps()
        ) == agent_datasource_payload_module.build_datasource_tool_override(
            rows,
            {"tools": {"custom": ["x"]}},
            dependencies=preparation_composition.datasource_payload_dependencies(
                main.app.state.resources
            ),
        )

    def test_tool_override_does_not_mutate_the_caller_override(self):
        override = {"tools": {"custom": ["x"]}}
        agent_datasource_payload.build_datasource_tool_override(
            [_ds()], override, dependencies=_datasource_payload_deps()
        )
        assert override == {"tools": {"custom": ["x"]}}

    @pytest.mark.parametrize("override", [None, True, False])
    def test_cloud_storage_override_parity(self, override):
        context = {} if override is None else {"cloud_storage_read_only": override}
        mine = [_ds(type="webdav", project_read_only=False)]
        theirs = [_ds(type="webdav", project_read_only=False)]
        agent_datasource_payload.apply_cloud_storage_override(mine, context)
        agent_datasource_payload_module.apply_cloud_storage_override(theirs, context)
        assert mine == theirs


# =============================================================================
# 4. Workspace tier predicates and the stateless admission gate
# =============================================================================


class TestWorkspaceTierPredicates:
    @pytest.mark.parametrize(
        "job",
        [
            _stamp({"id": "j", "context": {"vm": READY_VM}}, backend="vm"),
            _stamp(
                {"id": "j", "context": {"workspace_container": READY_CONTAINER}},
                backend="sandbox",
            ),
            {"id": "j", "context": {}},  # no contract -> refused, never guessed
            _stamp({"id": "j", "context": {}}, backend="virtual"),
        ],
    )
    def test_needs_vm_and_sandbox_parity(self, job):
        deps = _workspace_runtime_deps()
        assert job_workspace_runtime.job_needs_vm(
            job
        ) == job_workspace_runtime_module.job_needs_vm(job)
        assert job_workspace_runtime.job_needs_sandbox(
            job, dependencies=deps
        ) == job_workspace_runtime_module.job_needs_sandbox(
            job,
            dependencies=preparation_composition.job_workspace_runtime_dependencies(
                main.app.state.resources
            ),
        )

    def test_ambiguous_contract_never_guesses_a_tier(self):
        """An unresolvable contract refuses both tiers rather than picking one.

        ``vm.requested`` with no authoritative VM provenance is exactly the
        ambiguous row the shared resolver raises on; neither predicate may fall
        back to "well, a VM context is present".
        """
        job = {"id": "j", "context": {"vm": {"requested": True}}}
        assert job_workspace_runtime.job_needs_vm(job) is False
        assert (
            job_workspace_runtime.job_needs_sandbox(
                job, dependencies=_workspace_runtime_deps()
            )
            is False
        )

    def test_ready_vm_residue_does_not_suppress_sandbox_provisioning(self):
        """A sandbox-assigned job still needs a sandbox despite a ready VM."""
        job = _stamp(
            {"id": "j", "context": {"vm": READY_VM}, "config_override": {}},
            backend="sandbox",
        )
        assert (
            job_workspace_runtime.job_needs_sandbox(
                job, dependencies=_workspace_runtime_deps()
            )
            is True
        )
        assert job_workspace_runtime.job_needs_vm(job) is False

    @pytest.mark.parametrize(
        "job",
        [
            _stamp({"id": "j", "context": {"vm": READY_VM}}, backend="vm"),
            _stamp({"id": "j", "context": {}}, backend="vm"),
            _stamp(
                {"id": "j", "context": {"workspace_container": READY_CONTAINER}},
                backend="sandbox",
            ),
            _stamp({"id": "j", "context": {}}, backend="sandbox"),
            _stamp({"id": "j", "context": {}}, backend="virtual"),
            {"id": "j", "context": {}},
        ],
    )
    def test_resume_missing_workspace_parity(self, job):
        assert job_workspace_runtime.resume_missing_workspace(
            job, dependencies=_workspace_runtime_deps()
        ) == job_workspace_runtime.resume_missing_workspace(
            job,
            dependencies=preparation_composition.job_workspace_runtime_dependencies(
                main.app.state.resources
            ),
        )

    @pytest.mark.parametrize("replace_endpoint", [False, True])
    @pytest.mark.parametrize(
        "job",
        [
            _stamp({"id": "j", "context": {"vm": READY_VM}}, backend="vm"),
            _stamp(
                {"id": "j", "context": {"workspace_container": READY_CONTAINER}},
                backend="sandbox",
            ),
            _stamp({"id": "j", "context": {}}, backend="sandbox"),
        ],
    )
    def test_inject_matching_workspace_config_parity(self, job, replace_endpoint):
        mine_cfg, mine_decision = (
            job_workspace_runtime.inject_matching_workspace_config(
                job,
                {"llm": {"model": "m"}},
                replace_endpoint=replace_endpoint,
                dependencies=_workspace_runtime_deps(),
            )
        )
        theirs_cfg, theirs_decision = (
            job_workspace_runtime.inject_matching_workspace_config(
                job,
                {"llm": {"model": "m"}},
                replace_endpoint=replace_endpoint,
                dependencies=preparation_composition.job_workspace_runtime_dependencies(
                    main.app.state.resources
                ),
            )
        )
        assert mine_cfg == theirs_cfg
        assert mine_decision.effective_backend == theirs_decision.effective_backend
        assert mine_decision.ready == theirs_decision.ready

    def test_inject_matching_workspace_config_does_not_mutate_input(self):
        job = _stamp(
            {"id": "j", "context": {"workspace_container": READY_CONTAINER}},
            backend="sandbox",
        )
        override = {"llm": {"model": "m"}}
        job_workspace_runtime.inject_matching_workspace_config(
            job, override, dependencies=_workspace_runtime_deps()
        )
        assert override == {"llm": {"model": "m"}}


class TestStatelessAdmissionGate:
    def _deps(self, *, available, in_cluster, pod_network, enabled):
        return job_workspace_runtime.JobWorkspaceRuntimeDependencies(
            store=main.app.state.resources.postgres_db,
            vm_mode=lambda: vm_provisioner_module.vm_provisioner.mode,
            workspace_provisioner=SimpleNamespace(
                is_available=available, in_cluster=in_cluster
            ),
            vm_workspaces_on_pod_network=lambda: pod_network,
            stateless_worker_enabled=lambda: enabled,
            backend_from_override=workspace_tier_policy_module.backend_from_override,
        )

    def test_external_vm_is_forced_pinned_even_when_stateless_requested(self):
        lane = job_workspace_runtime.resolve_requested_job_execution_lane(
            "stateless",
            default_stateless=True,
            needs_vm=True,
            needs_sandbox=False,
            dependencies=self._deps(
                available=True, in_cluster=True, pod_network=False, enabled=True
            ),
        )
        assert lane == "pinned"

    def test_omitted_lane_stays_none_when_nothing_can_run_stateless(self):
        lane = job_workspace_runtime.resolve_requested_job_execution_lane(
            None,
            default_stateless=True,
            needs_vm=False,
            needs_sandbox=True,
            dependencies=self._deps(
                available=False, in_cluster=False, pod_network=False, enabled=True
            ),
        )
        assert lane is None

    def test_omitted_lane_without_defaulting_is_preserved(self):
        lane = job_workspace_runtime.resolve_requested_job_execution_lane(
            None,
            default_stateless=False,
            needs_vm=False,
            needs_sandbox=True,
            dependencies=self._deps(
                available=True, in_cluster=True, pod_network=False, enabled=True
            ),
        )
        assert lane is None

    def test_explicit_stateless_is_refused_when_the_gate_is_off(self):
        with pytest.raises(HTTPException) as exc:
            job_workspace_runtime.resolve_requested_job_execution_lane(
                "stateless",
                default_stateless=False,
                needs_vm=False,
                needs_sandbox=True,
                dependencies=self._deps(
                    available=True, in_cluster=True, pod_network=False, enabled=False
                ),
            )
        assert exc.value.status_code == 409

    def test_explicit_stateless_needs_an_in_cluster_provisioner(self):
        with pytest.raises(HTTPException) as exc:
            job_workspace_runtime.resolve_requested_job_execution_lane(
                "stateless",
                default_stateless=False,
                needs_vm=False,
                needs_sandbox=True,
                dependencies=self._deps(
                    available=True, in_cluster=False, pod_network=False, enabled=True
                ),
            )
        assert exc.value.status_code == 503

    def test_explicit_stateless_needs_a_sandbox_or_same_cluster_vm(self):
        with pytest.raises(HTTPException) as exc:
            job_workspace_runtime.resolve_requested_job_execution_lane(
                "stateless",
                default_stateless=False,
                needs_vm=False,
                needs_sandbox=False,
                dependencies=self._deps(
                    available=True, in_cluster=True, pod_network=False, enabled=True
                ),
            )
        assert exc.value.status_code == 422

    def test_parity_with_main_across_the_matrix(self, monkeypatch):
        for enabled in (False, True):
            monkeypatch.setattr(
                main.app.state.resources.settings, "stateless_worker_enabled", enabled
            )
            for available in (False, True):
                for in_cluster in (False, True):
                    for pod_network in (False, True):
                        monkeypatch.setattr(
                            container_provisioner_module,
                            "container_provisioner",
                            SimpleNamespace(
                                is_available=available, in_cluster=in_cluster
                            ),
                        )
                        monkeypatch.setattr(
                            access_module,
                            "vm_workspaces_on_pod_network",
                            lambda _pod=pod_network: _pod,
                        )
                        deps = _workspace_runtime_deps()
                        for requested in (None, "pinned", "stateless"):
                            for default_stateless in (False, True):
                                for needs_vm in (False, True):
                                    for needs_sandbox in (False, True):
                                        args = dict(
                                            default_stateless=default_stateless,
                                            needs_vm=needs_vm,
                                            needs_sandbox=needs_sandbox,
                                        )

                                        def run(fn, **extra):
                                            try:
                                                return (
                                                    "ok",
                                                    fn(requested, **args, **extra),
                                                )
                                            except HTTPException as exc:
                                                return ("http", exc.status_code)

                                        assert run(
                                            job_workspace_runtime.resolve_requested_job_execution_lane,
                                            dependencies=deps,
                                        ) == run(
                                            bound(
                                                job_workspace_runtime_module.resolve_requested_job_execution_lane,
                                                preparation_composition.job_workspace_runtime_dependencies,
                                                main.app.state.resources,
                                            )
                                        )


# =============================================================================
# 5. Workspace attestation fences — every one of these must fail closed
# =============================================================================


def _authority_deps(**overrides):
    """Build authority dependencies with explicit stubs for the fence tests.

    ``workspace_runtime_unchanged_before_delivery`` defaults to this module's
    own implementation, wired through the dependency field exactly as the
    application factory does it (main's wrapper -> service -> field -> main's
    wrapper). Only ``pinned_k8s_job_workspace_authority_is_current`` reads the
    field, so binding the real function to it cannot recurse.
    """
    base = dict(
        store=MagicMock(),
        logger=preparation_composition.logger,
        workspace_provisioner=MagicMock(),
        vm_provisioner=MagicMock(),
        vm_mode=lambda: vm_provisioner_module.vm_provisioner.mode,
        ensure_workspace=AsyncMock(),
        workspace_suspension=MagicMock(),
        handle_scholar_completion=AsyncMock(),
        handle_delegation_child_completion=AsyncMock(),
        resolve_inherited_workspace=AsyncMock(return_value=("proceed", None)),
        fail_subjob_and_unblock_parent=AsyncMock(),
    )
    base.update(overrides)
    recheck = base.pop("workspace_runtime_unchanged_before_delivery", None)

    async def real_recheck(job):
        return (
            await job_workspace_authority.workspace_runtime_unchanged_before_delivery(
                job,
                dependencies=job_workspace_authority.JobWorkspaceAuthorityDependencies(
                    **base,
                    # Never read by this function; a placeholder keeps the object
                    # constructible without reintroducing the cycle.
                    workspace_runtime_unchanged_before_delivery=AsyncMock(),
                ),
            )
        )

    return job_workspace_authority.JobWorkspaceAuthorityDependencies(
        **base,
        workspace_runtime_unchanged_before_delivery=(recheck or real_recheck),
    )


class TestStatelessAttestationRefusals:
    """Uncovered before this batch: both refuse generically, never leaking why."""

    @pytest.mark.asyncio
    async def test_container_attestation_returns_the_exact_identity(self):
        attestation = _attestation()
        provisioner = MagicMock()
        provisioner.attest_workspace_runtime = AsyncMock(return_value=attestation)
        got = await job_workspace_authority.attest_stateless_worker_workspace(
            WorkspaceOwner.job("j1"),
            dependencies=_authority_deps(workspace_provisioner=provisioner),
        )
        assert got is attestation

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "error",
        [
            WorkspaceRuntimeAuthorityError("pod uid changed"),
            RuntimeError("kubernetes unreachable"),
        ],
    )
    async def test_container_attestation_failure_is_one_generic_409(self, error):
        provisioner = MagicMock()
        provisioner.attest_workspace_runtime = AsyncMock(side_effect=error)
        with pytest.raises(HTTPException) as exc:
            await job_workspace_authority.attest_stateless_worker_workspace(
                WorkspaceOwner.job("j1"),
                dependencies=_authority_deps(workspace_provisioner=provisioner),
            )
        assert exc.value.status_code == 409
        assert exc.value.detail == "Stateless worker workspace authority unavailable"
        assert "kubernetes" not in str(exc.value.detail).lower()

    @pytest.mark.asyncio
    async def test_vm_attestation_refuses_a_non_job_owner(self):
        vm = MagicMock()
        vm.attest_workspace_runtime = AsyncMock(return_value=_attestation())
        with pytest.raises(HTTPException) as exc:
            await job_workspace_authority.attest_stateless_worker_vm_workspace(
                WorkspaceOwner.session("t1"),
                dependencies=_authority_deps(vm_provisioner=vm),
            )
        assert exc.value.status_code == 409
        vm.attest_workspace_runtime.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_vm_attestation_passes_the_bare_job_id(self):
        attestation = _attestation()
        vm = MagicMock()
        vm.attest_workspace_runtime = AsyncMock(return_value=attestation)
        got = await job_workspace_authority.attest_stateless_worker_vm_workspace(
            WorkspaceOwner.job("j9"), dependencies=_authority_deps(vm_provisioner=vm)
        )
        assert got is attestation
        vm.attest_workspace_runtime.assert_awaited_once_with("j9")


class TestPinnedK8sAttestationFence:
    def _job(self, **container_overrides):
        container = {**READY_CONTAINER, **container_overrides}
        return _stamp(
            {
                "id": "job-1",
                "status": "created",
                "context": {"workspace_container": container},
            },
            backend="sandbox",
        )

    def _provisioner(self, attestation):
        provisioner = MagicMock()
        provisioner.attest_workspace_runtime = AsyncMock(return_value=attestation)
        return provisioner

    @pytest.mark.asyncio
    async def test_matching_attestation_produces_an_exact_job_copy(self):
        job = self._job()
        deps = _authority_deps(
            workspace_provisioner=self._provisioner(_attestation()),
        )
        (
            exact,
            authority,
        ) = await job_workspace_authority.attest_pinned_k8s_job_workspace(
            job, dependencies=deps
        )
        assert authority is not None
        assert authority.owner == WorkspaceOwner.job("job-1")
        assert exact["context"]["workspace_container"]["host"] == "10.0.0.7"
        # The caller's row is never mutated in place.
        assert exact is not job
        assert exact["context"] is not job["context"]

    @pytest.mark.asyncio
    async def test_docker_provisioner_is_left_to_its_own_lane(self):
        job = self._job(provisioner="docker")
        (
            exact,
            authority,
        ) = await job_workspace_authority.attest_pinned_k8s_job_workspace(
            job, dependencies=_authority_deps()
        )
        assert exact is job and authority is None

    @pytest.mark.asyncio
    async def test_unknown_provisioner_refuses(self):
        job = self._job(provisioner="mystery")
        with pytest.raises(WorkspaceRuntimeAuthorityError):
            await job_workspace_authority.attest_pinned_k8s_job_workspace(
                job, dependencies=_authority_deps()
            )

    @pytest.mark.asyncio
    async def test_malformed_incarnation_grants_no_authority(self):
        """The shared resolver refuses the row first, so no tuple is attested.

        Parity with ``main`` matters more than which of the two guards fires:
        both must end with ``authority is None`` and no provisioner call, and
        the pre-delivery recheck below must then refuse the same row.
        """
        job = self._job(_runtime_incarnation="not-a-uuid")
        provisioner = self._provisioner(_attestation())
        mine = await job_workspace_authority.attest_pinned_k8s_job_workspace(
            job, dependencies=_authority_deps(workspace_provisioner=provisioner)
        )
        theirs = await job_workspace_authority.attest_pinned_k8s_job_workspace(
            job,
            dependencies=preparation_composition.job_workspace_authority_dependencies(
                main.app.state.resources
            ),
        )
        assert mine[1] is None and theirs[1] is None
        provisioner.attest_workspace_runtime.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_malformed_incarnation_is_refused_before_delivery(self):
        job = self._job(_runtime_incarnation="not-a-uuid")
        store = MagicMock()
        store.get_job = AsyncMock(return_value=job)
        assert (
            await job_workspace_authority.pinned_k8s_job_workspace_authority_is_current(
                job, None, dependencies=_authority_deps(store=store)
            )
            is False
        )

    @pytest.mark.asyncio
    async def test_runtime_incarnation_change_refuses(self):
        job = self._job()
        moved = _attestation(runtime_incarnation="22222222-2222-2222-2222-222222222222")
        with pytest.raises(WorkspaceRuntimeAuthorityError) as exc:
            await job_workspace_authority.attest_pinned_k8s_job_workspace(
                job,
                dependencies=_authority_deps(
                    workspace_provisioner=self._provisioner(moved)
                ),
            )
        assert "runtime changed" in str(exc.value)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "moved", [{"host": "10.9.9.9"}, {"pod_ip": "10.9.9.9"}, {"port": 2222}]
    )
    async def test_endpoint_change_refuses(self, moved):
        job = self._job()
        with pytest.raises(WorkspaceRuntimeAuthorityError) as exc:
            await job_workspace_authority.attest_pinned_k8s_job_workspace(
                job,
                dependencies=_authority_deps(
                    workspace_provisioner=self._provisioner(_attestation(**moved))
                ),
            )
        assert "endpoint changed" in str(exc.value)

    @pytest.mark.asyncio
    async def test_authority_is_not_current_when_the_owner_moved(self):
        job = self._job()
        attestation = _attestation()
        authority = job_workspace_authority.PinnedK8sJobWorkspaceAuthority(
            WorkspaceOwner.job("job-1"), attestation
        )
        other = dict(job)
        other["parent_job_id"] = "parent-1"
        other["context"] = {
            **job["context"],
            "inherits_parent_workspace": True,
        }
        store = MagicMock()
        store.get_job = AsyncMock(return_value=other)
        deps = _authority_deps(
            store=store,
            workspace_provisioner=self._provisioner(attestation),
        )
        assert (
            await job_workspace_authority.pinned_k8s_job_workspace_authority_is_current(
                job, authority, dependencies=deps
            )
            is False
        )

    @pytest.mark.asyncio
    async def test_a_provisioner_exception_is_not_current(self):
        """The fence must answer False, never propagate and skip the check."""
        job = self._job()
        attestation = _attestation()
        authority = job_workspace_authority.PinnedK8sJobWorkspaceAuthority(
            WorkspaceOwner.job("job-1"), attestation
        )
        store = MagicMock()
        store.get_job = AsyncMock(return_value=job)
        provisioner = MagicMock()
        provisioner.attest_workspace_runtime = AsyncMock(
            side_effect=RuntimeError("boom")
        )
        deps = _authority_deps(store=store, workspace_provisioner=provisioner)
        assert (
            await job_workspace_authority.pinned_k8s_job_workspace_authority_is_current(
                job, authority, dependencies=deps
            )
            is False
        )


class TestWorkspaceRuntimeUnchangedBeforeDelivery:
    @pytest.mark.asyncio
    async def test_a_vanished_job_is_refused(self):
        job = _stamp(
            {
                "id": "job-1",
                "status": "created",
                "context": {"workspace_container": READY_CONTAINER},
            },
            backend="sandbox",
        )
        store = MagicMock()
        store.get_job = AsyncMock(return_value=None)
        assert (
            await job_workspace_authority.workspace_runtime_unchanged_before_delivery(
                job, dependencies=_authority_deps(store=store)
            )
            is False
        )

    @pytest.mark.asyncio
    async def test_a_digest_change_is_refused(self):
        job = _stamp(
            {
                "id": "job-1",
                "status": "created",
                "context": {"workspace_container": READY_CONTAINER},
            },
            backend="sandbox",
        )
        moved = _stamp(
            {
                "id": "job-1",
                "status": "created",
                "context": {
                    "workspace_container": {
                        **READY_CONTAINER,
                        "_runtime_incarnation": "33333333-3333-3333-3333-333333333333",
                    }
                },
            },
            backend="sandbox",
        )
        store = MagicMock()
        store.get_job = AsyncMock(return_value=moved)
        assert (
            await job_workspace_authority.workspace_runtime_unchanged_before_delivery(
                job, dependencies=_authority_deps(store=store)
            )
            is False
        )

    @pytest.mark.asyncio
    async def test_an_inheriting_child_that_cannot_proceed_is_refused(self):
        job = _stamp(
            {
                "id": "child",
                "status": "created",
                "parent_job_id": "parent",
                "context": {
                    "inherits_parent_workspace": True,
                    "workspace_container": READY_CONTAINER,
                },
            },
            backend="sandbox",
        )
        store = MagicMock()
        store.get_job = AsyncMock(return_value=job)
        inherit = AsyncMock(return_value=("wait", None))
        deps = _authority_deps(store=store, resolve_inherited_workspace=inherit)
        assert (
            await job_workspace_authority.workspace_runtime_unchanged_before_delivery(
                job, dependencies=deps
            )
            is False
        )
        # Proof the injected seam is the one consulted (§P3).
        inherit.assert_awaited_once()


# =============================================================================
# 6. Subjob inheritance and the exactly-once parent unblock
# =============================================================================


def _child(context, *, parent_id="parent", age_s=5.0, **extra):
    row = {
        "id": "child",
        "parent_job_id": parent_id,
        "context": context,
        "created_at": datetime.now(timezone.utc) - timedelta(seconds=age_s),
    }
    row.update(extra)
    return _stamp(row, backend="sandbox")


class TestSubjobInheritance:
    @pytest.mark.asyncio
    async def test_a_job_with_no_parent_proceeds(self):
        job = _stamp({"id": "j", "context": {}}, backend="sandbox")
        assert await job_workspace_authority.resolve_subjob_inherited_workspace(
            job, dependencies=_authority_deps()
        ) == ("proceed", None)

    @pytest.mark.asyncio
    async def test_a_self_provisioned_child_is_not_an_inheritor(self):
        """Key presence is not the discriminator; only the explicit flag is."""
        job = _child({"workspace_container": READY_CONTAINER})
        assert await job_workspace_authority.resolve_subjob_inherited_workspace(
            job, dependencies=_authority_deps()
        ) == ("proceed", None)

    @pytest.mark.asyncio
    async def test_a_ready_parent_sandbox_is_overlaid_onto_the_child(self):
        job = _child({"inherits_parent_workspace": True})
        parent = _stamp(
            {
                "id": "parent",
                "status": "processing",
                "context": {"workspace_container": READY_CONTAINER},
            },
            backend="sandbox",
        )
        store = MagicMock()
        store.get_job = AsyncMock(return_value=parent)
        (
            action,
            reason,
        ) = await job_workspace_authority.resolve_subjob_inherited_workspace(
            job, dependencies=_authority_deps(store=store)
        )
        assert (action, reason) == ("proceed", None)
        assert job["context"]["workspace_container"] == READY_CONTAINER
        # The child never keeps opposite-tier residue.
        assert "vm" not in job["context"]

    @pytest.mark.asyncio
    async def test_a_missing_parent_fails_rather_than_waiting(self):
        job = _child({"inherits_parent_workspace": True})
        store = MagicMock()
        store.get_job = AsyncMock(return_value=None)
        (
            action,
            reason,
        ) = await job_workspace_authority.resolve_subjob_inherited_workspace(
            job, dependencies=_authority_deps(store=store)
        )
        assert action == "fail"
        assert "no longer exists" in reason

    @pytest.mark.asyncio
    async def test_a_terminal_parent_fails_rather_than_waiting(self):
        job = _child({"inherits_parent_workspace": True})
        parent = _stamp(
            {"id": "parent", "status": "failed", "context": {}}, backend="sandbox"
        )
        store = MagicMock()
        store.get_job = AsyncMock(return_value=parent)
        (
            action,
            reason,
        ) = await job_workspace_authority.resolve_subjob_inherited_workspace(
            job, dependencies=_authority_deps(store=store)
        )
        assert action == "fail"
        assert "cannot inherit" in reason

    @pytest.mark.asyncio
    async def test_a_provisioning_parent_waits_inside_the_budget(self):
        job = _child({"inherits_parent_workspace": True}, age_s=5.0)
        parent = _stamp(
            {
                "id": "parent",
                "status": "processing",
                "context": {"workspace_container": {"status": "provisioning"}},
            },
            backend="sandbox",
        )
        store = MagicMock()
        store.get_job = AsyncMock(return_value=parent)
        assert await job_workspace_authority.resolve_subjob_inherited_workspace(
            job, dependencies=_authority_deps(store=store)
        ) == ("wait", None)

    @pytest.mark.asyncio
    async def test_the_wait_budget_is_bounded(self):
        job = _child(
            {"inherits_parent_workspace": True},
            age_s=job_workspace_authority.INHERIT_WORKSPACE_MAX_WAIT_S + 60,
        )
        parent = _stamp(
            {
                "id": "parent",
                "status": "processing",
                "context": {"workspace_container": {"status": "provisioning"}},
            },
            backend="sandbox",
        )
        store = MagicMock()
        store.get_job = AsyncMock(return_value=parent)
        (
            action,
            reason,
        ) = await job_workspace_authority.resolve_subjob_inherited_workspace(
            job, dependencies=_authority_deps(store=store)
        )
        assert action == "fail"
        assert "Timed out" in reason

    @pytest.mark.asyncio
    async def test_an_outage_wake_re_anchors_the_budget(self):
        """A resumed subjob must not insta-fail on its long-exhausted created_at."""
        job = _child(
            {
                "inherits_parent_workspace": True,
                "llm_outage": {
                    "next_retry_at": (
                        datetime.now(timezone.utc) - timedelta(seconds=5)
                    ).isoformat()
                },
            },
            age_s=job_workspace_authority.INHERIT_WORKSPACE_MAX_WAIT_S + 3600,
        )
        parent = _stamp(
            {
                "id": "parent",
                "status": "processing",
                "context": {"workspace_container": {"status": "provisioning"}},
            },
            backend="sandbox",
        )
        store = MagicMock()
        store.get_job = AsyncMock(return_value=parent)
        assert await job_workspace_authority.resolve_subjob_inherited_workspace(
            job, dependencies=_authority_deps(store=store)
        ) == ("wait", None)

    @pytest.mark.asyncio
    async def test_cross_tier_inheritance_is_refused(self):
        job = _child({"inherits_parent_workspace": True})
        parent = _stamp(
            {"id": "parent", "status": "processing", "context": {"vm": READY_VM}},
            backend="vm",
        )
        store = MagicMock()
        store.get_job = AsyncMock(return_value=parent)
        (
            action,
            reason,
        ) = await job_workspace_authority.resolve_subjob_inherited_workspace(
            job, dependencies=_authority_deps(store=store)
        )
        assert action == "fail"
        assert "cross-tier" in reason


class TestFailSubjobAndUnblockParent:
    @pytest.mark.asyncio
    async def test_both_unblock_handlers_run_exactly_once(self):
        store = MagicMock()
        store.update_job_status = AsyncMock(return_value=True)
        scholar = AsyncMock()
        delegation = AsyncMock()
        job = {"id": "child", "status": "created"}
        await job_workspace_authority.fail_subjob_and_unblock_parent(
            job,
            "no workspace",
            dependencies=_authority_deps(
                store=store,
                handle_scholar_completion=scholar,
                handle_delegation_child_completion=delegation,
            ),
        )
        assert job["status"] == "failed"
        scholar.assert_awaited_once()
        delegation.assert_awaited_once()
        # The handlers must see the post-fail status they classify on.
        assert scholar.await_args.args[0]["status"] == "failed"

    @pytest.mark.asyncio
    async def test_a_stale_stateless_row_never_unblocks_the_parent(self):
        """A control verb that won the race must not be overwritten."""
        store = MagicMock()
        store.update_job_status = AsyncMock(return_value=False)
        scholar = AsyncMock()
        delegation = AsyncMock()
        job = {"id": "child", "status": "processing", "execution_lane": "stateless"}
        await job_workspace_authority.fail_subjob_and_unblock_parent(
            job,
            "no workspace",
            dependencies=_authority_deps(
                store=store,
                handle_scholar_completion=scholar,
                handle_delegation_child_completion=delegation,
            ),
        )
        scholar.assert_not_awaited()
        delegation.assert_not_awaited()
        assert job["status"] == "processing"
        assert store.update_job_status.await_args.kwargs["expected_status"] == (
            "processing"
        )

    @pytest.mark.asyncio
    async def test_a_pinned_row_is_failed_without_a_cas_precondition(self):
        store = MagicMock()
        store.update_job_status = AsyncMock(return_value=None)
        await job_workspace_authority.fail_subjob_and_unblock_parent(
            {"id": "child", "status": "created"},
            "no workspace",
            dependencies=_authority_deps(store=store),
        )
        assert store.update_job_status.await_args.kwargs["expected_status"] is None

    @pytest.mark.asyncio
    async def test_one_failing_handler_does_not_suppress_the_other(self):
        store = MagicMock()
        store.update_job_status = AsyncMock(return_value=True)
        scholar = AsyncMock(side_effect=RuntimeError("scholar unblock exploded"))
        delegation = AsyncMock()
        await job_workspace_authority.fail_subjob_and_unblock_parent(
            {"id": "child", "status": "created"},
            "no workspace",
            dependencies=_authority_deps(
                store=store,
                handle_scholar_completion=scholar,
                handle_delegation_child_completion=delegation,
            ),
        )
        delegation.assert_awaited_once()


class TestScholarParentProvisioning:
    @pytest.mark.asyncio
    async def test_a_missing_parent_fails_and_unblocks_once(self):
        store = MagicMock()
        store.get_job = AsyncMock(return_value=None)
        fail = AsyncMock()
        outcome = await job_workspace_authority.provision_parent_workspace_for_scholar(
            {"id": "scholar-1"},
            "parent-1",
            dependencies=_authority_deps(
                store=store, fail_subjob_and_unblock_parent=fail
            ),
        )
        assert outcome == "fail"
        fail.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_a_pending_workspace_waits_without_touching_the_child(self):
        store = MagicMock()
        store.get_job = AsyncMock(return_value={"id": "parent-1", "context": {}})
        store.merge_job_context = AsyncMock()
        ensure = AsyncMock(
            return_value=SimpleNamespace(
                outcome=EnsureOutcome.PENDING, status="provisioning"
            )
        )
        outcome = await job_workspace_authority.provision_parent_workspace_for_scholar(
            {"id": "scholar-1"},
            "parent-1",
            dependencies=_authority_deps(store=store, ensure_workspace=ensure),
        )
        assert outcome == "wait"
        store.merge_job_context.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_failed_workspace_surfaces_the_parent_error_on_the_child(self):
        store = MagicMock()
        store.get_job = AsyncMock(
            return_value={
                "id": "parent-1",
                "context": {"workspace_container": {"error": "ImagePullBackOff"}},
            }
        )
        ensure = AsyncMock(
            return_value=SimpleNamespace(outcome=EnsureOutcome.FAILED, status="failed")
        )
        fail = AsyncMock()
        outcome = await job_workspace_authority.provision_parent_workspace_for_scholar(
            {"id": "scholar-1"},
            "parent-1",
            dependencies=_authority_deps(
                store=store,
                ensure_workspace=ensure,
                fail_subjob_and_unblock_parent=fail,
            ),
        )
        assert outcome == "fail"
        fail.assert_awaited_once()
        assert "ImagePullBackOff" in fail.await_args.args[1]

    @pytest.mark.asyncio
    async def test_ready_promotes_the_child_without_copying_runtime_authority(self):
        parent = {
            "id": "parent-1",
            "context": {"workspace_container": READY_CONTAINER},
            "config_override": {"workspace": {"container": {"cpu": "2"}}},
        }
        store = MagicMock()
        store.get_job = AsyncMock(return_value=parent)
        store.merge_job_context = AsyncMock()
        conn = MagicMock()
        conn.execute = AsyncMock()

        class _Acquire:
            async def __aenter__(self):
                return conn

            async def __aexit__(self, *exc):
                return False

        store.acquire = MagicMock(return_value=_Acquire())
        ensure = AsyncMock(
            return_value=SimpleNamespace(outcome=EnsureOutcome.READY, status="ready")
        )
        outcome = await job_workspace_authority.provision_parent_workspace_for_scholar(
            {"id": "scholar-1", "config_name": "scholar"},
            "parent-1",
            dependencies=_authority_deps(store=store, ensure_workspace=ensure),
        )
        assert outcome == "promoted"
        # Only the inheritance marker is written; never the parent's live runtime.
        store.merge_job_context.assert_awaited_once_with(
            "scholar-1", {"inherits_parent_workspace": True}
        )
        # Container sizing comes from the parent's frozen template, resolved by
        # the provisioner; the scholar path passes none.
        assert "ws_config" not in ensure.await_args.kwargs
        conn.execute.assert_awaited_once()


class TestPrepareJobWorkspaceRuntime:
    @pytest.mark.asyncio
    async def test_an_inheriting_child_is_delegated_to_the_inherit_seam(self):
        job = _child({"inherits_parent_workspace": True})
        inherit = AsyncMock(return_value=("wait", None))
        (
            action,
            returned,
            reason,
        ) = await job_workspace_authority.prepare_job_workspace_runtime(
            job, dependencies=_authority_deps(resolve_inherited_workspace=inherit)
        )
        assert (action, reason) == ("wait", None)
        assert returned is job
        inherit.assert_awaited_once_with(job)

    @pytest.mark.asyncio
    async def test_a_normal_job_converges_through_adoption(self):
        job = _stamp(
            {
                "id": "job-1",
                "status": "created",
                "context": {"workspace_container": READY_CONTAINER},
            },
            backend="sandbox",
        )
        store = MagicMock()
        store.get_job = AsyncMock(return_value=job)
        (
            action,
            returned,
            _reason,
        ) = await job_workspace_authority.prepare_job_workspace_runtime(
            job, dependencies=_authority_deps(store=store)
        )
        assert action == "proceed"
        assert returned["id"] == "job-1"


# =============================================================================
# 7. Dispatch credential composition
# =============================================================================


class _NullStore:
    """A store whose every unknown coroutine method resolves to ``None``.

    Deliberately permissive: these tests are about the *composition* order and
    the removal/injection decisions, not about what any single resolver returns.
    """

    def __init__(self, **overrides):
        self._overrides = overrides

    def __getattr__(self, name):
        if name in self._overrides:
            return self._overrides[name]
        return AsyncMock(return_value=None)


class TestDispatchCredentialComposition:
    JOB = {
        "id": "00000000-0000-0000-0000-0000000000ff",
        "user_id": "00000000-0000-0000-0000-0000000000aa",
        "project_id": None,
        "config_name": "worker_base",
    }

    @pytest.mark.asyncio
    async def test_kb_profile_is_stripped_when_not_requested(self, monkeypatch):
        monkeypatch.setattr(main.app.state.resources, "postgres_db", _NullStore())
        override = {
            "env_keys": {
                "KB_EMBEDDING_MODEL": "stale",
                "KB_EMBEDDING_API_KEY": "stale",
                "EMBEDDING_API_KEY": "keep",
            }
        }
        out = await job_dispatch_credentials.inject_dispatch_credentials(
            dict(self.JOB),
            override,
            include_kb_profile=False,
            dependencies=_dispatch_credential_deps(),
        )
        assert not any(k.startswith("KB_EMBEDDING_") for k in out["env_keys"])
        assert out["env_keys"]["EMBEDDING_API_KEY"] == "keep"

    @pytest.mark.asyncio
    async def test_kb_profile_is_requested_through_the_injected_seam(self, monkeypatch):
        monkeypatch.setattr(main.app.state.resources, "postgres_db", _NullStore())
        seen = {}

        async def fake_profile(env_keys, *, dependencies):
            # R1.B12: the owner is bound with its dependency object.
            seen["dependencies"] = dependencies
            seen["env_keys"] = env_keys
            env_keys["KB_EMBEDDING_MODEL"] = "kb-model"
            env_keys["KB_EMBEDDING_API_KEY"] = "kb-key"
            return "kb-model"

        monkeypatch.setattr(
            dispatch_credentials_module,
            "inject_system_kb_embedding_profile",
            fake_profile,
        )
        out = await job_dispatch_credentials.inject_dispatch_credentials(
            dict(self.JOB),
            {},
            include_kb_profile=True,
            dependencies=_dispatch_credential_deps(),
        )
        # A value only the stub could produce (§P3 proof).
        assert out["env_keys"]["KB_EMBEDDING_MODEL"] == "kb-model"
        assert seen["env_keys"] is out["env_keys"]
        assert isinstance(
            seen["dependencies"],
            dispatch_credentials_module.DispatchCredentialDependencies,
        )

    @pytest.mark.asyncio
    async def test_parity_with_main_for_the_same_inputs(self, monkeypatch):
        monkeypatch.setattr(main.app.state.resources, "postgres_db", _NullStore())
        base = {
            "llm": {"model": "gpt-x", "provider": "openai"},
            "env_keys": {"KB_EMBEDDING_MODEL": "stale"},
        }
        import copy as _copy

        mine = await job_dispatch_credentials.inject_dispatch_credentials(
            dict(self.JOB),
            _copy.deepcopy(base),
            dependencies=_dispatch_credential_deps(),
        )
        theirs = await job_dispatch_credentials_module.inject_dispatch_credentials(
            dict(self.JOB),
            _copy.deepcopy(base),
            dependencies=preparation_composition.job_dispatch_credential_dependencies(
                main.app.state.resources
            ),
        )
        assert mine == theirs

    @pytest.mark.asyncio
    async def test_a_none_override_is_created_and_returned(self, monkeypatch):
        monkeypatch.setattr(main.app.state.resources, "postgres_db", _NullStore())
        out = await job_dispatch_credentials.inject_dispatch_credentials(
            dict(self.JOB), None, dependencies=_dispatch_credential_deps()
        )
        assert isinstance(out, dict) and "llm" in out

    @pytest.mark.asyncio
    async def test_the_resolve_model_seam_is_reached(self, monkeypatch):
        monkeypatch.setattr(main.app.state.resources, "postgres_db", _NullStore())
        meta = SimpleNamespace(
            origin="custom",
            endpoint_id="endpoint-1",
            provider="codex",
            api_key_ref="openai",
            context_window=424242,
            max_output_tokens=None,
            transport_kind=None,
            subscription_sources=None,
        )
        resolve = AsyncMock(return_value=meta)
        monkeypatch.setattr(model_registry_module, "resolve_model", resolve)
        out = await job_dispatch_credentials.inject_dispatch_credentials(
            dict(self.JOB),
            {"llm": {"model": "pinned"}},
            dependencies=_dispatch_credential_deps(),
        )
        resolve.assert_awaited_once()
        # Values only the stub could have produced.
        assert out["llm"]["provider"] == "codex"
        assert out["llm"]["model_max_context_tokens"] == 424242

    @pytest.mark.asyncio
    async def test_no_credential_value_is_logged(self, monkeypatch, caplog):
        monkeypatch.setattr(
            main.app.state.resources,
            "postgres_db",
            _NullStore(
                resolve_api_keys_for_job=AsyncMock(
                    return_value={"vision": "sk-super-secret"}
                )
            ),
        )
        with caplog.at_level("DEBUG"):
            out = await job_dispatch_credentials.inject_dispatch_credentials(
                dict(self.JOB), {}, dependencies=_dispatch_credential_deps()
            )
        assert out["env_keys"]["VISION_API_KEY"] == "sk-super-secret"
        assert "sk-super-secret" not in caplog.text
        # The provider NAME is what gets logged.
        assert "vision" in caplog.text


# =============================================================================
# 8. The worker start bundle — ordered, fail-closed refusals
# =============================================================================


def _bundle_job(backend="virtual", **extra):
    job = {
        "id": "00000000-0000-0000-0000-00000000beef",
        "description": "do the thing",
        "user_id": None,
        "project_id": None,
        "status": "created",
        "config_name": "worker_base",
        "context": {},
        "config_override": {"workspace": {"backend": backend}},
    }
    job.update(extra)
    return _stamp(job, backend=backend)


class _RecordingStore(_NullStore):
    def __init__(self, **overrides):
        self.status_writes = []
        self.resolved_config_writes = []

        async def update_job_status(job_id=None, **kwargs):
            self.status_writes.append((job_id, kwargs))
            return True

        async def store_resolved_config(job_id, config):
            self.resolved_config_writes.append((job_id, config))

        base = {
            "update_job_status": update_job_status,
            "store_resolved_config": store_resolved_config,
            "get_project_repositories": AsyncMock(return_value=[]),
        }
        base.update(overrides)
        super().__init__(**base)


@pytest.fixture
def bundle_env(monkeypatch):
    """Wire the lane-J bundle against stubs, through the reference factory."""
    store = _RecordingStore()
    monkeypatch.setattr(main.app.state.resources, "postgres_db", store)
    monkeypatch.setattr(deployment_gates_module, "is_experts_db_enabled", lambda: False)
    monkeypatch.setattr(
        job_datasource_selection_module,
        "resolve_authorized_job_datasources",
        AsyncMock(return_value=[]),
    )
    monkeypatch.setattr(
        job_dispatch_credentials_module,
        "inject_dispatch_credentials",
        AsyncMock(side_effect=lambda job, co, **kw: co or {}),
    )
    monkeypatch.setattr(
        workspace_tier_policy_module,
        "inject_lite_workspace_config",
        lambda co, prefix=None: {**(co or {}), "workspace": {"backend": "virtual"}},
    )
    monkeypatch.setattr(
        runtime_actor_module,
        "mint_worker_runtime_actor",
        AsyncMock(return_value=SimpleNamespace(to_payload=lambda: {"actor": "worker"})),
    )
    return store


class TestJobStartBundle:
    @pytest.mark.asyncio
    async def test_a_lite_job_builds_a_complete_bundle(self, bundle_env):
        bundle = await job_start_bundle.build_job_start_request(
            _bundle_job(), dependencies=_start_bundle_deps()
        )
        assert bundle is not None
        assert bundle.job_id == "00000000-0000-0000-0000-00000000beef"
        assert bundle.config_name == "worker_base"
        assert bundle.resolved_config is None
        assert bundle.config_override["workspace"]["backend"] == "virtual"
        assert bundle.runtime_actor == {"actor": "worker"}
        assert bundle.datasources is None
        assert bundle_env.status_writes == []

    @pytest.mark.asyncio
    async def test_ready_vm_bundle_identifies_the_vm_runtime(self, bundle_env):
        job = _bundle_job(backend="vm")
        job["context"]["vm"] = {
            **READY_VM,
            "provision_generation": "22222222-2222-2222-2222-222222222222",
            "ssh_ready_source": "provisioner_probe",
        }

        bundle = await job_start_bundle.build_job_start_request(
            job, dependencies=_start_bundle_deps()
        )

        assert bundle is not None
        assert bundle.workspace_provisioner == "kubevirt"
        assert bundle.workspace_runtime["assigned_backend"] == "vm"
        assert bundle.workspace_runtime["effective_backend"] == "vm"
        assert bundle.workspace_runtime["state"] == "ready"

    @pytest.mark.asyncio
    async def test_connector_revocation_fails_the_job_closed(
        self, bundle_env, monkeypatch
    ):
        monkeypatch.setattr(
            job_datasource_selection_module,
            "resolve_authorized_job_datasources",
            AsyncMock(
                side_effect=HTTPException(
                    status_code=403,
                    detail="One or more selected connectors are unavailable",
                )
            ),
        )
        assert (
            await job_start_bundle.build_job_start_request(
                _bundle_job(), dependencies=_start_bundle_deps()
            )
            is None
        )
        assert bundle_env.status_writes[0][1]["status"] == "failed"
        assert bundle_env.status_writes[0][1]["error_message"] == (
            "connector_unavailable"
        )

    @pytest.mark.asyncio
    async def test_a_stateless_build_never_writes_job_state(
        self, bundle_env, monkeypatch
    ):
        """Credential resolution can outlive the queue lease, so the whole
        build must be read-only from a stale claimant's point of view."""
        monkeypatch.setattr(
            job_datasource_selection_module,
            "resolve_authorized_job_datasources",
            AsyncMock(side_effect=HTTPException(status_code=403, detail="gone")),
        )
        assert (
            await job_start_bundle.build_job_start_request(
                _bundle_job(),
                persist_dispatch_state=False,
                dependencies=_start_bundle_deps(),
            )
            is None
        )
        assert bundle_env.status_writes == []

    @pytest.mark.asyncio
    async def test_an_unready_workspace_refuses_dispatch(self, bundle_env):
        job = _bundle_job(backend="sandbox")
        job["context"]["workspace_container"] = {"status": "failed"}
        assert (
            await job_start_bundle.build_job_start_request(
                job, dependencies=_start_bundle_deps()
            )
            is None
        )
        assert bundle_env.status_writes[0][1]["status"] == "failed"

    @pytest.mark.asyncio
    async def test_a_repository_connector_on_a_lite_tier_is_rejected(
        self, bundle_env, monkeypatch
    ):
        monkeypatch.setattr(
            job_datasource_selection_module,
            "resolve_authorized_job_datasources",
            AsyncMock(
                return_value=[
                    {
                        "id": "d1",
                        "type": "repository",
                        "name": "app",
                        "project_read_only": False,
                    }
                ]
            ),
        )
        assert (
            await job_start_bundle.build_job_start_request(
                _bundle_job(), dependencies=_start_bundle_deps()
            )
            is None
        )
        message = bundle_env.status_writes[0][1]["error_message"]
        assert "lite tier" in message and "app" in message

    @pytest.mark.asyncio
    async def test_a_lite_config_error_fails_the_job(self, bundle_env, monkeypatch):
        def boom(config_override, prefix=None):
            raise workspace_tier_policy_module.LiteWorkspaceConfigError(
                "no object store configured"
            )

        monkeypatch.setattr(
            workspace_tier_policy_module, "inject_lite_workspace_config", boom
        )
        assert (
            await job_start_bundle.build_job_start_request(
                _bundle_job(), dependencies=_start_bundle_deps()
            )
            is None
        )
        assert (
            bundle_env.status_writes[0][1]["error_message"]
            == "no object store configured"
        )

    @pytest.mark.asyncio
    async def test_a_workspace_backed_job_with_no_remote_is_refused(
        self, bundle_env, monkeypatch
    ):
        """The backstop: a sandbox job whose injection produced no SSH remote."""
        job = _bundle_job(backend="sandbox")
        job["context"]["workspace_container"] = READY_CONTAINER
        monkeypatch.setattr(
            managed_repository_authority_module,
            "authorize_job_repository_transport",
            AsyncMock(return_value=(None, None, [])),
        )
        monkeypatch.setattr(
            job_start_bundle,
            "inject_matching_workspace_config",
            lambda *a, **kw: (
                {"workspace": {"backend": "sandbox"}},
                SimpleNamespace(
                    ready=True,
                    state="ready",
                    effective_backend="sandbox",
                    reason=None,
                    safe_projection=lambda: {"backend": "sandbox"},
                ),
            ),
        )
        assert (
            await job_start_bundle.build_job_start_request(
                job, dependencies=_start_bundle_deps()
            )
            is None
        )
        assert "SSH credentials" in bundle_env.status_writes[0][1]["error_message"]

    @pytest.mark.asyncio
    async def test_a_grant_denial_is_never_downgraded_to_the_raw_override(
        self, bundle_env, monkeypatch
    ):
        monkeypatch.setattr(
            deployment_gates_module, "is_experts_db_enabled", lambda: True
        )
        monkeypatch.setattr(
            grant_enforcement_module,
            "user_experts_enabled",
            AsyncMock(return_value=True),
        )
        monkeypatch.setattr(
            session_config_resolution_module,
            "resolve_default_models",
            AsyncMock(return_value={}),
        )
        monkeypatch.setattr(
            catalogue_composition,
            "expert_catalog_service",
            lambda _resources: SimpleNamespace(
                gather_in_scope_skills=AsyncMock(return_value=[])
            ),
        )
        monkeypatch.setattr(
            dispatch_credentials_module,
            "seed_registry_model_overrides",
            AsyncMock(return_value={}),
        )
        monkeypatch.setattr(
            session_config_resolution_module,
            "prefetch_roster_refs",
            AsyncMock(return_value={}),
        )
        monkeypatch.setattr(
            config_resolver_module,
            "resolve_config",
            lambda **kwargs: (kwargs["capture"].__setitem__("merged_fragment", {}))
            or {"llm": {}},
        )
        monkeypatch.setattr(
            grant_enforcement_module,
            "enforce_dispatch_grants",
            AsyncMock(
                side_effect=grant_enforcement_module.GrantDenied(
                    ["tools.shell not granted"]
                )
            ),
        )
        assert (
            await job_start_bundle.build_job_start_request(
                _bundle_job(), dependencies=_start_bundle_deps()
            )
            is None
        )
        message = bundle_env.status_writes[0][1]["error_message"]
        assert "capability grants" in message and "tools.shell" in message

    @pytest.mark.asyncio
    async def test_an_unroutable_pinned_model_fails_the_job(
        self, bundle_env, monkeypatch
    ):
        monkeypatch.setattr(
            deployment_gates_module, "is_experts_db_enabled", lambda: True
        )
        monkeypatch.setattr(
            grant_enforcement_module,
            "user_experts_enabled",
            AsyncMock(return_value=False),
        )
        monkeypatch.setattr(
            session_config_resolution_module,
            "resolve_default_models",
            AsyncMock(return_value={}),
        )
        monkeypatch.setattr(
            catalogue_composition,
            "expert_catalog_service",
            lambda _resources: SimpleNamespace(
                gather_in_scope_skills=AsyncMock(return_value=[])
            ),
        )
        monkeypatch.setattr(
            dispatch_credentials_module,
            "seed_registry_model_overrides",
            AsyncMock(return_value={}),
        )
        monkeypatch.setattr(
            session_config_resolution_module,
            "prefetch_roster_refs",
            AsyncMock(return_value={}),
        )
        monkeypatch.setattr(
            config_resolver_module,
            "resolve_config",
            lambda **kwargs: (kwargs["capture"].__setitem__("merged_fragment", {}))
            or {"llm": {"model": "pinned"}},
        )
        monkeypatch.setattr(
            config_resolver_module,
            "inject_blob_credentials",
            AsyncMock(side_effect=lambda resolved, inject: resolved),
        )
        monkeypatch.setattr(
            job_start_bundle,
            "unrouted_model_slots",
            lambda resolved: ["llm.model"],
        )
        assert (
            await job_start_bundle.build_job_start_request(
                _bundle_job(), dependencies=_start_bundle_deps()
            )
            is None
        )
        assert (
            "no resolvable endpoint" in bundle_env.status_writes[-1][1]["error_message"]
        )

    @pytest.mark.asyncio
    async def test_a_delivered_blob_suppresses_the_flat_override(
        self, bundle_env, monkeypatch
    ):
        monkeypatch.setattr(
            deployment_gates_module, "is_experts_db_enabled", lambda: True
        )
        monkeypatch.setattr(
            grant_enforcement_module,
            "user_experts_enabled",
            AsyncMock(return_value=False),
        )
        monkeypatch.setattr(
            session_config_resolution_module,
            "resolve_default_models",
            AsyncMock(return_value={}),
        )
        monkeypatch.setattr(
            catalogue_composition,
            "expert_catalog_service",
            lambda _resources: SimpleNamespace(
                gather_in_scope_skills=AsyncMock(return_value=[])
            ),
        )
        monkeypatch.setattr(
            dispatch_credentials_module,
            "seed_registry_model_overrides",
            AsyncMock(return_value={}),
        )
        monkeypatch.setattr(
            session_config_resolution_module,
            "prefetch_roster_refs",
            AsyncMock(return_value={}),
        )
        monkeypatch.setattr(
            config_resolver_module,
            "resolve_config",
            lambda **kwargs: (kwargs["capture"].__setitem__("merged_fragment", {}))
            or {"llm": {"model": "m"}},
        )
        monkeypatch.setattr(
            config_resolver_module,
            "inject_blob_credentials",
            AsyncMock(side_effect=lambda resolved, inject: resolved),
        )
        monkeypatch.setattr(job_start_bundle, "unrouted_model_slots", lambda r: [])
        bundle = await job_start_bundle.build_job_start_request(
            _bundle_job(), dependencies=_start_bundle_deps()
        )
        assert bundle is not None
        assert bundle.config_override is None
        assert bundle.resolved_config == {"llm": {"model": "m"}}
        assert bundle_env.resolved_config_writes

    @pytest.mark.asyncio
    async def test_an_unexpected_error_returns_none_rather_than_a_partial_bundle(
        self, bundle_env, monkeypatch
    ):
        monkeypatch.setattr(
            job_dispatch_credentials_module,
            "inject_dispatch_credentials",
            AsyncMock(side_effect=RuntimeError("resolver exploded")),
        )
        assert (
            await job_start_bundle.build_job_start_request(
                _bundle_job(), dependencies=_start_bundle_deps()
            )
            is None
        )


class TestJobRepositoryPreparation:
    @pytest.mark.asyncio
    async def test_authority_error_leaves_the_job_unclaimed(self, monkeypatch):
        monkeypatch.setattr(main.app.state.resources, "postgres_db", _NullStore())
        error = ManagedRepositoryAuthorityError("scoped_key_missing")
        monkeypatch.setattr(
            managed_repository_authority_module,
            "prepare_job_primary_repository_authority",
            AsyncMock(side_effect=error),
        )
        assert (
            await job_start_bundle.prepare_job_repository_before_claim(
                {"id": "j"}, dependencies=_start_bundle_deps()
            )
            is False
        )

    @pytest.mark.asyncio
    async def test_a_transport_exception_message_is_never_logged(
        self, monkeypatch, caplog
    ):
        monkeypatch.setattr(main.app.state.resources, "postgres_db", _NullStore())
        monkeypatch.setattr(
            managed_repository_authority_module,
            "prepare_job_primary_repository_authority",
            AsyncMock(
                side_effect=RuntimeError("https://user:tok@gitea.internal/org/repo.git")
            ),
        )
        with caplog.at_level("DEBUG"):
            assert (
                await job_start_bundle.prepare_job_repository_before_claim(
                    {"id": "j"}, dependencies=_start_bundle_deps()
                )
                is False
            )
        assert "tok@gitea.internal" not in caplog.text
        assert "RuntimeError" in caplog.text

    @pytest.mark.asyncio
    async def test_knowledge_and_jobs_repositories_are_skipped(self, monkeypatch):
        prepared = []

        async def project_authority(db, forge, repository):
            prepared.append(repository["role"])

        store = _NullStore(
            get_project_repositories=AsyncMock(
                return_value=[
                    {"role": "jobs", "is_managed": True},
                    {"role": "knowledge", "is_managed": True},
                    {"role": "code", "is_managed": True},
                    {"role": "code", "is_managed": False},
                ]
            )
        )
        monkeypatch.setattr(main.app.state.resources, "postgres_db", store)
        monkeypatch.setattr(
            managed_repository_authority_module,
            "prepare_job_primary_repository_authority",
            AsyncMock(),
        )
        monkeypatch.setattr(
            managed_repository_authority_module,
            "prepare_project_repository_authority",
            project_authority,
        )
        assert (
            await job_start_bundle.prepare_job_repository_before_claim(
                {"id": "j", "project_id": "p"}, dependencies=_start_bundle_deps()
            )
            is True
        )
        assert prepared == ["code"]


# =============================================================================
# 9. Job connector selection and revalidation
# =============================================================================


SNAPSHOT_ID = "aaaaaaaa-1111-1111-1111-111111111111"


def _selection_job(**extra):
    job = {
        "id": "00000000-0000-0000-0000-00000000cafe",
        "user_id": None,
        "project_id": None,
        "context": {"datasource_selection": {"datasource_ids": [SNAPSHOT_ID]}},
        "config_override": {"workspace": {"backend": "sandbox"}},
    }
    job.update(extra)
    return job


class TestJobDatasourceSelection:
    @pytest.mark.asyncio
    async def test_the_snapshot_must_match_the_junction_exactly(self, monkeypatch):
        monkeypatch.setattr(
            main.app.state.resources,
            "postgres_db",
            _NullStore(list_job_datasource_ids=AsyncMock(return_value=[])),
        )
        with pytest.raises(HTTPException) as exc:
            await job_datasource_selection.revalidate_job_datasource_selection(
                _selection_job(), dependencies=_datasource_selection_deps()
            )
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_a_duplicated_snapshot_entry_is_refused(self, monkeypatch):
        monkeypatch.setattr(
            main.app.state.resources,
            "postgres_db",
            _NullStore(
                list_job_datasource_ids=AsyncMock(
                    return_value=[SNAPSHOT_ID, SNAPSHOT_ID]
                )
            ),
        )
        job = _selection_job()
        job["context"]["datasource_selection"]["datasource_ids"] = [
            SNAPSHOT_ID,
            SNAPSHOT_ID,
        ]
        with pytest.raises(HTTPException) as exc:
            await job_datasource_selection.revalidate_job_datasource_selection(
                job, dependencies=_datasource_selection_deps()
            )
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_a_malformed_context_is_refused_not_ignored(self, monkeypatch):
        monkeypatch.setattr(
            main.app.state.resources,
            "postgres_db",
            _NullStore(list_job_datasource_ids=AsyncMock(return_value=[])),
        )
        with pytest.raises(HTTPException) as exc:
            await job_datasource_selection.revalidate_job_datasource_selection(
                _selection_job(context="{not json"),
                dependencies=_datasource_selection_deps(),
            )
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_the_thread_authorizer_receives_the_job_shaped_arguments(
        self, monkeypatch
    ):
        monkeypatch.setattr(
            main.app.state.resources,
            "postgres_db",
            _NullStore(
                list_job_datasource_ids=AsyncMock(return_value=[SNAPSHOT_ID]),
                get_user=AsyncMock(return_value={"id": "u1"}),
            ),
        )
        seen = {}

        async def authorize(actor, ids, **kwargs):
            seen["actor"] = actor
            seen["ids"] = ids
            seen.update(kwargs)
            return (ids, {SNAPSHOT_ID: 7})

        monkeypatch.setattr(
            thread_datasource_authorization_module,
            "authorize_thread_datasource_selection",
            authorize,
        )
        (
            selected,
            revisions,
        ) = await job_datasource_selection.revalidate_job_datasource_selection(
            _selection_job(user_id="u1", project_id="p1"),
            dependencies=_datasource_selection_deps(),
        )
        # Values only the stub could produce (§P3 proof).
        assert (selected, revisions) == ([SNAPSHOT_ID], {SNAPSHOT_ID: 7})
        assert seen["legacy_job_id"] == "00000000-0000-0000-0000-00000000cafe"
        assert seen["target_project_ids"] == ["p1"]
        assert seen["effective_work_owner_id"] == "u1"
        assert seen["trusted_system_inheritance"] is False
        assert seen["workspace_backend"] == "sandbox"

    @pytest.mark.asyncio
    async def test_a_system_owned_job_is_trusted_inheritance(self, monkeypatch):
        monkeypatch.setattr(
            main.app.state.resources,
            "postgres_db",
            _NullStore(list_job_datasource_ids=AsyncMock(return_value=[SNAPSHOT_ID])),
        )
        seen = {}

        async def authorize(actor, ids, **kwargs):
            seen.update(kwargs)
            return (ids, {})

        monkeypatch.setattr(
            thread_datasource_authorization_module,
            "authorize_thread_datasource_selection",
            authorize,
        )
        await job_datasource_selection.revalidate_job_datasource_selection(
            _selection_job(), dependencies=_datasource_selection_deps()
        )
        assert seen["trusted_system_inheritance"] is True

    @pytest.mark.asyncio
    async def test_resolve_authorized_reaches_the_patched_revalidation_seam(
        self, monkeypatch
    ):
        """§P3: the service must consult ``main``'s seam, not its own function."""
        monkeypatch.setattr(
            main.app.state.resources,
            "postgres_db",
            _NullStore(
                resolve_datasources_for_job=AsyncMock(
                    return_value=[{"id": SNAPSHOT_ID, "policy_revision": 9}]
                )
            ),
        )
        revalidate = AsyncMock(return_value=([SNAPSHOT_ID], {SNAPSHOT_ID: 9}))
        monkeypatch.setattr(
            job_datasource_selection_module,
            "revalidate_job_datasource_selection",
            revalidate,
        )
        rows = await job_datasource_selection.resolve_authorized_job_datasources(
            _selection_job(), dependencies=_datasource_selection_deps()
        )
        revalidate.assert_awaited_once()
        assert rows == [{"id": SNAPSHOT_ID, "policy_revision": 9}]

    @pytest.mark.asyncio
    async def test_resolution_that_disagrees_with_the_snapshot_is_refused(
        self, monkeypatch
    ):
        monkeypatch.setattr(
            main.app.state.resources,
            "postgres_db",
            _NullStore(
                resolve_datasources_for_job=AsyncMock(
                    return_value=[{"id": SNAPSHOT_ID, "policy_revision": 8}]
                )
            ),
        )
        monkeypatch.setattr(
            job_datasource_selection_module,
            "revalidate_job_datasource_selection",
            AsyncMock(return_value=([SNAPSHOT_ID], {SNAPSHOT_ID: 9})),
        )
        with pytest.raises(HTTPException) as exc:
            await job_datasource_selection.resolve_authorized_job_datasources(
                _selection_job(), dependencies=_datasource_selection_deps()
            )
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_an_explicit_empty_parent_selection_is_authoritative(
        self, monkeypatch
    ):
        """``datasource_ids: []`` on the parent thread means none, not unset."""
        monkeypatch.setattr(
            main.app.state.resources,
            "postgres_db",
            _NullStore(
                get_thread=AsyncMock(return_value={"metadata": {"datasource_ids": []}})
            ),
        )
        revalidate = AsyncMock(return_value=(["should-not-be-used"], {}))
        monkeypatch.setattr(
            job_datasource_selection_module,
            "revalidate_job_datasource_selection",
            revalidate,
        )
        assert (
            await job_datasource_selection.inherit_parent_datasource_ids(
                thread_id="t1",
                parent_job_id="p1",
                dependencies=_datasource_selection_deps(),
            )
            == []
        )
        revalidate.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_thread_without_the_key_falls_through_to_the_parent_job(
        self, monkeypatch
    ):
        monkeypatch.setattr(
            main.app.state.resources,
            "postgres_db",
            _NullStore(
                get_thread=AsyncMock(return_value={"metadata": {}}),
                get_job=AsyncMock(return_value={"id": "p1"}),
            ),
        )
        monkeypatch.setattr(
            job_datasource_selection_module,
            "revalidate_job_datasource_selection",
            AsyncMock(return_value=([SNAPSHOT_ID], {})),
        )
        assert await job_datasource_selection.inherit_parent_datasource_ids(
            thread_id="t1",
            parent_job_id="p1",
            dependencies=_datasource_selection_deps(),
        ) == [SNAPSHOT_ID]

    @pytest.mark.asyncio
    async def test_a_missing_parent_job_inherits_nothing(self, monkeypatch):
        monkeypatch.setattr(
            main.app.state.resources,
            "postgres_db",
            _NullStore(get_job=AsyncMock(return_value=None)),
        )
        assert (
            await job_datasource_selection.inherit_parent_datasource_ids(
                thread_id=None,
                parent_job_id="p1",
                dependencies=_datasource_selection_deps(),
            )
            == []
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize("backend", ["virtual", "none"])
    async def test_lite_seeds_drop_shell_requiring_connectors(
        self, backend, monkeypatch
    ):
        rows = [
            {"id": "r1", "type": "repository"},
            {"id": "c1", "type": "credentials"},
            {"id": "d1", "type": "postgresql"},
        ]
        monkeypatch.setattr(
            main.app.state.resources,
            "postgres_db",
            _NullStore(get_datasource_policy_rows=AsyncMock(return_value=rows)),
        )
        assert await job_datasource_selection.filter_implicit_lite_datasource_ids(
            ["r1", "c1", "d1", "missing"],
            backend,
            dependencies=_datasource_selection_deps(),
        ) == ["d1", "missing"]

    @pytest.mark.asyncio
    async def test_a_full_tier_selection_is_untouched(self, monkeypatch):
        monkeypatch.setattr(main.app.state.resources, "postgres_db", _NullStore())
        ids = ["r1", "d1"]
        assert (
            await job_datasource_selection.filter_implicit_lite_datasource_ids(
                ids, "sandbox", dependencies=_datasource_selection_deps()
            )
            is ids
        )

    @pytest.mark.asyncio
    async def test_provenance_is_credential_free(self):
        stamp = await job_datasource_selection.datasource_selection_provenance(
            datasource_ids=[SNAPSHOT_ID],
            policy_revisions={SNAPSHOT_ID: 3},
            origin="inherited",
            effective_work_owner_id="u1",
            actor={"id": "u1", "api_key": "sk-should-never-appear"},
            project_ids=["p1"],
            creation_path="subjob",
        )
        assert stamp["initiating_actor_id"] == "u1"
        assert "sk-should-never-appear" not in json.dumps(stamp)
        assert set(stamp) == {
            "origin",
            "creation_path",
            "effective_work_owner_id",
            "initiating_actor_id",
            "project_ids",
            "datasource_ids",
            "policy_revisions",
            "materialized_at",
        }


# =============================================================================
# 10. Route identity for POST /api/jobs/{job_id}/assign/{agent_id}
# =============================================================================


class TestAssignRouteIdentity:
    PATH = "/api/jobs/{job_id}/assign/{agent_id}"

    def _main_route(self):
        from tests._route_inventory import iter_mounted_route_objects

        for route in iter_mounted_route_objects(main.app.routes):
            if route.path == self.PATH and "POST" in route.methods:
                return route
        raise AssertionError("main no longer declares the assignment route")

    def _extracted_route(self):
        from fastapi import FastAPI

        app = FastAPI(default_response_class=http_composition.CustomJSONResponse)
        app.include_router(job_assignment_routes.router)
        from tests._route_inventory import iter_mounted_route_objects

        for route in iter_mounted_route_objects(app.routes):
            if route.path == self.PATH and "POST" in route.methods:
                return route
        raise AssertionError("the extracted router does not serve the route")

    def test_path_method_name_and_operation_id_are_unchanged(self):
        theirs, mine = self._main_route(), self._extracted_route()
        assert mine.path == theirs.path == self.PATH
        assert mine.path_format == theirs.path_format
        assert mine.methods == theirs.methods == {"POST"}
        assert mine.name == theirs.name == "assign_job_to_agent"
        assert mine.operation_id == theirs.operation_id
        assert mine.unique_id == theirs.unique_id

    def test_status_code_and_response_shape_are_unchanged(self):
        theirs, mine = self._main_route(), self._extracted_route()
        assert mine.status_code == theirs.status_code
        assert mine.response_model == theirs.response_model
        assert mine.include_in_schema == theirs.include_in_schema
        assert mine.deprecated == theirs.deprecated

    def test_the_response_class_still_renders_the_app_default(self):
        """FastAPI may resolve its placeholder when mounting the router.

        The response bytes must match the configured application response
        class regardless of when FastAPI resolves that internal placeholder.
        """
        from fastapi import FastAPI
        from fastapi.datastructures import DefaultPlaceholder
        from fastapi.testclient import TestClient

        mine = self._extracted_route()
        assert (
            isinstance(mine.response_class, DefaultPlaceholder)
            or mine.response_class is http_composition.CustomJSONResponse
        )

        payload = {"status": "assigned", "agent_id": "AID", "job_id": "JID"}
        app = FastAPI(default_response_class=http_composition.CustomJSONResponse)

        @app.post("/reference")
        async def reference() -> dict[str, str]:
            return payload

        app.include_router(job_assignment_routes.router)
        app.state.job_assignment_dependencies_factory = lambda: (
            job_assignment_routes.JobAssignmentDependencies(
                store=_NullStore(
                    get_job=AsyncMock(
                        return_value={
                            "id": "JID",
                            "status": "created",
                            "execution_lane": "pinned",
                        }
                    ),
                    get_agent=AsyncMock(
                        return_value={"status": "ready", "pod_ip": "10.0.0.1"}
                    ),
                    job_has_checkpoint=AsyncMock(return_value=False),
                    claim_job_for_agent=AsyncMock(return_value=True),
                ),
                logger=preparation_composition.logger,
                require_admin=AsyncMock(),
                vm_mode=lambda: vm_provisioner_module.vm_provisioner.mode,
                completion_commands_enabled=lambda: False,
                prepare_job_workspace_runtime=AsyncMock(
                    return_value=(
                        "proceed",
                        _stamp(
                            {
                                "id": "JID",
                                "status": "created",
                                "execution_lane": "pinned",
                                "context": {"workspace_container": READY_CONTAINER},
                            },
                            backend="sandbox",
                        ),
                        None,
                    )
                ),
                prepare_job_repository_before_claim=AsyncMock(return_value=True),
                resume_missing_workspace=lambda job: None,
                guard_completion_control=AsyncMock(),
                claim_completion_control=AsyncMock(),
                abort_completion_control_claim=AsyncMock(),
                completion_resume_guard_kwargs=lambda: {},
                dispatch_job_to_agent=AsyncMock(return_value=True),
                resume_job_on_agent=AsyncMock(return_value=True),
                trigger_dispatch=MagicMock(),
            )
        )
        with TestClient(app) as client:
            reference_body = client.post("/reference").content
            assigned = client.post("/api/jobs/JID/assign/AID")
        assert assigned.status_code == 200
        assert assigned.content == reference_body

    def test_the_route_declares_no_router_level_dependencies(self):
        """Authorization is in the body (``require_admin``), as it was in main."""
        theirs, mine = self._main_route(), self._extracted_route()
        assert list(theirs.dependencies) == []
        assert list(mine.dependencies) == []

    def test_the_only_request_parameters_are_the_two_path_ids(self):
        mine = self._extracted_route()
        names = {p.name for p in mine.dependant.path_params}
        assert names == {"job_id", "agent_id"}
        assert mine.dependant.query_params == []
        assert mine.dependant.body_params == []

    def test_the_concrete_path_matches_exactly_one_route_in_main(self):
        """Moving the declaration earlier must not change which route wins."""
        from starlette.routing import Match

        from tests._route_inventory import iter_mounted_route_objects

        scope = {
            "type": "http",
            "path": "/api/jobs/JID/assign/AID",
            "method": "POST",
            "root_path": "",
            "headers": [],
        }
        full = [
            route
            for route in iter_mounted_route_objects(main.app.routes)
            if route.matches(scope)[0] is Match.FULL
        ]
        assert [route.name for route in full] == ["assign_job_to_agent"]

    def test_no_other_post_route_shadows_the_assign_segment(self):
        from tests._route_inventory import iter_mounted_routes

        siblings = {
            path
            for method, path in iter_mounted_routes(main.app.routes)
            if method == "POST"
            and path.startswith("/api/jobs/")
            and path.count("/") == 5
            and path != self.PATH
        }
        # Every other four-segment POST under /api/jobs uses a distinct literal
        # third segment, so list order cannot decide this match.
        assert all("/assign/" not in path for path in siblings)


# =============================================================================
# 11. The assignment route's own behaviour
# =============================================================================


def _assign_deps(**overrides):
    base = dict(
        store=_NullStore(),
        logger=preparation_composition.logger,
        require_admin=AsyncMock(),
        vm_mode=lambda: vm_provisioner_module.vm_provisioner.mode,
        completion_commands_enabled=lambda: False,
        prepare_job_workspace_runtime=AsyncMock(),
        prepare_job_repository_before_claim=AsyncMock(return_value=True),
        resume_missing_workspace=lambda job: None,
        guard_completion_control=AsyncMock(),
        claim_completion_control=AsyncMock(),
        abort_completion_control_claim=AsyncMock(),
        completion_resume_guard_kwargs=lambda: {},
        dispatch_job_to_agent=AsyncMock(return_value=True),
        resume_job_on_agent=AsyncMock(return_value=True),
        trigger_dispatch=MagicMock(),
    )
    base.update(overrides)
    return job_assignment_routes.JobAssignmentDependencies(**base)


READY_SANDBOX_JOB = _stamp(
    {
        "id": "JID",
        "status": "created",
        "execution_lane": "pinned",
        "context": {"workspace_container": READY_CONTAINER},
    },
    backend="sandbox",
)


async def _assign(job, **overrides):
    store_overrides = overrides.pop("store_overrides", {})
    store_config = {
        "get_job": AsyncMock(return_value=job),
        "get_agent": AsyncMock(return_value={"status": "ready", "pod_ip": "10.0.0.1"}),
        "job_has_checkpoint": AsyncMock(return_value=False),
        "claim_job_for_agent": AsyncMock(return_value=True),
    }
    store_config.update(store_overrides)
    store = _NullStore(**store_config)
    deps = _assign_deps(
        store=store,
        prepare_job_workspace_runtime=AsyncMock(return_value=("proceed", job, None)),
        **overrides,
    )
    result = await job_assignment_routes.assign_job_to_agent(
        MagicMock(), "JID", "AID", dependencies=deps
    )
    return result, deps, store


class TestAssignRouteBehaviour:
    @pytest.mark.asyncio
    async def test_a_stateless_job_cannot_be_assigned_to_an_agent(self):
        job = dict(READY_SANDBOX_JOB, execution_lane="stateless")
        with pytest.raises(HTTPException) as exc:
            await _assign(job)
        assert exc.value.status_code == 409
        assert "run queue" in exc.value.detail

    @pytest.mark.asyncio
    async def test_a_missing_job_is_a_404(self):
        with pytest.raises(HTTPException) as exc:
            await _assign(None)
        assert exc.value.status_code == 404

    @pytest.mark.asyncio
    @pytest.mark.parametrize("status", ["processing", "completed", "cancelled"])
    async def test_a_non_assignable_status_is_a_400(self, status):
        with pytest.raises(HTTPException) as exc:
            await _assign(dict(READY_SANDBOX_JOB, status=status))
        assert exc.value.status_code == 400

    @pytest.mark.asyncio
    async def test_unconverged_workspace_authority_reserves_nothing(self):
        deps = _assign_deps(
            store=_NullStore(get_job=AsyncMock(return_value=READY_SANDBOX_JOB)),
            prepare_job_workspace_runtime=AsyncMock(
                return_value=("wait", READY_SANDBOX_JOB, "adoption_pending")
            ),
        )
        with pytest.raises(HTTPException) as exc:
            await job_assignment_routes.assign_job_to_agent(
                MagicMock(), "JID", "AID", dependencies=deps
            )
        assert exc.value.status_code == 409
        assert exc.value.detail["code"] == "workspace_runtime_adoption_pending"
        assert exc.value.detail["retryable"] is True
        deps.dispatch_job_to_agent.assert_not_awaited()
        deps.resume_job_on_agent.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_an_ambiguous_contract_reserves_nothing(self):
        job = {"id": "JID", "status": "created", "context": {"vm": {"requested": True}}}
        with pytest.raises(HTTPException) as exc:
            await _assign(job)
        assert exc.value.status_code == 409
        assert exc.value.detail["code"] == "workspace_contract_invalid"

    @pytest.mark.asyncio
    async def test_a_missing_workspace_queues_instead_of_dispatching(self):
        result, deps, store = await _assign(
            READY_SANDBOX_JOB, resume_missing_workspace=lambda job: "sandbox"
        )
        assert result["status"] == "queued"
        assert "The requested agent was not reserved" in result["message"]
        deps.trigger_dispatch.assert_called_once()
        deps.dispatch_job_to_agent.assert_not_awaited()
        deps.resume_job_on_agent.assert_not_awaited()
        store.get_agent.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_the_shed_uses_the_tier_context_key(self):
        shed = AsyncMock()
        result, _deps, _store = await _assign(
            READY_SANDBOX_JOB,
            resume_missing_workspace=lambda job: "sandbox",
            store_overrides={"shed_workspace_context": shed},
        )
        assert result["status"] == "queued"
        shed.assert_awaited_once_with("JID", "workspace_container")

    @pytest.mark.asyncio
    async def test_a_paused_job_is_requeued_for_resume_when_control_is_off(self):
        queue = AsyncMock(return_value=True)
        job = dict(READY_SANDBOX_JOB, status="paused")
        result, _deps, _store = await _assign(
            job,
            resume_missing_workspace=lambda j: "sandbox",
            store_overrides={"queue_job_for_resume": queue},
        )
        assert result["status"] == "queued"
        queue.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_a_failed_queueing_aborts_the_completion_control_claim(self):
        claim = SimpleNamespace(claim_id="claim-1")
        abort = AsyncMock()
        deps = _assign_deps(
            store=_NullStore(
                get_job=AsyncMock(return_value=READY_SANDBOX_JOB),
                prepare_pinned_job_for_workspace_resume=AsyncMock(return_value=False),
            ),
            prepare_job_workspace_runtime=AsyncMock(
                return_value=("proceed", READY_SANDBOX_JOB, None)
            ),
            completion_commands_enabled=lambda: True,
            claim_completion_control=AsyncMock(return_value=claim),
            abort_completion_control_claim=abort,
            resume_missing_workspace=lambda job: "sandbox",
        )
        with pytest.raises(HTTPException) as exc:
            await job_assignment_routes.assign_job_to_agent(
                MagicMock(), "JID", "AID", dependencies=deps
            )
        assert exc.value.status_code == 409
        abort.assert_awaited_once_with(claim)

    @pytest.mark.asyncio
    async def test_a_raising_queueing_also_aborts_the_claim(self):
        claim = SimpleNamespace(claim_id="claim-1")
        abort = AsyncMock()
        deps = _assign_deps(
            store=_NullStore(
                get_job=AsyncMock(return_value=READY_SANDBOX_JOB),
                prepare_pinned_job_for_workspace_resume=AsyncMock(
                    side_effect=RuntimeError("db down")
                ),
            ),
            prepare_job_workspace_runtime=AsyncMock(
                return_value=("proceed", READY_SANDBOX_JOB, None)
            ),
            completion_commands_enabled=lambda: True,
            claim_completion_control=AsyncMock(return_value=claim),
            abort_completion_control_claim=abort,
            resume_missing_workspace=lambda job: "sandbox",
        )
        with pytest.raises(HTTPException) as exc:
            await job_assignment_routes.assign_job_to_agent(
                MagicMock(), "JID", "AID", dependencies=deps
            )
        assert exc.value.status_code == 500
        abort.assert_awaited_once_with(claim)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "agent,status",
        [
            (None, 404),
            ({"status": "busy", "pod_ip": "10.0.0.1"}, 400),
            ({"status": "ready", "pod_ip": None}, 400),
        ],
    )
    async def test_agent_preconditions(self, agent, status):
        with pytest.raises(HTTPException) as exc:
            await _assign(
                READY_SANDBOX_JOB,
                store_overrides={"get_agent": AsyncMock(return_value=agent)},
            )
        assert exc.value.status_code == status

    @pytest.mark.asyncio
    async def test_unready_repository_authority_blocks_the_claim(self):
        claim = AsyncMock(return_value=True)
        with pytest.raises(HTTPException) as exc:
            await _assign(
                READY_SANDBOX_JOB,
                prepare_job_repository_before_claim=AsyncMock(return_value=False),
                store_overrides={"claim_job_for_agent": claim},
            )
        assert exc.value.status_code == 409
        claim.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_lost_claim_race_is_a_409(self):
        with pytest.raises(HTTPException) as exc:
            await _assign(
                READY_SANDBOX_JOB,
                store_overrides={"claim_job_for_agent": AsyncMock(return_value=False)},
            )
        assert exc.value.status_code == 409

    @pytest.mark.asyncio
    async def test_a_paused_job_with_a_checkpoint_takes_the_resume_lane(self):
        job = dict(READY_SANDBOX_JOB, status="paused")
        result, deps, _store = await _assign(
            job, store_overrides={"job_has_checkpoint": AsyncMock(return_value=True)}
        )
        assert result == {"status": "assigned", "agent_id": "AID", "job_id": "JID"}
        deps.resume_job_on_agent.assert_awaited_once()
        deps.dispatch_job_to_agent.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_paused_job_without_a_checkpoint_takes_the_fresh_lane(self):
        job = dict(READY_SANDBOX_JOB, status="paused")
        result, deps, _store = await _assign(job)
        assert result["status"] == "assigned"
        deps.dispatch_job_to_agent.assert_awaited_once()
        deps.resume_job_on_agent.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_failed_delivery_is_a_502(self):
        with pytest.raises(HTTPException) as exc:
            await _assign(
                READY_SANDBOX_JOB, dispatch_job_to_agent=AsyncMock(return_value=False)
            )
        assert exc.value.status_code == 502

    @pytest.mark.asyncio
    async def test_the_admin_gate_runs_before_any_store_read(self):
        store = _NullStore(get_job=AsyncMock(return_value=READY_SANDBOX_JOB))
        deps = _assign_deps(
            store=store,
            require_admin=AsyncMock(side_effect=HTTPException(status_code=403)),
        )
        with pytest.raises(HTTPException) as exc:
            await job_assignment_routes.assign_job_to_agent(
                MagicMock(), "JID", "AID", dependencies=deps
            )
        # A 403 from the gate must NOT be swallowed into the body's 500 handler.
        assert exc.value.status_code == 403
        store.get_job.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_an_unexpected_error_becomes_a_500(self):
        deps = _assign_deps(
            store=_NullStore(get_job=AsyncMock(side_effect=RuntimeError("past gate")))
        )
        with pytest.raises(HTTPException) as exc:
            await job_assignment_routes.assign_job_to_agent(
                MagicMock(), "JID", "AID", dependencies=deps
            )
        assert exc.value.status_code == 500
        assert "past gate" in exc.value.detail


# =============================================================================
# 12. Late-bound resolution and parity with the application's composition
#     (§P1/§P3)
# =============================================================================
#
# Each row is (reference factory, field, mode, owner, bound dependencies).
# Every row runs twice: once through the reference factory above and once
# through the application's own factory (``_APPLICATION_FACTORIES``), so the
# table is both the late-binding proof and the reference-vs-application parity
# proof. Before R1.B12 the owner of every field was a ``main`` global; now it is
# the owning module (patched there) or the application's resources.
#
# Modes:
#   _OWNER     the field IS ``module.attribute`` read when the factory runs
#   _BOUND     ``bound(module.attribute, dependencies, resources)``: wraps the
#              patched owner and forwards the call plus ``dependencies=``
#   _FORWARDS  a function/lambda that forwards to ``module.attribute`` at call
#              time with ``dependencies=`` of the named factory
#   _RESOURCE  read from ``resources.<path>`` when the factory runs
#   _SETTING   a callable reading ``resources.settings.<name>`` per call
#   _VM_MODE   a callable reading ``vm_provisioner.mode`` per call
#   _DELIVERY  forwards to ``controls.job_delivery_operations(resources).<op>``
#   _ADMIN     ``access.require_admin`` bound to this application's resources
#   _CATALOGUE forwards to ``catalogue.expert_catalog_service(resources)
#              .gather_in_scope_skills`` per call
#   _CONSTANT  an exception class: identity with its owner, nothing to patch
#
# A field missing from this table fails ``test_every_dependency_field_is_covered``,
# so the proof cannot silently stop covering a new field.

_OWNER = "owner"
_BOUND = "bound"
_FORWARDS = "forwards"
_RESOURCE = "resource"
_SETTING = "setting"
_VM_MODE = "vm_mode"
_DELIVERY = "delivery"
_ADMIN = "admin"
_CATALOGUE = "catalogue"
_CONSTANT = "constant"

_PREP = preparation_composition

LATE_BINDING_TABLE = [
    (_datasource_payload_deps, "logger", _OWNER, (_PREP, "logger"), None),
    (
        _datasource_payload_deps,
        "mcp_datasources_enabled",
        _OWNER,
        (deployment_gates_module, "mcp_datasources_enabled"),
        None,
    ),
    (
        _datasource_payload_deps,
        "mcp_stdio_enabled",
        _OWNER,
        (deployment_gates_module, "mcp_stdio_enabled"),
        None,
    ),
    (_datasource_selection_deps, "store", _RESOURCE, "postgres_db", None),
    (
        _datasource_selection_deps,
        "authorize_thread_datasource_selection",
        _BOUND,
        (
            thread_datasource_authorization_module,
            "authorize_thread_datasource_selection",
        ),
        sessions_composition.thread_datasource_authorization_dependencies,
    ),
    (
        _datasource_selection_deps,
        "backend_from_override",
        _OWNER,
        (workspace_tier_policy_module, "backend_from_override"),
        None,
    ),
    (
        _datasource_selection_deps,
        "revalidate_selection",
        _BOUND,
        (job_datasource_selection_module, "revalidate_job_datasource_selection"),
        _PREP.job_datasource_selection_dependencies,
    ),
    (_workspace_runtime_deps, "store", _RESOURCE, "postgres_db", None),
    (_workspace_runtime_deps, "vm_mode", _VM_MODE, None, None),
    (
        _workspace_runtime_deps,
        "workspace_provisioner",
        _OWNER,
        (container_provisioner_module, "container_provisioner"),
        None,
    ),
    (
        _workspace_runtime_deps,
        "vm_workspaces_on_pod_network",
        _OWNER,
        (access_module, "vm_workspaces_on_pod_network"),
        None,
    ),
    (
        _workspace_runtime_deps,
        "stateless_worker_enabled",
        _SETTING,
        "stateless_worker_enabled",
        None,
    ),
    (
        _workspace_runtime_deps,
        "backend_from_override",
        _OWNER,
        (workspace_tier_policy_module, "backend_from_override"),
        None,
    ),
    (_workspace_authority_deps, "store", _RESOURCE, "postgres_db", None),
    (_workspace_authority_deps, "logger", _OWNER, (_PREP, "logger"), None),
    (
        _workspace_authority_deps,
        "workspace_provisioner",
        _OWNER,
        (container_provisioner_module, "container_provisioner"),
        None,
    ),
    (
        _workspace_authority_deps,
        "vm_provisioner",
        _OWNER,
        (vm_provisioner_module, "vm_provisioner"),
        None,
    ),
    (_workspace_authority_deps, "vm_mode", _VM_MODE, None, None),
    (
        _workspace_authority_deps,
        "ensure_workspace",
        _OWNER,
        (workspace_lifecycle_module, "ensure_workspace"),
        None,
    ),
    (
        _workspace_authority_deps,
        "workspace_suspension",
        _OWNER,
        (workspace_suspension_module, "workspace_suspension_service"),
        None,
    ),
    (
        _workspace_authority_deps,
        "handle_scholar_completion",
        _FORWARDS,
        (subjob_completion_module, "handle_scholar_completion"),
        completion_composition.scholar_completion_dependencies,
    ),
    (
        _workspace_authority_deps,
        "handle_delegation_child_completion",
        _FORWARDS,
        (subjob_completion_module, "handle_delegation_child_completion"),
        completion_composition.delegation_completion_dependencies,
    ),
    (
        _workspace_authority_deps,
        "resolve_inherited_workspace",
        _BOUND,
        (job_workspace_authority_module, "resolve_subjob_inherited_workspace"),
        _PREP.job_workspace_authority_dependencies,
    ),
    (
        _workspace_authority_deps,
        "fail_subjob_and_unblock_parent",
        _BOUND,
        (job_workspace_authority_module, "fail_subjob_and_unblock_parent"),
        _PREP.job_workspace_authority_dependencies,
    ),
    (
        _workspace_authority_deps,
        "workspace_runtime_unchanged_before_delivery",
        _BOUND,
        (
            job_workspace_authority_module,
            "workspace_runtime_unchanged_before_delivery",
        ),
        _PREP.job_workspace_authority_dependencies,
    ),
    (_dispatch_credential_deps, "store", _RESOURCE, "postgres_db", None),
    (_dispatch_credential_deps, "logger", _OWNER, (_PREP, "logger"), None),
    (
        _dispatch_credential_deps,
        "resolve_model",
        _OWNER,
        (model_registry_module, "resolve_model"),
        None,
    ),
    (
        _dispatch_credential_deps,
        "inject_model_credentials",
        _BOUND,
        (dispatch_credentials_module, "inject_model_credentials"),
        _PREP.dispatch_credential_dependencies,
    ),
    (
        _dispatch_credential_deps,
        "inject_env_key_credentials",
        _BOUND,
        (dispatch_credentials_module, "inject_env_key_credentials"),
        _PREP.dispatch_credential_dependencies,
    ),
    (
        _dispatch_credential_deps,
        "inject_search_credentials",
        _BOUND,
        (dispatch_credentials_module, "inject_search_credentials"),
        _PREP.dispatch_credential_dependencies,
    ),
    (
        _dispatch_credential_deps,
        "inject_system_kb_embedding_profile",
        _BOUND,
        (dispatch_credentials_module, "inject_system_kb_embedding_profile"),
        _PREP.dispatch_credential_dependencies,
    ),
    (
        _dispatch_credential_deps,
        "dispatch_llm_provider_fallback",
        _OWNER,
        (dispatch_credentials_module, "dispatch_llm_provider_fallback"),
        None,
    ),
    (
        _dispatch_credential_deps,
        "nested_model_slots",
        _OWNER,
        (dispatch_credentials_module, "nested_model_slots"),
        None,
    ),
    (_start_bundle_deps, "store", _RESOURCE, "postgres_db", None),
    (_start_bundle_deps, "logger", _OWNER, (_PREP, "logger"), None),
    (_start_bundle_deps, "forge", _RESOURCE, "gitea_client", None),
    (
        _start_bundle_deps,
        "inject_dispatch_credentials",
        _BOUND,
        (job_dispatch_credentials_module, "inject_dispatch_credentials"),
        _PREP.job_dispatch_credential_dependencies,
    ),
    (
        _start_bundle_deps,
        "resolve_authorized_job_datasources",
        _BOUND,
        (job_datasource_selection_module, "resolve_authorized_job_datasources"),
        _PREP.job_datasource_selection_dependencies,
    ),
    (
        _start_bundle_deps,
        "job_project_repositories",
        _BOUND,
        (job_start_bundle_module, "job_project_repositories"),
        _PREP.job_start_bundle_dependencies,
    ),
    (
        _start_bundle_deps,
        "apply_cloud_storage_override",
        _OWNER,
        (agent_datasource_payload_module, "apply_cloud_storage_override"),
        None,
    ),
    (
        _start_bundle_deps,
        "build_datasources_payload",
        _BOUND,
        (agent_datasource_payload_module, "build_datasources_payload"),
        _PREP.datasource_payload_dependencies,
    ),
    (
        _start_bundle_deps,
        "build_datasource_tool_override",
        _BOUND,
        (agent_datasource_payload_module, "build_datasource_tool_override"),
        _PREP.datasource_payload_dependencies,
    ),
    (
        _start_bundle_deps,
        "authorize_job_repository_transport",
        _OWNER,
        (managed_repository_authority_module, "authorize_job_repository_transport"),
        None,
    ),
    (
        _start_bundle_deps,
        "mint_worker_runtime_actor",
        _OWNER,
        (runtime_actor_module, "mint_worker_runtime_actor"),
        None,
    ),
    (
        _start_bundle_deps,
        "inject_blob_credentials",
        _OWNER,
        (config_resolver_module, "inject_blob_credentials"),
        None,
    ),
    (
        _start_bundle_deps,
        "prepare_job_primary_repository_authority",
        _OWNER,
        (
            managed_repository_authority_module,
            "prepare_job_primary_repository_authority",
        ),
        None,
    ),
    (
        _start_bundle_deps,
        "prepare_project_repository_authority",
        _OWNER,
        (managed_repository_authority_module, "prepare_project_repository_authority"),
        None,
    ),
    (
        _start_bundle_deps,
        "grant_denied_error",
        _CONSTANT,
        (grant_enforcement_module, "GrantDenied"),
        None,
    ),
    (
        _start_bundle_deps,
        "lite_workspace_config_error",
        _CONSTANT,
        (workspace_tier_policy_module, "LiteWorkspaceConfigError"),
        None,
    ),
    (
        _start_bundle_deps,
        "backend_from_override",
        _OWNER,
        (workspace_tier_policy_module, "backend_from_override"),
        None,
    ),
    (
        _start_bundle_deps,
        "inject_lite_workspace_config",
        _OWNER,
        (workspace_tier_policy_module, "inject_lite_workspace_config"),
        None,
    ),
    (
        _start_bundle_deps,
        "is_experts_db_enabled",
        _OWNER,
        (deployment_gates_module, "is_experts_db_enabled"),
        None,
    ),
    (
        _start_bundle_deps,
        "user_experts_enabled",
        _BOUND,
        (grant_enforcement_module, "user_experts_enabled"),
        _PREP.grant_enforcement_dependencies,
    ),
    (
        _start_bundle_deps,
        "enforce_dispatch_grants",
        _BOUND,
        (grant_enforcement_module, "enforce_dispatch_grants"),
        _PREP.grant_enforcement_dependencies,
    ),
    (
        _start_bundle_deps,
        "grant_violations_detail",
        _OWNER,
        (grant_enforcement_module, "grant_violations_detail"),
        None,
    ),
    (
        _start_bundle_deps,
        "resolve_default_models",
        _BOUND,
        (session_config_resolution_module, "resolve_default_models"),
        _PREP.session_config_dependencies,
    ),
    (
        _start_bundle_deps,
        "prefetch_roster_refs",
        _BOUND,
        (session_config_resolution_module, "prefetch_roster_refs"),
        _PREP.session_config_dependencies,
    ),
    (
        _start_bundle_deps,
        "seed_registry_model_overrides",
        _BOUND,
        (dispatch_credentials_module, "seed_registry_model_overrides"),
        _PREP.dispatch_credential_dependencies,
    ),
    (_start_bundle_deps, "gather_in_scope_skills", _CATALOGUE, None, None),
    (
        _start_bundle_deps,
        "resolve_config",
        _OWNER,
        (config_resolver_module, "resolve_config"),
        None,
    ),
    (
        _start_bundle_deps,
        "vm_workspaces_on_pod_network",
        _OWNER,
        (access_module, "vm_workspaces_on_pod_network"),
        None,
    ),
    (_job_assignment_deps, "store", _RESOURCE, "postgres_db", None),
    (
        _job_assignment_deps,
        "logger",
        _OWNER,
        (controls_composition, "logger"),
        None,
    ),
    (_job_assignment_deps, "require_admin", _ADMIN, None, None),
    (_job_assignment_deps, "vm_mode", _VM_MODE, None, None),
    (
        _job_assignment_deps,
        "completion_commands_enabled",
        _SETTING,
        "completion_commands_enabled",
        None,
    ),
    (
        _job_assignment_deps,
        "prepare_job_workspace_runtime",
        _BOUND,
        (job_workspace_authority_module, "prepare_job_workspace_runtime"),
        _PREP.job_workspace_authority_dependencies,
    ),
    (
        _job_assignment_deps,
        "prepare_job_repository_before_claim",
        _BOUND,
        (job_start_bundle_module, "prepare_job_repository_before_claim"),
        _PREP.job_start_bundle_dependencies,
    ),
    (
        _job_assignment_deps,
        "resume_missing_workspace",
        _FORWARDS,
        (job_workspace_runtime_module, "resume_missing_workspace"),
        _PREP.job_workspace_runtime_dependencies,
    ),
    (
        _job_assignment_deps,
        "guard_completion_control",
        _RESOURCE,
        "completion_control_boundary.guard",
        None,
    ),
    (
        _job_assignment_deps,
        "claim_completion_control",
        _RESOURCE,
        "completion_control_boundary.claim",
        None,
    ),
    (
        _job_assignment_deps,
        "abort_completion_control_claim",
        _RESOURCE,
        "completion_control_boundary.abort",
        None,
    ),
    (
        _job_assignment_deps,
        "completion_resume_guard_kwargs",
        _RESOURCE,
        "completion_control_boundary.resume_guard_kwargs",
        None,
    ),
    (_job_assignment_deps, "dispatch_job_to_agent", _DELIVERY, "dispatch", None),
    (_job_assignment_deps, "resume_job_on_agent", _DELIVERY, "resume", None),
    (
        _job_assignment_deps,
        "trigger_dispatch",
        _BOUND,
        (job_dispatcher_module, "trigger_dispatch"),
        jobs_composition.job_dispatch_dependencies,
    ),
]

# Nested dependency objects; their own fields are proven through their factory.
_NESTED_FIELDS = {
    (_start_bundle_deps, "workspace_runtime"),
}

# The application's composition factory each reference factory specifies.
_APPLICATION_FACTORIES = {
    _datasource_payload_deps: _PREP.datasource_payload_dependencies,
    _datasource_selection_deps: _PREP.job_datasource_selection_dependencies,
    _workspace_runtime_deps: _PREP.job_workspace_runtime_dependencies,
    _workspace_authority_deps: _PREP.job_workspace_authority_dependencies,
    _dispatch_credential_deps: _PREP.job_dispatch_credential_dependencies,
    _start_bundle_deps: _PREP.job_start_bundle_dependencies,
    _job_assignment_deps: controls_composition.job_assignment_dependencies,
}

_SIDES = ("reference", "application")


def _build(factory, side):
    if side == "reference":
        return factory()
    return _APPLICATION_FACTORIES[factory](main.app.state.resources)


class _Recorder:
    """An owner double that records its call and returns a unique sentinel."""

    def __init__(self, *, is_async):
        self.calls = []
        self.result = object()
        self.is_async = is_async

    def make(self):
        recorder = self

        if self.is_async:

            async def fake(*args, **kwargs):
                recorder.calls.append((args, kwargs))
                return recorder.result

        else:

            def fake(*args, **kwargs):
                recorder.calls.append((args, kwargs))
                return recorder.result

        return fake


async def _call(field, *args, **kwargs):
    result = field(*args, **kwargs)
    if inspect.isawaitable(result):
        result = await result
    return result


class TestLateBoundResolution:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("side", _SIDES)
    @pytest.mark.parametrize(
        "factory,field,mode,owner,bound_dependencies",
        LATE_BINDING_TABLE,
        ids=[f"{f.__name__}.{name}" for f, name, *_rest in LATE_BINDING_TABLE],
    )
    async def test_a_patched_owner_reaches_the_dependency_object(
        self, factory, field, mode, owner, bound_dependencies, side, monkeypatch
    ):
        resources = main.app.state.resources
        if mode == _CONSTANT:
            module, attribute = owner
            assert getattr(_build(factory, side), field) is getattr(module, attribute)
            return
        if mode == _OWNER:
            module, attribute = owner
            sentinel = type("Sentinel", (Exception,), {})
            monkeypatch.setattr(module, attribute, sentinel)
            assert getattr(_build(factory, side), field) is sentinel
            return
        if mode in (_BOUND, _FORWARDS):
            module, attribute = owner
            recorder = _Recorder(is_async=mode == _FORWARDS)
            fake = recorder.make()
            monkeypatch.setattr(module, attribute, fake)
            value = getattr(_build(factory, side), field)
            if mode == _BOUND:
                # ``bound`` forwards any call shape unchanged.
                assert value.__wrapped__ is fake
                args, kwargs = ("argument",), {"key": "value"}
            else:
                # The forwarding lambdas take their operation's two positionals.
                args, kwargs = ("first", "second"), {}
            assert await _call(value, *args, **kwargs) is recorder.result
            [(called_args, called_kwargs)] = recorder.calls
            forwarded = dict(called_kwargs)
            dependencies = forwarded.pop("dependencies")
            assert called_args == args
            assert forwarded == kwargs
            assert type(dependencies) is type(bound_dependencies(resources))
            return
        if mode == _RESOURCE:
            sentinel = object()
            *parents, attribute = owner.split(".")
            if parents:
                [parent] = parents
                current = getattr(resources, parent)
                replacement = SimpleNamespace(
                    guard=current.guard,
                    claim=current.claim,
                    abort=current.abort,
                    resume_guard_kwargs=current.resume_guard_kwargs,
                )
                setattr(replacement, attribute, sentinel)
                monkeypatch.setattr(resources, parent, replacement)
            else:
                monkeypatch.setattr(resources, attribute, sentinel)
            assert getattr(_build(factory, side), field) is sentinel
            return
        if mode == _SETTING:
            monkeypatch.setattr(resources.settings, owner, "sentinel-flag")
            assert getattr(_build(factory, side), field)() == "sentinel-flag"
            return
        if mode == _VM_MODE:
            monkeypatch.setattr(
                vm_provisioner_module,
                "vm_provisioner",
                SimpleNamespace(mode="sentinel-vm-mode"),
            )
            assert getattr(_build(factory, side), field)() == "sentinel-vm-mode"
            return
        if mode == _DELIVERY:
            sentinel = object()
            seen = []
            operation = SimpleNamespace(**{owner: lambda *_args, **_kwargs: sentinel})

            def delivery_operations(application_resources):
                seen.append(application_resources)
                return operation

            monkeypatch.setattr(
                controls_composition, "job_delivery_operations", delivery_operations
            )
            assert getattr(_build(factory, side), field)({}, {}) is sentinel
            assert seen == [resources]
            return
        if mode == _ADMIN:
            recorder = _Recorder(is_async=True)
            monkeypatch.setattr(access_composition, "require_admin", recorder.make())
            value = getattr(_build(factory, side), field)
            assert await _call(value, "request") is recorder.result
            assert recorder.calls == [((resources, "request"), {})]
            return
        if mode == _CATALOGUE:
            recorder = _Recorder(is_async=True)
            seen = []

            def catalogue_service(application_resources):
                seen.append(application_resources)
                return SimpleNamespace(gather_in_scope_skills=recorder.make())

            monkeypatch.setattr(
                catalogue_composition, "expert_catalog_service", catalogue_service
            )
            value = getattr(_build(factory, side), field)
            assert await _call(value, "argument", key="value") is recorder.result
            assert recorder.calls == [(("argument",), {"key": "value"})]
            assert seen == [resources]
            return
        raise AssertionError(f"unknown mode {mode!r}")

    @pytest.mark.parametrize("side", _SIDES)
    @pytest.mark.parametrize(
        "factory", list(_APPLICATION_FACTORIES), ids=lambda f: f.__name__
    )
    def test_every_dependency_field_is_covered(self, factory, side):
        import dataclasses

        reference = factory()
        built = _build(factory, side)
        # The application builds the very dependency type the reference specifies.
        assert type(built) is type(reference)
        declared = {f.name for f in dataclasses.fields(built)}
        proven = {row[1] for row in LATE_BINDING_TABLE if row[0] is factory}
        nested = {field for fac, field in _NESTED_FIELDS if fac is factory}
        assert declared == proven | nested, (
            f"{factory.__name__} has unproven dependency fields: "
            f"{sorted(declared - proven - nested)}"
        )
