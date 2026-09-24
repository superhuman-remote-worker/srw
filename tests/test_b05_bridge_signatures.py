"""Every former B05 bridge's owner is bound by the composition with its shape.

R1.B05 left ~50 thin wrappers in ``orchestrator.main`` because callers owned by
later batches still resolved those names, and this file pinned each wrapper
against the operation it forwarded to. Both halves of that pin had failed
during integration in ways nothing else caught:

* ``_validate_mcp_datasource`` lost a positional parameter, so every caller
  raised ``TypeError`` — loud, but only at the call site.
* ``resume_missing_workspace`` once gained an ``async``, so its synchronous caller
  stored a coroutine object and logged ``Failed to resume job …: <coroutine
  object …>``. Four more (``_account_defaults_layer``, ``_grant_project_ids``,
  ``_enforce_save_grants``, ``_strip_save_grants``) lost theirs.

R1.B12 removed the wrappers on purpose. A consumer now binds the owning
operation itself: ``bound(owner, dependency_factory, resources)`` in
``orchestrator.application``, which rebuilds the dependency object per call.
The two properties the wrappers had to keep are now properties of those
bindings, and they are checked here for every binding of every former bridge's
owner:

* **await parity** — a bound site awaits exactly when its owner does (the
  dangerous half: an un-awaited coroutine never raises at the boundary);
* **call shape** — the site forwards to its owner (by identity), passes every
  argument through unchanged and adds only ``dependencies=`` built from this
  application's resources by the owner's own dependency factory.

Sites are discovered by building every ``*_dependencies`` / ``*_operations``
composition factory and walking what it returns, so a new binding is checked
without editing this file, and every former bridge must keep at least one.

Deleted with the wrappers, because nothing is left for them to inspect: the
AST parse of ``main``'s wrapper body (one forwarding statement, ``*args`` and
``**kwargs`` both present, target resolving through ``main``'s namespace) and
the parameter-list comparison for the named-parameter wrappers. ``bound``
forwards ``*args``/``**kwargs`` by construction and exposes the owner's own
signature through ``functools.wraps``; the call-through case below is the
falsifiable remainder of both.
"""

from __future__ import annotations

import dataclasses
import functools
import importlib
import inspect
import pkgutil
from types import ModuleType
from typing import Any, Callable

import pytest

import orchestrator.application as application_package
import orchestrator.main as main
from orchestrator.application import catalogue as catalogue_composition
from orchestrator.application import preparation
from orchestrator.application.resources import ApplicationResources, bound
from orchestrator.services import (
    agent_datasource_payload,
    agent_toolset_probe,
    dispatch_credentials,
    grant_enforcement,
    job_datasource_selection,
    job_dispatch_credentials,
    job_start_bundle,
    job_workspace_authority,
    job_workspace_runtime,
    session_config_resolution,
    stateless_workspace_scheduler,
    thread_mount_rows,
    thread_workspace_delivery,
    vm_workspace_policy,
)

# former main bridge -> (owning module, operation, the dependency factory the
# bridge built its ``dependencies=`` with)
BRIDGES: dict[str, tuple[ModuleType, str, Callable[[ApplicationResources], Any]]] = {
    "_resolve_default_models": (
        session_config_resolution,
        "resolve_default_models",
        preparation.session_config_dependencies,
    ),
    "_prefetch_roster_refs": (
        session_config_resolution,
        "prefetch_roster_refs",
        preparation.session_config_dependencies,
    ),
    "_account_defaults_layer": (
        session_config_resolution,
        "account_defaults_layer",
        preparation.session_config_dependencies,
    ),
    "_acknowledged_grant_strip": (
        session_config_resolution,
        "acknowledged_grant_strip",
        preparation.session_config_dependencies,
    ),
    "_resolve_session_config": (
        session_config_resolution,
        "resolve_session_config",
        preparation.session_config_dependencies,
    ),
    "_agent_toolset_measurement": (
        agent_toolset_probe,
        "agent_toolset_measurement",
        preparation.agent_toolset_dependencies,
    ),
    "_user_experts_enabled": (
        grant_enforcement,
        "user_experts_enabled",
        preparation.grant_enforcement_dependencies,
    ),
    "_grant_project_ids": (
        grant_enforcement,
        "grant_project_ids",
        preparation.grant_enforcement_dependencies,
    ),
    "_strip_save_grants": (
        grant_enforcement,
        "strip_save_grants",
        preparation.grant_enforcement_dependencies,
    ),
    "_enforce_expert_save_prelude": (
        grant_enforcement,
        "enforce_expert_save_prelude",
        preparation.grant_enforcement_dependencies,
    ),
    "_enforce_expert_save": (
        grant_enforcement,
        "enforce_expert_save",
        preparation.grant_enforcement_dependencies,
    ),
    "_resolve_runner_grants": (
        grant_enforcement,
        "resolve_runner_grants",
        preparation.grant_enforcement_dependencies,
    ),
    "_enforce_dispatch_grants": (
        grant_enforcement,
        "enforce_dispatch_grants",
        preparation.grant_enforcement_dependencies,
    ),
    "_enforce_session_create_grants": (
        grant_enforcement,
        "enforce_session_create_grants",
        preparation.grant_enforcement_dependencies,
    ),
    "_enforce_job_create_grants": (
        grant_enforcement,
        "enforce_job_create_grants",
        preparation.grant_enforcement_dependencies,
    ),
    "_check_vm_permission": (
        vm_workspace_policy,
        "check_vm_permission",
        preparation.vm_permission_dependencies,
    ),
    "_enforce_job_workspace_upgrade_grants": (
        grant_enforcement,
        "enforce_job_workspace_upgrade_grants",
        preparation.grant_enforcement_dependencies,
    ),
    "_seed_registry_model_overrides": (
        dispatch_credentials,
        "seed_registry_model_overrides",
        preparation.dispatch_credential_dependencies,
    ),
    "_inject_model_credentials": (
        dispatch_credentials,
        "inject_model_credentials",
        preparation.dispatch_credential_dependencies,
    ),
    "_inject_env_key_credentials": (
        dispatch_credentials,
        "inject_env_key_credentials",
        preparation.dispatch_credential_dependencies,
    ),
    "_inject_search_credentials": (
        dispatch_credentials,
        "inject_search_credentials",
        preparation.dispatch_credential_dependencies,
    ),
    "_inject_system_kb_embedding_profile": (
        dispatch_credentials,
        "inject_system_kb_embedding_profile",
        preparation.dispatch_credential_dependencies,
    ),
    "_inject_thread_dispatch_credentials": (
        dispatch_credentials,
        "inject_thread_dispatch_credentials",
        preparation.dispatch_credential_dependencies,
    ),
    "_build_datasource_tool_override": (
        agent_datasource_payload,
        "build_datasource_tool_override",
        preparation.datasource_payload_dependencies,
    ),
    "_build_datasources_payload": (
        agent_datasource_payload,
        "build_datasources_payload",
        preparation.datasource_payload_dependencies,
    ),
    "_inherit_parent_datasource_ids": (
        job_datasource_selection,
        "inherit_parent_datasource_ids",
        preparation.job_datasource_selection_dependencies,
    ),
    "_filter_implicit_lite_datasource_ids": (
        job_datasource_selection,
        "filter_implicit_lite_datasource_ids",
        preparation.job_datasource_selection_dependencies,
    ),
    "_revalidate_job_datasource_selection": (
        job_datasource_selection,
        "revalidate_job_datasource_selection",
        preparation.job_datasource_selection_dependencies,
    ),
    "_resolve_authorized_job_datasources": (
        job_datasource_selection,
        "resolve_authorized_job_datasources",
        preparation.job_datasource_selection_dependencies,
    ),
    "_fail_vm_parked_job": (
        job_workspace_runtime,
        "fail_vm_parked_job",
        preparation.job_workspace_runtime_dependencies,
    ),
    "_job_needs_sandbox": (
        job_workspace_runtime,
        "job_needs_sandbox",
        preparation.job_workspace_runtime_dependencies,
    ),
    "_resolve_requested_job_execution_lane": (
        job_workspace_runtime,
        "resolve_requested_job_execution_lane",
        preparation.job_workspace_runtime_dependencies,
    ),
    "_scholar_should_provision_parent_container": (
        job_workspace_runtime,
        "scholar_should_provision_parent_container",
        preparation.job_workspace_runtime_dependencies,
    ),
    "_workspace_runtime_unchanged_before_delivery": (
        job_workspace_authority,
        "workspace_runtime_unchanged_before_delivery",
        preparation.job_workspace_authority_dependencies,
    ),
    "_resolve_subjob_inherited_workspace": (
        job_workspace_authority,
        "resolve_subjob_inherited_workspace",
        preparation.job_workspace_authority_dependencies,
    ),
    "_prepare_job_workspace_runtime": (
        job_workspace_authority,
        "prepare_job_workspace_runtime",
        preparation.job_workspace_authority_dependencies,
    ),
    "_fail_subjob_and_unblock_parent": (
        job_workspace_authority,
        "fail_subjob_and_unblock_parent",
        preparation.job_workspace_authority_dependencies,
    ),
    "_provision_parent_workspace_for_scholar": (
        job_workspace_authority,
        "provision_parent_workspace_for_scholar",
        preparation.job_workspace_authority_dependencies,
    ),
    "_inject_dispatch_credentials": (
        job_dispatch_credentials,
        "inject_dispatch_credentials",
        preparation.job_dispatch_credential_dependencies,
    ),
    "_job_project_repositories": (
        job_start_bundle,
        "job_project_repositories",
        preparation.job_start_bundle_dependencies,
    ),
    "_prepare_job_repository_before_claim": (
        job_start_bundle,
        "prepare_job_repository_before_claim",
        preparation.job_start_bundle_dependencies,
    ),
    "_thread_project_ids": (
        thread_mount_rows,
        "thread_project_ids",
        preparation.thread_mount_dependencies,
    ),
    "_should_skip_session_folder": (
        thread_mount_rows,
        "should_skip_session_folder",
        preparation.thread_mount_dependencies,
    ),
    "_resolve_thread_datasources": (
        thread_mount_rows,
        "resolve_thread_datasources",
        preparation.thread_mount_dependencies,
    ),
    "_resolve_thread_repositories": (
        thread_mount_rows,
        "resolve_thread_repositories",
        preparation.thread_mount_dependencies,
    ),
    "_require_pinned_workspace_credential_owner": (
        thread_workspace_delivery,
        "require_pinned_workspace_credential_owner",
        preparation.thread_workspace_delivery_dependencies,
    ),
    "_schedule_stateless_workspace_ensure": (
        stateless_workspace_scheduler,
        "schedule_stateless_workspace_ensure",
        preparation.stateless_workspace_schedule_dependencies,
    ),
}


# -- site discovery ----------------------------------------------------------


def _code_of_bound(operation: Callable[..., Any]) -> Any:
    return bound(operation, lambda _resources: None, main.app.state.resources).__code__


def _sync_probe(*_args: Any, **_kwargs: Any) -> None:
    return None


async def _async_probe(*_args: Any, **_kwargs: Any) -> None:
    return None


#: The two function bodies ``bound`` returns; a site is a function running one.
_BOUND_CODES = {_code_of_bound(_sync_probe), _code_of_bound(_async_probe)}

_REQUEST = object()


def _roots() -> dict[str, Callable[[ApplicationResources], Any]]:
    """Every composition factory, plus the catalogue's per-request policy."""

    roots: dict[str, Callable[[ApplicationResources], Any]] = {}
    for info in pkgutil.iter_modules(application_package.__path__):
        module = importlib.import_module(f"{application_package.__name__}.{info.name}")
        for name, function in vars(module).items():
            if (
                not inspect.isfunction(function)
                or function.__module__ != module.__name__
            ):
                continue
            if not name.endswith(("_dependencies", "_operations")):
                continue
            if list(inspect.signature(function).parameters) != ["resources"]:
                continue
            roots[f"{info.name}.{name}"] = function
    # The expert write policy is built per request by a lambda field.
    roots["catalogue.expert_catalog_dependencies.write_policy_factory(request)"] = (
        lambda resources: catalogue_composition.expert_catalog_dependencies(
            resources
        ).write_policy_factory(_REQUEST)
    )
    return roots


def _children(value: Any) -> list[tuple[str, Any]]:
    if isinstance(value, (type, ModuleType, ApplicationResources)):
        return []
    if dataclasses.is_dataclass(value):
        return [(f.name, getattr(value, f.name)) for f in dataclasses.fields(value)]
    module = type(value).__module__ or ""
    if module.startswith(("orchestrator.services", "orchestrator.routers")):
        return list(getattr(value, "__dict__", {}).items())
    return []


def _site(value: Any) -> tuple[Callable[..., Any], tuple[Any, ...]] | None:
    """``(bound wrapper, positional args a partial pre-binds)`` or ``None``."""

    prefix: tuple[Any, ...] = ()
    if isinstance(value, functools.partial):
        prefix, value = value.args, value.func
    if getattr(value, "__code__", None) in _BOUND_CODES:
        return value, prefix
    return None


def _walk(value: Any, path: tuple[str, ...], seen: set[int], out: list) -> None:
    if id(value) in seen or len(path) > 5:
        return
    seen.add(id(value))
    for name, child in _children(value):
        found = _site(child)
        if found is not None:
            out.append((path + (name,), *found))
        else:
            _walk(child, path + (name,), seen, out)


@functools.cache
def _all_sites() -> dict[tuple[str, str], list[tuple[str, tuple[str, ...]]]]:
    """``(owner module, operation) -> [(root, attribute path)]``."""

    resources = main.app.state.resources
    sites: dict[tuple[str, str], list[tuple[str, tuple[str, ...]]]] = {}
    for label, root in _roots().items():
        found: list = []
        _walk(root(resources), (), set(), found)
        for path, wrapper, _prefix in found:
            operation = inspect.getclosurevars(wrapper).nonlocals["operation"]
            key = (operation.__module__, operation.__name__)
            sites.setdefault(key, []).append((label, path))
    return sites


def _resolve(label: str, path: tuple[str, ...]):
    """Rebuild one site from its root (after any patch) and return it."""

    value = _roots()[label](main.app.state.resources)
    for name in path:
        value = getattr(value, name)
    found = _site(value)
    assert found is not None, f"{label}:{'.'.join(path)} is no longer a bound site"
    return found


def _sites_of(bridge_name: str):
    module, attribute, _factory = BRIDGES[bridge_name]
    return _all_sites().get((module.__name__, attribute), [])


# -- the contract ------------------------------------------------------------


@pytest.mark.parametrize("bridge_name", sorted(BRIDGES))
def test_the_owner_is_bound_by_the_composition(bridge_name: str) -> None:
    """The consumers that resolved ``main.<bridge>`` now bind the owner."""

    assert _sites_of(bridge_name), (
        f"no composition factory binds the owner of former main.{bridge_name}"
    )


@pytest.mark.parametrize("bridge_name", sorted(BRIDGES))
def test_a_bound_site_awaits_what_its_owner_awaits(bridge_name: str) -> None:
    """The dangerous half: a flipped ``async`` never raises at the boundary.

    A synchronous caller handed a coroutine stores it, never runs it, and fails
    somewhere else entirely — which is exactly how ``_resume_missing_workspace``
    surfaced, as ``Failed to resume job …: <coroutine object …>``.
    """
    module, attribute, _factory = BRIDGES[bridge_name]
    owner = getattr(module, attribute)
    for label, path in _sites_of(bridge_name):
        wrapper, _prefix = _resolve(label, path)
        assert inspect.iscoroutinefunction(wrapper) is inspect.iscoroutinefunction(
            owner
        ), (
            f"{label}:{'.'.join(path)} is "
            f"{'async' if inspect.iscoroutinefunction(wrapper) else 'sync'} but "
            f"{owner.__module__}.{owner.__name__} is "
            f"{'async' if inspect.iscoroutinefunction(owner) else 'sync'}"
        )


@pytest.mark.parametrize("bridge_name", sorted(BRIDGES))
def test_a_bound_site_forwards_to_its_owner_with_its_dependency_factory(
    bridge_name: str,
) -> None:
    """Forwarding to the wrong function was the other failure; so was the
    wrong dependency object. Each binding names the owner, the owner's own
    dependency factory and this application's resources."""

    module, attribute, factory = BRIDGES[bridge_name]
    owner = getattr(module, attribute)
    for label, path in _sites_of(bridge_name):
        wrapper, _prefix = _resolve(label, path)
        closure = inspect.getclosurevars(wrapper).nonlocals
        where = f"{label}:{'.'.join(path)}"
        assert wrapper.__wrapped__ is owner, f"{where} does not wrap the owner"
        assert closure["operation"] is owner, f"{where} forwards elsewhere"
        assert closure["dependencies"] is factory, (
            f"{where} builds dependencies with {closure['dependencies'].__name__}, "
            f"not {factory.__name__}"
        )
        assert closure["resources"] is main.app.state.resources, (
            f"{where} is bound to another application's resources"
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("bridge_name", sorted(BRIDGES))
async def test_a_bound_site_passes_the_call_through_unchanged(
    bridge_name: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Call every site with a recording owner: it must receive exactly the
    caller's arguments plus ``dependencies=`` of its factory's type — the
    falsifiable half of "accepts what its target accepts"."""

    module, attribute, factory = BRIDGES[bridge_name]
    owner_is_async = inspect.iscoroutinefunction(getattr(module, attribute))
    # Discover before patching: sites are keyed by the real owner's name.
    sites = _sites_of(bridge_name)
    assert sites
    calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []
    result = object()

    if owner_is_async:

        async def recording_owner(*args: Any, **kwargs: Any) -> object:
            calls.append((args, kwargs))
            return result

    else:

        def recording_owner(*args: Any, **kwargs: Any) -> object:
            calls.append((args, kwargs))
            return result

    monkeypatch.setattr(module, attribute, recording_owner)
    expected_type = type(factory(main.app.state.resources))
    for label, path in sites:
        calls.clear()
        wrapper, prefix = _resolve(label, path)
        assert wrapper.__wrapped__ is recording_owner, (
            f"{label}:{'.'.join(path)} did not pick up the patched owner"
        )
        returned = wrapper(*prefix, "argument", keyword="value")
        if owner_is_async:
            returned = await returned
        assert returned is result
        [(args, kwargs)] = calls
        forwarded = dict(kwargs)
        dependencies = forwarded.pop("dependencies")
        assert args == (*prefix, "argument")
        assert forwarded == {"keyword": "value"}
        assert type(dependencies) is expected_type
