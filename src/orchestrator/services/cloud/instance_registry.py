"""Durable activation service for main-cloud backend installations.

The active provider name is not routing authority.  This module is the only
application seam that turns the deployment's configuration into an attested,
immutable backend-instance row and swaps the process-local router.  Historical
adapters are always rebuilt from their retained instance authority.

The configuration is Helm's alone (main_cloud_as_connectors.md,
"Configuration: Helm only"); the admin connection form and its API are gone.
At startup :func:`initialize_main_cloud_instance_authority` compares what Helm
describes with the active instance and reconciles them:

* the same routing and secret references: the active instance is loaded;
* the same installation (equal installation proof), described differently:
  Helm's description is attested and activated, as the form's save did;
* a different installation: activated only when ``replace_installation``
  (``cloud.replaceInstallation`` → ``MAIN_CLOUD_REPLACE_INSTALLATION``) names
  the active instance id. Moving new work to another cloud fences everything
  stamped with the old installation to it, so it never follows from values
  that merely drifted, such as an instance an admin activated through the
  removed form while Helm still describes the bundled cloud;
* a description that fails to attest leaves the active instance serving.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from collections.abc import Awaitable, Callable
from typing import Any, Literal
from uuid import uuid4

from orchestrator.services.cloud import (
    MainCloudBackend,
    MainCloudRouter,
    build_backend_from_config,
)
from orchestrator.services.cloud.backend_instance_authority import (
    MainCloudBackendInstanceAuthority,
)
from orchestrator.services.cloud.config import (
    load_main_cloud_config,
    main_cloud_routing_snapshot,
    main_cloud_secret_references,
)

logger = logging.getLogger(__name__)


def _active_coordinates(value: Any) -> tuple[str, int, str] | None:
    if not isinstance(value, dict):
        return None
    authority = value.get("authority")
    revision = value.get("activation_revision")
    if (
        not isinstance(authority, MainCloudBackendInstanceAuthority)
        or type(revision) is not int
        or revision <= 0
    ):
        return None
    return (
        authority.backend_instance_id,
        revision,
        authority.canonical_json,
    )


#: Who activates an instance Helm describes, on the active pointer's record.
HELM_ACTOR = "helm"

#: An attested, not yet activated ``(adapter, authority)`` pair.
Candidate = tuple[MainCloudBackend, MainCloudBackendInstanceAuthority]

HelmState = Literal["matches", "differs", "invalid"]


@dataclass(frozen=True)
class HelmConfigurationStatus:
    """How Helm's description compares with the active instance, offline.

    ``matches``: same provider, routing and secret references. ``differs``:
    something changed; ``same_installation`` is unknown (``None``) until an
    attestation proves it. ``invalid``: Helm describes no adoptable config
    (a required secret is unset). Never carries a secret value.
    """

    state: HelmState
    backend_id: str | None = None
    detail: str = ""


def helm_configuration_status(
    authority: MainCloudBackendInstanceAuthority | None,
) -> HelmConfigurationStatus:
    """Compare the deployment's description with ``authority`` (no network)."""

    try:
        settings = load_main_cloud_config()
        routing = main_cloud_routing_snapshot(settings)
        secret_refs = main_cloud_secret_references(settings.backend_id)
    except Exception as exc:
        return HelmConfigurationStatus("invalid", detail=type(exc).__name__)
    if authority is None:
        return HelmConfigurationStatus("differs", settings.backend_id, "no_active")
    described = MainCloudBackendInstanceAuthority.capture(
        backend_instance_id=authority.backend_instance_id,
        backend_id=settings.backend_id,
        routing=routing,
        installation_proof_sha256=authority.installation_proof_sha256,
        secret_refs=secret_refs,
        secret_revision=authority.secret_revision,
    )
    if described.backend_id != authority.backend_id:
        detail = "provider"
    elif described.routing_sha256 != authority.routing_sha256:
        detail = "routing"
    elif described.secret_refs != authority.secret_refs:
        detail = "secret_references"
    else:
        return HelmConfigurationStatus("matches", settings.backend_id)
    return HelmConfigurationStatus("differs", settings.backend_id, detail)


async def build_attested_main_cloud_candidate(
    *,
    backend_instance_id: str | None = None,
    secret_revision: int = 1,
) -> tuple[MainCloudBackend, MainCloudBackendInstanceAuthority]:
    """Build and remotely attest Helm's config before any DB activation."""

    settings = load_main_cloud_config()
    secret_refs = main_cloud_secret_references(settings.backend_id)
    backend = build_backend_from_config(settings)
    proposed_instance_id = backend_instance_id or str(uuid4())
    try:
        backend.prepare_backend_instance_attestation(proposed_instance_id)
        initialized = await backend.ensure_initialized()
        proof = backend.installation_proof_sha256
        if not initialized or proof is None:
            raise RuntimeError(
                "main-cloud backend did not attest a stable installation identity"
            )
        authority = MainCloudBackendInstanceAuthority.capture(
            backend_instance_id=proposed_instance_id,
            backend_id=settings.backend_id,
            routing=main_cloud_routing_snapshot(settings),
            installation_proof_sha256=proof,
            secret_refs=secret_refs,
            secret_revision=secret_revision,
        )
        return backend, authority
    except BaseException:
        try:
            await backend.close()
        except Exception:
            pass
        raise


async def reload_active_main_cloud_instance(
    db: Any,
    router: MainCloudRouter,
    *,
    force_rebuild: bool = False,
) -> bool | None:
    """Install the exact current DB pointer after a post-build reread.

    ``None`` means no durable active instance exists. ``False`` means the
    pointer changed while the candidate was being built; the caller should
    retry or wait for the next notification. No stale candidate is activated.
    """

    active = await db.get_active_main_cloud_backend_instance()
    before = _active_coordinates(active)
    if before is None:
        return None
    authority = active["authority"]
    backend = await router.resolve_backend_instance(
        authority,
        force_rebuild=force_rebuild,
    )
    current = await db.get_active_main_cloud_backend_instance()
    if _active_coordinates(current) != before:
        logger.info(
            "Main-cloud active instance changed during adapter attestation; "
            "discarding stale activation"
        )
        return False
    await router.replace_active(backend, authority=authority)
    return True


async def preload_retained_main_cloud_instances(
    db: Any,
    router: MainCloudRouter,
) -> dict[str, str]:
    """Rebuild every retained adapter cache entry without fallback.

    One unavailable historical installation must not relabel its resources to
    the active backend. It remains absent from the cache and callers receive a
    typed refusal; the returned mapping is safe diagnostic state.
    """

    failures: dict[str, str] = {}
    authorities = await db.list_main_cloud_backend_instances()
    for authority in authorities:
        try:
            await router.resolve_backend_instance(authority)
        except Exception as exc:
            failures[authority.backend_instance_id] = type(exc).__name__
            logger.warning(
                "Retained main-cloud instance %s is unresolved (%s)",
                authority.backend_instance_id,
                type(exc).__name__,
            )
    return failures


async def initialize_main_cloud_instance_authority(
    db: Any,
    router: MainCloudRouter,
    *,
    replace_installation: str | None = None,
    notify: Callable[[str], Awaitable[None]] | None = None,
    activated_by: str = "orchestrator-startup",
) -> dict[str, Any]:
    """Resolve existing authority, reconciled with Helm (module docstring).

    The first boot adopts Helm's description transactionally. Afterwards the
    active instance is loaded, and Helm's description replaces it only as the
    module docstring says. ``notify`` receives the instance id after such a
    replacement, to fan it out to the other replicas.
    """

    active = await db.get_active_main_cloud_backend_instance()
    if _active_coordinates(active) is not None:
        reconciled = await _reconcile_with_helm(
            db,
            router,
            active,
            replace_installation=replace_installation,
        )
        if reconciled is not None:
            if notify is not None:
                await notify(reconciled["authority"].backend_instance_id)
            return reconciled
        loaded = await reload_active_main_cloud_instance(db, router)
        if loaded is not True:
            raise RuntimeError("main-cloud active instance changed during startup")
        return active

    backend, proposed = await build_attested_main_cloud_candidate()
    try:
        installed = await db.install_initial_main_cloud_backend_instance(
            proposed,
            activated_by=activated_by,
        )
        if _active_coordinates(installed) is None:
            # A racing replica may have installed a different valid instance.
            # Never relabel this candidate: close it and resolve the winner.
            await backend.close()
            loaded = await reload_active_main_cloud_instance(db, router)
            if loaded is not True:
                raise RuntimeError(
                    "main-cloud initial instance adoption lost authority"
                )
            winner = await db.get_active_main_cloud_backend_instance()
            if _active_coordinates(winner) is None:
                raise RuntimeError("main-cloud active instance is unavailable")
            return winner
        adopted = installed["authority"]
        if (
            adopted.backend_id != proposed.backend_id
            or adopted.routing != proposed.routing
            or adopted.installation_proof_sha256 != proposed.installation_proof_sha256
            or adopted.secret_refs != proposed.secret_refs
            or adopted.secret_revision != proposed.secret_revision
        ):
            raise RuntimeError("main-cloud initial instance adoption changed authority")
        await router.replace_active(backend, authority=adopted)
        return installed
    except BaseException:
        if backend is not router.active:
            try:
                await backend.close()
            except Exception:
                pass
        raise


async def activate_main_cloud_config(
    db: Any,
    router: MainCloudRouter,
    *,
    expected_activation_revision: int,
    activated_by: str,
    candidate: Candidate | None = None,
) -> dict[str, Any] | None:
    """Attest and CAS-activate Helm's described config.

    A routing or installation change creates a new immutable UUID. A change to
    secret references for the same proven installation rotates only the exact
    instance's secret revision. No unresolved adapter is installed locally.
    ``candidate`` is an already attested ``(backend, authority)`` pair; this
    function owns it from here (it is closed unless installed).
    """

    current = await db.get_active_main_cloud_backend_instance()
    current_coordinates = _active_coordinates(current)
    if current_coordinates is None:
        if expected_activation_revision != 0:
            await _close_candidate(candidate)
            return None
        backend, candidate_authority = (
            candidate or await build_attested_main_cloud_candidate()
        )
        try:
            installed = await db.install_initial_main_cloud_backend_instance(
                candidate_authority,
                activated_by=activated_by,
            )
            installed_coordinates = _active_coordinates(installed)
            if installed_coordinates is None:
                return None
            authority = installed["authority"]
            if (
                authority.backend_id != candidate_authority.backend_id
                or authority.routing != candidate_authority.routing
                or authority.installation_proof_sha256
                != candidate_authority.installation_proof_sha256
                or authority.secret_refs != candidate_authority.secret_refs
            ):
                return None
            current_after = await db.get_active_main_cloud_backend_instance()
            if _active_coordinates(current_after) != installed_coordinates:
                return None
            await router.replace_active(backend, authority=authority)
            return installed
        finally:
            if backend is not router.active:
                try:
                    await backend.close()
                except Exception:
                    pass
    if current_coordinates[1] != expected_activation_revision:
        await _close_candidate(candidate)
        return None
    current_authority = current["authority"]
    backend, candidate_authority = (
        candidate or await build_attested_main_cloud_candidate()
    )
    installed: dict[str, Any] | None = None
    authority: MainCloudBackendInstanceAuthority | None = None
    try:
        same_installation = (
            candidate_authority.backend_id == current_authority.backend_id
            and candidate_authority.routing == current_authority.routing
            and candidate_authority.installation_proof_sha256
            == current_authority.installation_proof_sha256
        )
        if same_installation:
            if candidate_authority.secret_refs == current_authority.secret_refs:
                authority = current_authority
                installed = current
            else:
                authority = MainCloudBackendInstanceAuthority.capture(
                    backend_instance_id=current_authority.backend_instance_id,
                    backend_id=current_authority.backend_id,
                    routing=current_authority.routing,
                    installation_proof_sha256=(
                        current_authority.installation_proof_sha256
                    ),
                    secret_refs=candidate_authority.secret_refs,
                    secret_revision=current_authority.secret_revision + 1,
                )
                authority = await db.rotate_main_cloud_backend_secret_refs(
                    authority,
                    expected_secret_revision=current_authority.secret_revision,
                )
                if authority is None:
                    return None
                # The active pointer does not rotate for a reference-only
                # change, but its adapter revision does.
                installed = {
                    "authority": authority,
                    "activation_revision": expected_activation_revision,
                }
        else:
            authority = await db.register_main_cloud_backend_instance(
                candidate_authority
            )
            if authority is None:
                return None
            installed = await db.activate_main_cloud_backend_instance(
                authority.backend_instance_id,
                expected_activation_revision=expected_activation_revision,
                activated_by=activated_by,
            )
            if _active_coordinates(installed) is None:
                return None

        assert authority is not None
        current_after = await db.get_active_main_cloud_backend_instance()
        expected_after = _active_coordinates(installed)
        if (
            expected_after is None
            or _active_coordinates(current_after) != expected_after
        ):
            return None
        await router.replace_active(backend, authority=authority)
        return installed
    finally:
        if backend is not router.active:
            try:
                await backend.close()
            except Exception:
                pass


async def _close_candidate(candidate: Candidate | None) -> None:
    if candidate is None:
        return
    try:
        await candidate[0].close()
    except Exception:
        pass


async def _reconcile_with_helm(
    db: Any,
    router: MainCloudRouter,
    active: dict[str, Any],
    *,
    replace_installation: str | None,
) -> dict[str, Any] | None:
    """Activate Helm's description over ``active`` where the rules allow it.

    Returns the new active record, or ``None`` when ``active`` keeps serving
    (Helm matches it, describes nothing adoptable, fails to attest, describes
    another installation without the confirmation, or a racing replica moved
    the pointer first). Every outcome is logged without a secret value.
    """

    authority = active["authority"]
    status = helm_configuration_status(authority)
    if status.state == "matches":
        return None
    if status.state == "invalid":
        logger.error(
            "Main cloud: the deployment's configuration is not adoptable (%s); "
            "installation %s keeps serving",
            status.detail,
            authority.backend_instance_id,
        )
        return None
    try:
        candidate = await build_attested_main_cloud_candidate()
    except Exception as exc:
        logger.error(
            "Main cloud: the configuration Helm describes did not attest (%s); "
            "installation %s keeps serving",
            type(exc).__name__,
            authority.backend_instance_id,
        )
        return None
    proposed = candidate[1]
    same_installation = (
        proposed.backend_id == authority.backend_id
        and proposed.installation_proof_sha256 == authority.installation_proof_sha256
    )
    if not same_installation and replace_installation != (
        authority.backend_instance_id
    ):
        await _close_candidate(candidate)
        logger.warning(
            "Main cloud: Helm describes a different installation (%s) than the "
            "active one (%s, instance %s); it keeps serving. To move new work "
            "to the described one, set cloud.replaceInstallation to %s.",
            proposed.backend_id,
            authority.backend_id,
            authority.backend_instance_id,
            authority.backend_instance_id,
        )
        return None
    activated = await activate_main_cloud_config(
        db,
        router,
        expected_activation_revision=active["activation_revision"],
        activated_by=HELM_ACTOR,
        candidate=candidate,
    )
    if activated is not None:
        logger.warning(
            "Main cloud: adopted the configuration Helm describes (%s, %s); "
            "instance %s replaces %s for new work",
            status.detail,
            "same installation" if same_installation else "new installation",
            activated["authority"].backend_instance_id,
            authority.backend_instance_id,
        )
    return activated


__all__ = [
    "HELM_ACTOR",
    "HelmConfigurationStatus",
    "activate_main_cloud_config",
    "build_attested_main_cloud_candidate",
    "helm_configuration_status",
    "initialize_main_cloud_instance_authority",
    "preload_retained_main_cloud_instances",
    "reload_active_main_cloud_instance",
]
