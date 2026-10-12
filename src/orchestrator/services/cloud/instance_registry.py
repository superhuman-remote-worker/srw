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
* the same installation (equal installation proof), described differently in
  fields its attestation proves (the adapter's ``attested_fields``: for
  Nextcloud the internal URL, the admin account and the protected-effect
  lane): Helm's description is activated, as the form's save did;
* any other change -- a field the attestation does not prove (the public URL,
  the agent account or its secret reference), or a different installation --
  is activated only when ``replace_installation`` (``cloud.replaceInstallation``
  → ``MAIN_CLOUD_REPLACE_INSTALLATION``) names the active instance id. Moving
  new work to another cloud fences everything stamped with the old
  installation to it, so it never follows from values that merely drifted,
  such as an instance an admin activated through the removed form while Helm
  still describes the bundled cloud. The confirmation also attests a
  description that matches, so a cloud reinstalled at the same address (a new
  proof) can be adopted;
* a description that fails to attest leaves the active instance serving.

A legacy ``system_settings.main_cloud`` row (the removed form's input before
migration 0186) is no configuration any more. It is removed once an instance
is active; before that, a first boot refuses to adopt Helm's description if
the row describes another cloud, and keeps the row.
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
    provider_adapter,
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


#: The legacy overlay's routing fields that hold a URL (compared without a
#: trailing slash, as the settings model may add one).
_URL_FIELDS = frozenset(
    {"base_url", "public_url", "keycloak_issuer", "protected_effect_url"}
)


def legacy_overlay_summary(overlay: Any) -> dict[str, Any]:
    """The non-secret identity of a legacy ``system_settings.main_cloud`` row,
    for a log line: its provider and internal URL."""
    value = overlay.get("value") if isinstance(overlay, dict) else None
    value = value if isinstance(value, dict) else {}
    return {"backend_id": value.get("backend_id"), "base_url": value.get("base_url")}


def legacy_overlay_differences(overlay: Any) -> list[str]:
    """The fields in which a legacy overlay row differs from Helm's description.

    The row was the removed form's one-time input before migration 0186. A
    deployment that never adopted an instance since may still hold it; if it
    describes another cloud than Helm, adopting Helm's would silently move
    the deployment. Names only, never a value. Raises when Helm describes
    nothing adoptable (the first boot would fail on it anyway).
    """
    value = overlay.get("value") if isinstance(overlay, dict) else None
    if not isinstance(value, dict) or not value:
        return []
    settings = load_main_cloud_config()
    routing = main_cloud_routing_snapshot(settings)
    differences: list[str] = []
    for key, stored in sorted(value.items()):
        if key == "__secret_fields__" or stored in (None, ""):
            continue
        described = routing.get(key)
        if key in _URL_FIELDS:
            same = str(stored).rstrip("/") == str(described or "").rstrip("/")
        else:
            same = stored == described
        if not same:
            differences.append(key)
    credentials_ref = overlay.get("credentials_ref")
    secret_fields = value.get("__secret_fields__") or []
    if isinstance(credentials_ref, str) and credentials_ref.startswith("env:"):
        refs = main_cloud_secret_references(settings.backend_id)
        differences += [
            field
            for field in secret_fields
            if isinstance(field, str) and refs.get(field) != credentials_ref
        ]
    return differences


async def initialize_main_cloud_instance_authority(
    db: Any,
    router: MainCloudRouter,
    *,
    replace_installation: str | None = None,
    notify: Callable[[str], Awaitable[None]] | None = None,
    legacy_overlay: dict[str, Any] | None = None,
    activated_by: str = "orchestrator-startup",
) -> dict[str, Any]:
    """Resolve existing authority, reconciled with Helm (module docstring).

    The first boot adopts Helm's description transactionally, unless a
    legacy ``system_settings.main_cloud`` row (``legacy_overlay``) describes
    another cloud: then nothing is adopted, the row's non-secret identity is
    logged and the caller keeps it, so cloud effects stay disabled until Helm
    describes the same cloud or an operator removes the row. Afterwards the
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

    if legacy_overlay is not None:
        differences = legacy_overlay_differences(legacy_overlay)
        if differences:
            summary = legacy_overlay_summary(legacy_overlay)
            logger.error(
                "Main cloud: a legacy main_cloud settings row (backend %s, base "
                "URL %s) differs from Helm's description in %s; no installation "
                "is adopted and the row is kept. Describe that cloud in Helm, or "
                "delete the row, then restart.",
                summary["backend_id"],
                summary["base_url"],
                differences,
            )
            raise RuntimeError("legacy main_cloud settings row differs from Helm")

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


def confirms_replacement(
    replace_installation: str | None,
    authority: MainCloudBackendInstanceAuthority,
) -> bool:
    """Whether the operator's ``cloud.replaceInstallation`` names this
    active instance (a UUID, compared case-insensitively)."""
    return bool(replace_installation) and (
        replace_installation.strip().lower() == authority.backend_instance_id.lower()
    )


def changed_fields(
    active: MainCloudBackendInstanceAuthority,
    proposed: MainCloudBackendInstanceAuthority,
) -> frozenset[str]:
    """The routing keys and secret-reference fields that differ (names only)."""
    routing_a, routing_b = active.routing, proposed.routing
    refs_a, refs_b = active.secret_refs, proposed.secret_refs
    return frozenset(
        {
            k
            for k in routing_a.keys() | routing_b.keys()
            if routing_a.get(k) != routing_b.get(k)
        }
        | {k for k in refs_a.keys() | refs_b.keys() if refs_a.get(k) != refs_b.get(k)}
    )


def unattested_changes(
    active: MainCloudBackendInstanceAuthority,
    proposed: MainCloudBackendInstanceAuthority,
) -> frozenset[str]:
    """Changes the provider's attestation does not prove.

    ``ensure_initialized`` proves only what it exercises (the adapter's
    ``attested_fields``: for Nextcloud the internal URL, the admin account
    and the protected-effect lane, never the agent account or the public
    URL). A change to anything else could point delivery at another server or
    credential while the installation proof still matches, so it is adopted
    only on the operator's confirmation.
    """
    adapter = provider_adapter(proposed.backend_id)
    attested = getattr(adapter, "attested_fields", frozenset())
    return changed_fields(active, proposed) - attested


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
    another installation or an unattested change without the confirmation, or
    a racing replica moved the pointer first). A confirmation naming the
    active instance attests Helm's description even when it matches, so a
    cloud reinstalled at the same address (a new installation proof) can be
    adopted. Every outcome is logged without a secret value.
    """

    authority = active["authority"]
    confirmed = confirms_replacement(replace_installation, authority)
    status = helm_configuration_status(authority)
    if status.state == "matches" and not confirmed:
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
    if same_installation:
        if not changed_fields(authority, proposed):
            await _close_candidate(candidate)  # nothing to change
            return None
        unattested = unattested_changes(authority, proposed)
        if unattested and not confirmed:
            await _close_candidate(candidate)
            logger.warning(
                "Main cloud: Helm changes %s of installation %s, which its "
                "attestation does not prove; it keeps serving as before. To "
                "adopt the change, set cloud.replaceInstallation to %s.",
                sorted(unattested),
                authority.backend_instance_id,
                authority.backend_instance_id,
            )
            return None
    elif not confirmed:
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
    if activated is None:
        logger.warning(
            "Main cloud: Helm's configuration was attested but not activated "
            "(the active pointer moved, or an instance with the same routing "
            "and proof is registered with other secret references); "
            "installation %s keeps serving",
            authority.backend_instance_id,
        )
        return None
    logger.warning(
        "Main cloud: adopted the configuration Helm describes (%s, %s); "
        "instance %s replaces %s for new work",
        status.detail or "confirmed",
        "same installation" if same_installation else "new installation",
        activated["authority"].backend_instance_id,
        authority.backend_instance_id,
    )
    return activated


__all__ = [
    "HELM_ACTOR",
    "HelmConfigurationStatus",
    "changed_fields",
    "confirms_replacement",
    "unattested_changes",
    "activate_main_cloud_config",
    "build_attested_main_cloud_candidate",
    "helm_configuration_status",
    "initialize_main_cloud_instance_authority",
    "legacy_overlay_differences",
    "legacy_overlay_summary",
    "preload_retained_main_cloud_instances",
    "reload_active_main_cloud_instance",
]
