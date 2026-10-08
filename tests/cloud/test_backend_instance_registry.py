from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from orchestrator.services.cloud import MainCloudRouter
from orchestrator.services.cloud.backend_instance_authority import (
    MainCloudBackendInstanceAuthority,
    main_cloud_installation_proof_sha256,
)
from orchestrator.services.cloud.instance_registry import (
    activate_main_cloud_config,
    build_attested_main_cloud_candidate,
    initialize_main_cloud_instance_authority,
    reload_active_main_cloud_instance,
)
from tests.cloud.fake import FakeMainCloudBackend


_A = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
_B = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
_PROOF_A = main_cloud_installation_proof_sha256(
    backend_id="nextcloud",
    remote_identity="installation-a",
)
_PROOF_B = main_cloud_installation_proof_sha256(
    backend_id="nextcloud",
    remote_identity="installation-b",
)


def _authority(
    instance_id: str = _A,
    *,
    proof: str = _PROOF_A,
    base_url: str = "https://a.internal.example",
    public_url: str | None = None,
    agent_user: str = "agent-service",
    refs: dict[str, str] | None = None,
    secret_revision: int = 1,
) -> MainCloudBackendInstanceAuthority:
    return MainCloudBackendInstanceAuthority.capture(
        backend_instance_id=instance_id,
        backend_id="nextcloud",
        routing={
            "version": 1,
            "backend_id": "nextcloud",
            "base_url": base_url,
            "public_url": public_url or base_url.replace("internal.", ""),
            "admin_user": "admin",
            "agent_user": agent_user,
            "protected_effect_url": None,
            "protected_effect_config_sha256": None,
        },
        installation_proof_sha256=proof,
        secret_refs=refs
        or {
            "admin_password": "env:NEXTCLOUD_ADMIN_PASSWORD",
            "agent_password": "env:NEXTCLOUD_AGENT_PASSWORD",
        },
        secret_revision=secret_revision,
    )


def _backend(authority: MainCloudBackendInstanceAuthority) -> FakeMainCloudBackend:
    backend = FakeMainCloudBackend(start_initialized=True)
    backend.backend_id = authority.backend_id
    backend._installation_proof_sha256 = authority.installation_proof_sha256
    return backend


def _active(
    authority: MainCloudBackendInstanceAuthority,
    revision: int,
) -> dict[str, object]:
    return {"authority": authority, "activation_revision": revision}


@pytest.mark.asyncio
async def test_candidate_prepares_provisional_instance_before_remote_attestation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[tuple[str, str | None]] = []

    class _TrackingBackend(FakeMainCloudBackend):
        backend_id = "nextcloud"

        def prepare_backend_instance_attestation(
            self,
            backend_instance_id: str,
        ) -> None:
            events.append(("prepare", backend_instance_id))
            super().prepare_backend_instance_attestation(backend_instance_id)

        async def ensure_initialized(self) -> bool:
            events.append(("initialize", self.backend_instance_id))
            self._installation_proof_sha256 = _PROOF_A
            return await super().ensure_initialized()

    backend = _TrackingBackend(start_initialized=False)
    authority = _authority()
    import orchestrator.services.cloud.instance_registry as registry

    settings = type("Settings", (), {"backend_id": "nextcloud"})()
    monkeypatch.setattr(registry, "load_main_cloud_config", lambda **_kwargs: settings)
    monkeypatch.setattr(
        registry,
        "main_cloud_secret_references",
        lambda *_args: authority.secret_refs,
    )
    monkeypatch.setattr(
        registry,
        "main_cloud_routing_snapshot",
        lambda _settings: authority.routing,
    )
    monkeypatch.setattr(
        registry,
        "build_backend_from_config",
        lambda _settings: backend,
    )

    candidate_backend, candidate = await build_attested_main_cloud_candidate(
        backend_instance_id=_A,
    )

    assert candidate_backend is backend
    assert candidate.backend_instance_id == _A
    assert backend.backend_instance_id is None
    assert events == [("prepare", _A), ("initialize", None)]


@pytest.mark.asyncio
async def test_reload_rereads_pointer_before_process_local_swap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority_a = _authority()
    authority_b = _authority(
        _B,
        proof=_PROOF_B,
        base_url="https://b.internal.example",
    )
    candidate = _backend(authority_a)
    router = MainCloudRouter(_backend(authority_b))
    db = type("DB", (), {})()
    db.get_active_main_cloud_backend_instance = AsyncMock(
        side_effect=[_active(authority_a, 1), _active(authority_b, 2)]
    )
    import orchestrator.services.cloud as cloud_pkg

    monkeypatch.setattr(
        cloud_pkg,
        "build_backend_from_instance",
        lambda authority: candidate,
    )

    assert await reload_active_main_cloud_instance(db, router) is False
    assert router.active is not candidate


@pytest.mark.asyncio
async def test_reload_installs_exact_attested_instance(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority = _authority()
    candidate = _backend(authority)
    previous = _backend(_authority(_B))
    router = MainCloudRouter(previous)
    db = type("DB", (), {})()
    db.get_active_main_cloud_backend_instance = AsyncMock(
        return_value=_active(authority, 3)
    )
    import orchestrator.services.cloud as cloud_pkg

    monkeypatch.setattr(
        cloud_pkg,
        "build_backend_from_instance",
        lambda value: candidate,
    )

    assert await reload_active_main_cloud_instance(db, router) is True
    assert router.active is candidate
    assert candidate.backend_instance_id == _A
    assert previous.is_initialized is False


@pytest.mark.asyncio
async def test_first_boot_adopts_candidate_before_installing_router(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority = _authority()
    candidate = _backend(authority)
    previous = _backend(_authority(_B))
    router = MainCloudRouter(previous)
    db = type("DB", (), {})()
    db.get_active_main_cloud_backend_instance = AsyncMock(return_value=None)
    db.install_initial_main_cloud_backend_instance = AsyncMock(
        return_value=_active(authority, 1)
    )
    import orchestrator.services.cloud.instance_registry as registry

    monkeypatch.setattr(
        registry,
        "build_attested_main_cloud_candidate",
        AsyncMock(return_value=(candidate, authority)),
    )

    result = await initialize_main_cloud_instance_authority(db, router)

    assert result == _active(authority, 1)
    assert router.active is candidate
    assert candidate.backend_instance_id == _A


@pytest.mark.asyncio
async def test_activation_creates_new_instance_under_pointer_cas(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority_a = _authority()
    authority_b = _authority(
        _B,
        proof=_PROOF_B,
        base_url="https://b.internal.example",
    )
    candidate = _backend(authority_b)
    previous = _backend(authority_a)
    previous.bind_backend_instance(_A)
    router = MainCloudRouter(previous)
    router._instances[_A] = previous
    router._instance_secret_revisions[_A] = 1
    db = type("DB", (), {})()
    db.get_active_main_cloud_backend_instance = AsyncMock(
        side_effect=[_active(authority_a, 4), _active(authority_b, 5)]
    )
    db.register_main_cloud_backend_instance = AsyncMock(return_value=authority_b)
    db.activate_main_cloud_backend_instance = AsyncMock(
        return_value=_active(authority_b, 5)
    )
    db.rotate_main_cloud_backend_secret_refs = AsyncMock()
    import orchestrator.services.cloud.instance_registry as registry

    monkeypatch.setattr(
        registry,
        "build_attested_main_cloud_candidate",
        AsyncMock(return_value=(candidate, authority_b)),
    )

    result = await activate_main_cloud_config(
        db,
        router,
        expected_activation_revision=4,
        activated_by="admin",
    )

    assert result == _active(authority_b, 5)
    db.activate_main_cloud_backend_instance.assert_awaited_once_with(
        _B,
        expected_activation_revision=4,
        activated_by="admin",
    )
    assert router.active is candidate
    assert previous.is_initialized is True


@pytest.mark.asyncio
async def test_same_installation_rotates_only_secret_reference_revision(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority_a = _authority()
    proposed = _authority(
        _B,
        refs={
            "admin_password": "env:ROTATED_ADMIN_PASSWORD",
            "agent_password": "env:ROTATED_AGENT_PASSWORD",
        },
    )
    rotated = _authority(
        _A,
        refs=proposed.secret_refs,
        secret_revision=2,
    )
    candidate = _backend(proposed)
    previous = _backend(authority_a)
    previous.bind_backend_instance(_A)
    router = MainCloudRouter(previous)
    router._instances[_A] = previous
    router._instance_secret_revisions[_A] = 1
    db = type("DB", (), {})()
    db.get_active_main_cloud_backend_instance = AsyncMock(
        side_effect=[_active(authority_a, 7), _active(rotated, 7)]
    )
    db.rotate_main_cloud_backend_secret_refs = AsyncMock(return_value=rotated)
    db.register_main_cloud_backend_instance = AsyncMock()
    db.activate_main_cloud_backend_instance = AsyncMock()
    import orchestrator.services.cloud.instance_registry as registry

    monkeypatch.setattr(
        registry,
        "build_attested_main_cloud_candidate",
        AsyncMock(return_value=(candidate, proposed)),
    )

    result = await activate_main_cloud_config(
        db,
        router,
        expected_activation_revision=7,
        activated_by="admin",
    )

    assert result == _active(rotated, 7)
    db.rotate_main_cloud_backend_secret_refs.assert_awaited_once()
    db.register_main_cloud_backend_instance.assert_not_awaited()
    db.activate_main_cloud_backend_instance.assert_not_awaited()
    assert router.active is candidate
    assert candidate.backend_instance_id == _A


@pytest.mark.asyncio
async def test_stale_activation_revision_performs_no_probe_or_write(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority = _authority()
    db = type("DB", (), {})()
    db.get_active_main_cloud_backend_instance = AsyncMock(
        return_value=_active(authority, 9)
    )
    builder = AsyncMock()
    import orchestrator.services.cloud.instance_registry as registry

    monkeypatch.setattr(registry, "build_attested_main_cloud_candidate", builder)

    result = await activate_main_cloud_config(
        db,
        MainCloudRouter(_backend(authority)),
        expected_activation_revision=8,
        activated_by="admin",
    )

    assert result is None
    builder.assert_not_awaited()


# =============================================================================
# Helm only: startup reconciles the active instance with Helm's description
# (main_cloud_as_connectors.md, "Configuration: Helm only")
# =============================================================================


def _describe(
    monkeypatch: pytest.MonkeyPatch,
    authority: MainCloudBackendInstanceAuthority,
) -> None:
    """Make Helm describe exactly ``authority``'s routing and references."""
    import orchestrator.services.cloud.instance_registry as registry

    settings = type("Settings", (), {"backend_id": authority.backend_id})()
    monkeypatch.setattr(registry, "load_main_cloud_config", lambda **_kw: settings)
    monkeypatch.setattr(
        registry, "main_cloud_routing_snapshot", lambda _s: authority.routing
    )
    monkeypatch.setattr(
        registry, "main_cloud_secret_references", lambda *_a: authority.secret_refs
    )


class _Startup:
    """A DB whose active pointer is ``active`` until something activates.

    Startup reads the pointer once, the activation's CAS once more, then the
    post-activation check. ``raced`` moves the pointer before the CAS read,
    as a replica that activated first does.
    """

    def __init__(self, active, after=None, *, raced=False):
        reads = [active] if raced else [active, active]
        self.get_active_main_cloud_backend_instance = AsyncMock(
            side_effect=[*reads, *([after] * 4)] if after else None,
            return_value=None if after else active,
        )
        self.register_main_cloud_backend_instance = AsyncMock()
        self.activate_main_cloud_backend_instance = AsyncMock()
        self.rotate_main_cloud_backend_secret_refs = AsyncMock()


def test_helm_status_compares_without_the_network(monkeypatch):
    from orchestrator.services.cloud.instance_registry import (
        helm_configuration_status,
    )

    active = _authority()
    _describe(monkeypatch, active)
    assert helm_configuration_status(active).state == "matches"
    moved = _authority(base_url="https://moved.internal.example")
    _describe(monkeypatch, moved)
    status = helm_configuration_status(active)
    assert (status.state, status.detail) == ("differs", "routing")
    rotated = _authority(refs={"admin_password": "env:A", "agent_password": "env:B"})
    _describe(monkeypatch, rotated)
    assert helm_configuration_status(active).detail == "secret_references"


def test_helm_status_reports_a_description_it_cannot_adopt(monkeypatch):
    import orchestrator.services.cloud.instance_registry as registry

    def _unset(*_a):
        raise ValueError("required secret env is unset for nextcloud.admin_password")

    _describe(monkeypatch, _authority())
    monkeypatch.setattr(registry, "main_cloud_secret_references", _unset)
    status = registry.helm_configuration_status(_authority())
    assert (status.state, status.detail) == ("invalid", "ValueError")


@pytest.mark.asyncio
async def test_a_matching_description_just_loads_the_active_instance(monkeypatch):
    import orchestrator.services.cloud.instance_registry as registry

    authority = _authority()
    _describe(monkeypatch, authority)
    db = _Startup(_active(authority, 3))
    builder = AsyncMock()
    reload = AsyncMock(return_value=True)
    monkeypatch.setattr(registry, "build_attested_main_cloud_candidate", builder)
    monkeypatch.setattr(registry, "reload_active_main_cloud_instance", reload)
    notify = AsyncMock()

    result = await initialize_main_cloud_instance_authority(
        db, MainCloudRouter(_backend(authority)), notify=notify
    )

    assert result == _active(authority, 3)
    builder.assert_not_awaited()
    reload.assert_awaited_once()
    notify.assert_not_awaited()


@pytest.mark.asyncio
async def test_the_same_installation_described_anew_is_adopted(monkeypatch):
    """A Helm routing change on the same cloud: attest, activate, fan out."""
    import orchestrator.services.cloud.instance_registry as registry

    active = _authority()
    moved = _authority(
        _B, base_url="https://moved.internal.example", public_url="https://a.example"
    )
    _describe(monkeypatch, moved)
    candidate = _backend(moved)
    db = _Startup(_active(active, 4), after=_active(moved, 5))
    db.register_main_cloud_backend_instance.return_value = moved
    db.activate_main_cloud_backend_instance.return_value = _active(moved, 5)
    monkeypatch.setattr(
        registry,
        "build_attested_main_cloud_candidate",
        AsyncMock(return_value=(candidate, moved)),
    )
    previous = _backend(active)
    previous.bind_backend_instance(_A)
    router = MainCloudRouter(previous)
    notify = AsyncMock()

    result = await initialize_main_cloud_instance_authority(db, router, notify=notify)

    assert result == _active(moved, 5)
    db.activate_main_cloud_backend_instance.assert_awaited_once_with(
        _B, expected_activation_revision=4, activated_by="helm"
    )
    assert router.active is candidate
    notify.assert_awaited_once_with(_B)


@pytest.mark.asyncio
async def test_another_installation_needs_the_operators_confirmation(monkeypatch):
    """An overlay-born instance keeps serving: Helm's other cloud is not
    adopted from values that merely drifted."""
    import orchestrator.services.cloud.instance_registry as registry

    active = _authority()
    other = _authority(_B, proof=_PROOF_B, base_url="https://b.internal.example")
    _describe(monkeypatch, other)
    candidate = _backend(other)
    db = _Startup(_active(active, 2))
    monkeypatch.setattr(
        registry,
        "build_attested_main_cloud_candidate",
        AsyncMock(return_value=(candidate, other)),
    )
    reload = AsyncMock(return_value=True)
    monkeypatch.setattr(registry, "reload_active_main_cloud_instance", reload)
    notify = AsyncMock()

    result = await initialize_main_cloud_instance_authority(
        db,
        MainCloudRouter(_backend(active)),
        replace_installation="some-other-instance",
        notify=notify,
    )

    assert result == _active(active, 2)
    db.register_main_cloud_backend_instance.assert_not_awaited()
    db.activate_main_cloud_backend_instance.assert_not_awaited()
    assert candidate.is_initialized is False  # closed, never installed
    reload.assert_awaited_once()
    notify.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_confirmed_replacement_moves_new_work_to_the_described_cloud(
    monkeypatch,
):
    import orchestrator.services.cloud.instance_registry as registry

    active = _authority()
    other = _authority(_B, proof=_PROOF_B, base_url="https://b.internal.example")
    _describe(monkeypatch, other)
    candidate = _backend(other)
    db = _Startup(_active(active, 2), after=_active(other, 3))
    db.register_main_cloud_backend_instance.return_value = other
    db.activate_main_cloud_backend_instance.return_value = _active(other, 3)
    monkeypatch.setattr(
        registry,
        "build_attested_main_cloud_candidate",
        AsyncMock(return_value=(candidate, other)),
    )
    previous = _backend(active)
    previous.bind_backend_instance(_A)
    router = MainCloudRouter(previous)
    router._instances[_A] = previous
    notify = AsyncMock()

    result = await initialize_main_cloud_instance_authority(
        db, router, replace_installation=_A, notify=notify
    )

    assert result == _active(other, 3)
    assert router.active is candidate
    # The replaced installation stays cached for what is stamped with it.
    assert previous.is_initialized is True
    notify.assert_awaited_once_with(_B)


@pytest.mark.asyncio
async def test_a_description_that_does_not_attest_leaves_the_active_serving(
    monkeypatch,
):
    import orchestrator.services.cloud.instance_registry as registry

    active = _authority()
    _describe(monkeypatch, _authority(base_url="https://unreachable.example"))
    db = _Startup(_active(active, 6))
    monkeypatch.setattr(
        registry,
        "build_attested_main_cloud_candidate",
        AsyncMock(side_effect=RuntimeError("did not attest")),
    )
    reload = AsyncMock(return_value=True)
    monkeypatch.setattr(registry, "reload_active_main_cloud_instance", reload)

    result = await initialize_main_cloud_instance_authority(
        db, MainCloudRouter(_backend(active))
    )

    assert result == _active(active, 6)
    reload.assert_awaited_once()
    db.activate_main_cloud_backend_instance.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_racing_replica_that_activated_first_wins(monkeypatch):
    """The CAS refuses a stale revision: the loser loads what won."""
    import orchestrator.services.cloud.instance_registry as registry

    active = _authority()
    moved = _authority(_B, base_url="https://moved.internal.example")
    _describe(monkeypatch, moved)
    candidate = _backend(moved)
    db = _Startup(_active(active, 4), after=_active(moved, 5), raced=True)
    monkeypatch.setattr(
        registry,
        "build_attested_main_cloud_candidate",
        AsyncMock(return_value=(candidate, moved)),
    )
    reload = AsyncMock(return_value=True)
    monkeypatch.setattr(registry, "reload_active_main_cloud_instance", reload)
    notify = AsyncMock()

    await initialize_main_cloud_instance_authority(
        db, MainCloudRouter(_backend(active)), notify=notify
    )

    db.register_main_cloud_backend_instance.assert_not_awaited()
    reload.assert_awaited_once()
    notify.assert_not_awaited()
    assert candidate.is_initialized is False


def _registry():
    import orchestrator.services.cloud.instance_registry as registry

    return registry


@pytest.mark.parametrize(
    "change",
    [
        {"public_url": "https://elsewhere.example"},
        {"agent_user": "someone-else"},
        {
            "refs": {
                "admin_password": "env:NEXTCLOUD_ADMIN_PASSWORD",
                "agent_password": "env:SOME_OTHER_PASSWORD",
            }
        },
    ],
)
def test_a_change_the_attestation_does_not_prove_is_unattested(change):
    registry = _registry()
    active = _authority()
    proposed = _authority(_B, **change)
    assert registry.unattested_changes(active, proposed)
    # The internal URL, the admin account and its secret are proven.
    proven = _authority(
        _B,
        base_url="https://moved.internal.example",
        public_url="https://a.example",
        refs={
            "admin_password": "env:ROTATED_ADMIN_PASSWORD",
            "agent_password": "env:NEXTCLOUD_AGENT_PASSWORD",
        },
    )
    assert registry.changed_fields(active, proven) == {"base_url", "admin_password"}
    assert registry.unattested_changes(active, proven) == frozenset()


@pytest.mark.parametrize("confirmation", [None, "some-other-instance"])
@pytest.mark.asyncio
async def test_an_unattested_change_needs_the_confirmation(monkeypatch, confirmation):
    """A new public URL on the same installation: the proof still matches,
    but nothing proved the URL, so the active instance keeps serving."""
    registry = _registry()
    active = _authority()
    moved = _authority(_B, public_url="https://elsewhere.example")
    _describe(monkeypatch, moved)
    candidate = _backend(moved)
    db = _Startup(_active(active, 2))
    monkeypatch.setattr(
        registry,
        "build_attested_main_cloud_candidate",
        AsyncMock(return_value=(candidate, moved)),
    )
    reload = AsyncMock(return_value=True)
    monkeypatch.setattr(registry, "reload_active_main_cloud_instance", reload)

    await initialize_main_cloud_instance_authority(
        db, MainCloudRouter(_backend(active)), replace_installation=confirmation
    )

    db.register_main_cloud_backend_instance.assert_not_awaited()
    assert candidate.is_initialized is False
    reload.assert_awaited_once()


@pytest.mark.asyncio
async def test_a_confirmed_unattested_change_is_adopted(monkeypatch):
    registry = _registry()
    active = _authority()
    moved = _authority(_B, public_url="https://elsewhere.example")
    _describe(monkeypatch, moved)
    candidate = _backend(moved)
    db = _Startup(_active(active, 2), after=_active(moved, 3))
    db.register_main_cloud_backend_instance.return_value = moved
    db.activate_main_cloud_backend_instance.return_value = _active(moved, 3)
    monkeypatch.setattr(
        registry,
        "build_attested_main_cloud_candidate",
        AsyncMock(return_value=(candidate, moved)),
    )
    notify = AsyncMock()

    result = await initialize_main_cloud_instance_authority(
        db,
        MainCloudRouter(_backend(active)),
        # The operator's confirmation is compared case-insensitively.
        replace_installation=f" {_A.upper()} ",
        notify=notify,
    )

    assert result == _active(moved, 3)
    notify.assert_awaited_once_with(_B)


@pytest.mark.asyncio
async def test_a_cloud_reinstalled_at_the_same_address_is_recovered(monkeypatch):
    """Helm matches the routing, but the installation behind it is new (a new
    proof): with the confirmation it is attested and adopted, instead of the
    stale instance failing every boot."""
    registry = _registry()
    active = _authority()
    _describe(monkeypatch, active)  # Helm's routing and references match
    reinstalled = _authority(_B, proof=_PROOF_B)
    candidate = _backend(reinstalled)
    db = _Startup(_active(active, 3), after=_active(reinstalled, 4))
    db.register_main_cloud_backend_instance.return_value = reinstalled
    db.activate_main_cloud_backend_instance.return_value = _active(reinstalled, 4)
    monkeypatch.setattr(
        registry,
        "build_attested_main_cloud_candidate",
        AsyncMock(return_value=(candidate, reinstalled)),
    )
    monkeypatch.setattr(
        registry,
        "reload_active_main_cloud_instance",
        AsyncMock(side_effect=RuntimeError("proof mismatch")),
    )
    notify = AsyncMock()

    result = await initialize_main_cloud_instance_authority(
        db, MainCloudRouter(_backend(active)), replace_installation=_A, notify=notify
    )

    assert result == _active(reinstalled, 4)
    db.activate_main_cloud_backend_instance.assert_awaited_once_with(
        _B, expected_activation_revision=3, activated_by="helm"
    )
    notify.assert_awaited_once_with(_B)


@pytest.mark.asyncio
async def test_without_the_confirmation_a_matching_description_is_not_attested(
    monkeypatch,
):
    registry = _registry()
    active = _authority()
    _describe(monkeypatch, active)
    builder = AsyncMock()
    monkeypatch.setattr(registry, "build_attested_main_cloud_candidate", builder)
    monkeypatch.setattr(
        registry,
        "reload_active_main_cloud_instance",
        AsyncMock(side_effect=RuntimeError("proof mismatch")),
    )

    with pytest.raises(RuntimeError):
        await initialize_main_cloud_instance_authority(
            _Startup(_active(active, 3)), MainCloudRouter(_backend(active))
        )
    builder.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_confirmed_match_on_the_same_installation_changes_nothing(
    monkeypatch,
):
    registry = _registry()
    active = _authority()
    _describe(monkeypatch, active)
    candidate = _backend(active)
    db = _Startup(_active(active, 3))
    monkeypatch.setattr(
        registry,
        "build_attested_main_cloud_candidate",
        AsyncMock(return_value=(candidate, _authority(_B))),
    )
    reload = AsyncMock(return_value=True)
    monkeypatch.setattr(registry, "reload_active_main_cloud_instance", reload)

    await initialize_main_cloud_instance_authority(
        db, MainCloudRouter(_backend(active)), replace_installation=_A
    )

    db.register_main_cloud_backend_instance.assert_not_awaited()
    assert candidate.is_initialized is False
    reload.assert_awaited_once()


@pytest.mark.asyncio
async def test_an_activation_that_does_not_take_effect_is_logged(monkeypatch, caplog):
    registry = _registry()
    active = _authority()
    moved = _authority(
        _B, base_url="https://moved.internal.example", public_url="https://a.example"
    )
    _describe(monkeypatch, moved)
    db = _Startup(_active(active, 4))
    db.register_main_cloud_backend_instance.return_value = None  # older revision
    monkeypatch.setattr(
        registry,
        "build_attested_main_cloud_candidate",
        AsyncMock(return_value=(_backend(moved), moved)),
    )
    monkeypatch.setattr(
        registry, "reload_active_main_cloud_instance", AsyncMock(return_value=True)
    )

    with caplog.at_level("WARNING", logger=registry.__name__):
        await initialize_main_cloud_instance_authority(
            db, MainCloudRouter(_backend(active))
        )

    assert "attested but not activated" in caplog.text


# =============================================================================
# A legacy system_settings.main_cloud row before any instance is active
# =============================================================================


def _helm_nextcloud(monkeypatch, **extra):
    env = {
        "MAIN_CLOUD_BACKEND": "nextcloud",
        "NEXTCLOUD_URL": "http://srw-nextcloud",
        "NEXTCLOUD_PUBLIC_URL": "https://cloud.localhost",
        "NEXTCLOUD_ADMIN_USER": "admin",
        "NEXTCLOUD_ADMIN_PASSWORD": "admin-secret",
        "NEXTCLOUD_AGENT_PASSWORD": "agent-secret",
        **extra,
    }
    for key, value in env.items():
        monkeypatch.setenv(key, value)


@pytest.mark.parametrize(
    ("value", "credentials_ref", "expected"),
    [
        ({}, None, []),
        (
            {
                "backend_id": "nextcloud",
                "base_url": "http://srw-nextcloud/",
                "public_url": "https://cloud.localhost",
                "admin_user": "admin",
                "__secret_fields__": ["admin_password", "agent_password"],
            },
            None,
            [],
        ),
        ({"backend_id": "opencloud"}, None, ["backend_id"]),
        (
            {"backend_id": "nextcloud", "base_url": "https://other.example"},
            None,
            ["base_url"],
        ),
        (
            {
                "backend_id": "nextcloud",
                "__secret_fields__": ["agent_password"],
            },
            "env:VAULT_AGENT_PASSWORD",
            ["agent_password"],
        ),
    ],
)
def test_a_legacy_row_is_compared_with_helm_by_field_name(
    monkeypatch, value, credentials_ref, expected
):
    import orchestrator.services.cloud.instance_registry as registry

    _helm_nextcloud(monkeypatch)
    overlay = {"value": value, "credentials_ref": credentials_ref}
    assert registry.legacy_overlay_differences(overlay) == expected


def test_the_legacy_summary_names_no_secret():
    import orchestrator.services.cloud.instance_registry as registry

    overlay = {
        "value": {
            "backend_id": "nextcloud",
            "base_url": "https://x",
            "admin_user": "a",
        },
        "credentials_ref": "env:SECRET_NAME",
    }
    assert registry.legacy_overlay_summary(overlay) == {
        "backend_id": "nextcloud",
        "base_url": "https://x",
    }


@pytest.mark.asyncio
async def test_a_first_boot_refuses_helm_when_a_legacy_row_describes_another_cloud(
    monkeypatch, caplog
):
    import orchestrator.services.cloud.instance_registry as registry

    _helm_nextcloud(monkeypatch)
    builder = AsyncMock()
    monkeypatch.setattr(registry, "build_attested_main_cloud_candidate", builder)
    db = type("DB", (), {})()
    db.get_active_main_cloud_backend_instance = AsyncMock(return_value=None)
    db.install_initial_main_cloud_backend_instance = AsyncMock()
    overlay = {
        "value": {"backend_id": "opencloud", "base_url": "https://legacy.example"},
        "credentials_ref": None,
    }

    with caplog.at_level("ERROR", logger=registry.__name__):
        with pytest.raises(RuntimeError, match="differs from Helm"):
            await initialize_main_cloud_instance_authority(
                db, MainCloudRouter(_backend(_authority())), legacy_overlay=overlay
            )

    builder.assert_not_awaited()
    db.install_initial_main_cloud_backend_instance.assert_not_awaited()
    assert "opencloud" in caplog.text and "https://legacy.example" in caplog.text


@pytest.mark.asyncio
async def test_a_first_boot_adopts_helm_when_the_legacy_row_agrees(monkeypatch):
    import orchestrator.services.cloud.instance_registry as registry

    _helm_nextcloud(monkeypatch)
    authority = _authority()
    candidate = _backend(authority)
    monkeypatch.setattr(
        registry,
        "build_attested_main_cloud_candidate",
        AsyncMock(return_value=(candidate, authority)),
    )
    db = type("DB", (), {})()
    db.get_active_main_cloud_backend_instance = AsyncMock(return_value=None)
    db.install_initial_main_cloud_backend_instance = AsyncMock(
        return_value=_active(authority, 1)
    )
    overlay = {"value": {"backend_id": "nextcloud"}, "credentials_ref": None}

    result = await initialize_main_cloud_instance_authority(
        db, MainCloudRouter(_backend(_authority(_B))), legacy_overlay=overlay
    )

    assert result == _active(authority, 1)


@pytest.mark.asyncio
async def test_a_legacy_row_is_not_consulted_once_an_instance_is_active(monkeypatch):
    import orchestrator.services.cloud.instance_registry as registry

    active = _authority()
    _describe(monkeypatch, active)
    reload = AsyncMock(return_value=True)
    monkeypatch.setattr(registry, "reload_active_main_cloud_instance", reload)
    differences = AsyncMock()
    monkeypatch.setattr(registry, "legacy_overlay_differences", differences)

    await initialize_main_cloud_instance_authority(
        _Startup(_active(active, 2)),
        MainCloudRouter(_backend(active)),
        legacy_overlay={"value": {"backend_id": "opencloud"}},
    )

    differences.assert_not_called()
    reload.assert_awaited_once()
