"""Tests for ``MainCloudRouter.replace_active`` and instance routing.

These tests exercise ``replace_active`` closing the old backend when the id
is unchanged, versus demoting it into ``_legacy`` when the id changes. The
overlay hot reload (``reload_from_db``) went with the connection form: the
main cloud is configured by Helm only (main_cloud_as_connectors.md, slice 2).
"""

from __future__ import annotations

from typing import Any

import pytest

from orchestrator.services.cloud import (
    FeatureNotAvailable,
    MainCloudRouter,
)
from orchestrator.services.cloud.backend_instance_authority import (
    MainCloudBackendInstanceAuthority,
    main_cloud_installation_proof_sha256,
)
from tests.cloud.fake import FakeMainCloudBackend


_INSTANCE_A = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
_INSTANCE_B = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
_PROOF = main_cloud_installation_proof_sha256(
    backend_id="nextcloud",
    remote_identity="installation-1",
)


def _authority(
    *,
    instance_id: str = _INSTANCE_A,
    secret_revision: int = 1,
) -> MainCloudBackendInstanceAuthority:
    return MainCloudBackendInstanceAuthority.capture(
        backend_instance_id=instance_id,
        backend_id="nextcloud",
        routing={
            "version": 1,
            "backend_id": "nextcloud",
            "base_url": "https://cloud.internal.example",
            "public_url": "https://cloud.example",
            "admin_user": "admin",
            "agent_user": "agent-service",
            "protected_effect_url": None,
            "protected_effect_config_sha256": None,
        },
        installation_proof_sha256=_PROOF,
        secret_refs={
            "admin_password": "env:NEXTCLOUD_ADMIN_PASSWORD",
            "agent_password": "env:NEXTCLOUD_AGENT_PASSWORD",
        },
        secret_revision=secret_revision,
    )


def _attested_fake(*, initialized: bool = True) -> FakeMainCloudBackend:
    backend = FakeMainCloudBackend(start_initialized=initialized)
    backend.backend_id = "nextcloud"
    backend._installation_proof_sha256 = _PROOF
    return backend


class _AltFakeBackend(FakeMainCloudBackend):
    """A second fake with a different ``backend_id`` for swap tests."""

    backend_id = "fake-alt"


class TestReplaceActive:
    @pytest.mark.asyncio
    async def test_same_id_closes_old_backend(self):
        old = FakeMainCloudBackend(start_initialized=True)
        new = FakeMainCloudBackend(start_initialized=True)
        router = MainCloudRouter(old)

        await router.replace_active(new)

        assert router.active is new
        assert old.is_initialized is False  # close() called
        assert router._legacy == {}

    @pytest.mark.asyncio
    async def test_different_id_demotes_old_to_legacy(self):
        old = FakeMainCloudBackend(start_initialized=True)
        new = _AltFakeBackend(start_initialized=True)
        router = MainCloudRouter(old)

        await router.replace_active(new)

        assert router.active is new
        # Old backend is kept alive for projects that were created on it.
        assert router._legacy == {"fake": old}
        assert old.is_initialized is True  # NOT closed


class TestBackendInstanceRouting:
    def test_bound_active_requires_exact_instance_provider_and_secret_revision(self):
        backend = _attested_fake()
        router = MainCloudRouter(backend)
        authority = _authority()

        router.bind_active_instance(authority)

        assert router.active_instance_id == _INSTANCE_A
        assert (
            router.for_backend_instance(
                _INSTANCE_A,
                expected_backend_id="nextcloud",
                expected_secret_revision=1,
            )
            is backend
        )
        with pytest.raises(FeatureNotAvailable):
            router.for_backend_instance(
                _INSTANCE_A,
                expected_backend_id="nextcloud",
                expected_secret_revision=2,
            )
        with pytest.raises(FeatureNotAvailable):
            router.for_backend_instance(
                _INSTANCE_A,
                expected_backend_id="opencloud",
                expected_secret_revision=1,
            )
        with pytest.raises(FeatureNotAvailable):
            router.for_backend_instance(
                _INSTANCE_B,
                expected_backend_id="nextcloud",
                expected_secret_revision=1,
            )

    @pytest.mark.asyncio
    async def test_secret_revision_rebuilds_exact_cached_instance(
        self, monkeypatch: pytest.MonkeyPatch
    ):
        initial = _attested_fake()
        router = MainCloudRouter(initial)
        router.bind_active_instance(_authority())
        replacement = _attested_fake(initialized=False)
        import orchestrator.services.cloud as cloud_pkg

        monkeypatch.setattr(
            cloud_pkg,
            "build_backend_from_instance",
            lambda authority: replacement,
        )

        resolved = await router.resolve_backend_instance(_authority(secret_revision=2))

        assert resolved is replacement
        assert replacement.is_initialized is True
        assert replacement.backend_instance_id == _INSTANCE_A
        assert (
            router.for_backend_instance(
                _INSTANCE_A,
                expected_backend_id="nextcloud",
                expected_secret_revision=2,
            )
            is replacement
        )
        # The still-active old adapter is retired only when replace_active
        # installs the fully attested replacement.
        assert initial.is_initialized is True
        await router.replace_active(replacement)
        assert initial.is_initialized is False

    @pytest.mark.asyncio
    async def test_resolve_refuses_wrong_installation_proof_and_closes_candidate(
        self, monkeypatch: pytest.MonkeyPatch
    ):
        router = MainCloudRouter(_attested_fake())
        candidate = _attested_fake(initialized=False)
        candidate._installation_proof_sha256 = "0" * 64
        import orchestrator.services.cloud as cloud_pkg

        monkeypatch.setattr(
            cloud_pkg,
            "build_backend_from_instance",
            lambda authority: candidate,
        )

        with pytest.raises(FeatureNotAvailable):
            await router.resolve_backend_instance(_authority())
        assert candidate.is_initialized is False

    @pytest.mark.asyncio
    async def test_same_provider_new_instance_retains_old_adapter(self):
        old = _attested_fake()
        new = _attested_fake()
        router = MainCloudRouter(old)
        router.bind_active_instance(_authority())
        new.bind_backend_instance(_INSTANCE_B)
        router._instance_secret_revisions[_INSTANCE_B] = 1

        await router.replace_active(new)

        assert old.is_initialized is True
        assert (
            router.for_backend_instance(
                _INSTANCE_A,
                expected_backend_id="nextcloud",
                expected_secret_revision=1,
            )
            is old
        )


class TestLegacyProviderOnlyRows:
    """Provider kind alone is never historical routing authority."""

    @pytest.mark.asyncio
    async def test_for_project_dispatches_to_demoted(self):
        old = FakeMainCloudBackend(start_initialized=True)
        new = _AltFakeBackend(start_initialized=False)

        router = MainCloudRouter(old)

        await router.replace_active(new)
        assert router.active is new

        legacy_row = {"main_cloud_backend": "fake"}
        new_row = {"main_cloud_backend": "fake-alt"}
        unknown_row: dict[str, Any] = {}

        with pytest.raises(FeatureNotAvailable):
            router.for_project(legacy_row)
        with pytest.raises(FeatureNotAvailable):
            router.for_project(new_row)
        assert router.for_project(unknown_row) is new  # None → active

    @pytest.mark.asyncio
    async def test_for_thread_dispatches_to_demoted(self):
        """Issue 16 resume hazard: a thread created on the old backend must
        still resolve to it after the active backend is swapped, so resume
        re-provisioning (``main.py`` ``_late_cloud_setup``) does not land on
        the wrong cloud."""
        old = FakeMainCloudBackend(start_initialized=True)
        new = _AltFakeBackend(start_initialized=False)
        router = MainCloudRouter(old)

        await router.replace_active(new)

        pinned_to_old = {"main_cloud_backend": "fake"}
        pinned_to_new = {"main_cloud_backend": "fake-alt"}
        unpinned: dict[str, Any] = {}

        with pytest.raises(FeatureNotAvailable):
            router.for_thread(pinned_to_old)
        with pytest.raises(FeatureNotAvailable):
            router.for_thread(pinned_to_new)
        assert router.for_thread(unpinned) is new  # None → active


class TestForOwner:
    """``for_owner`` is the resolution seam for *fresh* creates that aren't
    tied to a project/thread row yet (Issue 16). It returns the active
    backend today; the ``owner`` argument is reserved for per-org resolution
    under multi-tenancy."""

    def test_returns_active_ignoring_owner(self):
        active = _attested_fake()
        router = MainCloudRouter(active)
        router.bind_active_instance(_authority())
        assert router.for_owner({"id": "u1", "email": "a@b.c"}) is active
        assert router.for_owner(None) is active
        assert router.for_owner() is active

    def test_refuses_fresh_effect_without_durable_active_instance(self):
        router = MainCloudRouter(_attested_fake())
        with pytest.raises(FeatureNotAvailable):
            router.for_owner({"id": "u1"})

    @pytest.mark.asyncio
    async def test_fresh_create_follows_active_after_swap(self):
        """A fresh create always lands on the *new* active backend after a
        swap — never on a demoted legacy backend."""
        old = _attested_fake()
        new = _attested_fake()
        router = MainCloudRouter(old)
        router.bind_active_instance(_authority())

        await router.replace_active(new, authority=_authority(instance_id=_INSTANCE_B))
        assert router.for_owner({"id": "u1"}) is new
