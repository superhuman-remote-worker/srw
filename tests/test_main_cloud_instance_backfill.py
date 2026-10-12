"""The pre-0186 instance-authority backfill — mostly its refusals.

Stamping a row with an installation UUID is a one-way act: from then on every
effect (member grants, shares, folder deletes) is dispatched against that
installation and nothing downstream questions it again. A wrong stamp is
therefore strictly worse than the fail-closed 500 it replaces, which is why
this endpoint is a re-attested, admin-gated operation rather than a SQL
migration — psql cannot obtain a live installation proof.

These tests exist to keep the refusals refusing. The happy path is one test;
the rest pin the conditions under which we must decline to guess.
"""

from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest
from fastapi import HTTPException

from orchestrator.routers.main_cloud_settings import (
    backfill_main_cloud_instance_authority,
)
from orchestrator.application import access as access_composition
from orchestrator.application import workspace as workspace_composition


PROOF = "025b48d99509423b4b10d0b8963270a2b0202f167345f1f506eb798f884c4768"
OTHER_PROOF = "f" * 64
ACTIVE_INSTANCE = "4e72e665-1f70-4b69-9804-d981b51416e6"


def _authority(backend_id="nextcloud", instance_id=ACTIVE_INSTANCE, proof=PROOF):
    return SimpleNamespace(
        backend_id=backend_id,
        backend_instance_id=instance_id,
        installation_proof_sha256=proof,
    )


def _db(*, projects=None, threads=None, registry=None, active=None):
    db = MagicMock()
    db.survey_unstamped_main_cloud_rows = AsyncMock(
        return_value={
            "projects": projects if projects is not None else [],
            "threads": threads if threads is not None else [],
        }
    )
    db.list_main_cloud_backend_instances = AsyncMock(
        return_value=registry if registry is not None else [_authority()]
    )
    db.get_active_main_cloud_backend_instance = AsyncMock(
        return_value=(
            active
            if active is not None
            else {"authority": _authority(), "activation_revision": 2}
        )
    )
    db.stamp_main_cloud_instance_authority = AsyncMock(
        return_value={"projects": 0, "threads": 0}
    )
    return db


def _legacy_project(name="Better Resavio", provider="nextcloud"):
    return {
        "id": uuid4(),
        "name": name,
        "status": "active",
        "main_cloud_backend": provider,
        "main_cloud_folder_handle": "nextcloud:12345",
    }


@contextmanager
def _run(db, *, reattest=True):
    """Call the endpoint with the admin gate and re-attestation stubbed.

    The route moved to ``routers.main_cloud_settings`` and its body to
    ``services.main_cloud_settings``; both read their collaborators from the
    injected ``MainCloudSettingsRouteDependencies``, which the workspace
    composition builds from the application's resources this patches; the
    admin gate is the access composition's, bound to the same application.
    ``reload_active_main_cloud_instance`` is imported into the *service*
    module's namespace, so it has to be patched there.
    """
    import orchestrator.main
    from orchestrator.services import main_cloud_settings as ops

    resources = orchestrator.main.app.state.resources
    admin_gate = AsyncMock(return_value={"id": "admin"})

    async def require_admin(bound_resources, request):
        assert bound_resources is resources
        return await admin_gate(request)

    reattest_stub = AsyncMock(return_value=reattest)
    with (
        patch.object(access_composition, "require_admin", require_admin),
        patch.object(resources, "postgres_db", db),
        patch.object(ops, "reload_active_main_cloud_instance", reattest_stub),
    ):
        dependencies = workspace_composition.main_cloud_settings_dependencies(resources)
        # Proof the patches intercept: the factory reads the application's
        # resources and the gate owner live, so the endpoint runs against
        # exactly the doubles wired here.
        assert dependencies.operations.store is db
        assert dependencies.require_admin.func is require_admin
        assert dependencies.require_admin.args == (resources,)
        yield SimpleNamespace(
            dependencies=dependencies,
            admin_gate=admin_gate,
            reattest=reattest_stub,
        )


class TestBackfillRefusals:
    @pytest.mark.asyncio
    async def test_two_distinct_installations_abort(self, fake_request):
        """Two *proofs* for one provider is the real ambiguity — refuse."""
        db = _db(
            projects=[_legacy_project()],
            registry=[
                _authority(proof=PROOF),
                _authority(instance_id=str(uuid4()), proof=OTHER_PROOF),
            ],
        )
        with _run(db) as wired:
            with pytest.raises(HTTPException) as exc:
                await backfill_main_cloud_instance_authority(
                    fake_request, apply=True, dependencies=wired.dependencies
                )
        assert exc.value.status_code == 409
        assert "distinct" in exc.value.detail
        wired.reattest.assert_awaited_once()
        db.stamp_main_cloud_instance_authority.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_two_routing_snapshots_of_one_installation_are_not_ambiguous(
        self, fake_request
    ):
        """The dev-cluster shape: two registry rows, one installation.

        Routing snapshots multiply when a non-secret setting changes. They
        share an installation proof, and the folders live in the installation
        — not in the snapshot — so this must NOT abort.
        """
        db = _db(
            projects=[_legacy_project()],
            registry=[
                _authority(instance_id=str(uuid4()), proof=PROOF),
                _authority(instance_id=ACTIVE_INSTANCE, proof=PROOF),
            ],
        )
        db.stamp_main_cloud_instance_authority = AsyncMock(
            return_value={"projects": 1, "threads": 0}
        )
        with _run(db) as wired:
            result = await backfill_main_cloud_instance_authority(
                fake_request, apply=True, dependencies=wired.dependencies
            )
        assert result["status"] == "ok"
        assert result["projects"] == 1
        wired.reattest.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_non_active_provider_refuses(self, fake_request):
        """A provider we cannot re-attest right now is never stamped."""
        db = _db(
            projects=[_legacy_project(provider="opencloud")],
            registry=[_authority(backend_id="opencloud", proof=PROOF)],
            active={"authority": _authority(backend_id="nextcloud")},
        )
        with _run(db) as wired:
            with pytest.raises(HTTPException) as exc:
                await backfill_main_cloud_instance_authority(
                    fake_request, apply=True, dependencies=wired.dependencies
                )
        assert exc.value.status_code == 409
        assert "not the active backend" in exc.value.detail
        wired.reattest.assert_awaited_once()
        db.stamp_main_cloud_instance_authority.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_registry_proof_must_match_the_live_attestation(self, fake_request):
        """Registry says one installation, the live probe says another."""
        db = _db(
            projects=[_legacy_project()],
            registry=[_authority(proof=OTHER_PROOF)],
            active={"authority": _authority(proof=PROOF)},
        )
        with _run(db) as wired:
            with pytest.raises(HTTPException) as exc:
                await backfill_main_cloud_instance_authority(
                    fake_request, apply=True, dependencies=wired.dependencies
                )
        assert exc.value.status_code == 409
        assert "does not match" in exc.value.detail
        wired.reattest.assert_awaited_once()
        db.stamp_main_cloud_instance_authority.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_failed_reattestation_refuses(self, fake_request):
        """No live proof, no stamp — the proof is the whole safety argument."""
        db = _db(projects=[_legacy_project()])
        with _run(db, reattest=False) as wired:
            with pytest.raises(HTTPException) as exc:
                await backfill_main_cloud_instance_authority(
                    fake_request, apply=True, dependencies=wired.dependencies
                )
        assert exc.value.status_code == 409
        # The refusal is the stub's ``False`` reaching the service, which is
        # also the proof that patching it there intercepts.
        wired.reattest.assert_awaited_once()
        db.stamp_main_cloud_instance_authority.assert_not_awaited()


class TestBackfillHappyPath:
    @pytest.mark.asyncio
    async def test_dry_run_is_the_default_and_writes_nothing(self, fake_request):
        db = _db(projects=[_legacy_project(), _legacy_project("Test")])
        with _run(db) as wired:
            result = await backfill_main_cloud_instance_authority(
                fake_request, dependencies=wired.dependencies
            )

        assert result["status"] == "dry_run"
        assert result["applied"] is False
        assert result["projects"] == 2
        assert result["plan"][0]["backend_instance_id"] == ACTIVE_INSTANCE
        wired.reattest.assert_awaited_once()
        db.stamp_main_cloud_instance_authority.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_apply_stamps_and_reports(self, fake_request):
        db = _db(
            projects=[_legacy_project()],
            threads=[{"id": uuid4(), "main_cloud_backend": "nextcloud"}],
        )
        db.stamp_main_cloud_instance_authority = AsyncMock(
            return_value={"projects": 1, "threads": 1}
        )
        with _run(db) as wired:
            result = await backfill_main_cloud_instance_authority(
                fake_request, apply=True, dependencies=wired.dependencies
            )

        wired.reattest.assert_awaited_once()
        assert result["status"] == "ok"
        assert (result["projects"], result["threads"]) == (1, 1)
        db.stamp_main_cloud_instance_authority.assert_awaited_once_with(
            backend_id="nextcloud", backend_instance_id=ACTIVE_INSTANCE
        )

    @pytest.mark.asyncio
    async def test_nothing_to_do_is_a_noop_without_reattesting(self, fake_request):
        """An already-clean deployment must not be made to probe its cloud."""
        db = _db()
        with _run(db) as wired:
            result = await backfill_main_cloud_instance_authority(
                fake_request, apply=True, dependencies=wired.dependencies
            )
        assert result["status"] == "noop"
        assert result["applied"] is False
        wired.reattest.assert_not_awaited()
        db.get_active_main_cloud_backend_instance.assert_not_awaited()
        db.stamp_main_cloud_instance_authority.assert_not_awaited()
