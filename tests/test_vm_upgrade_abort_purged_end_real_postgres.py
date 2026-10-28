"""E4b: End of a failed VM upgrade accepts the disk its own abort purged.

Where VM resources are accounted, the abort of a failed upgrade releases the
VM with its disk and leaves the charge to End (E4). End's soft cleanup proves
a stop with the disk retained, which can't be true once the abort purged it,
so the charge stayed held and End never finished (dev's 06f21b14, 2026-10-20).
"""

import json
from uuid import UUID

import asyncpg
import pytest

from orchestrator.services.vm_provisioner import (
    VMProvisioner,
    VMTeardownIdentity,
    VMTeardownResult,
    _VMTeardownProbe,
)
from orchestrator.services.vm_provisioning_phases import VMProvisioningPhaseStore
from orchestrator.services.vm_upgrade_abort_cleanup import (
    release_aborted_upgrade_vm,
    upgrade_abort_purged_disk,
)
from orchestrator.services.vm_workspace_recovery_store import VMWorkspaceRecoveryStore
from shared.vm_resource_admission import ResourceAdmissionError
from tests.test_vm_resource_thread_cleanup_real_postgres import (
    operations,
    db as _db,
    thread_schema,  # noqa: F401
    _base_db,  # noqa: F401
    _schema_applied,  # noqa: F401
    pg_dsn as _pg_dsn,
)
from tests.test_vm_resource_thread_source_real_postgres import _ready_charged_thread
from tests.test_vm_thread_retained_disk_purge_real_postgres import permanent_begin

db = _db
pg_dsn = _pg_dsn


async def record_process_zero(db, case):
    await db.execute(
        "INSERT INTO managed_repository_process_zero_receipts "
        "(owner_kind,owner_id,scope,provisioner,runtime_incarnation) "
        "VALUES('thread',$1,'vm','vm',$2) ON CONFLICT DO NOTHING",
        case["thread_id"],
        str(case["generation"]),
    )


class ProcessZeroUnproven:
    """The abort's first release, before the controller proved process zero."""

    lifecycle_available = True

    async def release_vm_captured(self, owner, identity, **kwargs):
        return VMTeardownResult("process_zero_unproven", False)


class Superseded:
    """An abort release that found another VM identity: no purge proven."""

    lifecycle_available = True

    async def release_vm_captured(self, owner, identity, **kwargs):
        return VMTeardownResult("identity_superseded", False)


class VmAndDiskGone:
    """The controller once the abort released the VM with its disk.

    A release answers what ``release_vm_captured`` answers when the runtime
    and the disk are both proven absent and process zero is recorded:
    completed, with the thread's VM recorded deleted. Attestation runs the
    real rule against that observation.
    """

    lifecycle_available = True

    def __init__(self, db, case):
        self.db, self.case = db, case
        self.releases = []

    async def release_vm_captured(self, owner, identity, **kwargs):
        assert owner == str(self.case["thread_id"])
        assert identity.vm_uid == self.case["vm_uid"]
        assert identity.rootdisk_pvc_uid == self.case["pvc_uid"]
        self.releases.append(kwargs["purge_disk"])
        await record_process_zero(self.db, self.case)
        await self.db.merge_thread_vm_context_if_provision_generation(
            owner, identity.provision_generation, {"status": "deleted"}
        )
        return VMTeardownResult("completed", True)

    async def attest_vm_cleanup_stop(self, candidate):
        return await VMProvisioner.attest_vm_cleanup_stop(self, candidate)

    async def _current_provision_generation(self, owner_kind, owner_id):
        assert owner_kind == "thread"
        return await self.db.fetchval(
            "SELECT metadata->'vm'->>'provision_generation' FROM threads WHERE id=$1",
            UUID(owner_id),
        )

    async def _probe_vm_teardown_identity(self, owner, generation, **kwargs):
        return _VMTeardownProbe(
            "absent",
            VMTeardownIdentity(
                provision_generation=generation, vm_uid=None, rootdisk_pvc_uid=None
            ),
            rootdisk_identity_known=True,
            runtime_absence_known=True,
            vmi_absent=True,
            launcher_absent=True,
        )


async def failed_upgrade(db, monkeypatch, *, abort, permanent=False):
    """A charged VM Session whose upgrade failed, its owner's End begun.

    ``abort``: ``"finished"`` (the route released the VM and disk, then marked
    the record aborted, its undo refused while the charge is open), ``"open"``
    (the route's release wasn't proven yet, dev's 06f21b14: crash recovery
    finishes it, E4 part 3), ``"superseded"`` (the abort completed as
    identity_superseded, which proves no purge) or ``None`` (the disk vanished
    with no abort).
    """
    case = await _ready_charged_thread(db, monkeypatch)
    thread_id, generation = str(case["thread_id"]), str(case["generation"])
    assert await VMProvisioningPhaseStore(db).publish_thread_ready(
        thread_id, generation, case["registration"], case["vm_uid"], case["updates"]
    )
    await db.execute(
        "UPDATE threads SET metadata=jsonb_set(metadata,'{config_override}',"
        '\'{"workspace":{"backend":"vm"}}\') WHERE id=$1',
        case["thread_id"],
    )
    case["pvc_uid"] = await db.fetchval(
        "SELECT metadata->'vm'->>'rootdisk_pvc_uid' FROM threads WHERE id=$1",
        case["thread_id"],
    )
    identity = VMTeardownIdentity(
        provision_generation=generation,
        vm_uid=case["vm_uid"],
        rootdisk_pvc_uid=case["pvc_uid"],
    )
    recovery = VMWorkspaceRecoveryStore(db)
    if abort == "finished":
        assert (
            await release_aborted_upgrade_vm(
                recovery, VmAndDiskGone(db, case), thread_id, identity
            )
            == "completed"
        )
        status = "aborted"
    elif abort == "superseded":
        assert (
            await release_aborted_upgrade_vm(
                recovery, Superseded(), thread_id, identity
            )
            == "identity_superseded"
        )
        await record_process_zero(db, case)
        status = "aborted"
    elif abort == "open":
        assert (
            await release_aborted_upgrade_vm(
                recovery, ProcessZeroUnproven(), thread_id, identity
            )
            == "process_zero_unproven"
        )
        status = "retiring_process_zero"
    else:
        await record_process_zero(db, case)
        status = "deleted"
    assert await db.merge_thread_vm_context_if_provision_generation(
        thread_id, generation, {"status": status}
    )
    retirement = await db.begin_pinned_thread_retirement(thread_id, permanent=permanent)
    assert retirement["state"] == "pending", retirement
    assert await db.authorize_pinned_thread_retirement(
        thread_id,
        token=retirement["token"],
        generation=retirement["generation"],
        settle_status="ended",
    )
    case["retirement"] = retirement
    return case


async def charge_state(db, case):
    return await db.fetchval(
        "SELECT state FROM vm_resource_reservations WHERE id=$1",
        UUID(case["admitted"]["reservation_id"]),
    )


async def end(db, case):
    controller = VmAndDiskGone(db, case)
    await operations(db, controller).cleanup_pinned_thread_retirement(
        case["retirement"], cleanup_agent_pod=False
    )
    return controller


@pytest.mark.asyncio
@pytest.mark.parametrize("abort", ["finished", "open"])
async def test_end_after_a_failed_upgrade_releases_the_charge_and_settles(
    db, monkeypatch, abort
):
    case = await failed_upgrade(db, monkeypatch, abort=abort)

    controller = await end(db, case)

    # An open abort is finished first (with the disk), then End's own release.
    assert controller.releases == ([True, False] if abort == "open" else [False])
    assert await charge_state(db, case) == "released"
    stop = await db.fetchval(
        "SELECT s.stop_evidence FROM vm_resource_thread_cleanup_stops s "
        "JOIN vm_resource_thread_cleanup_authorities a USING(cleanup_admission_id) "
        "WHERE a.thread_id=$1",
        case["thread_id"],
    )
    assert json.loads(stop)["pvc_disposition"] == "purged_by_upgrade_abort"
    assert await db.settle_pinned_thread_retirement(
        str(case["thread_id"]),
        token=case["retirement"]["token"],
        generation=case["retirement"]["generation"],
    )
    assert (await db.get_thread(str(case["thread_id"])))["status"] == "ended"


@pytest.mark.asyncio
async def test_resume_after_a_failed_upgrade_says_its_vm_is_gone(db, monkeypatch):
    """Roadmap item 10: the retained-resume check failed with a bare 23514."""
    from orchestrator.services.vm_thread_retained_resume import VmUpgradeDiskPurged

    case = await failed_upgrade(db, monkeypatch, abort="finished")
    await end(db, case)
    assert await db.settle_pinned_thread_retirement(
        str(case["thread_id"]),
        token=case["retirement"]["token"],
        generation=case["retirement"]["generation"],
    )

    with pytest.raises(VmUpgradeDiskPurged):
        await db.resume_thread(str(case["thread_id"]))
    assert (await db.get_thread(str(case["thread_id"])))["status"] == "ended"


@pytest.mark.asyncio
async def test_a_failed_upgrade_session_is_deleted_after_its_end(db, monkeypatch):
    case = await failed_upgrade(db, monkeypatch, abort="open")
    await end(db, case)
    assert await db.settle_pinned_thread_retirement(
        str(case["thread_id"]),
        token=case["retirement"]["token"],
        generation=case["retirement"]["generation"],
    )

    permanent = await permanent_begin(db, case)
    await operations(db, VmAndDiskGone(db, case)).cleanup_pinned_thread_retirement(
        permanent, cleanup_agent_pod=False
    )
    await db.delete_thread(
        str(case["thread_id"]),
        expected_runtime_retirement_token=permanent["token"],
        expected_runtime_generation=permanent["generation"],
    )

    assert await db.get_thread(str(case["thread_id"])) is None
    receipt = await db.fetchval(
        "SELECT deletion_receipt FROM vm_thread_creation_owners WHERE thread_id=$1",
        case["thread_id"],
    )
    # The purge after the soft End confirmed the disk already absent.
    assert json.loads(receipt)["kind"] == "retained_vm_disk_purge"


@pytest.mark.asyncio
@pytest.mark.parametrize("abort", [None, "superseded"])
async def test_a_disk_gone_without_the_sessions_own_abort_keeps_the_charge(
    db, monkeypatch, abort
):
    case = await failed_upgrade(db, monkeypatch, abort=abort)

    with pytest.raises(ResourceAdmissionError, match="resource_cleanup_stop_unproven"):
        await end(db, case)

    assert await charge_state(db, case) == "teardown"
    assert not await db.fetchval(
        "SELECT EXISTS(SELECT 1 FROM vm_resource_thread_cleanup_stops s "
        "JOIN vm_resource_thread_cleanup_authorities a USING(cleanup_admission_id) "
        "WHERE a.thread_id=$1)",
        case["thread_id"],
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["reopened", "other_vm"])
async def test_the_abort_purged_stop_holds_only_with_the_sessions_completed_abort(
    db, monkeypatch, fault
):
    case = await failed_upgrade(db, monkeypatch, abort="finished")
    await end(db, case)
    validate = (
        "SELECT public.validate_vm_thread_cleanup_stop(a,s,true) "
        "FROM vm_resource_thread_cleanup_authorities a "
        "JOIN vm_resource_thread_cleanup_stops s USING(cleanup_admission_id) "
        "WHERE a.thread_id=$1"
    )
    assert await db.fetchval(validate, case["thread_id"]) is True
    assert await witnesses(db, case) == (True, True)

    # A privileged writer reopens the abort's admission, or points it at
    # another VM; the recorded stop no longer validates.
    async with db.acquire() as conn, conn.transaction():
        await conn.execute("SET LOCAL session_replication_role='replica'")
        await conn.execute(
            "UPDATE vm_workspace_cleanup_admissions SET "
            + (
                "completed_at=NULL,outcome=NULL"
                if fault == "reopened"
                else "intent_digest='sha256:other'"
            )
            + " WHERE owner_id=$1 AND source='abort_thread_vm_upgrade'",
            case["thread_id"],
        )

    assert await witnesses(db, case) == (False, False)
    with pytest.raises(asyncpg.CheckViolationError):
        await db.fetchval(validate, case["thread_id"])


async def witnesses(db, case):
    """End's candidate flag (Python) and the stop's SQL witness, for its authority."""
    async with db.acquire() as conn:
        authority = await conn.fetchrow(
            "SELECT * FROM vm_resource_thread_cleanup_authorities WHERE thread_id=$1",
            case["thread_id"],
        )
        return (
            await upgrade_abort_purged_disk(conn, authority),
            await conn.fetchval(
                "SELECT public.vm_thread_cleanup_disk_purged_by_upgrade_abort(a) "
                "FROM vm_resource_thread_cleanup_authorities a WHERE thread_id=$1",
                case["thread_id"],
            ),
        )


@pytest.mark.asyncio
async def test_a_stop_recorded_as_retained_still_holds_after_the_abort(db, monkeypatch):
    """Like the retained-disk purge, the stop check accepts either record."""
    case = await failed_upgrade(db, monkeypatch, abort="finished")
    await end(db, case)
    async with db.acquire() as conn, conn.transaction():
        await conn.execute("SET LOCAL session_replication_role='replica'")
        await conn.execute(
            "UPDATE vm_resource_thread_cleanup_stops s SET stop_evidence="
            "jsonb_set(s.stop_evidence,'{pvc_disposition}','\"retained\"') "
            "FROM vm_resource_thread_cleanup_authorities a "
            "WHERE a.cleanup_admission_id=s.cleanup_admission_id AND a.thread_id=$1",
            case["thread_id"],
        )

    assert await db.fetchval(
        "SELECT public.validate_vm_thread_cleanup_stop(a,s,true) "
        "FROM vm_resource_thread_cleanup_authorities a "
        "JOIN vm_resource_thread_cleanup_stops s USING(cleanup_admission_id) "
        "WHERE a.thread_id=$1",
        case["thread_id"],
    )


@pytest.mark.asyncio
async def test_a_failed_upgrade_session_deleted_without_a_soft_end_first(
    db, monkeypatch
):
    """A permanent End purges the disk anyway: its proof expects it gone."""
    case = await failed_upgrade(db, monkeypatch, abort="finished", permanent=True)

    await end(db, case)

    assert await charge_state(db, case) == "released"
    await db.delete_thread(
        str(case["thread_id"]),
        expected_runtime_retirement_token=case["retirement"]["token"],
        expected_runtime_generation=case["retirement"]["generation"],
    )
    assert await db.get_thread(str(case["thread_id"])) is None


@pytest.mark.asyncio
async def test_the_aborted_record_is_not_written_once_end_began(db, monkeypatch):
    """E4b review: End owns the VM record once it began (the route's guard)."""
    case = await failed_upgrade(db, monkeypatch, abort="finished")
    await end(db, case)
    thread_id, generation = str(case["thread_id"]), str(case["generation"])

    assert not await db.merge_thread_vm_context_if_provision_generation(
        thread_id, generation, {"status": "aborted"}, require_not_retiring=True
    )
    assert (
        await db.fetchval(
            "SELECT metadata->'vm'->>'status' FROM threads WHERE id=$1",
            case["thread_id"],
        )
        == "deleted"
    )
