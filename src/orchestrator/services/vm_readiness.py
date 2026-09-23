"""Controller-attested SSH readiness for same-cluster VM workspaces."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping
from datetime import datetime, timezone
import json
import ipaddress
import logging
import os
import secrets
import shlex
import time
from typing import Any
from uuid import UUID, uuid4

from orchestrator.services import resolve_ssh_key_path

from orchestrator.services.ide_settings import (
    IdeSettingsStore,
    seed_ide_config_for_user,
    seed_ide_profile,
)
from orchestrator.services.ssh_helpers import wait_for_agent_ssh
from orchestrator.services.ssh_helpers import pinned_agent_ssh_command
from orchestrator.services.subprocess_effect import (
    communicate_bounded,
    create_owned_subprocess_exec,
)

logger = logging.getLogger(__name__)


def _vm_object(value: object) -> dict[str, Any]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (TypeError, ValueError):
            return {}
    return dict(value) if isinstance(value, Mapping) else {}


def _generation(value: object) -> str | None:
    try:
        parsed = UUID(str(value))
    except (TypeError, ValueError):
        return None
    return str(parsed) if str(parsed) == value else None


def _machine_identity(value: object) -> str | None:
    """Return one canonical, nonzero systemd machine ID."""

    if (
        not isinstance(value, str)
        or len(value) != 32
        or value != value.lower()
        or value == "0" * 32
        or any(character not in "0123456789abcdef" for character in value)
    ):
        return None
    return value


def _complete_recovery_network(
    value: object, *, challenge: str, expected_mac: str,
    network_profile: object = None,
) -> bool:
    if not isinstance(value, Mapping):
        return False
    interfaces = value.get("interfaces")
    routes = value.get("routes")
    default_route = value.get("default_route")
    netplan = value.get("netplan_sha256")
    networkd = value.get("networkd_sha256")
    if (
        value.get("challenge") != challenge
        or _generation(value.get("boot_id")) is None
        or _machine_identity(value.get("machine_id")) is None
        or "registration_id" in value
        or not isinstance(interfaces, list)
        or not interfaces
        or not any(
            isinstance(interface, Mapping)
            and interface.get("mac") == expected_mac
            and isinstance(interface.get("address"), str)
            and bool(interface["address"].strip())
            for interface in interfaces
        )
        or not isinstance(value.get("address"), str)
        or not value["address"].strip()
        or not isinstance(routes, list)
        or not routes
        or not isinstance(default_route, Mapping)
        or default_route.get("dst") not in {"default", "0.0.0.0/0", "::/0"}
        or not isinstance(value.get("dns"), str)
        or not value["dns"].strip()
        or not isinstance(netplan, Mapping)
        or not isinstance(networkd, Mapping)
        or not (netplan or networkd)
        or not isinstance(value.get("cloud_init_instance_id"), str)
        or not value["cloud_init_instance_id"].strip()
        or not isinstance(value.get("cloud_init_cache_identity"), str)
        or not value["cloud_init_cache_identity"].strip()
        or value.get("cloud_init_cache_cleaned") is not False
    ):
        return False
    if network_profile is not None:
        from shared.vm_network_profile import validate_network_profile

        try:
            validate_network_profile(network_profile)
        except ValueError:
            return False
        rule = value.get("network_profile_rule")
        if (
            not isinstance(rule, Mapping)
            or set(rule) != {
                "kind", "interface", "name_only_dhcp", "network_file_sha256",
                "dhcp4_address", "dhcp4_gateway", "dhcp4_lease_sha256", "dhcp4_ifindex",
            }
            or rule["kind"] != "networkd-name-dhcp-v1"
            or rule["interface"] != "enp1s0"
            or rule["name_only_dhcp"] is not True
            or not isinstance(rule["network_file_sha256"], str)
            or rule["network_file_sha256"] not in networkd.values()
            or not isinstance(rule["dhcp4_lease_sha256"], str)
            or len(rule["dhcp4_lease_sha256"]) != 64
            or any(char not in "0123456789abcdef" for char in rule["dhcp4_lease_sha256"])
            or type(rule["dhcp4_ifindex"]) is not int
            or rule["dhcp4_ifindex"] <= 0
            or rule["dhcp4_address"] != value.get("address")
            or rule["dhcp4_gateway"] != default_route.get("gateway")
            or default_route.get("dev") != "enp1s0"
            or default_route.get("protocol") != "dhcp"
            or default_route not in routes
            or value.get("cloud_init_cached_instance_id") != value.get("cloud_init_instance_id")
            or sum(
                1 for item in interfaces
                if isinstance(item, Mapping)
                and item.get("ifname") == "enp1s0"
                and item.get("mac") == expected_mac
                and item.get("address") == value.get("address")
            ) != 1
        ):
            return False
        try:
            ipaddress.IPv4Address(rule["dhcp4_address"])
            ipaddress.IPv4Address(rule["dhcp4_gateway"])
        except (ipaddress.AddressValueError, TypeError):
            return False
    return all(
        isinstance(path, str)
        and bool(path)
        and isinstance(digest, str)
        and bool(digest)
        for mapping in (netplan, networkd)
        for path, digest in mapping.items()
    )


async def qualify_recovery_successor(
    successor: Mapping[str, Any], *, host_key_fingerprint: str,
    network_profile: object = None,
) -> dict[str, Any] | None:
    """Read-only pinned-SSH qualification for a controller-observed successor."""

    pod_ip = successor.get("pod_ip")
    if not isinstance(pod_ip, str) or not pod_ip or pod_ip != pod_ip.strip():
        return None
    if (
        _generation(successor.get("vmi_uid")) is None
        or _generation(successor.get("launcher_uid")) is None
    ):
        return None
    interface_mac = successor.get("interface_mac")
    if not isinstance(interface_mac, str) or not interface_mac.strip():
        return None
    ready, _attempts, _error = await wait_for_agent_ssh(
        pod_ip,
        22,
        key_path=resolve_ssh_key_path(),
        deadline_s=10.0,
        connect_timeout_s=10,
        interval_s=0.5,
        expected_host_key_fingerprint=host_key_fingerprint,
    )
    if not ready:
        return None
    challenge = secrets.token_urlsafe(32)
    try:
        async with pinned_agent_ssh_command(
            pod_ip,
            22,
            (
                "/usr/local/bin/srw-network-profile-qualification "
                if network_profile is not None
                else "/usr/local/bin/srw-network-qualification "
            ) + shlex.quote(challenge),
            expected_host_key_fingerprint=host_key_fingerprint,
            key_path=resolve_ssh_key_path(),
            connect_timeout_s=10,
            batch_mode=True,
        ) as command:
            process = await create_owned_subprocess_exec(
                *command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, _stderr = await communicate_bounded(
                process,
                timeout=15,
                stdout_limit=128 * 1024,
                stderr_limit=16 * 1024,
            )
        network = json.loads(stdout) if process.returncode == 0 else None
    except Exception:
        return None
    if not _complete_recovery_network(
        network, challenge=challenge, expected_mac=interface_mac,
        network_profile=network_profile,
    ):
        return None
    return {
        "pod_ip": pod_ip,
        "ssh_registration_id": uuid4().hex,
        "guest_boot_id": network["boot_id"],
        "guest_machine_id": network["machine_id"],
        "guest_network": dict(network),
    }


class VMReadinessService:
    """DB-rearmable readiness prober with bounded concurrency and backoff."""

    def __init__(
        self,
        db: Any,
        provisioner: Any,
        *,
        trigger_dispatch: Callable[[], None],
        max_inflight: int | None = None,
        ready_rescan_s: float = 60.0,
    ) -> None:
        self._db = db
        self._provisioner = provisioner
        self._trigger_dispatch = trigger_dispatch
        configured = max_inflight or int(os.getenv("VM_READINESS_MAX_INFLIGHT", "8"))
        self._max_inflight = max(1, configured)
        self._inflight: set[tuple[str, str, str]] = set()
        self._tasks: set[asyncio.Task[None]] = set()
        self._retry_after: dict[tuple[str, str, str], float] = {}
        self._failures: dict[tuple[str, str, str], int] = {}
        self._ready_rescan_s = ready_rescan_s
        self._last_ready_scan = 0.0

    async def run(self, shutdown_event: asyncio.Event) -> None:
        logger.info("Same-cluster VM readiness prober started")
        while not shutdown_event.is_set():
            try:
                await self.run_cycle()
            except Exception:
                logger.exception("VM readiness cycle failed")
            try:
                await asyncio.wait_for(shutdown_event.wait(), timeout=3.0)
            except asyncio.TimeoutError:
                pass
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        logger.info("Same-cluster VM readiness prober stopped")

    async def run_cycle(self) -> None:
        rows: list[tuple[str, Mapping[str, Any], bool]] = []
        jobs, threads = await asyncio.gather(
            self._db.list_job_vm_readiness_candidates(),
            self._db.list_thread_vm_readiness_candidates(),
        )
        rows.extend(("job", row, False) for row in jobs)
        rows.extend(("thread", row, False) for row in threads)

        now = time.monotonic()
        if now - self._last_ready_scan >= self._ready_rescan_s:
            ready_jobs, ready_threads = await asyncio.gather(
                self._db.list_job_vm_readiness_candidates(ready=True),
                self._db.list_thread_vm_readiness_candidates(ready=True),
            )
            rows.extend(("job", row, True) for row in ready_jobs)
            rows.extend(("thread", row, True) for row in ready_threads)
            self._last_ready_scan = now

        candidate_keys = {
            key
            for entity_type, row, _reprobe in rows
            if (key := self._candidate_key(entity_type, row)) is not None
        }
        self._failures = {
            key: failures
            for key, failures in self._failures.items()
            if key in candidate_keys
        }
        self._retry_after = {
            key: retry_at
            for key, retry_at in self._retry_after.items()
            if key in candidate_keys
        }
        for entity_type, row, reprobe in rows:
            if len(self._inflight) >= self._max_inflight:
                break
            key = self._candidate_key(entity_type, row)
            if (
                key is None
                or key in self._inflight
                or time.monotonic() < self._retry_after.get(key, 0.0)
            ):
                continue
            self._inflight.add(key)
            task = asyncio.create_task(
                self._run_probe_task(entity_type, row, reprobe, key)
            )
            self._tasks.add(task)
            task.add_done_callback(self._probe_done)

        # Give freshly-created tasks a chance to start without making a DB scan
        # wait for controller/SSH timeouts. The next cycle deduplicates them via
        # ``_inflight``.
        await asyncio.sleep(0)

    @staticmethod
    def _candidate_key(
        entity_type: str, row: Mapping[str, Any]
    ) -> tuple[str, str, str] | None:
        entity_id = str(row.get("entity_id") or "")
        generation = _generation(_vm_object(row.get("vm")).get("provision_generation"))
        if not entity_id or generation is None:
            return None
        return entity_type, entity_id, generation

    async def _run_probe_task(
        self,
        entity_type: str,
        row: Mapping[str, Any],
        reprobe: bool,
        key: tuple[str, str, str],
    ) -> None:
        _, entity_id, generation = key
        vm = _vm_object(row.get("vm"))
        try:
            await self._probe(entity_type, entity_id, generation, vm, row, reprobe)
        finally:
            self._inflight.discard(key)

    def _probe_done(self, task: asyncio.Task[None]) -> None:
        self._tasks.discard(task)
        if task.cancelled():
            return
        error = task.exception()
        if error is not None:
            logger.error(
                "VM readiness probe task failed",
                exc_info=(type(error), error, error.__traceback__),
            )

    async def _probe(
        self,
        entity_type: str,
        entity_id: str,
        generation: str,
        vm: Mapping[str, Any],
        row: Mapping[str, Any],
        reprobe: bool,
    ) -> None:
        key = (entity_type, entity_id, generation)
        if await self._recovery_owns_authority(entity_type, entity_id):
            logger.debug(
                "Recovery owns VM readiness authority for %s %s",
                entity_type,
                entity_id,
            )
            return
        if entity_type == "thread" and (
            vm.get("status")
            in {
                "waiting_preparation",
                "waiting_golden",
                "waiting_capacity",
                "waiting_headscale",
            }
            or vm.get("status") == "provisioning"
            and vm.get("preparation_request") is not None
            and vm.get("identity_authenticated") is False
        ):
            await self._provisioner.poll_thread_vm(entity_id, generation)
            self._retry_after[key] = time.monotonic() + 5.0
            return
        status = await self._provisioner.query_status(
            entity_id, entity_type=entity_type
        )
        if not isinstance(status, Mapping):
            if reprobe:
                logger.debug(
                    "Ready VM %s %s status query unavailable; preserving readiness",
                    entity_type,
                    entity_id,
                )
                return
            await self._transient_failure(
                key,
                entity_type,
                entity_id,
                generation,
                vm,
                "controller status unavailable",
                reprobe=False,
            )
            return

        # Admission may race the controller read. Once recovery has installed
        # its durable hold, the ordinary readiness path must remain read-only;
        # only the recovery final CAS can bind the successor identity.
        if await self._recovery_owns_authority(entity_type, entity_id):
            return

        reported_generation = status.get("provision_generation")
        if reported_generation not in (None, generation):
            logger.debug(
                "Ignoring VM status for stale generation on %s %s",
                entity_type,
                entity_id,
            )
            return

        if vm.get("preparation_request") is not None:
            prepared = status.get("preparation") or vm.get("preparation") or {}
            if not isinstance(prepared, Mapping) or prepared.get("phase") not in {
                "Succeeded",
                "ExistingWorkspace",
            }:
                await self._transient_failure(
                    key,
                    entity_type,
                    entity_id,
                    generation,
                    vm,
                    "prepared workspace artifact is not attested",
                    reprobe=False,
                )
                return

        if status.get("status") == "not_found":
            await self._provisioner._set_context_if_generation(
                entity_type,
                entity_id,
                generation,
                {"status": "ssh_unreachable", "ssh_probe_error": "vm not found"},
                require_status_not_ready=not reprobe,
            )
            return

        phase = str(status.get("phase") or "")
        if (
            phase.lower() == "stopped"
            and not reprobe
            and (
                status.get("credential_runtime_started") is False
                or status.get("vmi_phase") in {"Pending", "Scheduling", "Scheduled"}
            )
        ):
            # KubeVirt briefly reports Stopped before the first VMI runs,
            # including with a Pending VMI while CDI allocates its disk. Keep this initial
            # allocation in the bounded boot loop, not outside the candidate
            # query as an unreachable formerly-running guest.
            await self._transient_failure(
                key,
                entity_type,
                entity_id,
                generation,
                vm,
                "Waiting for the first VM instance",
                reprobe=False,
            )
            return
        if phase.lower() in {"stopped", "succeeded"}:
            await self._provisioner._set_context_if_generation(
                entity_type,
                entity_id,
                generation,
                {"status": "ssh_unreachable", "ssh_probe_error": "vm stopped"},
                require_status_not_ready=not reprobe,
            )
            return

        pod_ip = status.get("pod_ip")
        active_pod_uid = status.get("active_pod_uid")
        if (
            not isinstance(pod_ip, str)
            or not pod_ip
            or not isinstance(active_pod_uid, str)
            or not active_pod_uid
            or status.get("ready") is not True
        ):
            return

        host_key_fingerprint = vm.get("ssh_host_key_fingerprint")
        frozen_request = _vm_object(_vm_object(vm.get("creation_preflight")).get("request"))
        network_profile = (
            frozen_request.get("network_profile") if entity_type == "job" else None
        )
        if not isinstance(host_key_fingerprint, str) or not host_key_fingerprint:
            await self._transient_failure(
                key,
                entity_type,
                entity_id,
                generation,
                vm,
                "SSH host-key fingerprint pin is absent",
                reprobe=reprobe,
            )
            return

        unchanged_ready_identity = (
            reprobe
            and pod_ip == vm.get("pod_ip")
            and active_pod_uid == vm.get("active_pod_uid")
        )
        ready, _attempts, error = await wait_for_agent_ssh(
            pod_ip,
            22,
            key_path=resolve_ssh_key_path(),
            deadline_s=10.0,
            connect_timeout_s=10,
            interval_s=0.5,
            expected_host_key_fingerprint=host_key_fingerprint,
        )
        if not ready:
            # Availability blips on an already-ready VM preserve the existing
            # endpoint, but identity failures never do: a missing/invalid pin
            # or mismatched presented key must demote the VM fail-closed.
            # Two identity sources: the scan's fingerprint verdicts, and the
            # ssh process itself refusing the one-use known_hosts line (the
            # scan->connect race). The scan's no-key-seen availability error
            # deliberately avoids both wordings.
            lowered = (error or "").lower()
            identity_failure = (
                "fingerprint" in lowered or "host key verification failed" in lowered
            )
            if reprobe and not identity_failure:
                logger.debug(
                    "Ready VM %s %s failed SSH auth; preserving readiness",
                    entity_type,
                    entity_id,
                )
                return
            await self._transient_failure(
                key,
                entity_type,
                entity_id,
                generation,
                vm,
                error or "SSH authentication failed",
                reprobe=reprobe,
            )
            return
        exact_profile_receipt = False
        if unchanged_ready_identity and network_profile is not None:
            from shared.vm_network_profile import reusable_profile_evidence

            exact_profile_receipt = (
                all(
                    vm.get(identity) in (None, status.get(identity))
                    for identity in ("vm_uid", "rootdisk_pvc_uid", "vmi_uid")
                )
                and reusable_profile_evidence(
                    vm.get("network_profile_evidence"), network_profile,
                    provision_generation=generation,
                    vm_uid=status.get("vm_uid"),
                    pvc_uid=status.get("rootdisk_pvc_uid"),
                    vmi_uid=status.get("vmi_uid"),
                    launcher_uid=status.get("active_pod_uid"),
                )
            )
        if unchanged_ready_identity and (network_profile is None or exact_profile_receipt):
            if vm.get("initialization") is None:
                return
            from shared.workspace_initialization import (
                initialization_receipt,
                validate_initialization_request,
            )

            try:
                request = validate_initialization_request(vm["initialization"])
                previous = initialization_receipt(
                    vm.get("initialization_receipt"),
                    owner_id=(vm.get("workspace_storage") or {}).get("uid", entity_id),
                    revision=request["revision"],
                )
            except ValueError:
                pass
            else:
                if previous["phase"] == "Succeeded" and previous["step"] == len(
                    request["steps"]
                ):
                    return

        verified_at = datetime.now(timezone.utc).isoformat()
        registration_id = uuid4().hex
        # Persist the exact controller-observed launcher identity before any
        # remote write. This is deliberately still non-ready: a crash between
        # this CAS and final promotion is rearmed by the next readiness scan.
        prepared = await self._provisioner._set_context_if_generation(
            entity_type,
            entity_id,
            generation,
            {
                "status": "ssh_pending",
                "ssh_host": pod_ip,
                "pod_ip": pod_ip,
                "ssh_port": 22,
                "active_pod_uid": active_pod_uid,
                "ssh_ready_source": "provisioner_probe",
                "ssh_verified_at": verified_at,
                "ssh_registration_id": registration_id,
                "ssh_probe_error": None,
                "recovering": False,
            },
            require_status_not_ready=not reprobe,
        )
        if not prepared:
            return

        try:
            initial_attestation = await self._provisioner.attest_workspace_runtime(
                entity_id,
                entity_type=entity_type,
            )
        except Exception as exc:
            await self._transient_failure(
                key,
                entity_type,
                entity_id,
                generation,
                vm,
                f"VM mutation authority unavailable: {exc}",
                reprobe=False,
            )
            return

        network_profile_evidence = None
        if network_profile is not None:
            from shared.vm_network_profile import validate_network_profile

            try:
                validate_network_profile(network_profile)
            except ValueError:
                return
            if (
                status.get("vm_uid") != initial_attestation.vm_uid
                or status.get("rootdisk_pvc_uid") != initial_attestation.rootdisk_pvc_uid
                or status.get("vmi_uid") != initial_attestation.vmi_uid
                or status.get("active_pod_uid") != initial_attestation.runtime_incarnation
                or initial_attestation.workspace_generation != generation
                or initial_attestation.rootdisk_pvc_uid is None
            ):
                await self._transient_failure(
                    key, entity_type, entity_id, generation, vm,
                    "VM network profile runtime identity changed", reprobe=False,
                )
                return
            qualified = await qualify_recovery_successor(
                {
                    "pod_ip": pod_ip,
                    "vmi_uid": status["vmi_uid"],
                    "launcher_uid": active_pod_uid,
                    "interface_mac": status.get("interface_mac"),
                },
                host_key_fingerprint=host_key_fingerprint,
                network_profile=network_profile,
            )
            if qualified is None:
                await self._transient_failure(
                    key, entity_type, entity_id, generation, vm,
                    "VM network profile rule or guest network is unproven",
                    reprobe=False,
                )
                return
            guest_network = qualified["guest_network"]
            network_profile_evidence = {
                "profile": network_profile,
                "provision_generation": generation,
                "vm_uid": initial_attestation.vm_uid,
                "pvc_uid": initial_attestation.rootdisk_pvc_uid,
                "vmi_uid": initial_attestation.vmi_uid,
                "launcher_uid": initial_attestation.runtime_incarnation,
                "guest_boot_id": qualified["guest_boot_id"],
                "cloud_init_instance_id": guest_network["cloud_init_instance_id"],
                "cloud_init_cached_instance_id": guest_network["cloud_init_cached_instance_id"],
                "network_file_sha256": guest_network["network_profile_rule"]["network_file_sha256"],
                "name_only_dhcp": True,
            }

        async def mutation_authority() -> tuple[str, int, str] | None:
            """Re-prove the exact launcher immediately before each SSH write."""

            try:
                current = await self._provisioner.attest_workspace_runtime(
                    entity_id,
                    entity_type=entity_type,
                )
            except Exception:
                return None
            if (
                current.workspace_generation != initial_attestation.workspace_generation
                or current.runtime_incarnation
                != initial_attestation.runtime_incarnation
                or current.backing_id != initial_attestation.backing_id
                or current.host != initial_attestation.host
                or current.port != initial_attestation.port
                or current.ssh_host_key_fingerprint
                != initial_attestation.ssh_host_key_fingerprint
            ):
                return None
            return (
                current.host,
                current.port,
                current.ssh_host_key_fingerprint,
            )

        if vm.get("initialization") is not None:
            from orchestrator.services.vm_initialization import read_vm_initialization
            from shared.workspace_initialization import TIMEOUT_SECONDS

            try:
                receipt = await read_vm_initialization(
                    initial_attestation,
                    owner_id=(vm.get("workspace_storage") or {}).get("uid", entity_id),
                    request=vm["initialization"],
                )
            except Exception:
                receipt = None
            if await mutation_authority() is None:
                return
            now = time.time()
            started = vm.get("initialization_started_at")
            if started is None:
                started = now
            valid_start = type(started) in (int, float) and 0 < started <= now
            updates = {
                "initialization_started_at": started,
                "initialization_receipt": receipt,
            }
            if receipt is None or receipt["phase"] != "Succeeded":
                failed = receipt is not None and receipt["phase"] == "Failed"
                expired = not valid_start or now - started > TIMEOUT_SECONDS + 60
                error = (
                    f"Workspace initialization failed at step {receipt['step'] + 1} "
                    f"(exit {receipt['exitCode']})"
                    if failed
                    else "Workspace initialization timed out"
                    if expired
                    else "Waiting for workspace initialization"
                )
                updates.update(
                    status="failed" if failed or expired else "ssh_pending",
                    ssh_probe_error=error,
                    error=error if failed or expired else None,
                )
            record = (
                self._db.merge_thread_vm_context_if_current
                if entity_type == "thread"
                else self._db.merge_vm_context_if_current
            )
            accepted = await record(entity_id, registration_id, updates)
            if not accepted or receipt is None or receipt["phase"] != "Succeeded":
                if (
                    accepted
                    and updates.get("status") == "failed"
                    and entity_type == "job"
                ):
                    self._trigger_dispatch()
                return

        seeded = await seed_ide_config_for_user(
            self._db,
            row.get("user_id"),
            initial_attestation.host,
            initial_attestation.port,
            expected_host_key_fingerprint=(
                initial_attestation.ssh_host_key_fingerprint
            ),
            mutation_authority=mutation_authority,
        )
        try:
            from orchestrator.services.snapshot_service import snapshot_service

            if seeded and row.get("user_id") and snapshot_service.is_available:
                from orchestrator.services.ide_profile_store import IdeProfileStore

                user_id = str(row["user_id"])
                seeded = await seed_ide_profile(
                    user_id=user_id,
                    ssh_host=initial_attestation.host,
                    ssh_port=initial_attestation.port,
                    profile_store=IdeProfileStore(
                        snapshot_service._s3,
                        snapshot_service._bucket,
                    ),
                    ext_items=await IdeSettingsStore(self._db).get_extensions(user_id),
                    expected_host_key_fingerprint=(
                        initial_attestation.ssh_host_key_fingerprint
                    ),
                    mutation_authority=mutation_authority,
                )
        except Exception:
            logger.exception(
                "IDE profile seed failed for %s %s", entity_type, entity_id
            )
            seeded = False

        try:
            final_attestation = await self._provisioner.attest_workspace_runtime(
                entity_id,
                entity_type=entity_type,
            )
        except Exception:
            final_attestation = None
        if (
            not seeded
            or final_attestation is None
            or final_attestation != initial_attestation
        ):
            await self._transient_failure(
                key,
                entity_type,
                entity_id,
                generation,
                vm,
                "VM IDE seed authority changed or seeding failed",
                reprobe=False,
            )
            return

        ready_updates = {
            "status": "ready",
            "ssh_host": final_attestation.host,
            "pod_ip": final_attestation.pod_ip,
            "ssh_port": final_attestation.port,
            "active_pod_uid": final_attestation.runtime_incarnation,
            "ssh_ready_source": "provisioner_probe",
            "ssh_verified_at": verified_at,
            "ssh_registration_id": registration_id,
            "ssh_probe_error": None,
            "recovering": False,
        }
        if network_profile_evidence is not None:
            ready_updates["network_profile_evidence"] = network_profile_evidence
        if (
            entity_type == "job"
            and getattr(self._db, "supports_vm_phase_observations", False) is True
        ):
            from orchestrator.services.vm_provisioning_phases import (
                VMProvisioningPhaseStore,
            )

            ready_updates["ssh_host_key_fingerprint"] = (
                final_attestation.ssh_host_key_fingerprint
            )
            promoted = await VMProvisioningPhaseStore(self._db).publish_ready(
                entity_id,
                generation,
                registration_id,
                status.get("vm_uid"),
                ready_updates,
            )
        else:
            promote = (
                self._db.merge_thread_vm_context_if_current
                if entity_type == "thread"
                else self._db.merge_vm_context_if_current
            )
            promoted = bool(await promote(entity_id, registration_id, ready_updates))
        if not promoted:
            return
        self._failures.pop(key, None)
        self._retry_after.pop(key, None)
        if entity_type == "job":
            self._trigger_dispatch()

    async def _recovery_owns_authority(self, entity_type: str, entity_id: str) -> bool:
        check = getattr(self._db, "vm_workspace_recovery_owns_authority", None)
        if not callable(check):
            return False
        try:
            return bool(await check(entity_type, entity_id))
        except Exception:
            # Losing the authority read is not permission to mutate a runtime.
            logger.exception(
                "Failed to read VM workspace recovery authority for %s %s",
                entity_type,
                entity_id,
            )
            return True

    async def _transient_failure(
        self,
        key: tuple[str, str, str],
        entity_type: str,
        entity_id: str,
        generation: str,
        vm: Mapping[str, Any],
        error: str,
        *,
        reprobe: bool,
    ) -> None:
        failures = self._failures.get(key, 0) + 1
        self._failures[key] = failures
        self._retry_after[key] = time.monotonic() + min(
            60.0, 3.0 * (2 ** (failures - 1))
        )
        await self._provisioner._set_context_if_generation(
            entity_type,
            entity_id,
            generation,
            {
                "status": "ssh_pending",
                "ssh_probe_error": error[:500],
                "ssh_probe_attempts": int(vm.get("ssh_probe_attempts") or 0) + 1,
            },
            require_status_not_ready=not reprobe,
        )


async def vm_readiness_prober(
    shutdown_event: asyncio.Event,
    *,
    db: Any,
    provisioner: Any,
    trigger_dispatch: Callable[[], None],
) -> None:
    if os.getenv("VM_MODE", "off").strip().lower() != "same-cluster":
        return
    from orchestrator.services.vm_preparation import cancellation_loop

    await asyncio.gather(
        VMReadinessService(db, provisioner, trigger_dispatch=trigger_dispatch).run(
            shutdown_event
        ),
        cancellation_loop(shutdown_event, db=db, provisioner=provisioner),
    )
