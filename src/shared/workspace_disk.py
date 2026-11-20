"""Workspace disk-full wire contract, shared by the agent and the orchestrator.

The stateless shell keeps its ownership record on the workspace volume. When
the volume fills, the record write fails and the shell cannot start. The agent
reports that as a ``workspace_unavailable`` error carrying ``reason:
workspace_disk_full`` and the volume's usage. Every existing recovery route
keeps holding such a report; an orchestrator that knows the reason grows the
volume once and resumes, or pauses with a reason that names the full disk
instead of a lost transport.

knowledge-base/knowledge/issues/workspace_disk_full_pauses_job_as_transport_lost.md

It also owns the clause every workspace hold ends with: what was kept and
what is gone (``workspace_kept_clause``), and the hold for a workspace that
ran out of memory: the termination record the orchestrator reads from the
dead Pod, the strict classifier over it, the one-time larger memory limit and
the wording (``WorkspaceTermination``, ``WorkspaceMemoryKill``).
knowledge-base/knowledge/issues/workspace_out_of_memory_is_reported_as_transport_lost.md

Stdlib-only: the agent and the orchestrator both import it.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal

WORKSPACE_DISK_FULL_REASON = "workspace_disk_full"

# The ownership record is one short line, so its write fails only at the
# volume's last block. The margin covers what other processes free or use
# between that write and the ``df`` that measures it.
_FULL_FREE_BYTES = 64 * 1024 * 1024
_FULL_FREE_INODES = 64
_MAX_COUNT = 2**63 - 1
_MAX_DETAIL_CHARS = 200

GrowthOutcome = Literal["resized", "pending", "refused", "used", "unsupported"]
_GROWTH_OUTCOMES = frozenset({"resized", "pending", "refused", "used", "unsupported"})


def format_bytes(value: int) -> str:
    """``9.8 GiB``, ``512 MiB``, ``0 B``: binary units, one decimal at most."""
    for unit, scale in (("GiB", 1 << 30), ("MiB", 1 << 20), ("KiB", 1 << 10)):
        if value >= scale:
            return f"{value / scale:.1f}".removesuffix(".0") + f" {unit}"
    return f"{value} B"


def _count(value: Any) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value if 0 <= value <= _MAX_COUNT else None


@dataclass(frozen=True, slots=True)
class WorkspaceDiskUsage:
    """One volume's block and inode usage, as the workspace's ``df`` saw it."""

    size_bytes: int
    used_bytes: int
    available_bytes: int
    inodes_total: int | None = None
    inodes_free: int | None = None

    @classmethod
    def from_df(cls, fields: Sequence[str]) -> WorkspaceDiskUsage | None:
        """Parse ``df -Pk`` size/used/available, then ``df -Pi`` total/used/free.

        The inode triple is optional: some filesystems report none.
        """
        if len(fields) not in (3, 6) or not all(f.isdigit() for f in fields):
            return None
        size, used, available = (int(f) * 1024 for f in fields[:3])
        inodes_total = inodes_free = None
        if len(fields) == 6 and int(fields[3]) > 0:
            inodes_total, inodes_free = int(fields[3]), int(fields[5])
        return cls.from_dict(
            {
                "size_bytes": size,
                "used_bytes": used,
                "available_bytes": available,
                "inodes_total": inodes_total,
                "inodes_free": inodes_free,
            }
        )

    @classmethod
    def from_dict(cls, value: Any) -> WorkspaceDiskUsage | None:
        """Validate a reported usage; the numbers come from the workspace."""
        if not isinstance(value, Mapping):
            return None
        size = _count(value.get("size_bytes"))
        used = _count(value.get("used_bytes"))
        available = _count(value.get("available_bytes"))
        if size is None or used is None or available is None:
            return None
        inodes_total = _count(value.get("inodes_total"))
        inodes_free = _count(value.get("inodes_free"))
        if inodes_total is None or inodes_free is None or inodes_free > inodes_total:
            inodes_total = inodes_free = None
        return cls(size, used, available, inodes_total, inodes_free)

    @property
    def inodes_exhausted(self) -> bool:
        return self.inodes_free is not None and self.inodes_free < _FULL_FREE_INODES

    @property
    def full(self) -> bool:
        return self.available_bytes < _FULL_FREE_BYTES or self.inodes_exhausted

    def as_dict(self) -> dict[str, int]:
        value = {
            "size_bytes": self.size_bytes,
            "used_bytes": self.used_bytes,
            "available_bytes": self.available_bytes,
        }
        if self.inodes_total is not None and self.inodes_free is not None:
            value["inodes_total"] = self.inodes_total
            value["inodes_free"] = self.inodes_free
        return value

    def describe(self) -> str:
        """``9.3 GiB of 9.8 GiB used, 0 B free``, plus inodes when those ran out."""
        blocks = (
            f"{format_bytes(self.used_bytes)} of {format_bytes(self.size_bytes)} "
            f"used, {format_bytes(self.available_bytes)} free"
        )
        if self.inodes_exhausted and self.inodes_total is not None:
            assert self.inodes_free is not None
            return (
                f"{self.inodes_total - self.inodes_free:,} of "
                f"{self.inodes_total:,} files used; {blocks}"
            )
        return blocks


@dataclass(frozen=True, slots=True)
class WorkspaceDiskFullReport:
    """The disk-full detail of a worker's ``workspace_unavailable`` report.

    ``command_in_flight`` is False only when the shell failed while starting,
    before it accepted any command. Anything else keeps the explicit-Resume
    hold, because an interrupted command's outcome is unknown.
    """

    usage: WorkspaceDiskUsage | None
    command_in_flight: bool = True

    @classmethod
    def from_error(cls, error: Any) -> WorkspaceDiskFullReport | None:
        if (
            not isinstance(error, Mapping)
            or error.get("type") != "workspace_unavailable"
            or error.get("reason") != WORKSPACE_DISK_FULL_REASON
        ):
            return None
        return cls(
            usage=WorkspaceDiskUsage.from_dict(error.get("disk")),
            command_in_flight=error.get("command_in_flight") is not False,
        )

    def error_fields(self) -> dict[str, Any]:
        """The keys a completion report's ``error`` dict adds for this report."""
        return {
            "reason": WORKSPACE_DISK_FULL_REASON,
            "disk": self.usage.as_dict() if self.usage is not None else None,
            "command_in_flight": self.command_in_flight,
        }


@dataclass(frozen=True, slots=True)
class WorkspaceVolumeGrowth:
    """What the orchestrator did about a full volume.

    ``resized``: the volume was grown and its filesystem shows the new size.
    ``pending``: the volume was grown; the filesystem has not caught up yet.
    ``refused``: the cluster refused to grow it. ``used``: it was grown once
    already. ``unsupported``: growth is off, or there is no volume to grow.
    """

    outcome: GrowthOutcome
    size_bytes: int | None = None
    detail: str | None = None

    def __post_init__(self) -> None:
        if self.outcome not in _GROWTH_OUTCOMES:
            raise ValueError(f"unknown volume growth outcome {self.outcome!r}")

    @property
    def grown(self) -> bool:
        return self.outcome in {"resized", "pending"}

    def as_dict(self) -> dict[str, Any]:
        value: dict[str, Any] = {"outcome": self.outcome}
        if self.size_bytes is not None:
            value["size_bytes"] = self.size_bytes
        if self.detail:
            value["detail"] = self.detail[:_MAX_DETAIL_CHARS]
        return value


def workspace_disk_full_action(
    report: WorkspaceDiskFullReport,
    growth: WorkspaceVolumeGrowth | None = None,
    *,
    resumed: bool = False,
) -> str:
    """The pause (or resume) reason a person reads for a full workspace disk.

    The worker measured the volume through a live transport, so the workspace
    is still up. The action comes before any detail so that a list's cut keeps it.
    """
    usage = f" ({report.usage.describe()})" if report.usage is not None else ""
    size = (
        format_bytes(growth.size_bytes)
        if growth is not None and growth.size_bytes is not None
        else "its growth limit"
    )
    kept = " " + workspace_kept_clause(runtime_replaced=False, volume_kept=None)
    if growth is not None and growth.outcome == "resized":
        grown = f"workspace disk was full{usage}; grew the volume to {size}; "
        if resumed:
            return grown + "the job resumes." + kept
        return (
            grown + "explicit Resume required because a command may have been "
            "running." + kept
        )
    if growth is not None and growth.outcome == "pending":
        return (
            f"workspace disk is full{usage}; growing the volume to {size} has not "
            "finished yet; Resume once it has." + kept
        )
    note = ""
    if growth is not None and growth.outcome == "used":
        note = f"; the volume was already grown once, to {size}"
    elif growth is not None and growth.outcome == "refused":
        note = "; growing the volume failed" + (
            f": {growth.detail[:_MAX_DETAIL_CHARS]}" if growth.detail else ""
        )
    return (
        f"workspace disk is full{usage}; free space or grow the volume, then "
        f"Resume{note}." + kept
    )


# --- What a workspace hold keeps ------------------------------------------
#
# Every hold for a lost or full workspace says what was kept and what is gone.
# Two facts recorded on the hold's ``attention`` decide it: whether the
# workspace's runtime (its Pod) is gone, and whether its home directory is on
# a persistent volume. One clause per state, so no producer or reader words
# it on its own.
# knowledge-base/knowledge/issues/workspace_loss_pause_text_and_liveness_time_mislead.md

WORKSPACE_HOME = "/home/agent-host"
_SAVED_PROGRESS = "The job's saved progress is kept."
RESUME_REQUIRED = "explicit Resume required"
# A dead-Pod hold on a workspace without a persistent volume: Resume demands
# the retained volume the recovery cleanup recorded, finds none and answers
# 409 every time, so the job can only be cancelled.
# knowledge-base/knowledge/issues/dead_pod_hold_without_volume_refuses_every_resume.md
RESUME_REFUSED_WITHOUT_VOLUME = "Resume cannot continue this job, so Cancel it"


def dead_workspace_resume_gate(*, recovery_cleanup: bool, volume_kept: Any) -> str:
    """What a dead-Pod hold asks for: an explicit Resume, unless its recovery
    cleanup left no volume for that Resume to reattach."""
    if recovery_cleanup and volume_kept is False:
        return RESUME_REFUSED_WITHOUT_VOLUME
    return RESUME_REQUIRED


def volume_kept_from_backing(backing_id: Any) -> bool | None:
    """Whether a Kubernetes workspace's home directory outlives its Pod.

    ``k8s-pvc:`` is a persistent volume; ``k8s-pod:`` is an ``emptyDir`` that
    dies with the Pod. Anything else is not known.
    """
    if not isinstance(backing_id, str):
        return None
    if backing_id.startswith("k8s-pvc:"):
        return True
    if backing_id.startswith("k8s-pod:"):
        return False
    return None


def workspace_hold_facts(
    *, runtime_replaced: bool | None, binding: Any
) -> dict[str, bool | None]:
    """The two facts a producer adds to a hold's ``attention``, the volume
    one from the job's ``_workspace_binding``."""
    backing_id = binding.get("backing_id") if isinstance(binding, Mapping) else None
    return {
        "runtime_replaced": runtime_replaced,
        "volume_kept": volume_kept_from_backing(backing_id),
    }


def _fact(value: Any) -> bool | None:
    return value if isinstance(value, bool) else None


def workspace_kept_clause(
    *, runtime_replaced: bool | None, volume_kept: bool | None
) -> str:
    """What a hold kept and what is gone, as whole sentences.

    ``runtime_replaced``: True when the Pod is gone (deleted by recovery or
    already retired), False when it is known to run, None when SRW removed
    nothing but did not confirm it still runs. ``volume_kept``: whether
    ``/home/agent-host`` is on a persistent volume; None when not known.
    """
    runtime_replaced = _fact(runtime_replaced)
    volume_kept = _fact(volume_kept)
    if runtime_replaced is False:
        return (
            "The workspace is still up and nothing was removed; its processes and "
            "/tmp last until it is released."
        )
    if runtime_replaced is True:
        if volume_kept is True:
            return (
                f"Files under {WORKSPACE_HOME} are kept; running processes and "
                f"other paths such as /tmp are gone. {_SAVED_PROGRESS}"
            )
        if volume_kept is False:
            return (
                "The workspace had no persistent volume, so its files went with "
                "it; what was pushed to the job repository and the job's saved "
                "progress are kept."
            )
        return (
            "Running processes are gone, and so are files not on a persistent "
            f"volume. {_SAVED_PROGRESS}"
        )
    if volume_kept is True:
        return (
            f"Nothing was removed and files under {WORKSPACE_HOME} are kept; "
            "whether the workspace's processes and /tmp survived is not known. "
            + _SAVED_PROGRESS
        )
    if volume_kept is False:
        return (
            "Nothing was removed, but whether the workspace still runs is not "
            "known; it has no persistent volume, so its files last only while it "
            f"runs. {_SAVED_PROGRESS}"
        )
    return (
        "Nothing was removed, but whether the workspace still runs is not known. "
        + _SAVED_PROGRESS
    )


def recorded_workspace_kept_clause(attention: Any) -> str:
    """The clause for a stored hold; one written before the facts says only
    what is always true."""
    if not isinstance(attention, Mapping) or "runtime_replaced" not in attention:
        return _SAVED_PROGRESS
    return workspace_kept_clause(
        runtime_replaced=attention.get("runtime_replaced"),
        volume_kept=attention.get("volume_kept"),
    )


# --- A workspace that ran out of memory -----------------------------------
#
# A process over the workspace container's memory limit gets every process in
# the container killed (cgroup v2 group kill), sshd included, and the Pod ends
# ``Failed``. The kubelet records ``OOMKilled`` on the container; nothing else
# does. The orchestrator reads that record from the dead Pod during its
# cleanup and keeps it on the job row (``workspace_container.last_termination``),
# so a hold can name the cause. The cause is stated only on that evidence: a
# bare exit 137 is also what a liveness kill or a lost runtime event leaves.
# knowledge-base/knowledge/issues/workspace_out_of_memory_is_reported_as_transport_lost.md

WORKSPACE_OUT_OF_MEMORY_REASON = "workspace_out_of_memory"
MORE_MEMORY_HINT = (
    "To run it with more memory, start a new job on a workspace template with "
    "more memory."
)
_MAX_RECORD_TEXT = 300

TerminationCause = Literal["out_of_memory", "evicted", "not_recorded"]
MemoryGrowthOutcome = Literal["granted", "used", "refused", "capped", "off"]
_MEMORY_GROWTH_OUTCOMES = frozenset({"granted", "used", "refused", "capped", "off"})


def _text(value: Any) -> str | None:
    if not isinstance(value, str) or not value:
        return None
    return value[:_MAX_RECORD_TEXT]


def _exit_code(value: Any) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value if -(2**31) <= value < 2**31 else None


def _instant(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else None


@dataclass(frozen=True, slots=True)
class WorkspaceTermination:
    """How a job's ``workspace`` container ended, read from its exact Pod.

    ``pod_uid`` ties the record to one runtime incarnation: a reader uses it
    only for the runtime it holds. ``deletion_timestamp`` is set when the Pod
    was already being deleted at the read, so a container SRW stopped is never
    taken for one that died on its own.
    """

    pod_uid: str
    container_reason: str | None = None
    exit_code: int | None = None
    finished_at: str | None = None
    pod_reason: str | None = None
    pod_message: str | None = None
    deletion_timestamp: str | None = None
    memory_limit_bytes: int | None = None

    @classmethod
    def from_dict(cls, value: Any) -> WorkspaceTermination | None:
        """Validate a stored record; anything malformed counts as absent."""
        if not isinstance(value, Mapping):
            return None
        pod_uid = _text(value.get("pod_uid"))
        if pod_uid is None:
            return None
        return cls(
            pod_uid=pod_uid,
            container_reason=_text(value.get("container_reason")),
            exit_code=_exit_code(value.get("exit_code")),
            finished_at=_text(value.get("finished_at")),
            pod_reason=_text(value.get("pod_reason")),
            pod_message=_text(value.get("pod_message")),
            deletion_timestamp=_text(value.get("deletion_timestamp")),
            memory_limit_bytes=_count(value.get("memory_limit_bytes")) or None,
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "pod_uid": self.pod_uid,
            "container_reason": self.container_reason,
            "exit_code": self.exit_code,
            "finished_at": self.finished_at,
            "pod_reason": self.pod_reason,
            "pod_message": self.pod_message,
            "deletion_timestamp": self.deletion_timestamp,
            "memory_limit_bytes": self.memory_limit_bytes,
        }

    def _finished_before_deletion(self) -> bool:
        if self.deletion_timestamp is None:
            return True
        finished = _instant(self.finished_at)
        deleted = _instant(self.deletion_timestamp)
        return finished is not None and deleted is not None and finished < deleted

    @property
    def cause(self) -> TerminationCause:
        """The strict classifier: a cause only on the kubelet's own record.

        Out of memory only for ``OOMKilled`` on a container that stopped before
        any deletion of its Pod. A Pod-level ``Evicted`` is its own cause.
        ``Error``/137, ``ContainerStatusUnknown``, ``Unknown``/255 and the
        rest are "not recorded"; the raw reason and exit code stay on the
        record.
        """
        if self.pod_reason == "Evicted":
            return "evicted"
        if self.container_reason == "OOMKilled" and self._finished_before_deletion():
            return "out_of_memory"
        return "not_recorded"


def recorded_termination(workspace: Any, runtime: Any) -> WorkspaceTermination | None:
    """The job row's termination record, only for the runtime ``runtime``."""
    if not isinstance(workspace, Mapping) or not isinstance(runtime, str):
        return None
    record = WorkspaceTermination.from_dict(workspace.get("last_termination"))
    return record if record is not None and record.pod_uid == runtime else None


def byte_quantity(value: int) -> str:
    """A Kubernetes quantity for ``value`` bytes: ``8Gi``, ``768Mi`` or bytes."""
    for suffix, scale in (("Gi", 1 << 30), ("Mi", 1 << 20), ("Ki", 1 << 10)):
        if value >= scale and value % scale == 0:
            return f"{value // scale}{suffix}"
    return str(value)


@dataclass(frozen=True, slots=True)
class WorkspaceMemoryGrowth:
    """What the next Resume does to a memory-killed job's limit.

    ``granted``: it runs with ``limit_bytes``, twice the killed limit or the
    installation's cap. ``used``: the limit was already raised once, to
    ``limit_bytes``. ``refused``: the cluster refused a Pod with the raised
    limit ``limit_bytes``, so the job is back on its own limit. ``capped``:
    the killed limit is at or above the cap.
    ``off``: growth is switched off, or the killed limit is not known.
    """

    outcome: MemoryGrowthOutcome
    limit_bytes: int | None = None

    def __post_init__(self) -> None:
        if self.outcome not in _MEMORY_GROWTH_OUTCOMES:
            raise ValueError(f"unknown memory growth outcome {self.outcome!r}")

    def as_dict(self) -> dict[str, Any]:
        value: dict[str, Any] = {"outcome": self.outcome}
        if self.limit_bytes is not None:
            value["limit_bytes"] = self.limit_bytes
        return value

    @classmethod
    def from_dict(cls, value: Any) -> WorkspaceMemoryGrowth:
        if not isinstance(value, Mapping) or value.get("outcome") not in (
            _MEMORY_GROWTH_OUTCOMES
        ):
            return cls("off")
        return cls(value["outcome"], _count(value.get("limit_bytes")) or None)


def memory_growth_record(value: Any) -> dict[str, Any] | None:
    """The job row's ``workspace_container.memory_growth``, validated."""
    if not isinstance(value, Mapping):
        return None
    limit = _count(value.get("limit_bytes"))
    runtime = _text(value.get("runtime_incarnation"))
    if not limit or runtime is None:
        return None
    return dict(value)


def decide_memory_growth(
    *,
    limit_bytes: int | None,
    previous: Any,
    cap_bytes: int | None,
    runtime_incarnation: str,
) -> WorkspaceMemoryGrowth:
    """Double the killed limit once per job, never above ``cap_bytes``.

    ``previous`` is the job row's ``memory_growth``. A record from this same
    runtime is this kill's own growth (a replay or a second writer), so it is
    granted again; one from an earlier runtime means the job already ran with
    the larger limit. A growth the cluster refused stays spent. ``cap_bytes``
    None means growth is off.
    """
    record = memory_growth_record(previous)
    if record is not None:
        outcome: MemoryGrowthOutcome = (
            "refused"
            if record.get("refused_at")
            else "granted"
            if record["runtime_incarnation"] == runtime_incarnation
            else "used"
        )
        return WorkspaceMemoryGrowth(outcome, int(record["limit_bytes"]))
    if cap_bytes is None or not limit_bytes:
        return WorkspaceMemoryGrowth("off")
    grown = min(2 * limit_bytes, cap_bytes)
    if grown <= limit_bytes:
        return WorkspaceMemoryGrowth("capped")
    return WorkspaceMemoryGrowth("granted", grown)


def new_memory_growth_record(
    growth: WorkspaceMemoryGrowth,
    *,
    from_bytes: int | None,
    runtime_incarnation: str,
    at: str,
    previous: Any,
) -> dict[str, Any] | None:
    """The ``memory_growth`` record a hold writes, or None when it writes none.

    Only a granted growth not already recorded for this runtime writes one.
    The Pod creator reads ``limit`` from it on top of the frozen sizes.
    """
    if (
        growth.outcome != "granted"
        or growth.limit_bytes is None
        or memory_growth_record(previous) is not None
    ):
        return None
    return {
        "limit": byte_quantity(growth.limit_bytes),
        "limit_bytes": growth.limit_bytes,
        "from_bytes": from_bytes,
        "runtime_incarnation": runtime_incarnation,
        "at": at,
    }


@dataclass(frozen=True, slots=True)
class WorkspaceMemoryKill:
    """A hold's out-of-memory facts: the killed limit and what Resume does."""

    limit_bytes: int | None
    growth: WorkspaceMemoryGrowth
    exit_code: int | None = None
    container_reason: str | None = None

    @classmethod
    def from_termination(
        cls, termination: WorkspaceTermination, growth: WorkspaceMemoryGrowth
    ) -> WorkspaceMemoryKill:
        return cls(
            limit_bytes=termination.memory_limit_bytes,
            growth=growth,
            exit_code=termination.exit_code,
            container_reason=termination.container_reason,
        )

    @classmethod
    def from_attention(cls, attention: Any) -> WorkspaceMemoryKill | None:
        """Read a stored hold; None unless it recorded a memory kill."""
        if (
            not isinstance(attention, Mapping)
            or attention.get("reason") != WORKSPACE_OUT_OF_MEMORY_REASON
        ):
            return None
        memory = attention.get("memory")
        if not isinstance(memory, Mapping):
            memory = {}
        return cls(
            limit_bytes=_count(memory.get("limit_bytes")) or None,
            growth=WorkspaceMemoryGrowth.from_dict(memory.get("growth")),
            exit_code=_exit_code(memory.get("exit_code")),
            container_reason=_text(memory.get("container_reason")),
        )

    def attention(self) -> dict[str, Any]:
        """The ``attention.memory`` value a hold stores."""
        return {
            "limit_bytes": self.limit_bytes,
            "exit_code": self.exit_code,
            "container_reason": self.container_reason,
            "growth": self.growth.as_dict(),
        }

    @property
    def cause(self) -> str:
        """``workspace ran out of memory (limit 4 GiB)``, with ``again`` after
        a raised limit."""
        again = self.growth.outcome == "used"
        details = []
        if self.limit_bytes is not None:
            details.append(f"limit {format_bytes(self.limit_bytes)}")
        if again:
            details.append("already raised once")
        detail = f" ({', '.join(details)})" if details else ""
        return f"workspace ran out of memory{' again' if again else ''}{detail}"

    def next_step(self, gate: str) -> str:
        """What Resume will do, or the hint when it cannot give more memory."""
        if (
            gate == RESUME_REQUIRED
            and self.growth.outcome == "granted"
            and self.growth.limit_bytes is not None
        ):
            return (
                "Resume raises the job's memory limit to "
                f"{format_bytes(self.growth.limit_bytes)}."
            )
        return MORE_MEMORY_HINT

    def action(self, gate: str) -> str:
        """The hold's text before the kept clause; the action comes first so
        a list's cut keeps it."""
        return (
            f"{self.cause} and was stopped; interrupted command outcome is unknown; "
            f"{gate}. {self.next_step(gate)}"
        )


def workspace_memory_attention(kill: WorkspaceMemoryKill) -> dict[str, Any]:
    """The keys an out-of-memory hold adds to its ``attention``."""
    return {"reason": WORKSPACE_OUT_OF_MEMORY_REASON, "memory": kill.attention()}


def workspace_hold_resume_reason(hold: Any) -> str | None:
    """The ``[FEEDBACK_RESUME]`` cause for lifting a workspace hold.

    Worded for the resumed agent, from the hold being lifted: why the job
    stopped and which files a new workspace still has. None for any other
    hold, which keeps the caller's own wording.
    """
    if not isinstance(hold, Mapping):
        return None
    source = hold.get("source")
    attention = hold.get("attention")
    kill = WorkspaceMemoryKill.from_attention(attention)
    if source == "workspace_recovery_unavailable":
        if kill is not None:
            cause = (
                f"This job's {kill.cause} and was stopped; the outcome of the "
                "command that was running is unknown."
            )
            if kill.growth.limit_bytes is not None and kill.growth.outcome in {
                "granted",
                "used",
            }:
                cause += (
                    f" Its memory limit is now {format_bytes(kill.growth.limit_bytes)}."
                )
            elif kill.limit_bytes is not None:
                cause += f" Its memory limit stays {format_bytes(kill.limit_bytes)}."
        else:
            cause = (
                "This job's workspace stopped answering; the outcome of the "
                "command that was running is unknown."
            )
    elif source == "workspace_recovery_exhausted":
        cause = "This job's workspace could not be recovered after repeated attempts."
    elif source == "workspace_disk_full":
        cause = "This job's workspace disk was full."
    else:
        return None
    return (
        f"{cause} {recorded_workspace_kept_clause(attention)} "
        "An operator resumed it with the feedback below."
    )
