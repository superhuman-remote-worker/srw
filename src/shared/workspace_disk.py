"""Workspace disk-full wire contract, shared by the agent and the orchestrator.

The stateless shell keeps its ownership record on the workspace volume. When
the volume fills, the record write fails and the shell cannot start. The agent
reports that as a ``workspace_unavailable`` error carrying ``reason:
workspace_disk_full`` and the volume's usage. Every existing recovery route
keeps holding such a report; an orchestrator that knows the reason grows the
volume once and resumes, or pauses with a reason that names the full disk
instead of a lost transport.

knowledge-base/knowledge/issues/workspace_disk_full_pauses_job_as_transport_lost.md

Stdlib-only: the agent and the orchestrator both import it.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
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
    """The pause (or resume) reason a person reads for a full workspace disk."""
    usage = f" ({report.usage.describe()})" if report.usage is not None else ""
    size = (
        format_bytes(growth.size_bytes)
        if growth is not None and growth.size_bytes is not None
        else "its growth limit"
    )
    if growth is not None and growth.outcome == "resized":
        grown = f"workspace disk was full{usage}; grew the volume to {size}; "
        if resumed:
            return grown + "workspace and checkpoints retained; the job resumes"
        return (
            grown + "workspace and checkpoints retained; explicit Resume required "
            "because a command may have been running"
        )
    if growth is not None and growth.outcome == "pending":
        return (
            f"workspace disk is full{usage}; growing the volume to {size} has not "
            "finished yet; workspace and checkpoints retained; Resume once it has"
        )
    note = ""
    if growth is not None and growth.outcome == "used":
        note = f"; the volume was already grown once, to {size}"
    elif growth is not None and growth.outcome == "refused":
        note = "; growing the volume failed" + (
            f": {growth.detail[:_MAX_DETAIL_CHARS]}" if growth.detail else ""
        )
    return (
        f"workspace disk is full{usage}{note}; workspace and checkpoints retained; "
        "free space or grow the volume, then Resume"
    )
