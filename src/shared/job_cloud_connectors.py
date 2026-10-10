"""``context.cloud_connectors``: what a job is told about its main-cloud connectors.

The orchestrator stamps the list when it decides a job's main-cloud delivery
(admission, or provisioning for work that is not admitted through it); the
agent renders it into the task brief (``agent.core.deliverables``). A job
was never told that ``projects/<slug>/`` is a copy of the project's cloud
folder, or what happens to its edits there, so 432 jobs on dev produced no
write-back at all (main_cloud_as_connectors.md, "Mode A and outbox
re-check").

Two kinds of entry:

* ``cloud_folder_checkout``: the project's cloud folder, copied into the
  job's repository at ``path`` when it starts. ``edits`` says what happens
  to changes under it (:data:`EDITS`), ``access`` the connector's level when
  the copy comes from one (none for a loop's or an older job's copy). How
  much was copied is the seed's record, ``context.cloud_baseline``.
* ``cloud_outbox``: when the job completes, SRW copies its declared
  deliverables into a cloud folder shared with the job's owner.

Standard library only: both the orchestrator and the agent import it.
"""

from __future__ import annotations

import json
from typing import Any

CLOUD_CONNECTORS_CONTEXT_KEY = "cloud_connectors"
#: The project's cloud folder, checked out into the job's repository.
CHECKOUT_KIND = "cloud_folder_checkout"
#: A folder SRW fills with the job's deliverables when it completes.
OUTBOX_KIND = "cloud_outbox"
KINDS: tuple[str, ...] = (CHECKOUT_KIND, OUTBOX_KIND)

#: SRW never applies the copy's changes (the checkout's ``read_only``).
EDITS_DISCARDED = "discarded"
#: Changes become a diff the job's owner reviews; only an accepted diff is
#: written back (the checkout's ``reviewed_write_back``, and every older
#: human-reviewed job).
EDITS_REVIEWED = "reviewed"
#: A conflict-free diff is applied automatically; anything else parks the
#: job for review (a project loop's copy).
EDITS_APPLIED = "applied"
EDITS: tuple[str, ...] = (EDITS_DISCARDED, EDITS_REVIEWED, EDITS_APPLIED)


def checkout_path(project_slug: str) -> str:
    """The copy's workspace-relative directory for a project slug."""
    return f"projects/{project_slug}/"


def checkout_entry(
    *,
    project_slug: str,
    folder: str,
    edits: str,
    access: str | None = None,
    datasource_id: str | None = None,
) -> dict[str, Any]:
    """One ``cloud_folder_checkout`` entry. ``folder`` is the cloud folder's
    name as the job's owner sees it (the project's)."""
    if edits not in EDITS:
        raise ValueError(f"unknown checkout edits {edits!r}")
    entry: dict[str, Any] = {
        "kind": CHECKOUT_KIND,
        "path": checkout_path(project_slug),
        "folder": folder,
        "edits": edits,
    }
    if access:
        entry["access"] = access
    if datasource_id:
        entry["datasource_id"] = datasource_id
    return entry


def outbox_entry(*, datasource_id: str | None = None) -> dict[str, Any]:
    """The ``cloud_outbox`` entry of a job whose deliverables SRW copies
    into a shared folder at completion."""
    entry: dict[str, Any] = {"kind": OUTBOX_KIND}
    if datasource_id:
        entry["datasource_id"] = datasource_id
    return entry


def parse_cloud_connectors(source: Any) -> list[dict[str, Any]]:
    """The well-formed entries of a ``cloud_connectors`` value.

    Accepts the list itself, or a context (or metadata) mapping that holds
    it, as a dict or its JSON text. Entries of an unknown kind, and a
    checkout entry without a path or with unknown ``edits``, are dropped:
    the agent is told nothing rather than something wrong.
    """
    value = source
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (TypeError, ValueError):
            return []
    if isinstance(value, dict):
        value = value.get(CLOUD_CONNECTORS_CONTEXT_KEY)
    if not isinstance(value, list):
        return []
    entries: list[dict[str, Any]] = []
    for item in value:
        if not isinstance(item, dict) or item.get("kind") not in KINDS:
            continue
        if item["kind"] == CHECKOUT_KIND and (
            not isinstance(item.get("path"), str)
            or not item["path"]
            or item.get("edits") not in EDITS
        ):
            continue
        entries.append(dict(item))
    return entries


def connector_entry(source: Any, kind: str) -> dict[str, Any] | None:
    """The first entry of ``kind``, or ``None``."""
    return next(
        (entry for entry in parse_cloud_connectors(source) if entry["kind"] == kind),
        None,
    )


__all__ = [
    "CHECKOUT_KIND",
    "CLOUD_CONNECTORS_CONTEXT_KEY",
    "EDITS",
    "EDITS_APPLIED",
    "EDITS_DISCARDED",
    "EDITS_REVIEWED",
    "KINDS",
    "OUTBOX_KIND",
    "checkout_entry",
    "checkout_path",
    "connector_entry",
    "outbox_entry",
    "parse_cloud_connectors",
]
