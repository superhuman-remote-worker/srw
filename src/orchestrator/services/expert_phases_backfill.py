"""Keep what existed before the ``phases`` setting running as it did, once.

Before the setting (``shared.job_phases``) every job planned in phases. With
it a job does not, unless its expert says so. So that nothing an operator set
up changes by itself, the first orchestrator start after the upgrade writes
``phases: true`` on every stored Expert manifest and every automation that
does not state the setting. The operator then turns it off where it is not
wanted.

Left alone, so that they follow the new default like a fresh installation:
the shared catalogue's copies of the experts this release ships without
phases (:data:`SHIPPED_WITHOUT_PHASES`), and a Project's frozen copy of one
(inline, with its managed Expert child). A copy a user made of one of them is
that user's own expert and is marked like any other.

An installation that has never created a job has nothing from before the
setting: the pass marks nothing there and only records that it ran.

Two limits, accepted: a Project's frozen copy is recognised by its
``asset_name``, so a Project's copy of a user's own copy of General Worker is
left alone like a copy of the catalogue's, although the user's expert itself
is marked; and an installation whose jobs were all deleted counts as one that
never created a job.

A stored Expert is a versioned manifest document (``srw_resources``), and a
Project keeps a frozen copy of each of its Experts inline plus one managed
Expert child per copy. Each mark is therefore a new revision saved through the
store, with the machinery of ``manifest_phase_prompt_retirement``: Projects
first, so a managed child is rewritten together with the Project that pins its
revision; a resource whose stored revision is not the digest of its content is
skipped and logged; earlier revisions and admitted executions are history and
keep what they had.

The pass runs once per installation. A ``system_settings`` row records it, so
an Expert or automation created afterwards without the key keeps the default.
A start that fails before the row is written runs the pass again; what it
marked the first time already states the setting and is not touched twice.
"""

from __future__ import annotations

from functools import partial
import json
import logging
from typing import Any

from orchestrator.services.manifest_phase_prompt_retirement import (
    _Skip,
    _retire_expert,
    _retire_project,
)
from orchestrator.services.manifest_store import ManifestStore
from shared.job_phases import PHASES_KEY, is_phases_value
from shared.runtime.core.srw_manifest_config import SRW_HARNESS_ADAPTER

logger = logging.getLogger(__name__)

#: The ``system_settings`` row that records the pass.
MARKER_KEY = "jobs.phases_backfill"
_MARKER_ACTOR = "startup:phases-backfill"

#: Bundled experts this release ships without ``phases``. Their copies in the
#: shared catalogue are not marked: they follow the default, as on a fresh
#: installation.
SHIPPED_WITHOUT_PHASES: tuple[str, ...] = ("general-worker", "engineer", "writer")

_CANDIDATES = """
SELECT id FROM srw_resources
WHERE kind = $1 AND deleted_at IS NULL
  AND (NOT $2::boolean OR managed_by IS NULL)
  AND NOT (kind = 'Expert' AND scope_kind = 'Catalog' AND name = ANY($3::text[]))
ORDER BY id
"""

# Absent, or a null left there by an older editor: both mean "not stated".
_MARK_AUTOMATIONS = """
UPDATE automations
   SET config_override =
       jsonb_set(COALESCE(config_override, '{}'::jsonb), '{phases}', 'true'::jsonb)
 WHERE COALESCE(config_override -> 'phases', 'null'::jsonb) = 'null'::jsonb
"""

_WRITE_MARKER = """
INSERT INTO system_settings
    (key, value, updated_at, updated_by, source, source_updated_at)
VALUES ($1, $2::jsonb, CURRENT_TIMESTAMP, $3, 'default', CURRENT_TIMESTAMP)
ON CONFLICT (key) DO NOTHING
"""


def mark_phases(value: Any, *, skip_shipped: bool = False) -> bool:
    """Write ``phases: true`` into every Expert spec inside ``value`` that runs
    SRW's own worker and does not state the setting, in place.

    ``value`` is an Expert spec or a Project spec with inline copies. With
    ``skip_shipped`` (a Project and its managed children), a copy of an expert
    the release ships without phases is left alone. True when anything was
    written.
    """
    changed = False
    if isinstance(value, dict):
        runtime = value.get("runtime")
        private = runtime.get("config") if isinstance(runtime, dict) else None
        if (
            isinstance(private, dict)
            and runtime.get("adapter") == SRW_HARNESS_ADAPTER
            and not (
                skip_shipped and private.get("asset_name") in SHIPPED_WITHOUT_PHASES
            )
        ):
            config = private.get("config")
            if config is None:
                config = private["config"] = {}
            if isinstance(config, dict) and not is_phases_value(config.get(PHASES_KEY)):
                config[PHASES_KEY] = True
                changed = True
        for child in value.values():
            changed = mark_phases(child, skip_shipped=skip_shipped) or changed
    elif isinstance(value, list):
        for child in value:
            changed = mark_phases(child, skip_shipped=skip_shipped) or changed
    return changed


async def backfill_existing_phases(db: Any) -> dict[str, int] | None:
    """Mark what exists, unless the pass ran before.

    Returns the counts it recorded, or ``None`` when the marker row was already
    there. Each resource commits on its own under the catalogue lock, so one
    failure never blocks the rest and two replicas starting together cannot
    write the same revision twice.
    """
    if await db.fetchval("SELECT 1 FROM system_settings WHERE key = $1", MARKER_KEY):
        return None
    counts = {"projects": 0, "experts": 0, "skipped": 0, "automations": 0}
    if not await db.fetchval("SELECT EXISTS (SELECT 1 FROM jobs)"):
        # Nothing ran here before the setting existed.
        await db.execute(_WRITE_MARKER, MARKER_KEY, json.dumps(counts), _MARKER_ACTOR)
        return counts
    for kind, unmanaged_only, rewrite, transform in (
        ("Project", False, _retire_project, partial(mark_phases, skip_shipped=True)),
        ("Expert", True, _retire_expert, mark_phases),
    ):
        for record in await db.fetch(
            _CANDIDATES, kind, unmanaged_only, list(SHIPPED_WITHOUT_PHASES)
        ):
            try:
                async with db.transaction_scope():
                    store = ManifestStore(db)
                    await store.lock_catalog()
                    if await rewrite(db, store, record["id"], transform):
                        counts["projects" if kind == "Project" else "experts"] += 1
            except _Skip as exc:
                counts["skipped"] += 1
                logger.warning(
                    "Phases backfill: left unmarked, its jobs follow the "
                    "default (no phases): %s",
                    exc,
                )
            except Exception:
                counts["skipped"] += 1
                logger.exception(
                    "Phases backfill: %s %s was not marked; its jobs follow the "
                    "default (no phases) until it states `phases: on`",
                    kind,
                    record["id"],
                )
    async with db.transaction_scope():
        status = await db.execute(_MARK_AUTOMATIONS)
        counts["automations"] = int(str(status).rsplit(" ", 1)[-1])
        await db.execute(_WRITE_MARKER, MARKER_KEY, json.dumps(counts), _MARKER_ACTOR)
    return counts


__all__ = [
    "MARKER_KEY",
    "SHIPPED_WITHOUT_PHASES",
    "backfill_existing_phases",
    "mark_phases",
]
