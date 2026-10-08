"""Whether a configured model can run, and the refusal when it cannot.

Design: ``knowledge-base/knowledge/features/unavailable_model_handling.md``.

The model registry (``shared.runtime.core.model_registry.resolve_model``: user
custom endpoints, system endpoint models, the admin catalog) is the only
authority for an executable model slot. When it cannot resolve a model the
answer is "not available" — dispatch never guesses a provider from the model
name. :class:`ModelUnavailable` is the one refusal for that case, raised by
the credential injectors once every slot has been checked, so a single
delivery reports all unavailable slots at once.

A slot that inherits its parent's model (``llm._inherit_llm`` on a roster
entry) is not checked by name: the child is overlaid with the parent's live
LLM when it is delegated (``agent/subagents/child.py::overlay_live_llm``), so
its frozen model name is a label, not a route.

Pure apart from :func:`unresolved_model_reason`, which reads the catalog
through the store it is given.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from shared.runtime.core.loader import INHERIT_MODEL, ROSTER_INHERIT_MARKER
from shared.runtime.core.model_registry import UnknownModelError

logger = logging.getLogger(__name__)

# Why a model cannot run. ``disabled``: the admin catalog has the model but
# every row is ``enabled = false``. ``capability``: an enabled row exists but
# not for the slot's capability. ``unknown``: no registry source knows it.
REASON_DISABLED = "disabled"
REASON_CAPABILITY = "capability"
REASON_UNKNOWN = "unknown"

MODEL_UNAVAILABLE_CODE = "model.unavailable"

# Where the user changes the model, for the refusal text (§ Messages).
WHERE_SESSION = "this session's settings"
WHERE_JOB = "the job's configuration"
WHERE_ACCOUNT = "Settings → Preferences"

# The cockpit's chat banner cuts a message at 240 characters (sanitizeError).
_MESSAGE_LIMIT = 240


@dataclass(frozen=True)
class ModelSlot:
    """One model-bearing section of a config (blob ``agent`` or override)."""

    label: str
    section: dict[str, Any]
    capability: str
    inherited: bool


def model_slots(config: dict[str, Any] | None) -> list[ModelSlot]:
    """Every model-bearing section of an ``agent`` blob or a
    ``config_override``-shaped dict — the two share these keys.

    ``llm``, the legacy ``llm.strategic`` / ``llm.tactical`` blocks (no-blob
    fallback path only), ``llm.summarization``, ``auxiliary``, the roster-wide
    ``subagents.llm`` and every roster entry's ``llm`` plus its own
    ``summarization``. Only mappings are returned; a section without a model is
    returned too (callers that inject credentials need it), so check
    :func:`slot_model` before treating a slot as a model.
    """
    if not isinstance(config, dict):
        return []
    out: list[ModelSlot] = []

    def _add(label: str, section: object, capability: str) -> None:
        if isinstance(section, dict):
            out.append(
                ModelSlot(
                    label=label,
                    section=section,
                    capability=capability,
                    inherited=bool(section.get(ROSTER_INHERIT_MARKER)),
                )
            )

    llm = config.get("llm")
    _add("llm", llm, "chat")
    if isinstance(llm, dict):
        for key in ("strategic", "tactical", "summarization"):
            _add(f"llm.{key}", llm.get(key), "chat")
    _add("auxiliary", config.get("auxiliary"), "auxiliary")
    subagents = config.get("subagents")
    if isinstance(subagents, dict):
        _add("subagents.llm", subagents.get("llm"), "chat")
        roster = subagents.get("roster")
        if isinstance(roster, dict):
            for name, entry in roster.items():
                if not isinstance(entry, dict):
                    continue
                entry_llm = entry.get("llm")
                _add(f"subagents.roster.{name}.llm", entry_llm, "chat")
                if isinstance(entry_llm, dict):
                    _add(
                        f"subagents.roster.{name}.llm.summarization",
                        entry_llm.get("summarization"),
                        "chat",
                    )
    return out


def slot_model(slot: ModelSlot) -> str | None:
    """The slot's model id, or ``None`` for no model / the bare ``inherit``
    sentinel (a model-less parent, not a model)."""
    model = slot.section.get("model")
    if not model or model == INHERIT_MODEL:
        return None
    return str(model)


@dataclass(frozen=True)
class UnavailableModel:
    slot: str
    model: str
    reason: str

    def as_dict(self) -> dict[str, str]:
        return {"slot": self.slot, "model": self.model, "reason": self.reason}


def slot_display(slot: str) -> str:
    """A readable name for a slot label, for messages."""
    if slot == "llm":
        return "main model"
    if slot == "auxiliary":
        return "auxiliary model"
    if slot == "llm.summarization":
        return "summarization model"
    if slot in ("llm.strategic", "llm.tactical"):
        return f"{slot.split('.', 1)[1]} model"
    if slot == "subagents.llm":
        return "helper agents' model"
    prefix = "subagents.roster."
    if slot.startswith(prefix):
        rest = slot[len(prefix) :]
        if rest.endswith(".llm.summarization"):
            return f"helper agent {rest[: -len('.llm.summarization')]}'s summarization model"
        if rest.endswith(".llm"):
            return f"helper agent {rest[: -len('.llm')]}"
    return slot


def _clip(text: str) -> str:
    if len(text) <= _MESSAGE_LIMIT:
        return text
    return text[: _MESSAGE_LIMIT - 1].rstrip() + "…"


class ModelUnavailable(Exception):
    """One or more configured models cannot run.

    ``entries`` names every unavailable slot. :meth:`message` renders the
    user-facing text; ``where`` says where the user can change the model and
    ``is_admin`` adds the re-enable hint for a disabled model.
    """

    code = MODEL_UNAVAILABLE_CODE

    def __init__(self, entries: list[UnavailableModel]):
        if not entries:
            raise ValueError("ModelUnavailable needs at least one entry")
        self.entries = list(entries)
        super().__init__(self.message())

    def message(self, *, where: str | None = None, is_admin: bool = False) -> str:
        return render_unavailable_message(self.entries, where=where, is_admin=is_admin)

    def detail(
        self, *, where: str | None = None, is_admin: bool = False
    ) -> dict[str, Any]:
        """The HTTP ``detail`` object: the cockpit shows ``message``."""
        return {
            "code": self.code,
            "message": self.message(where=where, is_admin=is_admin),
            "entries": [entry.as_dict() for entry in self.entries],
        }


def render_unavailable_message(
    entries: list[UnavailableModel],
    *,
    where: str | None = None,
    is_admin: bool = False,
) -> str:
    """The § Messages text of the design note, for one or more entries."""
    place = where or "the settings"
    if len(entries) == 1:
        entry = entries[0]
        name = slot_display(entry.slot)
        if entry.reason == REASON_UNKNOWN:
            text = (
                f"The model `{entry.model}` ({name}) is not configured on this "
                f"installation. Choose another model in {place}."
            )
        elif entry.reason == REASON_CAPABILITY:
            text = (
                f"The model `{entry.model}` cannot be used as the {name}. "
                f"Choose another model in {place}."
            )
        else:
            text = (
                f"The model `{entry.model}` ({name}) is no longer available. "
                f"Choose another model in {place}, or ask your administrator."
            )
    else:
        listed = ", ".join(f"`{e.model}` ({slot_display(e.slot)})" for e in entries)
        text = (
            f"These models are no longer available: {listed}. "
            f"Choose other models in {place}, or ask your administrator."
        )
    if is_admin and any(e.reason == REASON_DISABLED for e in entries):
        text += " You can re-enable it in Admin → Models."
    return _clip(text)


def keeps_explicit_transport(section: dict[str, Any], reason: str) -> bool:
    """Whether an unresolved slot still runs on a route its config names.

    A model the registry does not know (or not for this capability) may carry
    an explicit ``provider`` or ``base_url`` from its config — a self-hosted
    endpoint an expert names, say. That route is the caller's, not a guess, so
    it is kept. A *disabled* catalog model is refused even then: the admin
    turned it off.
    """
    if reason == REASON_DISABLED:
        return False
    return bool(section.get("base_url") or section.get("provider"))


def render_fallback_notice(notice: dict[str, Any]) -> str:
    """The § Messages fallback notice for a skipped account preference."""
    return _clip(
        f"Your default model `{notice['skipped']}` is no longer available, so "
        f"this session uses `{notice['used']}`. Choose a new default in "
        f"{WHERE_ACCOUNT}."
    )


async def stale_account_model_reason(
    model_id: str, *, user_id: str | None, capability: str, store: Any
) -> str | None:
    """Why a stored account preference names a model that cannot run, else
    ``None`` — also ``None`` when the registry has no source installed yet
    (before startup, or a unit test), because that is "cannot tell", not
    "unavailable". Uses the module resolver, which is what a test patches."""
    from shared.runtime.core import model_registry

    if not model_registry.lookups_registered():
        return None
    return await model_unavailable_reason(
        model_id,
        user_id=user_id,
        capability=capability,
        store=store,
        resolve_model=model_registry.resolve_model,
    )


async def unresolved_model_reason(store: Any, model_id: str) -> str:
    """Why the registry could not resolve ``model_id`` — call it only after
    ``resolve_model`` raised ``UnknownModelError``.

    Reads every catalog row naming the model regardless of ``enabled``
    (``store.catalog_model_states``). The lookup only words the refusal, so a
    store without that method (a test fake) or a failed read yields
    ``unknown`` rather than breaking the dispatch that is being refused.
    """
    lookup = getattr(store, "catalog_model_states", None)
    if lookup is None:
        return REASON_UNKNOWN
    try:
        rows = await lookup(model_id) or []
    except Exception:  # noqa: BLE001 — wording only; the refusal stands
        logger.warning(
            "Model availability: catalog lookup for %r failed; reporting it as unknown",
            model_id,
            exc_info=True,
        )
        return REASON_UNKNOWN
    if not rows:
        return REASON_UNKNOWN
    if any(row.get("enabled") for row in rows):
        return REASON_CAPABILITY
    return REASON_DISABLED


async def model_unavailable_reason(
    model_id: str,
    *,
    user_id: str | None,
    capability: str,
    store: Any,
    resolve_model: Callable[..., Awaitable[Any]],
) -> str | None:
    """``None`` when ``model_id`` resolves for ``capability``, else the reason.

    Goes through ``resolve_model`` with the user, so a user's own
    custom-endpoint model counts as available; the catalog alone would not.
    """
    try:
        await resolve_model(model_id, user_id=user_id, capability=capability)
        return None
    except UnknownModelError:
        return await unresolved_model_reason(store, model_id)


async def unavailable_slots(
    config: dict[str, Any] | None,
    *,
    user_id: str | None,
    store: Any,
    resolve_model: Callable[..., Awaitable[Any]],
) -> list[UnavailableModel]:
    """Every non-inherited model slot of ``config`` whose model cannot run.

    Used where the full credential injection is not wanted (input admission,
    a settings change): it only asks the registry.
    """
    found: list[UnavailableModel] = []
    for slot in model_slots(config):
        model = slot_model(slot)
        if model is None or slot.inherited:
            continue
        reason = await model_unavailable_reason(
            model,
            user_id=user_id,
            capability=slot.capability,
            store=store,
            resolve_model=resolve_model,
        )
        if reason is None or keeps_explicit_transport(slot.section, reason):
            continue
        found.append(UnavailableModel(slot=slot.label, model=model, reason=reason))
    return found


__all__ = [
    "MODEL_UNAVAILABLE_CODE",
    "REASON_CAPABILITY",
    "REASON_DISABLED",
    "REASON_UNKNOWN",
    "WHERE_ACCOUNT",
    "WHERE_JOB",
    "WHERE_SESSION",
    "ModelSlot",
    "ModelUnavailable",
    "UnavailableModel",
    "keeps_explicit_transport",
    "model_slots",
    "model_unavailable_reason",
    "render_fallback_notice",
    "render_unavailable_message",
    "slot_display",
    "slot_model",
    "stale_account_model_reason",
    "unavailable_slots",
    "unresolved_model_reason",
]
