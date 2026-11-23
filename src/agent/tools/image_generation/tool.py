"""``generate_image``: the agent's harness-side image generation tool.

Built only when dispatch delivered an ``image`` section (the pinned
``image_generation`` catalog row; owner decision D1, 2026-11-17), like the web
tools. The provider key stays in the agent process; the workspace only ever
receives the image files.

What the tool guarantees whether or not the ``image`` skill was loaded
(image_generation.md §6 FM8):

- **Spend control.** A paid row reserves its images against the job's or
  Session's cap before the call (a durable counter, shared with delegated
  children); a subscription row asks the orchestrator whether the shared
  Codex quota has room (D3) and runs one call at a time per agent process.
- **One attempt.** No retry: the proxy already retries, a moderation refusal
  must not be retried, and every attempt costs.
- **Context stays small.** One image comes back to the model; several come
  back as one numbered contact sheet. Seen images stay in history until
  compaction (decision D8), so a bulk run must never add N images.
- **Provenance.** Every image gets ``.provenance/<path>.json`` (prompt,
  model, settings, references, who paid), outside the deliverable folder.
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import posixpath
import re
import time
from datetime import datetime, timezone
from typing import Any, List, Literal, Mapping, Optional

import httpx
from langchain_core.tools import tool
from pydantic import BaseModel, Field

from agent.services.image_compose import (
    ImageComposeError,
    SheetTile,
    contact_sheet,
    fit_reference,
    image_size,
    sniff_mime,
    with_png_text,
)
from agent.services.image_content import IMAGE_DATA_TAG_TEMPLATE
from agent.tools.context import ToolContext
from agent.tools.image_generation.adapters import (
    ADAPTERS,
    MAX_REFERENCES,
    ImageProviderError,
    ImageRequest,
)
from shared.image_generation import BILLING_SUBSCRIPTION

logger = logging.getLogger(__name__)

#: Images per call. More than this belongs in a batch (the skill's runner).
MAX_IMAGES_PER_CALL = 4
#: Images per call on the subscription proxy (shared quota, one pod).
MAX_PROXY_IMAGES_PER_CALL = 2
#: Default folder for generated images.
DEFAULT_OUTPUT_DIR = "output/images"
PROVENANCE_DIR = ".provenance"
#: Reference images are fitted to this edge before upload.
REFERENCE_MAX_EDGE = 2048

_proxy_lock: asyncio.Lock | None = None

#: Provider errors raised before anything was generated (nothing billed).
_NOT_BILLED_KINDS = frozenset(
    {"no_plan", "auth", "quota", "rate_limit", "bad_request", "certificate"}
)

_EXT = {
    "image/png": ".png",
    "image/jpeg": ".jpg",
    "image/webp": ".webp",
    "image/gif": ".gif",
}


def _proxy_serializer() -> asyncio.Lock:
    global _proxy_lock
    if _proxy_lock is None:
        _proxy_lock = asyncio.Lock()
    return _proxy_lock


class GenerateImageArguments(BaseModel):
    prompt: str = Field(
        ...,
        description=(
            "What the picture shows: subject, setting, composition, light, style. "
            "For an edit, what to change and what to keep."
        ),
    )
    path: str = Field(
        "",
        description=(
            "Workspace path for the image, e.g. output/images/hero.png. With n>1 the "
            "images get -1, -2, … suffixes. Empty: output/images/<from the prompt>.png."
        ),
    )
    n: int = Field(
        1,
        ge=1,
        le=MAX_IMAGES_PER_CALL,
        description="How many variations (1–4; 1–2 on a subscription model).",
    )
    size: str = Field(
        "",
        description=(
            "WxH (e.g. 1536x1024), an aspect ratio (16:9, 2:3, 1:1) or "
            "square/landscape/portrait. Empty: the model's default."
        ),
    )
    quality: Literal["low", "medium", "high", "auto"] = Field(
        "low",
        description="low for drafts (cheap, fast); high for the final image.",
    )
    reference_images: List[str] = Field(
        default_factory=list,
        description=(
            "Workspace paths of images to edit or to keep consistent with "
            "(a character sheet, a style reference, the image to change)."
        ),
    )
    overwrite: bool = Field(False, description="Replace existing files at path.")


def _slug(text: str, limit: int = 40) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")
    return (slug[:limit].rstrip("-")) or "image"


def _target_paths(path: str, prompt: str, count: int, mime: str) -> list[str]:
    ext = _EXT.get(mime, ".png")
    raw = (path or "").strip().lstrip("/")
    if not raw:
        stem = posixpath.join(DEFAULT_OUTPUT_DIR, _slug(prompt))
    elif raw.endswith("/"):
        stem = posixpath.join(raw.rstrip("/"), _slug(prompt))
    else:
        root, given_ext = posixpath.splitext(raw)
        stem = root
        if given_ext.lower() in (".png", ".jpg", ".jpeg", ".webp", ".gif"):
            ext = given_ext.lower() if given_ext.lower() != ".jpeg" else ".jpg"
    if count == 1:
        return [stem + ext]
    return [f"{stem}-{i + 1}{ext}" for i in range(count)]


def _ref(context: ToolContext) -> tuple[str, str] | None:
    """The job or Session the counter belongs to."""
    if context.thread_id:
        return "thread", str(context.thread_id)
    if context.job_id:
        return "job", str(context.job_id)
    return None


async def _proxy_quota_verdict() -> Mapping[str, Any]:
    """Ask the orchestrator whether the subscription has room (fail closed)."""
    base = os.getenv("ORCHESTRATOR_URL", "http://localhost:8085").rstrip("/")
    headers = {}
    key = os.getenv("MCP_INTERNAL_KEY", "")
    if key:
        headers["X-Internal-Key"] = key
    try:
        async with httpx.AsyncClient(timeout=15.0, headers=headers) as client:
            response = await client.get(
                f"{base}/api/internal/image-generation/proxy-quota"
            )
        if response.status_code != 200:
            return {
                "allowed": False,
                "reason": f"quota check failed (HTTP {response.status_code})",
            }
        verdict = response.json()
        return (
            verdict
            if isinstance(verdict, Mapping)
            else {"allowed": False, "reason": "quota check failed"}
        )
    except Exception as exc:  # noqa: BLE001 - any failure refuses the call
        return {
            "allowed": False,
            "reason": f"quota check failed ({type(exc).__name__})",
        }


def _quota_refusal(verdict: Mapping[str, Any]) -> str:
    reason = verdict.get("reason")
    windows = verdict.get("windows") or []
    used = ", ".join(
        f"{w.get('window')} window {w.get('used_percent')}% used"
        for w in windows
        if isinstance(w, Mapping)
    )
    threshold = verdict.get("threshold_percent")
    if reason == "limit_reached":
        why = "the subscription's usage limit is reached"
    elif reason == "window_at_threshold":
        why = f"the subscription is at or above the {threshold}% image threshold"
    else:
        why = str(reason or "the quota check failed")
    return (
        f"Error: no image was generated: {why}"
        + (f" ({used})" if used else "")
        + ". Images share the installation's subscription with chat; wait for the "
        "window to reset, or ask the user for an API-key image model."
    )


def _archive(
    context: ToolContext,
    *,
    model: str,
    section: Mapping[str, Any],
    prompt: str,
    count: int,
    size: str,
    quality: str,
    references: int,
    paths: list[str],
    usage: Mapping[str, int],
    latency_ms: int,
) -> None:
    """One audit row per call, metered by the orchestrator (unit ``image``)."""
    ref = _ref(context)
    if ref is None:
        return
    try:
        from langchain_core.messages import AIMessage, HumanMessage

        from agent.core.archiver import get_archiver

        archiver = get_archiver()
        if not archiver:
            return
        archiver.archive(
            job_id=ref[1],
            agent_type="persistent" if ref[0] == "thread" else "universal",
            messages=[HumanMessage(content=f"[generate_image] {prompt[:2000]}")],
            response=AIMessage(content="\n".join(paths)),
            model=model,
            latency_ms=latency_ms,
            call_type="image_generation",
            auxiliary_metadata={
                "image_generation": {
                    "count": count,
                    "size": size or None,
                    "quality": quality,
                    "references": references,
                    "provider": section.get("provider"),
                    "billing": section.get("billing"),
                    "catalog_model": section.get("catalog_model"),
                    "usage": dict(usage),
                }
            },
        )
    except Exception:  # noqa: BLE001 - metering never fails the tool
        logger.warning("generate_image: archiving the call failed", exc_info=True)


def _can_look(context: ToolContext) -> bool:
    """Whether the agent can see what it generated: a multimodal model, or
    the VisionHelper with a key to describe it."""
    try:
        if context.get_phase_multimodal():
            return True
    except Exception:  # noqa: BLE001
        pass
    try:
        from agent.services.vision_helper import get_vision_helper

        return bool(get_vision_helper().api_key)
    except Exception:  # noqa: BLE001
        return False


def create_image_generation_tools(context: ToolContext) -> List[Any]:
    """``[generate_image]`` when an image model is pinned, else ``[]``."""
    section = context.config.get("image") if context.config else None
    if not isinstance(section, Mapping):
        return []
    provider = section.get("provider")
    adapter = ADAPTERS.get(str(provider))
    if adapter is None or not section.get("model"):
        logger.warning("generate_image: unusable image section (provider=%r)", provider)
        return []
    if not _can_look(context):
        # Generating images nobody can check is the failure mode the note's
        # binding rule prevents (image_generation.md WP4, Codex's gate).
        logger.warning(
            "generate_image: not bound: the model is text-only and no vision "
            "model is configured to describe the result"
        )
        return []
    subscription = section.get("billing") == BILLING_SUBSCRIPTION
    cap = int(section.get("max_paid_images") or 0)

    async def _read_reference(path: str) -> tuple[bytes, str]:
        workspace = context.workspace_manager

        def read() -> bytes:
            data = workspace.backend.read_file(path, binary=True)
            return data.encode() if isinstance(data, str) else data

        data = await asyncio.to_thread(read)
        return await asyncio.to_thread(fit_reference, data, max_edge=REFERENCE_MAX_EDGE)

    async def _save(path: str, data: bytes, overwrite: bool) -> str:
        from agent.tools.workspace.files import save_workspace_file

        return await asyncio.to_thread(
            save_workspace_file, context, path, data, overwrite=overwrite
        )

    @tool(args_schema=GenerateImageArguments)
    async def generate_image(
        prompt: str,
        path: str = "",
        n: int = 1,
        size: str = "",
        quality: str = "low",
        reference_images: Optional[List[str]] = None,
        overwrite: bool = False,
    ) -> str:
        """Generate or edit images with the installation's image model and save them in the workspace.

        Use it for pictures (photos, illustrations, characters, scenes). For
        charts, diagrams, icons and anything with exact text, render with code
        (HTML/SVG/matplotlib) instead. Start with low quality drafts, look at the
        result, change one thing at a time, and use high quality for the final
        image. Pass reference_images to edit an image or to keep a character or
        style consistent (generate each image from the approved reference, not
        from an edit of an edit). One image comes back for you to see; several
        come back as one numbered contact sheet — look closer with read_file
        before you use or deliver any of them. Load the `image` skill for the
        full workflow.
        """
        refs = [r for r in (reference_images or []) if isinstance(r, str) and r.strip()]
        text = (prompt or "").strip()
        if not text:
            return "Error: prompt is empty."
        if not context.has_workspace():
            return "Error: this agent has no workspace to save images into."
        count = max(1, min(int(n), MAX_IMAGES_PER_CALL))
        if subscription:
            count = min(count, MAX_PROXY_IMAGES_PER_CALL)
        limit = MAX_REFERENCES.get(str(provider), 0)
        if len(refs) > limit:
            return f"Error: at most {limit} reference images for this model."

        references: list[tuple[bytes, str]] = []
        for ref_path in refs:
            try:
                references.append(await _read_reference(ref_path.strip()))
            except (ImageComposeError, FileNotFoundError, OSError, ValueError) as exc:
                return f"Error: cannot use reference image {ref_path}: {exc}"

        reservation: tuple[str, str] | None = None
        if subscription:
            verdict = await _proxy_quota_verdict()
            if not verdict.get("allowed"):
                return _quota_refusal(verdict)
        elif cap > 0:
            ref = _ref(context)
            store = getattr(context, "postgres_db", None)
            if ref is not None and store is not None:
                try:
                    ok, used = await store.reserve_image_generation(
                        ref[0], ref[1], count, cap
                    )
                except Exception:  # noqa: BLE001
                    logger.warning(
                        "generate_image: cap reservation failed", exc_info=True
                    )
                    return "Error: no image was generated: the image budget could not be checked."
                if not ok:
                    left = max(cap - used, 0)
                    owner = "Session" if ref[0] == "thread" else "job"
                    return (
                        f"Error: no image was generated: this {owner} has used {used} of "
                        f"its {cap} paid images (the admin's cap), {left} left, and the "
                        f"call asked for {count}. "
                        + (
                            f"Ask for at most {left}, or reuse or edit the images you have."
                            if left
                            else "Reuse or edit the images you have, or ask the user "
                            "to have an admin raise the cap."
                        )
                    )
                reservation = ref

        request = ImageRequest(
            prompt=text,
            n=count,
            size=size or None,
            quality=quality,
            references=references,
        )
        started = time.monotonic()
        try:
            if subscription:
                async with _proxy_serializer():
                    result = await adapter(section, request)
            else:
                result = await adapter(section, request)
        except ImageProviderError as exc:
            # The counter counts calls sent (image_generation.md WP4): only a
            # refusal that happened before any generation gives the images
            # back. A timeout, an outage or a safety block may still be billed.
            if reservation is not None and exc.kind in _NOT_BILLED_KINDS:
                try:
                    await context.postgres_db.release_image_generation(
                        reservation[0], reservation[1], count
                    )
                except Exception:  # noqa: BLE001
                    logger.warning("generate_image: release failed", exc_info=True)
            hint = {
                "moderation": " The provider's safety system refused it; do not retry the same prompt.",
                "blocked": " Do not retry the same prompt; change what it asks for.",
                "no_plan": " The subscription has no plan that can make images.",
                "auth": " The image model's key or login is not valid.",
                "quota": " The provider's quota or credit is used up.",
                "rate_limit": " The provider is rate-limiting; try again later.",
                "certificate": " Do not retry; an admin must re-trust it in Admin \u2192 Models.",
                "timeout": " The call timed out; the image may still have been billed.",
            }.get(exc.kind, "")
            return f"Error: no image was generated ({exc.kind}): {str(exc).rstrip('.')}.{hint}"
        latency_ms = int((time.monotonic() - started) * 1000)
        images = result.images
        if reservation is not None and len(images) < count:
            try:
                await context.postgres_db.release_image_generation(
                    reservation[0], reservation[1], count - len(images)
                )
            except Exception:  # noqa: BLE001
                logger.warning("generate_image: partial release failed", exc_info=True)

        first_mime = sniff_mime(images[0].data) or images[0].mime_type
        targets = _target_paths(path, text, len(images), first_mime)
        saved: list[str] = []
        problems: list[str] = []
        now = datetime.now(timezone.utc).isoformat()
        for image, target in zip(images, targets):
            dims = image_size(image.data)
            provenance = {
                "tool": "generate_image",
                "created_at": now,
                "prompt": text,
                "model": section.get("model"),
                "provider": section.get("provider"),
                "billing": section.get("billing"),
                "size": size or None,
                "quality": quality,
                "references": refs,
                "width": dims[0] if dims else None,
                "height": dims[1] if dims else None,
            }
            # The prompt also travels inside a PNG (A1111/ComfyUI convention),
            # so a copied file keeps it; the sidecar is the readable record.
            stamped = with_png_text(
                image.data,
                {
                    "parameters": text,
                    "srw-provenance": json.dumps(provenance, ensure_ascii=False),
                },
            )
            outcome = await _save(target, stamped, overwrite)
            if outcome.startswith("Error"):
                problems.append(f"{target}: {outcome}")
                continue
            saved.append(target)
            await _save(
                posixpath.join(PROVENANCE_DIR, target + ".json"),
                json.dumps(provenance, indent=2).encode(),
                True,
            )
        _archive(
            context,
            model=str(section.get("catalog_model") or section.get("model")),
            section=section,
            prompt=text,
            count=len(images),
            size=size,
            quality=quality,
            references=len(references),
            paths=saved,
            usage=result.usage,
            latency_ms=latency_ms,
        )
        if not saved:
            return (
                "Error: the images were generated but could not be saved: "
                + "; ".join(problems)
            )

        lines = [
            f"Generated {len(saved)} image(s) with {section.get('model')} "
            f"({'subscription' if subscription else 'paid API'}) in {latency_ms / 1000:.0f}s:"
        ]
        lines += [f"{i + 1}. {p}" for i, p in enumerate(saved)]
        if problems:
            lines.append("Not saved: " + "; ".join(problems))
        if result.finish_note:
            lines.append(f"Provider note: {result.finish_note}")
        lines.append(
            f"Provenance: {PROVENANCE_DIR}/<path>.json next to each image's path."
        )

        shown: bytes
        if len(saved) == 1:
            shown = images[0].data
            shown_mime = first_mime
            lines.append(
                "The image is attached; look closer with read_file before using it."
            )
        else:
            tiles = [
                SheetTile(data=image.data, label=posixpath.basename(p))
                for image, p in zip(images, saved)
            ]
            try:
                shown = await asyncio.to_thread(contact_sheet, tiles)
            except ImageComposeError as exc:
                lines.append(f"(No contact sheet: {exc})")
                return "\n".join(lines)
            shown_mime = "image/png"
            lines.append(
                "A numbered contact sheet of all images is attached (cells are small): "
                "pick by number, then look closer at a candidate with read_file."
            )

        if context.get_phase_multimodal():
            b64 = base64.b64encode(shown).decode()
            return (
                "\n".join(lines)
                + "\n"
                + IMAGE_DATA_TAG_TEMPLATE.format(mime=shown_mime, b64=b64)
            )
        try:
            from agent.services.vision_helper import get_vision_helper

            description = await get_vision_helper().describe_image(
                base64.b64encode(shown).decode(),
                shown_mime,
                query=(
                    "Describe what this generated image shows, image by image if it is "
                    f"a numbered sheet. The request was: {text[:500]}"
                ),
                job_id=context.job_id,
            )
        except Exception:  # noqa: BLE001
            description = None
        from agent.services.vision_helper import vision_failed
        from shared.untrusted_content import fence_untrusted

        if not vision_failed(description):
            lines.append(
                "Description by a vision model (unverified for counts, spelling and "
                "hands):\n"
                + fence_untrusted(str(description), origin="image", strip_braces=False)
            )
        else:
            lines.append(
                "You did not see the image: no vision model could describe it."
            )
        return "\n".join(lines)

    return [generate_image]
