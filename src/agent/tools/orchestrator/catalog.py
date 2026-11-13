"""Expert and skill catalog tools for persistent sessions."""

from __future__ import annotations

import re
from typing import Any, Dict, List, Literal, Optional
from urllib.parse import quote

import httpx
from langchain_core.tools import tool

from agent.tools.context import ToolContext
from agent.tools.orchestrator.bundle_support import (
    _safe_limit as _safe_limit,
    _canonical_json as _canonical_json,
    _bundle_hash as _bundle_hash,
    _pretty_json as _pretty_json,
    _http_error as _http_error,
    _write_workspace_json as _write_workspace_json,
    _read_bundle_payload as _read_bundle_payload,
    _unwrap_bundle as _unwrap_bundle,
)
from shared.orch_surface.formatters import truncate_text as _truncate

from agent.tools.orchestrator.jobs import _get_client, _get_orchestrator_url

from shared.tool_catalog.definitions import (
    CATALOG_TOOLS_METADATA as CATALOG_TOOLS_METADATA,
)


_EXPERT_NAME_RE = re.compile(r"^[a-z][a-z0-9_-]*$")
_COLOR_RE = re.compile(r"^#[0-9A-Fa-f]{6}$")
_ALLOWED_EXPERT_PROMPT_KEYS = {
    "persona",
    "instructions",
    "summarization",
}

_EXPERT_CREATE_FIELDS = {
    "name",
    "display_name",
    "expert_type",
    "description",
    "icon",
    "color",
    "tags",
    "config",
    "prompts",
}
_EXPERT_UPDATE_FIELDS = {
    "display_name",
    "description",
    "icon",
    "color",
    "tags",
    "config",
    "prompts",
}
_SKILL_WRITE_FIELDS = {"files", "display_name", "icon", "color", "tags"}


def _normalize_expert_bundle(payload: Dict[str, Any]) -> Dict[str, Any]:
    raw = _unwrap_bundle(payload)
    return {key: raw[key] for key in _EXPERT_CREATE_FIELDS if key in raw}


def _normalize_skill_bundle(detail: Dict[str, Any]) -> Dict[str, Any]:
    raw = _unwrap_bundle(detail)
    files = raw.get("files") if isinstance(raw.get("files"), dict) else {}
    parsed_name = raw.get("name")
    parsed_description = raw.get("description") or ""
    skill_md = files.get("SKILL.md")
    if isinstance(skill_md, str):
        try:
            from shared.runtime.core.skill_format import (
                SkillFormatError,
                parse_skill_md,
                skill_identity,
            )

            frontmatter, _body = parse_skill_md(skill_md)
            parsed_name, parsed_description = skill_identity(frontmatter)
        except (SkillFormatError, TypeError, ValueError):
            pass
    return {
        "name": parsed_name,
        "display_name": raw.get("display_name") or parsed_name,
        "description": parsed_description,
        "icon": raw.get("icon", "extension"),
        "color": raw.get("color", "#6B7280"),
        "tags": raw.get("tags") or [],
        "files": files,
    }


def _validate_expert_bundle(
    bundle: Dict[str, Any],
    *,
    require_create_fields: bool,
) -> List[str]:
    from shared.runtime.core.expert_resolution import hard_deny_scan

    errors: List[str] = []
    if require_create_fields:
        for field in ("name", "display_name", "expert_type"):
            if not bundle.get(field):
                errors.append(f"{field} is required.")
    if bundle.get("name") and not _EXPERT_NAME_RE.match(str(bundle["name"])):
        errors.append("name must match ^[a-z][a-z0-9_-]*$.")
    if bundle.get("expert_type") and bundle["expert_type"] not in ("worker", "session"):
        errors.append("expert_type must be worker or session.")
    if (
        bundle.get("display_name") is not None
        and not str(bundle["display_name"]).strip()
    ):
        errors.append("display_name cannot be empty.")
    if bundle.get("color") and not _COLOR_RE.match(str(bundle["color"])):
        errors.append("color must be a hex color like #6B7280.")
    tags = bundle.get("tags", [])
    if not isinstance(tags, list) or not all(isinstance(tag, str) for tag in tags):
        errors.append("tags must be a list of strings.")
    config = bundle.get("config", {})
    if config is not None and not isinstance(config, dict):
        errors.append("config must be an object.")
    elif isinstance(config, dict):
        offending = hard_deny_scan(config)
        if offending:
            errors.append(
                "config may not set credential sections: "
                + ", ".join(sorted(offending))
            )
    prompts = bundle.get("prompts", {})
    if prompts is not None and not isinstance(prompts, dict):
        errors.append("prompts must be an object.")
    elif isinstance(prompts, dict):
        unknown = set(prompts) - _ALLOWED_EXPERT_PROMPT_KEYS
        if unknown:
            errors.append(f"prompts contains unknown keys: {sorted(unknown)}.")
    return errors


def _validate_skill_bundle(bundle: Dict[str, Any]) -> List[str]:
    from shared.runtime.core.expert_resolution import hard_deny_scan
    from shared.runtime.core.skill_format import (
        SkillFormatError,
        parse_skill_md,
        skill_identity,
        validate_skill_files,
    )

    errors: List[str] = []
    files = bundle.get("files")
    if not isinstance(files, dict) or not all(
        isinstance(path, str) and isinstance(content, str)
        for path, content in (files or {}).items()
    ):
        errors.append("files must be an object mapping paths to text content.")
        return errors
    try:
        validate_skill_files(files)
        frontmatter, _body = parse_skill_md(files["SKILL.md"])
        skill_identity(frontmatter)
    except SkillFormatError as e:
        errors.append(str(e))
    else:
        offending = hard_deny_scan(frontmatter)
        if offending:
            errors.append(
                "SKILL.md frontmatter may not set credential sections: "
                + ", ".join(sorted(offending))
            )
    if bundle.get("color") and not _COLOR_RE.match(str(bundle["color"])):
        errors.append("color must be a hex color like #6B7280.")
    tags = bundle.get("tags", [])
    if not isinstance(tags, list) or not all(isinstance(tag, str) for tag in tags):
        errors.append("tags must be a list of strings.")
    return errors


async def _fetch_expert_bundle(
    client: httpx.AsyncClient,
    base_url: str,
    expert_id: str,
) -> Dict[str, Any]:
    resp = await client.get(
        f"{base_url}/api/experts/{quote(str(expert_id), safe='')}/export"
    )
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, dict):
        raise ValueError("Expert export did not return a JSON object.")
    return _normalize_expert_bundle(data)


async def _fetch_skill_bundle(
    client: httpx.AsyncClient,
    base_url: str,
    skill_id: str,
) -> Dict[str, Any]:
    resp = await client.get(f"{base_url}/api/skills/{quote(str(skill_id), safe='')}")
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, dict):
        raise ValueError("Skill detail did not return a JSON object.")
    return _normalize_skill_bundle(data)


def _expert_create_body(bundle: Dict[str, Any]) -> Dict[str, Any]:
    body = {key: bundle[key] for key in _EXPERT_CREATE_FIELDS if key in bundle}
    body.setdefault("description", None)
    body.setdefault("icon", "smart_toy")
    body.setdefault("color", "#6B7280")
    body.setdefault("tags", [])
    body.setdefault("config", {})
    body.setdefault("prompts", {})
    return body


def _expert_update_body(bundle: Dict[str, Any]) -> Dict[str, Any]:
    return {key: bundle[key] for key in _EXPERT_UPDATE_FIELDS if key in bundle}


def _skill_write_body(bundle: Dict[str, Any]) -> Dict[str, Any]:
    body = {key: bundle[key] for key in _SKILL_WRITE_FIELDS if key in bundle}
    body.setdefault("icon", "extension")
    body.setdefault("color", "#6B7280")
    body.setdefault("tags", [])
    return body


def _dry_run_summary(
    *,
    kind: str,
    mode: str,
    target_id: Optional[str],
    bundle: Dict[str, Any],
) -> str:
    name = bundle.get("name") or "(unchanged)"
    display_name = bundle.get("display_name") or name
    lines = [
        f"Dry run OK: would {mode} {kind}.",
        f"Name: {name}",
        f"Display name: {display_name}",
    ]
    if target_id:
        lines.append(f"Target ID: {target_id}")
    lines.append(f"Bundle hash: {_bundle_hash(bundle)}")
    lines.append("No changes written. Call again with dry_run=false to write.")
    return "\n".join(lines)


async def _check_expected_hash(
    *,
    client: httpx.AsyncClient,
    base_url: str,
    kind: Literal["expert", "skill"],
    target_id: Optional[str],
    expected_hash: Optional[str],
) -> Optional[str]:
    if not expected_hash:
        return None
    if not target_id:
        return "expected_hash requires target_id so the current bundle can be checked."
    if kind == "expert":
        current = await _fetch_expert_bundle(client, base_url, target_id)
    else:
        current = await _fetch_skill_bundle(client, base_url, target_id)
    current_hash = _bundle_hash(current)
    if current_hash != expected_hash:
        return (
            f"Refusing to write: current {kind} bundle hash is {current_hash}, "
            f"not expected_hash {expected_hash}."
        )
    return None


def _tags_text(item: Dict[str, Any]) -> str:
    tags = item.get("tags") or []
    if not isinstance(tags, list) or not tags:
        return ""
    return ", ".join(str(tag) for tag in tags if tag)


def _matches_query(item: Dict[str, Any], query: Optional[str]) -> bool:
    if not query:
        return True
    needle = query.strip().lower()
    if not needle:
        return True
    haystack = [
        item.get("id"),
        item.get("name"),
        item.get("display_name"),
        item.get("description"),
        item.get("source"),
        item.get("expert_type"),
        *_tags_text(item).split(", "),
    ]
    return any(needle in str(value or "").lower() for value in haystack)


def _filter_catalog_items(
    items: List[Dict[str, Any]],
    *,
    query: Optional[str] = None,
    source: Optional[str] = None,
) -> List[Dict[str, Any]]:
    filtered: List[Dict[str, Any]] = []
    normalized_source = source.strip().lower() if source else None
    for item in items:
        if (
            normalized_source
            and str(item.get("source", "")).lower() != normalized_source
        ):
            continue
        if not _matches_query(item, query):
            continue
        filtered.append(item)
    return filtered


def _format_catalog_item(item: Dict[str, Any], *, kind: str) -> List[str]:
    item_id = item.get("id", "unknown")
    lines = [f"--- {kind}: {item_id} ---"]
    if item.get("name") and item.get("name") != item_id:
        lines.append(f"  Name: {_truncate(item.get('name'), limit=120)}")
    if item.get("display_name"):
        lines.append(
            f"  Display name: {_truncate(item.get('display_name'), limit=160)}"
        )
    if item.get("source"):
        lines.append(f"  Source: {item['source']}")
    if item.get("expert_type"):
        lines.append(f"  Type: {item['expert_type']}")
    if item.get("version") is not None:
        lines.append(f"  Version: {item['version']}")
    tags = _tags_text(item)
    if tags:
        lines.append(f"  Tags: {_truncate(tags, limit=160)}")
    if item.get("description"):
        lines.append(f"  Description: {_truncate(item.get('description'), limit=260)}")
    return lines


async def _fetch_experts(
    client: httpx.AsyncClient,
    base_url: str,
    *,
    expert_type: Optional[str] = None,
) -> List[Dict[str, Any]]:
    params: Dict[str, Any] = {}
    if expert_type:
        params["type"] = expert_type
    resp = await client.get(f"{base_url}/api/experts", params=params)
    resp.raise_for_status()
    data = resp.json()
    return data if isinstance(data, list) else data.get("experts", [])


async def _fetch_skills(
    client: httpx.AsyncClient,
    base_url: str,
) -> List[Dict[str, Any]]:
    resp = await client.get(f"{base_url}/api/skills")
    resp.raise_for_status()
    data = resp.json()
    return data if isinstance(data, list) else data.get("skills", [])


def _format_expert_detail(expert_id: str, detail: Dict[str, Any]) -> str:
    lines: List[str] = [f"Expert ID: {detail.get('id') or expert_id}"]
    if detail.get("name"):
        lines.append(f"Name: {_truncate(detail.get('name'), limit=160)}")
    if detail.get("display_name"):
        lines.append(
            f"Display name: {_truncate(detail.get('display_name'), limit=160)}"
        )
    if detail.get("source"):
        lines.append(f"Source: {detail['source']}")
    if detail.get("expert_type"):
        lines.append(f"Type: {detail['expert_type']}")
    tags = _tags_text(detail)
    if tags:
        lines.append(f"Tags: {_truncate(tags, limit=180)}")
    if detail.get("description"):
        lines.append(f"Description: {_truncate(detail.get('description'), limit=300)}")

    config = detail.get("config") if isinstance(detail.get("config"), dict) else {}
    if config.get("display_name") and not detail.get("display_name"):
        lines.append(
            f"Display name: {_truncate(config.get('display_name'), limit=160)}"
        )
    if config.get("description") and not detail.get("description"):
        lines.append(f"Description: {_truncate(config.get('description'), limit=300)}")
    if config.get("autonomy"):
        lines.append(f"Autonomy: {config['autonomy']}")
    workspace = (
        config.get("workspace") if isinstance(config.get("workspace"), dict) else {}
    )
    if workspace.get("backend"):
        lines.append(f"Workspace backend: {workspace['backend']}")
    llm = config.get("llm") if isinstance(config.get("llm"), dict) else {}
    if llm.get("model"):
        lines.append(f"Model: {llm['model']}")

    tools = config.get("tools") if isinstance(config.get("tools"), dict) else {}
    if tools:
        # The STORED row, not a resolved config — normalisation never runs on
        # this path, so every policy spelling arrives verbatim. `disabled` used
        # to test `value == []`, which is False for `False` (and for `{}`), so
        # an expert authored with `tools.shell: false` appeared in NEITHER
        # line: the agent reading this detail was told nothing at all about a
        # category the author had deliberately turned off. Falsy-vs-truthy is
        # the same partition `expand_tool_policy` applies (`false`/`[]`/`{}`
        # are all spellings of off), and it makes the two lines exhaustive.
        enabled = sorted(k for k, value in tools.items() if value)
        disabled = sorted(k for k, value in tools.items() if not value)
        if enabled:
            lines.append(f"Enabled tool categories: {', '.join(enabled)}")
        if disabled:
            lines.append(f"Disabled tool categories: {', '.join(disabled)}")

    effective_models = detail.get("effective_models")
    if isinstance(effective_models, dict) and effective_models:
        model_parts = []
        for slot in ("session", "strategic", "tactical", "subagent"):
            value = effective_models.get(slot)
            if isinstance(value, dict) and value.get("model"):
                source = value.get("source") or "unknown"
                model_parts.append(f"{slot}={value['model']} ({source})")
        if model_parts:
            lines.append(f"Effective models: {', '.join(model_parts)}")

    instructions = detail.get("instructions")
    if instructions:
        text = _truncate(instructions, limit=1200)
        lines.append("Instructions preview:")
        lines.append(text)
    return "\n".join(lines)


def _format_skill_detail(
    skill_id: str,
    detail: Dict[str, Any],
    *,
    include_files: bool,
) -> str:
    lines = _format_catalog_item(
        {"id": skill_id, **detail},
        kind="Skill",
    )
    files = detail.get("files") if isinstance(detail.get("files"), dict) else {}
    if files:
        lines.append(f"  Files: {len(files)}")
        for path, content in sorted(files.items())[:30]:
            lines.append(f"    - {path} ({len(str(content))} chars)")

        skill_md = files.get("SKILL.md")
        if skill_md:
            lines.append("")
            lines.append("SKILL.md preview:")
            lines.append(_truncate(skill_md, limit=1400))

        if include_files:
            lines.append("")
            lines.append("Selected file contents:")
            for path, content in sorted(files.items())[:8]:
                lines.append(f"--- {path} ({len(str(content))} chars) ---")
                lines.append(_truncate(content, limit=1200))
    return "\n".join(lines)


def create_catalog_tools(context: ToolContext) -> List[Any]:
    """Create expert and skill catalog tools with injected session context."""
    base_url = _get_orchestrator_url()

    @tool
    async def list_experts(
        expert_type: Optional[str] = None,
        source: Optional[str] = None,
        query: Optional[str] = None,
        limit: int = 30,
    ) -> str:
        """List visible experts.

        Args:
            expert_type: Optional DB expert type filter, such as worker or session.
            source: Optional source filter: bundled, user, or global.
            query: Optional text filter over id/name/description/tags.
            limit: Maximum experts to return.
        """
        effective_limit = _safe_limit(limit, default=30, maximum=100)
        async with _get_client(user_id=context.user_id) as client:
            try:
                experts = await _fetch_experts(
                    client, base_url, expert_type=expert_type
                )
                experts = _filter_catalog_items(experts, query=query, source=source)
                shown = experts[:effective_limit]
                if not shown:
                    return "No matching experts found."
                # One line, once, rather than a selector per entry: the id
                # printed below is the selector whichever store it came from,
                # and a caller who does not know that lands on the deployment
                # default forever (experts_one_catalogue_two_selection_paths).
                lines = [
                    f"Found {len(experts)} matching expert(s); showing {len(shown)}:",
                    'Hire one with create_job(expert="<id below>"); Source is '
                    "where the definition lives, not a different way to select "
                    "it.\n",
                ]
                for expert in shown:
                    lines.extend(_format_catalog_item(expert, kind="Expert"))
                    lines.append("")
                return "\n".join(lines)
            except httpx.HTTPStatusError as e:
                return f"Failed to list experts: HTTP {e.response.status_code}"
            except httpx.RequestError as e:
                return f"Failed to connect to orchestrator: {e}"

    @tool
    async def get_expert(expert_id: str) -> str:
        """Inspect an expert by id or bundled expert name.

        Args:
            expert_id: Expert UUID or bundled expert id/name.
        """
        if not expert_id:
            return "expert_id is required."
        async with _get_client(user_id=context.user_id) as client:
            try:
                resp = await client.get(
                    f"{base_url}/api/experts/{quote(str(expert_id), safe='')}"
                )
                resp.raise_for_status()
                return _format_expert_detail(str(expert_id), resp.json())
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404:
                    return f"Expert '{expert_id}' not found or not visible."
                return f"Failed to get expert: HTTP {e.response.status_code}"
            except httpx.RequestError as e:
                return f"Failed to connect to orchestrator: {e}"

    @tool
    async def list_skills(
        source: Optional[str] = None,
        query: Optional[str] = None,
        limit: int = 40,
    ) -> str:
        """List visible skills.

        Args:
            source: Optional source filter: bundled, user, or global.
            query: Optional text filter over id/name/description/tags.
            limit: Maximum skills to return.
        """
        effective_limit = _safe_limit(limit, default=40, maximum=150)
        async with _get_client(user_id=context.user_id) as client:
            try:
                skills = await _fetch_skills(client, base_url)
                skills = _filter_catalog_items(skills, query=query, source=source)
                shown = skills[:effective_limit]
                if not shown:
                    return "No matching skills found."
                lines = [
                    f"Found {len(skills)} matching skill(s); showing {len(shown)}:\n"
                ]
                for skill in shown:
                    lines.extend(_format_catalog_item(skill, kind="Skill"))
                    lines.append("")
                return "\n".join(lines)
            except httpx.HTTPStatusError as e:
                return f"Failed to list skills: HTTP {e.response.status_code}"
            except httpx.RequestError as e:
                return f"Failed to connect to orchestrator: {e}"

    @tool
    async def search_skills(query: str, limit: int = 10) -> str:
        """Search visible skills by text.

        Args:
            query: Search text. Matches id, name, display name, description, source,
                and tags.
            limit: Maximum skills to return.
        """
        if not query or not query.strip():
            return "query is required."
        effective_limit = _safe_limit(limit, default=10, maximum=50)
        async with _get_client(user_id=context.user_id) as client:
            try:
                skills = await _fetch_skills(client, base_url)
                skills = _filter_catalog_items(skills, query=query)
                shown = skills[:effective_limit]
                if not shown:
                    return f"No skills matched query '{query}'."
                lines = [
                    f"Found {len(skills)} skill(s) matching '{query}'; showing {len(shown)}:\n"
                ]
                for skill in shown:
                    lines.extend(_format_catalog_item(skill, kind="Skill"))
                    lines.append("")
                return "\n".join(lines)
            except httpx.HTTPStatusError as e:
                return f"Failed to search skills: HTTP {e.response.status_code}"
            except httpx.RequestError as e:
                return f"Failed to connect to orchestrator: {e}"

    @tool
    async def get_skill(skill_id: str, include_files: bool = False) -> str:
        """Inspect a skill by UUID or bundled skill name.

        Args:
            skill_id: Skill UUID or bundled skill id/name.
            include_files: Include truncated file contents beyond the SKILL.md preview.
        """
        if not skill_id:
            return "skill_id is required."
        async with _get_client(user_id=context.user_id) as client:
            try:
                resp = await client.get(
                    f"{base_url}/api/skills/{quote(str(skill_id), safe='')}"
                )
                resp.raise_for_status()
                return _format_skill_detail(
                    str(skill_id), resp.json(), include_files=include_files
                )
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404:
                    return f"Skill '{skill_id}' not found or not visible."
                return f"Failed to get skill: HTTP {e.response.status_code}"
            except httpx.RequestError as e:
                return f"Failed to connect to orchestrator: {e}"

    @tool
    async def get_expert_bundle(
        expert_id: str,
        destination_path: Optional[str] = None,
    ) -> str:
        """Get a portable JSON expert bundle for editing.

        Args:
            expert_id: Expert UUID or bundled expert id/name.
            destination_path: Optional workspace path to write the bundle JSON.
                When omitted, returns the JSON directly.
        """
        if not expert_id:
            return "expert_id is required."
        async with _get_client(user_id=context.user_id) as client:
            try:
                bundle = await _fetch_expert_bundle(client, base_url, expert_id)
                payload = {
                    "kind": "expert_bundle",
                    "id": str(expert_id),
                    "bundle_hash": _bundle_hash(bundle),
                    "bundle": bundle,
                }
                written = _write_workspace_json(context, destination_path, payload)
                if written:
                    return (
                        f"Wrote expert bundle for '{expert_id}' to {written}.\n"
                        f"Bundle hash: {payload['bundle_hash']}"
                    )
                return _pretty_json(payload)
            except ValueError as e:
                return str(e)
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404:
                    return f"Expert '{expert_id}' not found or not visible."
                return f"Failed to get expert bundle: {_http_error(e)}"
            except httpx.RequestError as e:
                return f"Failed to connect to orchestrator: {e}"

    @tool
    async def set_expert_bundle(
        mode: Literal["create", "update", "fork"],
        bundle: Optional[Dict[str, Any]] = None,
        bundle_path: Optional[str] = None,
        target_id: Optional[str] = None,
        expected_hash: Optional[str] = None,
        dry_run: bool = True,
    ) -> str:
        """Create, update, or fork an expert from a portable JSON bundle.

        Args:
            mode: create for a new exact name, update for an owned DB expert, or
                fork for import-style create with name-collision suffixing.
            bundle: Expert bundle JSON object, or the full object returned by
                get_expert_bundle.
            bundle_path: Optional workspace path containing the bundle JSON.
            target_id: Required for update. Optional with expected_hash for fork.
            expected_hash: Optional hash from get_expert_bundle; checked before
                writing when target_id is supplied.
            dry_run: Defaults true. Set false to write.
        """
        try:
            payload = _read_bundle_payload(
                context, bundle=bundle, bundle_path=bundle_path
            )
            expert_bundle = _normalize_expert_bundle(payload)
        except ValueError as e:
            return str(e)

        if mode not in ("create", "update", "fork"):
            return "mode must be create, update, or fork."
        if mode == "update" and not target_id:
            return "target_id is required for update."

        errors = _validate_expert_bundle(
            expert_bundle, require_create_fields=mode in ("create", "fork")
        )
        if errors:
            return "Expert bundle is invalid:\n- " + "\n- ".join(errors)

        async with _get_client(user_id=context.user_id) as client:
            try:
                mismatch = await _check_expected_hash(
                    client=client,
                    base_url=base_url,
                    kind="expert",
                    target_id=target_id,
                    expected_hash=expected_hash,
                )
                if mismatch:
                    return mismatch
                if dry_run:
                    return _dry_run_summary(
                        kind="expert",
                        mode=mode,
                        target_id=target_id,
                        bundle=expert_bundle,
                    )

                if mode == "update":
                    resp = await client.put(
                        f"{base_url}/api/experts/{quote(str(target_id), safe='')}",
                        json=_expert_update_body(expert_bundle),
                    )
                elif mode == "fork":
                    resp = await client.post(
                        f"{base_url}/api/experts/import",
                        json=_expert_create_body(expert_bundle),
                    )
                else:
                    resp = await client.post(
                        f"{base_url}/api/experts",
                        json=_expert_create_body(expert_bundle),
                    )
                resp.raise_for_status()
                result = resp.json()
                result_id = result.get("id") if isinstance(result, dict) else None
                lines = [f"Expert {mode} succeeded."]
                if result_id:
                    lines.append(f"Expert ID: {result_id}")
                lines.append(f"Bundle hash: {_bundle_hash(expert_bundle)}")
                return "\n".join(lines)
            except httpx.HTTPStatusError as e:
                return f"Failed to {mode} expert: {_http_error(e)}"
            except httpx.RequestError as e:
                return f"Failed to connect to orchestrator: {e}"
            except ValueError as e:
                return str(e)

    @tool
    async def get_skill_bundle(
        skill_id: str,
        destination_path: Optional[str] = None,
    ) -> str:
        """Get a portable JSON skill bundle with the full file tree.

        Args:
            skill_id: Skill UUID or bundled skill id/name.
            destination_path: Optional workspace path to write the bundle JSON.
                When omitted, returns the JSON directly.
        """
        if not skill_id:
            return "skill_id is required."
        async with _get_client(user_id=context.user_id) as client:
            try:
                bundle = await _fetch_skill_bundle(client, base_url, skill_id)
                payload = {
                    "kind": "skill_bundle",
                    "id": str(skill_id),
                    "bundle_hash": _bundle_hash(bundle),
                    "bundle": bundle,
                }
                written = _write_workspace_json(context, destination_path, payload)
                if written:
                    return (
                        f"Wrote skill bundle for '{skill_id}' to {written}.\n"
                        f"Bundle hash: {payload['bundle_hash']}"
                    )
                return _pretty_json(payload)
            except ValueError as e:
                return str(e)
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404:
                    return f"Skill '{skill_id}' not found or not visible."
                return f"Failed to get skill bundle: {_http_error(e)}"
            except httpx.RequestError as e:
                return f"Failed to connect to orchestrator: {e}"

    @tool
    async def set_skill_bundle(
        mode: Literal["create", "update", "fork"],
        bundle: Optional[Dict[str, Any]] = None,
        bundle_path: Optional[str] = None,
        target_id: Optional[str] = None,
        expected_hash: Optional[str] = None,
        dry_run: bool = True,
    ) -> str:
        """Create, update, or fork a skill from a portable JSON bundle.

        Args:
            mode: create for a new exact name, update for an owned DB skill, or
                fork for import-style create with name-collision suffixing.
            bundle: Skill bundle JSON object, or the full object returned by
                get_skill_bundle.
            bundle_path: Optional workspace path containing the bundle JSON.
            target_id: Required for update. Optional with expected_hash for fork.
            expected_hash: Optional hash from get_skill_bundle; checked before
                writing when target_id is supplied.
            dry_run: Defaults true. Set false to write.
        """
        try:
            payload = _read_bundle_payload(
                context, bundle=bundle, bundle_path=bundle_path
            )
            skill_bundle = _normalize_skill_bundle(payload)
        except ValueError as e:
            return str(e)

        if mode not in ("create", "update", "fork"):
            return "mode must be create, update, or fork."
        if mode == "update" and not target_id:
            return "target_id is required for update."

        errors = _validate_skill_bundle(skill_bundle)
        if errors:
            return "Skill bundle is invalid:\n- " + "\n- ".join(errors)

        async with _get_client(user_id=context.user_id) as client:
            try:
                mismatch = await _check_expected_hash(
                    client=client,
                    base_url=base_url,
                    kind="skill",
                    target_id=target_id,
                    expected_hash=expected_hash,
                )
                if mismatch:
                    return mismatch
                if dry_run:
                    return _dry_run_summary(
                        kind="skill",
                        mode=mode,
                        target_id=target_id,
                        bundle=skill_bundle,
                    )

                body = _skill_write_body(skill_bundle)
                if mode == "update":
                    resp = await client.put(
                        f"{base_url}/api/skills/{quote(str(target_id), safe='')}",
                        json=body,
                    )
                elif mode == "fork":
                    from shared.runtime.core.skill_format import pack_skill_zip

                    name = str(skill_bundle.get("name") or "skill")
                    zip_bytes = pack_skill_zip(name, skill_bundle["files"])
                    resp = await client.post(
                        f"{base_url}/api/skills/import",
                        files={
                            "file": (
                                f"{name}.zip",
                                zip_bytes,
                                "application/zip",
                            )
                        },
                    )
                else:
                    resp = await client.post(f"{base_url}/api/skills", json=body)
                resp.raise_for_status()
                result = resp.json()
                result_id = result.get("id") if isinstance(result, dict) else None
                lines = [f"Skill {mode} succeeded."]
                if result_id:
                    lines.append(f"Skill ID: {result_id}")
                lines.append(f"Bundle hash: {_bundle_hash(skill_bundle)}")
                return "\n".join(lines)
            except httpx.HTTPStatusError as e:
                return f"Failed to {mode} skill: {_http_error(e)}"
            except httpx.RequestError as e:
                return f"Failed to connect to orchestrator: {e}"
            except ValueError as e:
                return str(e)

    return [
        list_experts,
        get_expert,
        list_skills,
        search_skills,
        get_skill,
        get_expert_bundle,
        set_expert_bundle,
        get_skill_bundle,
        set_skill_bundle,
    ]
