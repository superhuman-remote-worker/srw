"""SKILL.md open-standard (agentskills.io) parsing + packaging for SRW skills.

A skill is a directory: a required SKILL.md (YAML frontmatter + markdown body)
plus optional reference/script/asset files. The SKILL.md is the canonical
artifact — we store its bytes verbatim and only PARSE it to denormalize
name/description onto the skills row. This module is pure (no DB, no FastAPI):
parse, validate paths, and pack/unpack the native zip used for import/export.

Design: knowledge-base/knowledge/features/agent_skills.md (Slice 1).
"""

from __future__ import annotations

import io
import re
import zipfile
from pathlib import Path
from typing import Any

import yaml

SKILL_MD = "SKILL.md"
# Skill name slug — same shape as expert names (see ExpertCreate in main.py).
_NAME_RE = re.compile(r"^[a-z][a-z0-9_-]*$")
# Leading '---' line, YAML, closing '---' line, then the body.
_FRONTMATTER_RE = re.compile(r"^---\n(.*?)\n---\n?(.*)$", re.DOTALL)
# Fixed timestamp so import->export is byte-reproducible (zip stores mtime).
_ZIP_EPOCH = (1980, 1, 1, 0, 0, 0)


class SkillFormatError(ValueError):
    """Raised when a skill bundle or SKILL.md is malformed (maps to HTTP 422)."""


def parse_skill_md(text: str) -> tuple[dict[str, Any], str]:
    """Split a SKILL.md into (frontmatter dict, body str)."""
    m = _FRONTMATTER_RE.match(text)
    if not m:
        raise SkillFormatError(
            "SKILL.md must start with a '---' YAML frontmatter block"
        )
    try:
        fm = yaml.safe_load(m.group(1)) or {}
    except yaml.YAMLError as e:
        raise SkillFormatError(f"SKILL.md frontmatter is not valid YAML: {e}") from e
    if not isinstance(fm, dict):
        raise SkillFormatError("SKILL.md frontmatter must be a mapping")
    return fm, m.group(2)


#: Frontmatter ``catalog: hidden`` — the skill is never offered in the
#: model-invoked menu or the skills list; it reaches an agent only through a
#: deterministic binding (the worker's phase skills). Still readable with
#: ``use_skill`` once bound and materialised.
CATALOG_HIDDEN = "hidden"


def is_catalog_hidden(frontmatter: dict[str, Any]) -> bool:
    """Whether a skill's frontmatter opts it out of every catalog."""
    return str(frontmatter.get("catalog", "") or "").strip().lower() == CATALOG_HIDDEN


def skill_body(text: str) -> str:
    """The body of a SKILL.md (frontmatter stripped); ``text`` unchanged when
    it carries no frontmatter block. Used where a skill's *instructions* are
    delivered rather than its file (the phase block, the fork bundle)."""
    try:
        _fm, body = parse_skill_md(text)
    except SkillFormatError:
        return text
    return body.lstrip("\n")


def skill_identity(frontmatter: dict[str, Any]) -> tuple[str, str]:
    """Extract and validate (name, description) from frontmatter."""
    name = str(frontmatter.get("name", "")).strip()
    if not _NAME_RE.match(name):
        raise SkillFormatError(
            f"SKILL.md 'name' must match ^[a-z][a-z0-9_-]*$ (got {name!r})"
        )
    description = str(frontmatter.get("description", "") or "").strip()
    return name, description


#: ``skills.name`` is VARCHAR(100) (migration 0031): no longer name was ever
#: stored or bundled.
MAX_SKILL_NAME_LENGTH = 100


def validate_skill_name(name: str) -> str:
    """Return ``name`` if it is a usable skill name, else raise.

    A skill name is exactly one path segment (``config/skills/<name>``,
    ``skills/<name>/SKILL.md`` in the workspace), so it must be the same slug
    :func:`skill_identity` enforces on a SKILL.md, at most
    :data:`MAX_SKILL_NAME_LENGTH` characters. That charset has no ``.``,
    ``/``, ``\\``, ``%``, ``?``, ``#`` or control character: a validated name
    can neither traverse nor carry an encoded or URL-significant byte.
    """
    if not isinstance(name, str):
        raise SkillFormatError(
            f"skill name must be a string, not {type(name).__name__}"
        )
    if len(name) > MAX_SKILL_NAME_LENGTH or not _NAME_RE.fullmatch(name):
        raise SkillFormatError(f"illegal skill name: {name[:60]!r}")
    return name


def skill_dir_under(root: Path, name: str) -> Path:
    """``root / name`` for a validated skill ``name``, confined to ``root``.

    The name check alone keeps the join one segment deep; the resolve check
    also refuses a skill directory that is a symlink out of ``root``, so
    whatever ``SKILL.md`` is read from the result lives under the skills root.
    The unresolved join is returned (the directory need not exist: a bound
    DB-only skill has no bundled one, and its read miss is the caller's).
    """
    candidate = Path(root) / validate_skill_name(name)
    if not candidate.resolve().is_relative_to(Path(root).resolve()):
        raise SkillFormatError(f"skill {name!r} resolves outside its skills root")
    return candidate


def validate_skill_path(path: str) -> str:
    """Return a safe relative path, or raise. Rejects abs/traversal/empty/backslash."""
    if not path or path.strip() != path or path.endswith("/"):
        raise SkillFormatError(f"illegal file path: {path!r}")
    if path.startswith("/") or "\\" in path or "\x00" in path:
        raise SkillFormatError(f"illegal file path: {path!r}")
    if any(seg in ("", ".", "..") for seg in path.split("/")):
        raise SkillFormatError(f"illegal file path: {path!r}")
    return path


def validate_skill_files(files: dict[str, str]) -> None:
    """Require a root SKILL.md and safe paths everywhere."""
    if SKILL_MD not in files:
        raise SkillFormatError("a skill must contain a SKILL.md at its root")
    for path in files:
        validate_skill_path(path)


def pack_skill_zip(name: str, files: dict[str, str]) -> bytes:
    """Pack files into a deterministic zip under a top-level <name>/ dir."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for path in sorted(files):
            info = zipfile.ZipInfo(f"{name}/{path}", date_time=_ZIP_EPOCH)
            info.external_attr = 0o644 << 16
            zf.writestr(info, files[path])
    return buf.getvalue()


def unpack_skill_zip(data: bytes) -> dict[str, str]:
    """Unpack a skill zip into a path->content map rooted at the skill dir."""
    try:
        zf = zipfile.ZipFile(io.BytesIO(data))
    except zipfile.BadZipFile as e:
        raise SkillFormatError(f"not a valid zip archive: {e}") from e
    names = [n for n in zf.namelist() if not n.endswith("/")]
    if not names:
        raise SkillFormatError("zip archive is empty")
    tops = {n.split("/", 1)[0] for n in names}
    strip = len(tops) == 1 and all("/" in n for n in names)
    prefix = f"{next(iter(tops))}/" if strip else ""
    files: dict[str, str] = {}
    for n in names:
        rel = n[len(prefix) :] if prefix and n.startswith(prefix) else n
        validate_skill_path(rel)
        try:
            files[rel] = zf.read(n).decode("utf-8")
        except UnicodeDecodeError as e:
            raise SkillFormatError(f"{rel}: only UTF-8 text files are supported") from e
    validate_skill_files(files)
    return files


def set_skill_name(text: str, new_name: str) -> str:
    """Rewrite frontmatter 'name:' to new_name, preserving the body. For forks."""
    m = _FRONTMATTER_RE.match(text)
    if not m:
        raise SkillFormatError(
            "SKILL.md must start with a '---' YAML frontmatter block"
        )
    block, body = m.group(1), m.group(2)
    new_block, n = re.subn(r"(?m)^name:.*$", f"name: {new_name}", block, count=1)
    if n == 0:
        new_block = f"name: {new_name}\n{block}"
    return f"---\n{new_block}\n---\n{body}"
