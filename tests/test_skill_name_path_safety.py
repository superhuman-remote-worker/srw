"""A bound skill's *name* is one path segment, refused before any disk read.

Leftover from the 2026-08-27 audit's adversarial re-pass
(bound_skill_dir_resolution_lacks_path_validation): every path *inside* a
skill bundle went through ``validate_skill_path``, but the skill name itself
was joined straight into ``<deployment_dir>/skills/<skill>`` and
``config/skills/<skill>``. ``pathlib`` does not normalise, so ``../../x``
walked out of the skills root, and an absolute name discarded the left side
of the join entirely. The name arrives through merged config (a bundled
expert, a DB expert row, a job/thread ``config_override``), and the
``SKILL.md`` found there was frozen into ``resolved_config`` and delivered to
the agent. Inert while no such file exists anywhere useful; these tests pin
that it stays inert when one does.
"""

import os
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from orchestrator.services.expert_catalog import ExpertCatalogService
from shared.runtime.core import skill_resolution
from shared.runtime.core.loader import (
    get_project_root,
    load_agent_config_from_dict,
    resolve_bound_skill_dir,
    serialize_resolved_config,
)
from shared.runtime.core.skill_format import (
    SkillFormatError,
    parse_skill_md,
    skill_dir_under,
    skill_identity,
    validate_skill_name,
)

HOSTILE_NAMES = [
    "..",
    ".",
    "",
    "../x",
    "../../secret",
    "a/b",
    "a\\b",
    "/etc",
    "%2e%2e",
    "x?y",
    "x#y",
    "x\x00y",
    "a" * 101,
    " research-guide",
    "research-guide\n",
    "Research-Guide",
    "1st-skill",
    "é-skill",
]

CONFIG = get_project_root() / "config"


def _bundled_skill_dirs() -> list[Path]:
    dirs = sorted(p.parent for p in (CONFIG / "skills").glob("*/SKILL.md"))
    dirs += sorted(p.parent for p in (CONFIG / "experts").glob("*/skills/*/SKILL.md"))
    assert len(dirs) >= 15  # guard against a moved tree silently passing
    return dirs


def _bound_skill_names() -> set[str]:
    """Every ``instruction_files[].skill`` any bundled YAML binds."""

    names: set[str] = set()

    def walk(node):
        if isinstance(node, dict):
            for key, value in node.items():
                if key == "instruction_files" and isinstance(value, list):
                    names.update(
                        entry["skill"]
                        for entry in value
                        if isinstance(entry, dict) and entry.get("skill")
                    )
                walk(value)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    for path in CONFIG.rglob("*.yaml"):
        walk(yaml.safe_load(path.read_text(encoding="utf-8")))
    assert {"strategic-phase", "tactical-phase", "verify-before-done"} <= names
    return names


def _planted(tmp_path: Path) -> tuple[Path, Path]:
    """An expert dir, plus a ``SKILL.md`` *outside* its skills root."""
    secret = tmp_path / "secret"
    secret.mkdir()
    (secret / "SKILL.md").write_text("---\nname: secret\n---\nLEAKED\n")
    expert = tmp_path / "experts" / "evil"
    (expert / "skills").mkdir(parents=True)
    return expert, secret


# ---------------------------------------------------------------------------
# The name rule
# ---------------------------------------------------------------------------


class TestValidateSkillName:
    @pytest.mark.parametrize("name", HOSTILE_NAMES)
    def test_rejects(self, name):
        with pytest.raises(SkillFormatError):
            validate_skill_name(name)

    @pytest.mark.parametrize("name", [None, 7, ["x"]])
    def test_non_string_is_refused(self, name):
        with pytest.raises(SkillFormatError):
            validate_skill_name(name)  # type: ignore[arg-type]

    def test_longest_storable_name_passes(self):
        # skills.name is VARCHAR(100).
        assert validate_skill_name("a" * 100) == "a" * 100

    @pytest.mark.parametrize("skill_dir", _bundled_skill_dirs(), ids=str)
    def test_every_bundled_skill_passes(self, skill_dir):
        assert validate_skill_name(skill_dir.name) == skill_dir.name
        fm, _ = parse_skill_md((skill_dir / "SKILL.md").read_text(encoding="utf-8"))
        assert validate_skill_name(skill_identity(fm)[0]) == skill_dir.name

    @pytest.mark.parametrize("name", sorted(_bound_skill_names()))
    def test_every_bundled_binding_passes_and_resolves(self, name):
        assert validate_skill_name(name) == name
        skill_md = resolve_bound_skill_dir(name, None) / "SKILL.md"
        assert skill_md.is_file()

    def test_message_is_bounded(self):
        with pytest.raises(SkillFormatError) as exc:
            validate_skill_name("../" * 10_000)
        assert len(str(exc.value)) < 200


# ---------------------------------------------------------------------------
# The sinks
# ---------------------------------------------------------------------------


class TestResolveBoundSkillDir:
    @pytest.mark.parametrize("name", HOSTILE_NAMES)
    def test_rejects_hostile_names_on_both_branches(self, tmp_path, name):
        expert, _secret = _planted(tmp_path)
        with pytest.raises(SkillFormatError):
            resolve_bound_skill_dir(name, str(expert))
        with pytest.raises(SkillFormatError):
            resolve_bound_skill_dir(name, None)

    def test_traversal_to_a_real_skill_md_is_refused(self, tmp_path):
        expert, secret = _planted(tmp_path)
        rel = os.path.relpath(secret, expert / "skills")
        assert (expert / "skills" / rel / "SKILL.md").is_file()  # the old hole
        with pytest.raises(SkillFormatError):
            resolve_bound_skill_dir(rel, str(expert))

    def test_absolute_name_is_refused(self, tmp_path):
        expert, secret = _planted(tmp_path)
        assert (expert / "skills" / str(secret)) == secret  # the join discards
        with pytest.raises(SkillFormatError):
            resolve_bound_skill_dir(str(secret), str(expert))

    def test_symlinked_skill_dir_escaping_the_root_is_refused(self, tmp_path):
        expert, secret = _planted(tmp_path)
        (expert / "skills" / "evil-skill").symlink_to(secret, target_is_directory=True)
        assert (expert / "skills" / "evil-skill" / "SKILL.md").is_file()
        with pytest.raises(SkillFormatError):
            resolve_bound_skill_dir("evil-skill", str(expert))

    def test_expert_local_skill_still_outranks_bundled(self, tmp_path):
        expert, _secret = _planted(tmp_path)
        local = expert / "skills" / "verify-before-done"
        local.mkdir()
        (local / "SKILL.md").write_text("---\nname: verify-before-done\n---\nx\n")
        assert resolve_bound_skill_dir("verify-before-done", str(expert)) == local
        assert (
            resolve_bound_skill_dir("verify-before-done", None)
            == CONFIG / "skills" / "verify-before-done"
        )


class TestSkillDirUnder:
    def test_returns_the_unresolved_join(self, tmp_path):
        assert skill_dir_under(tmp_path, "word-count") == tmp_path / "word-count"

    def test_missing_dir_is_not_an_error(self, tmp_path):
        # A DB-only bound skill has no bundled dir; the read miss is the
        # caller's (OSError), not a validation failure.
        assert skill_dir_under(tmp_path, "db-only") == tmp_path / "db-only"


class TestSerializeNeverReadsATraversedSkill:
    def test_hostile_binding_is_skipped_not_frozen(self, tmp_path):
        expert, secret = _planted(tmp_path)
        rel = os.path.relpath(secret, expert / "skills")
        cfg = load_agent_config_from_dict(
            {
                "agent_id": "t",
                "display_name": "T",
                "instruction_files": [
                    {"skill": rel, "trigger": "phase_start:tactical"},
                    {"skill": str(secret), "trigger": "phase_start:tactical"},
                    {"skill": "word-count", "trigger": "phase_start:tactical"},
                ],
            }
        )
        cfg._deployment_dir = str(expert)
        # No raise: a raise would abort config resolution for the whole job.
        blob = serialize_resolved_config(cfg)
        frozen = blob["instructions"]
        assert rel not in frozen
        assert str(secret) not in frozen
        assert not any("LEAKED" in str(body) for body in frozen.values())
        # A legitimate sibling binding still freezes.
        assert "Word Count" in frozen["word-count"]


class TestReadBundledSkill:
    @pytest.mark.parametrize("name", ["..", "../secret", "a/b", "%2e%2e", ""])
    def test_rejects_hostile_names(self, tmp_path, name):
        _planted(tmp_path)
        root = tmp_path / "skills"
        root.mkdir()
        with pytest.raises(SkillFormatError):
            skill_resolution._read_bundled_skill(name, skills_root=root)

    def test_reads_a_real_bundled_skill(self):
        fm, files = skill_resolution._read_bundled_skill("present-with-canvas")
        assert skill_identity(fm)[0] == "present-with-canvas"
        assert "SKILL.md" in files


class TestBundledSkillBundle:
    def _catalog(self, config_dir: Path) -> ExpertCatalogService:
        return ExpertCatalogService(
            SimpleNamespace(store=None, state=None, get_config_dir=lambda: config_dir)
        )

    @pytest.mark.parametrize(
        "name", ["..", ".", "../../secret", "a/b", "%2e%2e", "", "x?y"]
    )
    def test_hostile_id_is_not_found(self, tmp_path, name):
        _planted(tmp_path)
        config_dir = tmp_path / "config"
        (config_dir / "skills").mkdir(parents=True)
        # config/skills/../../secret/SKILL.md exists on disk.
        assert (config_dir / "skills" / "../../secret" / "SKILL.md").is_file()
        assert self._catalog(config_dir).bundled_skill_bundle(name) is None

    def test_bundled_skill_still_loads(self):
        bundle = self._catalog(CONFIG).bundled_skill_bundle("word-count")
        assert bundle is not None and bundle["name"] == "word-count"
