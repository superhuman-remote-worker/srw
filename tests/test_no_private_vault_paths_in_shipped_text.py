"""Text that ships to readers never points into the private design vault.

The design notes live in private repositories cloned at knowledge-base/ and
knowledge-history/. Users, operators, API integrators and the agent cannot
open them, so a pointer there is a dead end: the Automations page once linked
its "External API guide" to a note that had moved into the vault.

Code comments may keep their vault pointers; these tests cover only the text
that reaches a reader:

- helm/values.yaml, whole file: `helm show values` prints every comment, and the
  `# --` comments become the public Helm values reference;
- helm/values.schema.json descriptions (also on that reference);
- helm/templates outside template and YAML comments: NOTES, `fail` messages,
  init-container output, Keycloak client descriptions;
- config/: expert and overlay string values (comments are dropped), prompts,
  skills, templates and the two config schemas the Config Editor shows;
- the Cockpit's translations;
- the public docs (docs/, helm/README.md, ssh-access.md, README.md);
- the OpenAPI document served at /api/docs, built from route docstrings.

Bare design-note names ("officer_post.md §5") are dead ends too. They are
matched as lowercase words joined by underscores, which real workspace files
in the same text also use, so each surface lists the ones it may name.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parent.parent

VAULT_PATH = re.compile(r"knowledge-(?:base|history)/")
NOTE_NAME = re.compile(r"\b[a-z]+(?:_[a-z]+)+\.md\b")
PRIVATE_REPO = re.compile(r"\bHomeLab\b")

# Real files a reader may be told about, not design notes.
SCHEMA_FILES = {"todo_guide.md", "review_guide.md"}
OPENAPI_FILES = {"task_brief.md"}


def _offences(
    text: str, *, note_names: bool, allowed: frozenset[str] = frozenset()
) -> list[str]:
    found = [m.group(0) for m in VAULT_PATH.finditer(text)]
    found += [m.group(0) for m in PRIVATE_REPO.finditer(text)]
    if note_names:
        found += [
            m.group(0) for m in NOTE_NAME.finditer(text) if m.group(0) not in allowed
        ]
    return found


def _strings(node: object) -> list[str]:
    if isinstance(node, str):
        return [node]
    if isinstance(node, dict):
        return [
            s for key, value in node.items() for s in _strings(key) + _strings(value)
        ]
    if isinstance(node, list):
        return [s for item in node for s in _strings(item)]
    return []


def _line_hits(path: Path, *, note_names: bool, allowed=frozenset()) -> list[str]:
    hits = []
    for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        for offence in _offences(
            line, note_names=note_names, allowed=frozenset(allowed)
        ):
            hits.append(f"{path.relative_to(ROOT)}:{number}: {offence}")
    return hits


def test_helm_values_file_names_no_vault_note() -> None:
    assert _line_hits(ROOT / "helm/values.yaml", note_names=True) == []


def test_helm_values_schema_descriptions_name_no_vault_note() -> None:
    schema = json.loads((ROOT / "helm/values.schema.json").read_text(encoding="utf-8"))
    text = "\n".join(_strings(schema))
    assert _offences(text, note_names=True) == []


_TEMPLATE_COMMENT = re.compile(r"\{\{-?\s*/\*.*?\*/\s*-?\}\}", re.DOTALL)


def test_helm_template_output_names_no_vault_note() -> None:
    hits = []
    for path in sorted((ROOT / "helm/templates").rglob("*")):
        if not path.is_file():
            continue
        text = _TEMPLATE_COMMENT.sub(
            lambda m: "\n" * m.group(0).count("\n"), path.read_text(encoding="utf-8")
        )
        for number, line in enumerate(text.splitlines(), 1):
            if line.lstrip().startswith("#"):
                continue
            for offence in _offences(line, note_names=False):
                hits.append(f"{path.relative_to(ROOT)}:{number}: {offence}")
    assert hits == []


def test_config_text_the_agent_or_the_editor_reads_names_no_vault_note() -> None:
    hits = []
    for path in sorted((ROOT / "config").rglob("*")):
        if not path.is_file():
            continue
        rel = path.relative_to(ROOT)
        if path.suffix in {".yaml", ".yml"}:
            # Comments are for developers; every string value can reach the
            # agent, the expert picker or the Config Editor.
            for document in yaml.safe_load_all(path.read_text(encoding="utf-8")):
                for text in _strings(document):
                    hits += [f"{rel}: {o}" for o in _offences(text, note_names=False)]
        elif path.name == "schema.json":
            hits += _line_hits(path, note_names=True, allowed=SCHEMA_FILES)
        elif path.suffix in {".md", ".txt", ".json", ".j2", ".jinja"}:
            hits += _line_hits(path, note_names=False)
    assert hits == []


def test_config_editor_schema_names_no_vault_note() -> None:
    path = ROOT / "cockpit/src/assets/schema.json"
    assert _line_hits(path, note_names=True, allowed=SCHEMA_FILES) == []


@pytest.mark.parametrize("name", ["en.json", "de-DE.json"])
def test_cockpit_translations_name_no_vault_note(name: str) -> None:
    assert _line_hits(ROOT / "cockpit/src/assets/i18n" / name, note_names=False) == []


def test_public_docs_name_no_vault_note() -> None:
    paths = sorted((ROOT / "docs").rglob("*.md"))
    paths += [ROOT / "helm/README.md", ROOT / "ssh-access.md", ROOT / "README.md"]
    hits = [hit for path in paths for hit in _line_hits(path, note_names=False)]
    assert hits == []


def test_openapi_document_names_no_vault_note() -> None:
    from fastapi.testclient import TestClient

    import orchestrator.main

    spec = TestClient(orchestrator.main.app).get("/api/openapi.json").json()
    text = "\n".join(_strings(spec))
    assert _offences(text, note_names=True, allowed=frozenset(OPENAPI_FILES)) == []
