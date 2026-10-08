"""The ratchet on connector-type branches outside the connector drivers.

``scripts/check_connector_type_branches.py`` finds every place outside a
driver package that still decides by comparing a connector type;
``policy/connector_type_branches.txt`` is the reviewed inventory. A new site
fails here as ``unclassified``; a converted one drops out when the manifest is
regenerated. Slice D1's gate was that ``legacy-pending`` is empty; it is,
and the classification is retired, so no branch can be parked there again.
"""

from __future__ import annotations

import difflib
import importlib.util
import re
import sys
from dataclasses import replace
from pathlib import Path

import pytest

from orchestrator.services.datasource_policy import (
    LITE_WORKSPACE_BACKENDS,
    workspace_tier_refuses,
)
from shared.connectors.builtin import BUILTIN_SPECS, DATASOURCE_SPECS, GENERIC_SPEC

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPT = REPO_ROOT / "scripts" / "check_connector_type_branches.py"
MANIFEST = REPO_ROOT / "policy" / "connector_type_branches.txt"
#: The frozen baseline that let ``legacy-pending`` shrink (63 sites on
#: 2026-10-08, 24 after D1b, none after D1c). It is gone with the class.
BASELINE = REPO_ROOT / "policy" / "connector_type_branches_baseline.txt"


def _load_script():
    spec = importlib.util.spec_from_file_location(
        "check_connector_type_branches", SCRIPT
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules["check_connector_type_branches"] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def script():
    return _load_script()


@pytest.fixture(scope="module")
def inventory(script):
    return script.collect_sites(), script.read_classifications()


def _scan(script, source: str, type_ids=None):
    return script.sites_for_source("synthetic.py", source, type_ids)


def _kinds(sites) -> list[tuple[str, str, str]]:
    return [(site.qualname, site.kind, site.ids) for site in sites]


# =============================================================================
# The inventory
# =============================================================================


def test_connector_type_inventory_matches_manifest(script, inventory):
    sites, classifications = inventory
    rendered = script.render_manifest(sites, classifications)
    on_disk = MANIFEST.read_text()
    if rendered != on_disk:
        diff = "\n".join(
            difflib.unified_diff(
                on_disk.splitlines(),
                rendered.splitlines(),
                fromfile=str(MANIFEST.relative_to(REPO_ROOT)),
                tofile="<regenerated>",
                lineterm="",
            )
        )
        pytest.fail(
            "The connector-type inventory is stale. Convert a new branch to a "
            "spec flag or a driver capability, or review and classify it, then "
            "run `python scripts/check_connector_type_branches.py --write`. "
            "Converted sites drop out on --write. Diff:\n" + diff
        )


def test_every_site_has_a_reviewed_classification(script, inventory):
    sites, classifications = inventory
    assert script.problems(sites, classifications) == []
    assert set(classifications) == {site.key for site in sites}


def test_no_site_is_legacy_pending_any_more(script, inventory):
    """The D1 gate: every branch outside the drivers is converted or is
    something the remaining classifications explain."""
    sites, classifications = inventory
    pending = [
        f"{site.file} {site.qualname} ({site.kind} {site.ids})"
        for site in sites
        if classifications[site.key][0] == script.LEGACY_PENDING
    ]
    assert pending == []
    assert script.LEGACY_PENDING not in script.ALLOWED_CLASSIFICATIONS
    assert not BASELINE.exists()
    assert not hasattr(script, "BASELINE")


def test_collect_sites_is_deterministic(script):
    first = script.collect_sites()
    second = script.collect_sites()
    assert [site.key for site in first] == [site.key for site in second]
    assert len({site.key for site in first}) == len(first)


def test_empty_source_discovery_is_an_error(script, monkeypatch, tmp_path):
    monkeypatch.setattr(script, "SRC", tmp_path)
    with pytest.raises(RuntimeError, match="No Python sources"):
        script.collect_sites()


def test_the_driver_packages_are_allowlisted(script):
    scanned = {path.relative_to(REPO_ROOT).as_posix() for path in script.source_files()}
    assert scanned, "the scanner found no sources"
    assert not [path for path in scanned if path.startswith(script.ALLOWLIST)]
    assert "src/orchestrator/services/datasource_policy.py" in scanned
    assert script.is_allowlisted("src/orchestrator/services/connector_drivers/kb.py")
    assert script.is_allowlisted("src/shared/connectors/builtin.py")
    assert script.is_allowlisted("src/agent/connectors/materializers.py")
    assert not script.is_allowlisted("src/agent/core/datasource_setup.py")


def test_sql_files_the_cockpit_and_prompts_are_outside_the_gate(script):
    suffixes = {path.suffix for path in script.source_files()}
    assert suffixes == {".py"}
    assert all(path.is_relative_to(script.SRC) for path in script.source_files())


# =============================================================================
# Type ids come from the registry
# =============================================================================


def test_type_ids_are_the_specs_types_and_driver_names(script):
    expected = {spec.name for spec in BUILTIN_SPECS} | {
        spec.legacy_type for spec in BUILTIN_SPECS if spec.legacy_type
    }
    assert script.registry_type_ids() == expected
    assert {"postgresql", "kb", "srw.env/v1", "srw.kb/v1"} <= expected


def test_a_new_driver_extends_the_gate(script, monkeypatch):
    import shared.connectors.builtin as builtin

    ftp = replace(GENERIC_SPEC, name="srw.ftp/v1", legacy_type="ftp", title="FTP")
    source = 'def f(ds):\n    if ds["type"] == "ftp":\n        return 1\n'
    assert _scan(script, source) == []
    monkeypatch.setattr(builtin, "BUILTIN_SPECS", (*BUILTIN_SPECS, ftp))
    assert _kinds(_scan(script, source)) == [("f", "compare", "ftp")]


# =============================================================================
# What the scanner finds
# =============================================================================


def test_scanner_detects_a_synthetic_type_branch(script):
    """The self-test the design asks for: a plain branch on a type is caught."""
    source = """
def build(ds):
    if ds["type"] == "postgresql":
        return connect(ds)
"""
    assert _kinds(_scan(script, source)) == [("build", "compare", "postgresql")]


@pytest.mark.parametrize(
    ("statement", "kind", "ids"),
    (
        ('ok = ds.get("type") == "kb"', "compare", "kb"),
        ('ok = row.type != "mcp"', "compare", "mcp"),
        ('ok = driver.type_id == "email"', "compare", "email"),
        ('ok = ds_type == "neo4j"', "compare", "neo4j"),
        ('ok = datasource_type in ("webdav",)', "compare", "webdav"),
        (
            'ok = str(ds.get("type") or "").lower() in {"repository", "credentials"}',
            "compare",
            "credentials,repository",
        ),
        ('ok = (ds["type"] or "").strip() == "kb"', "compare", "kb"),
        ('ok = connector["driver"] == "srw.env/v1"', "compare", "srw.env/v1"),
        # A kind often carries a type (an identity's); it also names other
        # vocabularies, which the manifest classifies.
        ('ok = identity.kind == "ssh_key"', "compare", "ssh_key"),
        ('ok = kind == "repository"', "compare", "repository"),
        ('ok = row["kind"] != "mcp"', "compare", "mcp"),
        (
            'KINDS = frozenset({"kubeconfig", "ssh_key"})',
            "collection",
            "kubeconfig,ssh_key",
        ),
        ('check(("email", "webdav"))', "collection", "email,webdav"),
        (
            'NOTES = {"postgresql": 1, "neo4j": 2, "mongodb": 3}',
            "type-keyed-dict",
            "mongodb,neo4j,postgresql",
        ),
        ('slot = ctx.has_datasource("neo4j")', "registry-slot", "neo4j"),
        ('slot = ctx.get_datasource("email")', "registry-slot", "email"),
        # Planted by the D1c review and missed before: a harness slot read
        # by its type, a spec's legacy_type, a membership test against the
        # attached types, a driver looked up by a literal type.
        ('conn = ctx.datasources.get("neo4j")', "registry-slot", "neo4j"),
        ('conn = ctx.datasources["neo4j"]', "registry-slot", "neo4j"),
        ('conn = datasources_dict.get("mcp")', "registry-slot", "mcp"),
        ('conn = rt.connections.pop("webdav", None)', "registry-slot", "webdav"),
        ('conn = self._datasource_clients["mongodb"]', "registry-slot", "mongodb"),
        ('ok = spec.legacy_type == "neo4j"', "compare", "neo4j"),
        (
            'ok = "email" in facts.attached_datasource_types',
            "type-membership",
            "email",
        ),
        (
            'ok = "kb" not in attached_datasource_types',
            "type-membership",
            "kb",
        ),
        ('spec = spec_for_type("email")', "spec-lookup", "email"),
        ('spec = builtin.spec_for_type("kb")', "spec-lookup", "kb"),
        ('rows = store.list_datasources(ds_type="kb")', "type-keyword", "kb"),
        (
            "sql = \"SELECT 1 FROM datasources d WHERE d.type = 'credentials'\"",
            "sql-literal",
            "credentials",
        ),
        (
            "sql = \"SELECT 1 FROM datasources WHERE type IN ('kb', 'repository')\"",
            "sql-literal",
            "kb,repository",
        ),
    ),
)
def test_scanner_detects_each_kind_of_branch(script, statement, kind, ids):
    sites = _scan(
        script,
        "def f(ds, row, driver, ctx, store, check, spec, facts, rt, builtin,\n"
        "      datasources_dict, attached_datasource_types, self):\n"
        f"    {statement}\n",
    )
    assert _kinds(sites) == [("f", kind, ids)]


def test_scanner_detects_a_match_on_a_type(script):
    source = """
def route(ds):
    match ds["type"]:
        case "postgresql" | "neo4j":
            return 1
        case "other":
            return 2
"""
    assert _kinds(_scan(script, source)) == [("route", "match", "neo4j,postgresql")]


@pytest.mark.parametrize(
    "statement",
    (
        # A tool category coincides with a type id.
        'ok = category == "email"',
        # One type id is no collection of types; two keys are no type map.
        'ONE = ("email",)',
        'TWO = {"postgresql": 1, "neo4j": 2}',
        # A slot looked up by a variable names no type.
        "slot = ctx.get_datasource(datasource_id)",
        "conn = ctx.datasources.get(slot)",
        "spec = spec_for_type(row.type)",
        # Another mapping read by a word that happens to be a type id.
        'value = row.get("email")',
        'value = config["kb"]',
        # A tool category tested against the categories a context binds.
        'ok = "email" in tool_categories',
        # Prose about a type is not SQL testing one.
        'text = "the type of a kb row"',
    ),
)
def test_scanner_leaves_other_literals_alone(script, statement):
    assert _scan(script, f"def f(row, ctx, datasource_id):\n    {statement}\n") == []


def test_a_docstring_quoting_sql_is_not_a_site(script):
    source = '''
def f():
    """Rows WHERE d.type = 'kb' are skipped."""
    return 1
'''
    assert _scan(script, source) == []


def test_a_comparison_reports_its_collection_once(script):
    source = 'def f(ds):\n    return ds["type"] in ("repository", "credentials")\n'
    assert _kinds(_scan(script, source)) == [("f", "compare", "credentials,repository")]


def test_sites_carry_their_qualname(script):
    source = """
class Delivery:
    def attach(self, ds):
        def inner():
            return ds.type == "kb"
        return inner
"""
    assert _kinds(_scan(script, source)) == [("Delivery.attach.inner", "compare", "kb")]


# =============================================================================
# Site identity
# =============================================================================

_TWO_BRANCHES = """
def deliver(ds):
    if ds["type"] == "repository":
        clone(ds)
    if ds["type"] == "kb":
        index(ds)
"""

_TWO_BRANCHES_MOVED = """
import os


def helper():
    return os.getcwd()


def deliver(ds):
    log("delivering")
    if ds["type"] == "kb":
        index(ds)
    if ds["type"] == "repository":
        clone(ds)
"""


def test_moving_code_keeps_every_classification(script):
    original = _scan(script, _TWO_BRANCHES)
    moved = _scan(script, _TWO_BRANCHES_MOVED)
    assert len(original) == 2
    assert {site.key for site in original} == {site.key for site in moved}


def test_reshaping_a_branch_mints_an_unclassified_site(script):
    original = _scan(script, _TWO_BRANCHES)
    reshaped = _scan(script, _TWO_BRANCHES.replace('"kb"', '"email"'))
    reviewed = {site.key: ("kb-domain", "reviewed") for site in original}
    rendered = script.render_manifest(reshaped, reviewed)
    assert [line.split("  ")[6] for line in _body(rendered)] == [
        "kb-domain",
        "unclassified",
    ]


def test_identical_branches_in_one_scope_are_told_apart_by_ordinal(script):
    source = """
def f(ds, other):
    if ds["type"] == "kb":
        pass
    if ds["type"] == "kb":
        pass
"""
    sites = _scan(script, source)
    assert [site.ordinal for site in sites] == [1, 2]
    assert len({site.key for site in sites}) == 2


def test_a_new_type_in_an_old_collection_needs_review(script):
    source = 'KINDS = ("ftp", "ssh_key", "kubeconfig")\n'
    before = _scan(script, source)
    after = _scan(script, source, script.registry_type_ids() | {"ftp"})
    assert {site.key for site in before}.isdisjoint({site.key for site in after})


# =============================================================================
# The manifest format
# =============================================================================


def _body(rendered: str) -> list[str]:
    return [line for line in rendered.splitlines() if line and not line.startswith("#")]


def test_the_manifest_round_trips_with_reasons(script):
    sites = _scan(script, _TWO_BRANCHES)
    classifications = {
        sites[0].key: ("kb-domain", "the  KB indexer   decides"),
        sites[1].key: ("sql", "a type test in SQL"),
    }
    rendered = script.render_manifest(sites, classifications)
    assert script.read_classifications(rendered) == {
        sites[0].key: ("kb-domain", "the KB indexer decides"),
        sites[1].key: ("sql", "a type test in SQL"),
    }


def test_malformed_and_duplicate_manifest_lines_are_refused(script):
    with pytest.raises(ValueError, match="malformed"):
        script.read_classifications("src/x.py  f  compare\n")
    line = "src/x.py  f  compare  kb  abc123abc123  #1  sql  why\n"
    with pytest.raises(ValueError, match="duplicate"):
        script.read_classifications(line + line)


def test_a_legacy_pending_site_fails_with_or_without_a_reason(script):
    """Hand-classifying a new branch legacy-pending never gets it past."""
    sites = _scan(script, _TWO_BRANCHES)
    for reason in ("", "later"):
        pending = {site.key: ("legacy-pending", reason) for site in sites}
        assert script.problems(sites, pending) == [
            "legacy-pending is retired (convert the branch): synthetic.py "
            f"deliver (compare {site.ids})"
            for site in sites
        ]


def test_pending_d3_d4_is_frozen_at_its_reviewed_sites(script, inventory):
    """Like the legacy-pending baseline: the class cannot take a new site."""
    sites, classifications = inventory
    reviewed = {
        site.key for site in sites if classifications[site.key][0] == "pending-d3-d4"
    }
    assert reviewed == script.PENDING_D3_D4_SITES
    new = _scan(script, _TWO_BRANCHES)
    assert script.problems(
        new, {site.key: ("pending-d3-d4", "needs D3") for site in new}
    ) == [
        "pending-d3-d4 is frozen at its reviewed sites (convert the branch): "
        f"synthetic.py deliver (compare {site.ids})"
        for site in new
    ]


def test_review_problems_are_reported(script):
    sites = _scan(script, _TWO_BRANCHES)
    assert script.problems(sites, {}) == [
        f"unclassified: synthetic.py deliver (compare {site.ids})" for site in sites
    ]
    problems = script.problems(
        sites,
        {sites[0].key: ("sql", ""), sites[1].key: ("pending", "why")},
    )
    assert problems == [
        "sql without a reason: synthetic.py deliver (compare repository)",
        "unknown classification 'pending': synthetic.py deliver (compare kb)",
    ]


def test_every_classification_is_explained_in_the_manifest_header(script):
    header = MANIFEST.read_text().split("\n\n", 1)[0]
    for classification in script.ALLOWED_CLASSIFICATIONS:
        assert re.search(rf"#\s+{re.escape(classification)}\s", header), classification


# =============================================================================
# Copies outside the gate that must agree with the specs
# =============================================================================


def test_the_cockpit_picker_refuses_what_the_server_refuses_on_lite_tiers():
    """The picker's copy of the lite-tier rule, until D2 reads the specs."""
    source = (
        REPO_ROOT
        / "cockpit/src/app/views/agent-settings/datasources-group.component.ts"
    ).read_text()
    match = re.search(r"SHELL_WORKSPACE_TYPES = new Set\(\[([^\]]*)\]\)", source)
    assert match, "datasources-group no longer declares SHELL_WORKSPACE_TYPES"
    cockpit = set(re.findall(r"'([a-z_]+)'", match.group(1)))
    server = {
        spec.legacy_type
        for spec in DATASOURCE_SPECS
        if any(
            workspace_tier_refuses({"type": spec.legacy_type}, backend)
            for backend in LITE_WORKSPACE_BACKENDS
        )
    }
    # Credential files joined the shell drivers in D1d.
    needs_shell = {
        "repository",
        "credentials",
        "generic",
        "ssh_key",
        "kubeconfig",
        "generic_file",
    }
    assert cockpit == server == needs_shell
