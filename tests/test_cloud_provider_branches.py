"""The ratchet on main-cloud provider branches outside the adapters.

``scripts/check_cloud_provider_branches.py`` finds every place outside the
main-cloud adapters that still names a provider to decide something;
``policy/cloud_provider_branches.txt`` is the reviewed inventory. The slice 2
gate of main_cloud_as_connectors.md is that no Python code outside the adapter
modules branches on ``backend_id``: what is left is SQL text bound to
migration 0186, two frozen record-format classes, and the adapters' own
configuration and registry in the modules that hold them. A new site fails
here. The cockpit is outside this gate and still holds two provider branches
for the protected toggle (see the script's docstring); slice 5 removes them.
"""

from __future__ import annotations

import difflib
import importlib.util
import sys
from pathlib import Path

import pytest

from orchestrator.services.cloud import REGISTRY

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPT = REPO_ROOT / "scripts" / "check_cloud_provider_branches.py"
MANIFEST = REPO_ROOT / "policy" / "cloud_provider_branches.txt"


def _load_script():
    spec = importlib.util.spec_from_file_location(
        "check_cloud_provider_branches", SCRIPT
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules["check_cloud_provider_branches"] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def script():
    return _load_script()


@pytest.fixture(scope="module")
def inventory(script):
    return script.collect_sites(), script.read_classifications()


def _scan(script, source: str):
    return script.sites_for_source("synthetic.py", source)


def _kinds(sites) -> list[tuple[str, str, str]]:
    return [(site.qualname, site.kind, site.ids) for site in sites]


# =============================================================================
# The inventory
# =============================================================================


def test_provider_inventory_matches_manifest(script, inventory):
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
            "policy/cloud_provider_branches.txt is stale; run "
            "`python scripts/check_cloud_provider_branches.py --write` and "
            f"review the new sites:\n{diff}"
        )


def test_every_site_has_a_reviewed_classification(script, inventory):
    sites, classifications = inventory
    assert script.problems(sites, classifications) == []


def test_no_code_outside_the_adapters_branches_on_a_provider(script, inventory):
    """The slice 2 gate: only SQL, the frozen record formats and the
    adapters' own configuration and registry remain, each where it belongs."""
    sites, classifications = inventory
    kinds = {classifications[site.key][0] for site in sites}
    assert kinds <= {
        "sql",
        "protected-record",
        "legacy-column",
        "adapter-config",
        "adapter-registry",
    }
    branches = [
        site
        for site in sites
        if classifications[site.key][0] not in script.FROZEN_SITES
        and site.file not in script.CLASS_FILES.get(classifications[site.key][0], ())
        and site.kind != "sql-literal"
    ]
    assert branches == []
    outside_cloud = {
        site.file
        for site in sites
        if classifications[site.key][0] in ("adapter-config", "adapter-registry")
    }
    assert outside_cloud <= {
        "src/orchestrator/services/cloud/__init__.py",
        "src/orchestrator/services/cloud/config.py",
        "src/orchestrator/services/cloud/backend_instance_authority.py",
        "src/orchestrator/services/cloud/ro_probe.py",
        "src/agent/services/cloud_sync/__init__.py",
    }


def test_the_frozen_classes_hold_exactly_their_reviewed_sites(script, inventory):
    sites, classifications = inventory
    for classification, frozen in script.FROZEN_SITES.items():
        found = {
            site.key for site in sites if classifications[site.key][0] == classification
        }
        assert found == frozen, classification


def test_collect_sites_is_deterministic(script):
    assert script.collect_sites() == script.collect_sites()


def test_empty_source_discovery_is_an_error(script, monkeypatch, tmp_path):
    monkeypatch.setattr(script, "SRC", tmp_path)
    with pytest.raises(RuntimeError, match="No Python sources"):
        script.collect_sites()


def test_only_the_adapter_modules_are_allowlisted(script):
    scanned = {path.relative_to(REPO_ROOT).as_posix() for path in script.source_files()}
    assert not scanned & set(script.ALLOWLIST)
    for path in script.ADAPTER_MODULES:
        assert (REPO_ROOT / path).is_file(), path
        assert script.is_allowlisted(path)
    # The rest of both cloud packages is scanned, not exempt.
    for path in (
        "src/orchestrator/services/cloud/__init__.py",
        "src/orchestrator/services/cloud/config.py",
        "src/orchestrator/services/cloud/instance_registry.py",
        "src/agent/services/cloud_sync/__init__.py",
        "src/agent/services/cloud_sync/protected_lower.py",
        "src/orchestrator/services/agent_cloud_mounts.py",
        "src/agent/api/session_workspace.py",
    ):
        assert path in scanned, path
        assert not script.is_allowlisted(path)


def test_the_vocabulary_knows_the_adapters_constants_and_classes(script):
    vocab = script.vocabulary()
    constants = {
        (module, name): provider
        for (module, name), provider in vocab.constants.items()
        if name == "BACKEND_ID"
    }
    assert constants == {
        ("orchestrator.services.cloud.nextcloud", "BACKEND_ID"): "nextcloud",
        ("orchestrator.services.cloud.opencloud", "BACKEND_ID"): "opencloud",
    }
    assert vocab.classes["NextcloudBackend"] == "nextcloud"
    assert vocab.classes["OpenCloudBackend"] == "opencloud"
    assert vocab.classes["NextcloudWorkspaceSync"] == "nextcloud"


def test_provider_ids_are_the_adapter_settings(script):
    ids = script.provider_ids()
    assert set(REGISTRY) <= ids
    assert ids == {"nextcloud", "opencloud", "ms365"}


def test_a_new_provider_extends_the_gate(script, tmp_path):
    settings = tmp_path / "config.py"
    settings.write_text(
        "from typing import Literal\n"
        "class DropboxSettings:\n"
        '    backend_id: Literal["dropbox"] = "dropbox"\n'
    )
    assert script.provider_ids(settings) == {"dropbox"}


# =============================================================================
# The scanner
# =============================================================================


@pytest.mark.parametrize(
    ("statement", "kind"),
    [
        ('if backend.backend_id == "nextcloud": pass', "compare"),
        ('if row.get("backend") != "nextcloud": pass', "compare"),
        ('if cfg.get("vendor") == "nextcloud": pass', "compare"),
        ('if provider in ("nextcloud", "opencloud"): pass', "compare"),
        ('if x in frozenset({"opencloud"}): pass', "compare"),
        (
            'router.for_backend_instance(i, expected_backend_id="nextcloud")',
            "provider-keyword",
        ),
        ('load(backend_override="opencloud")', "provider-keyword"),
        ('ALLOWED = {"nextcloud", "opencloud"}', "collection"),
        ('FIELDS = {"nextcloud": [], "opencloud": []}', "provider-keyed-dict"),
        ('handler = {"nextcloud": f}.get(b)', "provider-keyed-dict"),
        ('cls = REGISTRY["nextcloud"]', "provider-lookup"),
        ('cls = REGISTRY.get("opencloud")', "provider-lookup"),
        ("if isinstance(b, NextcloudBackend): pass", "adapter-isinstance"),
        ("if issubclass(c, (OpenCloudBackend, X)): pass", "adapter-isinstance"),
        ('if b.startswith("nextcloud"): pass', "compare"),
        ('NC = "nextcloud"\nif row["backend"] == NC: pass', "compare"),
        (
            "from orchestrator.services.cloud.nextcloud import BACKEND_ID as NC\n"
            "if row['backend'] == NC: pass",
            "compare",
        ),
        (
            "from orchestrator.services.cloud import nextcloud\n"
            "if row['backend'] == nextcloud.BACKEND_ID: pass",
            "compare",
        ),
        ("if row['backend'] == NextcloudBackend.backend_id: pass", "compare"),
        (
            "from orchestrator.services.cloud.opencloud import BACKEND_ID\n"
            "x.for_backend_instance(i, expected_backend_id=BACKEND_ID)",
            "provider-keyword",
        ),
        ("q = \"SELECT 1 FROM t WHERE ro.backend = 'nextcloud'\"", "sql-literal"),
        ("q = \"SELECT 1 FROM t WHERE backend_id IN ('nextcloud')\"", "sql-literal"),
        (
            "q = \"WHERE main_cloud_backend IS DISTINCT FROM 'opencloud'\"",
            "sql-literal",
        ),
    ],
)
def test_scanner_detects_each_kind_of_branch(script, statement, kind):
    sites = _scan(script, statement)
    assert [site.kind for site in sites] == [kind]


def test_scanner_detects_a_match_on_a_provider(script):
    sites = _scan(
        script,
        'def f(b):\n    match b:\n        case "nextcloud":\n            return 1\n',
    )
    assert _kinds(sites) == [("f", "match", "nextcloud")]


@pytest.mark.parametrize(
    "statement",
    [
        'BACKEND_ID = "nextcloud"',
        'payload = {"backend": "nextcloud", "url": u}',
        "if handle.backend != backend.backend_id: pass",
        'if kind == "project": pass',
        'logger.info("mounted %s", "nextcloud")',
        "q = \"INSERT INTO t (backend) VALUES ('nextcloud')\"",
        'backend_id: Literal["nextcloud"] = "nextcloud"',
        "from orchestrator.services.cloud.nextcloud import BACKEND_ID",
        "payload = {'backend': BACKEND_ID}",
        "if isinstance(b, dict): pass",
    ],
)
def test_scanner_leaves_data_and_other_literals_alone(script, statement):
    assert _scan(script, statement) == []


def test_a_docstring_quoting_sql_is_not_a_site(script):
    sites = _scan(
        script,
        'def f():\n    """Reads WHERE backend = \'nextcloud\'."""\n    return 1\n',
    )
    assert sites == []


def test_a_comparison_reports_its_collection_once(script):
    sites = _scan(script, 'ok = b in {"nextcloud", "opencloud"}')
    assert _kinds(sites) == [("<module>", "compare", "nextcloud,opencloud")]


def test_reshaping_a_branch_mints_an_unclassified_site(script, inventory):
    _sites, classifications = inventory
    before = _scan(script, 'def f(b):\n    return b == "nextcloud"\n')
    after = _scan(script, 'def f(b):\n    return str(b) == "nextcloud"\n')
    assert before[0].key != after[0].key
    assert after[0].key not in classifications


def test_identical_branches_in_one_scope_are_told_apart_by_ordinal(script):
    sites = _scan(
        script,
        'def f(b):\n    x = b == "nextcloud"\n    y = b == "nextcloud"\n',
    )
    assert [site.ordinal for site in sites] == [1, 2]


# =============================================================================
# Review rules
# =============================================================================


def test_review_problems_are_reported(script):
    site = _scan(script, 'def f(b):\n    return b == "nextcloud"\n')[0]
    assert script.problems([site], {}) == [
        "unclassified: synthetic.py f (compare nextcloud)"
    ]
    frozen = script.problems([site], {site.key: ("protected-record", "a reason")})
    assert frozen and "frozen at its reviewed sites" in frozen[0]
    assert (
        "sql classifies SQL text only"
        in script.problems([site], {site.key: ("sql", "a reason")})[0]
    )
    assert (
        "unknown classification"
        in script.problems([site], {site.key: ("legacy-pending", "a reason")})[0]
    )
    assert (
        "is allowed only in"
        in script.problems([site], {site.key: ("adapter-config", "a reason")})[0]
    )


def test_a_classification_needs_a_reason(script):
    site = _scan(script, "q = \"WHERE backend = 'nextcloud'\"")[0]
    assert script.problems([site], {site.key: ("sql", "")}) == [
        "sql without a reason: synthetic.py <module> (sql-literal nextcloud)"
    ]


def test_the_manifest_round_trips_with_reasons(script, inventory):
    sites, classifications = inventory
    rendered = script.render_manifest(sites, classifications)
    assert script.read_classifications(rendered) == classifications


def test_malformed_and_duplicate_manifest_lines_are_refused(script):
    with pytest.raises(ValueError, match="malformed"):
        script.read_classifications("a  b  c\n")
    line = "f.py  q  compare  nextcloud  abc  #1  sql  r\n"
    with pytest.raises(ValueError, match="duplicate"):
        script.read_classifications(line + line)


def test_every_classification_is_explained_in_the_manifest_header(script):
    for classification in script.ALLOWED_CLASSIFICATIONS:
        assert classification in script.HEADER
