"""Dependency drift must fail before a stale or incompatible image can ship."""

import ast
import importlib.metadata
import importlib.util
import json
from pathlib import Path
import re
import shlex
import shutil
import sys

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "dependency_locks", ROOT / "scripts/lock_dependencies.py"
)
locks = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(locks)

#: The packages the orchestrator image copies in beside its locked environment.
ORCHESTRATOR_IMAGE_PACKAGES = {"orchestrator", "shared"}


def third_party_imports(package: Path) -> dict[str, set[str]]:
    """Top-level third-party import names under ``package``, by importing file.

    Function-local imports count: a lazy import of a missing package still
    boots, then fails every call (the connector probes' "connection failed").
    """
    found: dict[str, set[str]] = {}
    for path in sorted(package.rglob("*.py")):
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if isinstance(node, ast.Import):
                modules = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom) and node.level == 0:
                modules = [node.module]
            else:
                continue
            for module in modules:
                name = module.split(".")[0]
                if (
                    name not in sys.stdlib_module_names
                    and name not in ORCHESTRATOR_IMAGE_PACKAGES
                ):
                    found.setdefault(name, set()).add(
                        path.relative_to(package).as_posix()
                    )
    return found


def unlocked_imports(imports: dict[str, set[str]], pins: dict[str, str]) -> list:
    """Import names no pinned distribution provides, with their importers."""
    providers = importlib.metadata.packages_distributions()
    missing = []
    for name, importers in sorted(imports.items()):
        distributions = {name, *providers.get(name, ())}
        if not any(
            re.sub(r"[-_.]+", "-", distribution).lower() in pins
            for distribution in distributions
        ):
            missing.append((name, sorted(importers)))
    return missing


@pytest.fixture
def staged(tmp_path):
    for role in locks.ROLES:
        for name in (*locks.COMMON_INPUTS, *locks.paths(role)):
            target = tmp_path / name
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / name, target)
    return tmp_path


@pytest.mark.parametrize("role", locks.ROLES)
def test_repository_locks_match_all_current_inputs(role):
    assert locks.check(role)["packages"] > 0


@pytest.mark.parametrize("role", locks.ROLES)
@pytest.mark.parametrize("changed", (*locks.COMMON_INPUTS, "declaration", "lock"))
def test_input_or_output_edit_requires_regeneration(staged, role, changed):
    declaration, lock, _ = locks.paths(role)
    name = {"declaration": declaration, "lock": lock}.get(changed, changed)
    with (staged / name).open("a") as file:
        file.write("\n# changed since resolution\n")
    with pytest.raises(ValueError, match="stale lock"):
        locks.check(role, staged)


def test_role_manifest_cannot_be_swapped(staged):
    manifests = [locks.paths(role)[2] for role in locks.ROLES]
    shutil.copyfile(staged / manifests[0], staged / manifests[1])
    with pytest.raises(ValueError, match="stale lock"):
        locks.check("vm-controller", staged)


def test_nested_requirement_include_is_fingerprinted(staged):
    declaration = staged / locks.paths("orchestrator")[0]
    with declaration.open("a") as file:
        file.write("\n-r ../../requirements/extra.txt\n")
    extra = staged / "requirements/extra.txt"
    extra.write_text("httpx>=0.27\n")
    before = locks.inputs("orchestrator", staged)
    extra.write_text("httpx>=0.28\n")
    assert before != locks.inputs("orchestrator", staged)
    assert "requirements/extra.txt" in before


def test_wheel_only_lock_option_and_hashes_are_not_package_pins(tmp_path):
    lock = tmp_path / "lock.txt"
    lock.write_text(
        "--only-binary :all:\nhttpx==0.28.1 \\\n    --hash=sha256:fixture\n"
    )
    assert locks.pinned_versions(lock) == {"httpx": "0.28.1"}


def test_installed_version_mismatch_fails(staged, monkeypatch):
    monkeypatch.setattr(locks, "check_platform", lambda role: None)
    monkeypatch.setattr(locks.importlib.metadata, "version", lambda name: "0.0.0")
    with pytest.raises(ValueError, match="installed .* expected"):
        locks.check("orchestrator", staged, installed=True)


def test_wrong_python_target_fails(monkeypatch):
    monkeypatch.setattr(locks.sys, "version_info", (3, 12, 0))
    with pytest.raises(ValueError, match="requires Linux amd64 Python 3.11"):
        locks.check_platform("orchestrator")


@pytest.mark.parametrize("role", locks.ROLES)
def test_lock_inputs_invalidate_tilt_and_develop_image_identity(role):
    build = next(
        node
        for node in ast.walk(ast.parse((ROOT / "Tiltfile").read_text()))
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "docker_build"
        and node.args
        and isinstance(node.args[0], ast.Constant)
        and node.args[0].value == f"srw-{role}"
    )
    watched = ast.literal_eval(
        next(item.value for item in build.keywords if item.arg == "only")
    )
    workflow = (ROOT / ".github/workflows/develop.yml").read_text()
    variable = role.upper().replace("-", "_")
    match = re.search(
        rf"^\s*{variable}_PATHS=\((.*?)\)", workflow, re.MULTILINE | re.DOTALL
    )
    identity_inputs = shlex.split(match.group(1))
    groups = [watched, identity_inputs]
    if role == "orchestrator":
        fallback = next(
            node
            for node in ast.walk(build)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "fall_back_on"
        )
        groups.append(ast.literal_eval(fallback.args[0]))
    for name in (*locks.COMMON_INPUTS, *locks.paths(role)):
        for group in groups:
            assert any(
                name == path or (path.endswith("/") and name.startswith(path))
                for path in group
            ), (name, group)


@pytest.mark.parametrize("workflow", ("main", "develop"))
def test_ci_role_checks_gate_existing_build_and_publication_path(workflow):
    jobs = yaml.safe_load((ROOT / f".github/workflows/{workflow}.yml").read_text())[
        "jobs"
    ]
    assert "dependency-locks" in jobs["architecture"]["needs"]
    role_job = jobs["dependency-locks"]
    for item in role_job["strategy"]["matrix"]["include"]:
        assert item["python"] == locks.ROLES[item["role"]][1]
        assert item["lock"] == locks.paths(item["role"])[1]
    commands = "\n".join(step.get("run", "") for step in role_job["steps"])
    assert "--require-hashes -r ${{ matrix.lock }}" in commands
    assert "check ${{ matrix.role }} --installed" in commands
    assert "pip check" in commands
    for name, job in jobs.items():
        if name.startswith("build-"):
            assert "architecture" in job["needs"], name


@pytest.mark.parametrize("role", locks.ROLES)
def test_shared_sdk_policy_matches_both_resolutions(role):
    pins = locks.pinned_versions(ROOT / locks.paths(role)[1])
    policy = locks.pinned_versions(ROOT / "requirements/constraints.txt")
    assert {name: pins[name] for name in policy} == policy
    manifest = json.loads((ROOT / locks.paths(role)[2]).read_text())
    assert manifest["platform"] == "linux/amd64"


def test_orchestrator_imports_resolve_against_its_lock():
    """Every third-party package an orchestrator module imports is in its image."""
    pins = locks.pinned_versions(ROOT / locks.paths("orchestrator")[1])
    imports = third_party_imports(ROOT / "src/orchestrator")
    assert unlocked_imports(imports, pins) == []


def test_import_scan_sees_the_lazy_connector_probe_imports():
    imports = third_party_imports(ROOT / "src/orchestrator")
    probes = "services/connector_drivers/"
    for name in ("neo4j", "pymongo", "webdav3"):
        assert probes + "managed.py" in imports[name]
    assert probes + "mcp_client.py" in imports["mcp"]


def test_lazy_import_missing_from_the_lock_is_reported(tmp_path):
    (tmp_path / "probe.py").write_text(
        "import httpx\n\n\ndef probe():\n    from neo4j import GraphDatabase\n"
    )
    imports = third_party_imports(tmp_path)
    assert unlocked_imports(imports, {"httpx": "0.28.1"}) == [("neo4j", ["probe.py"])]
