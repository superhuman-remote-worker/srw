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
#: Roles whose image Tilt builds for the local inner loop. The voice gateway
#: is opt-in (voiceGateway.enabled) and its image comes from CI or a local
#: build, so Tilt does not rebuild it on every start.
TILT_ROLES = {"orchestrator", "vm-controller"}
#: Roles that talk to the Kubernetes API and so carry the shared SDK policy.
SDK_ROLES = {"orchestrator", "vm-controller"}


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
    workflow = (ROOT / ".github/workflows/develop.yml").read_text()
    variable = role.upper().replace("-", "_")
    match = re.search(
        rf"^\s*{variable}_PATHS=\((.*?)\)", workflow, re.MULTILINE | re.DOTALL
    )
    identity_inputs = shlex.split(match.group(1))
    if role not in TILT_ROLES:
        for name in (*locks.COMMON_INPUTS, *locks.paths(role)):
            assert any(
                name == path or (path.endswith("/") and name.startswith(path))
                for path in identity_inputs
            ), (name, identity_inputs)
        return
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
    if role in SDK_ROLES:
        assert set(policy) <= set(pins), role
    # A constraint binds every role that resolves the package at all.
    shared = set(policy) & set(pins)
    assert {name: pins[name] for name in shared} == {
        name: policy[name] for name in shared
    }
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


def test_excluded_distributions_leave_the_lock_and_only_them(tmp_path):
    """A role's sdist-only distribution its image never imports (docopt for
    the voice gateway) is resolved, then left out of the lock with its
    format option; nothing else is touched."""
    lock = tmp_path / "lock.txt"
    lock.write_text(
        "#\n# header\n#\n--no-binary docopt\n--only-binary :all:\n\n"
        "docopt==0.6.2 \\\n    --hash=sha256:aa\n    # via num2words\n"
        "num2words==0.5.14 \\\n    --hash=sha256:bb\n    # via pipecat-ai\n"
    )
    locks.strip_excluded("voice-gateway", lock)
    text = lock.read_text()
    assert "docopt==" not in text and "--no-binary" not in text
    assert "num2words==0.5.14" in text and "--only-binary :all:" in text
    assert locks.pinned_versions(lock) == {"num2words": "0.5.14"}
    before = lock.read_text()
    locks.strip_excluded("orchestrator", lock)
    assert lock.read_text() == before


def test_the_voice_gateway_lock_leaves_out_what_its_image_never_imports():
    pins = locks.pinned_versions(ROOT / locks.paths("voice-gateway")[1])
    assert "pipecat-ai" in pins and "onnxruntime" in pins
    assert set(locks.EXCLUDED["voice-gateway"]) & set(pins) == set()


def test_pip_check_tolerates_only_the_roles_exclusions(monkeypatch):
    class Result:
        returncode = 1
        stdout = (
            "num2words 0.5.14 requires docopt, which is not installed.\n"
            "pipecat-ai 1.12.0 has requirement numpy<3, but you have numpy 3.0.\n"
        )
        stderr = ""

    monkeypatch.setattr(locks.subprocess, "run", lambda *a, **k: Result())
    assert locks.pip_check("voice-gateway") == [
        "pipecat-ai 1.12.0 has requirement numpy<3, but you have numpy 3.0."
    ]
    assert len(locks.pip_check("orchestrator")) == 2
    Result.returncode, Result.stdout = 0, "No broken requirements found.\n"
    assert locks.pip_check("orchestrator") == []


def test_the_voice_gateway_image_installs_its_lock_like_ci_does():
    dockerfile = (ROOT / "docker/Dockerfile.voice-gateway").read_text()
    assert "--require-hashes --no-deps --only-binary :all:" in dockerfile
    assert "-r requirements/locks/voice-gateway-py312.txt" in dockerfile
    assert "check voice-gateway --installed" in dockerfile
    assert "pip-check voice-gateway" in dockerfile
